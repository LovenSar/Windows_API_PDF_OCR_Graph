package main

import (
	"bufio"
	"embed"
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

//go:embed static
var staticFS embed.FS

// ── data models ──

type EntityInfo struct {
	ID         string `json:"id"`
	File       string `json:"file"`
	Type       string `json:"type"`
	Name       string `json:"name,omitempty"`
	Desc       string `json:"description,omitempty"`
	SourceFile string `json:"source_file,omitempty"`
}

type Edge struct {
	Source string `json:"source"`
	Target string `json:"target"`
	Type   string `json:"type"`
}

type Graph struct {
	mu       sync.RWMutex
	Entities map[string]*EntityInfo
	Edges    []Edge
	adjOut   map[string][]int // node -> edge indices (as source)
	adjIn    map[string][]int // node -> edge indices (as target)
}

// ── undo / redo ──

type OpKind int

const (
	OpAddNode OpKind = iota
	OpDelNode
	OpUpdNode
	OpAddEdge
	OpDelEdge
	OpUpdEdge
)

type Operation struct {
	Kind     OpKind
	NodeName string      // for node ops
	OldNode  *EntityInfo // for del/upd restore
	NewNode  *EntityInfo // for add/upd
	EdgeIdx  int         // for edge ops
	OldEdge  *Edge
	NewEdge  *Edge
}

type History struct {
	mu       sync.Mutex
	undoStack [][]Operation // grouped ops
	redoStack [][]Operation
}

// ── branch ──

type BranchSnapshot struct {
	Name      string     `json:"name"`
	CreatedAt string     `json:"created_at"`
	Entities  map[string]*EntityInfo `json:"entities"`
	Edges     []Edge     `json:"edges"`
}

// ── app state ──

type App struct {
	graphPtr         atomic.Pointer[Graph]
	history          *History
	branches         map[string]*BranchSnapshot
	branchMu         sync.RWMutex
	dataDir          string
	ocrDir           string
	reloadSkipUntil  atomic.Int64 // unix nano: skip auto-reload until (after handleSave)
}

func (app *App) getGraph() *Graph {
	return app.graphPtr.Load()
}

// 与 pipeline 输出 JSON 基名匹配的 OCR 文件 stem（防止路径穿越）
var ocrBaseStemRe = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]*$`)

func readEntitySourceLine(dataDir, jsonBasename, entityName string) int {
	if jsonBasename == "" || entityName == "" {
		return 0
	}
	fp := filepath.Join(dataDir, jsonBasename)
	raw, err := os.ReadFile(fp)
	if err != nil {
		return 0
	}
	var doc struct {
		Entities []json.RawMessage `json:"entities"`
	}
	if json.Unmarshal(raw, &doc) != nil {
		return 0
	}
	for _, er := range doc.Entities {
		var e struct {
			Name string `json:"name"`
			SL   int    `json:"_source_line"`
		}
		if json.Unmarshal(er, &e) != nil {
			continue
		}
		if e.Name == entityName {
			return e.SL
		}
	}
	return 0
}

func readOCRLineContext(absPath string, line1 int, before, after int) string {
	if line1 < 1 || before < 0 || after < 0 {
		return ""
	}
	f, err := os.Open(absPath)
	if err != nil {
		return ""
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	// 放宽单行长度（OCR 行可能很长）
	const maxLine = 1024 * 512
	buf := make([]byte, 0, 64*1024)
	sc.Buffer(buf, maxLine)
	start := line1 - before
	if start < 1 {
		start = 1
	}
	end := line1 + after
	var b strings.Builder
	n := 0
	for sc.Scan() {
		n++
		if n < start {
			continue
		}
		if n > end {
			break
		}
		prefix := " "
		if n == line1 {
			prefix = ">"
		}
		line := sc.Text()
		if len(line) > 2000 {
			line = line[:2000] + "…"
		}
		fmt.Fprintf(&b, "%s %5d | %s\n", prefix, n, line)
	}
	return strings.TrimSpace(b.String())
}

func safeOCRFile(ocrDir, baseStem, variant string) (absPath string, downloadName string, ok bool) {
	if !ocrBaseStemRe.MatchString(baseStem) {
		return "", "", false
	}
	var fn string
	switch variant {
	case "p":
		fn = "[OCR]_windows-" + baseStem + ".p.txt"
	case "t":
		fn = "[OCR]_windows-" + baseStem + ".txt"
	default:
		return "", "", false
	}
	full := filepath.Join(ocrDir, fn)
	root, err := filepath.Abs(ocrDir)
	if err != nil {
		return "", "", false
	}
	clean, err := filepath.Abs(full)
	if err != nil {
		return "", "", false
	}
	rel, err := filepath.Rel(root, clean)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", "", false
	}
	st, err := os.Stat(clean)
	if err != nil || st.IsDir() {
		return "", "", false
	}
	return clean, fn, true
}

// ── graph methods ──

func NewGraph() *Graph {
	return &Graph{
		Entities: make(map[string]*EntityInfo),
		adjOut:   make(map[string][]int),
		adjIn:    make(map[string][]int),
	}
}

func (g *Graph) rebuildAdj() {
	g.adjOut = make(map[string][]int)
	g.adjIn = make(map[string][]int)
	for i, e := range g.Edges {
		g.adjOut[e.Source] = append(g.adjOut[e.Source], i)
		g.adjIn[e.Target] = append(g.adjIn[e.Target], i)
	}
}

func (g *Graph) Degree(name string) int {
	return len(g.adjOut[name]) + len(g.adjIn[name])
}

func (g *Graph) Neighbors(name string) map[string]bool {
	seen := make(map[string]bool)
	for _, i := range g.adjOut[name] {
		seen[g.Edges[i].Target] = true
	}
	for _, i := range g.adjIn[name] {
		seen[g.Edges[i].Source] = true
	}
	return seen
}

// ── load from json_output_v4 ──

func loadGraph(dataDir string) (*Graph, error) {
	g := NewGraph()

	idxPath := filepath.Join(dataDir, "global_entity_index.json")
	raw, err := os.ReadFile(idxPath)
	if err != nil {
		return nil, fmt.Errorf("read entity index: %w", err)
	}

	var idxFile struct {
		Entities map[string]json.RawMessage `json:"entities"`
	}
	if err := json.Unmarshal(raw, &idxFile); err != nil {
		return nil, fmt.Errorf("parse entity index: %w", err)
	}

	for name, raw := range idxFile.Entities {
		var info EntityInfo
		json.Unmarshal(raw, &info)
		if info.Name == "" {
			info.Name = name
		}
		g.Entities[name] = &info
	}

	edgePath := filepath.Join(dataDir, "global_edges.json")
	raw, err = os.ReadFile(edgePath)
	if err != nil {
		return nil, fmt.Errorf("read edges: %w", err)
	}

	var edgeFile struct {
		Edges []Edge `json:"edges"`
	}
	if err := json.Unmarshal(raw, &edgeFile); err != nil {
		return nil, fmt.Errorf("parse edges: %w", err)
	}
	g.Edges = edgeFile.Edges
	g.rebuildAdj()

	log.Printf("Loaded %d entities, %d edges", len(g.Entities), len(g.Edges))
	return g, nil
}

// ── history ──

func NewHistory() *History {
	return &History{}
}

func (h *History) Push(ops []Operation) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.undoStack = append(h.undoStack, ops)
	h.redoStack = nil
}

func (h *History) CanUndo() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.undoStack) > 0
}

func (h *History) PopUndo() []Operation {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.undoStack) == 0 {
		return nil
	}
	ops := h.undoStack[len(h.undoStack)-1]
	h.undoStack = h.undoStack[:len(h.undoStack)-1]
	h.redoStack = append(h.redoStack, ops)
	return ops
}

func (h *History) PopRedo() []Operation {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.redoStack) == 0 {
		return nil
	}
	ops := h.redoStack[len(h.redoStack)-1]
	h.redoStack = h.redoStack[:len(h.redoStack)-1]
	h.undoStack = append(h.undoStack, ops)
	return ops
}

func (h *History) Sizes() (int, int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.undoStack), len(h.redoStack)
}

// ── apply / revert operations ──

func (app *App) applyOp(op Operation) {
	g := app.getGraph()
	switch op.Kind {
	case OpAddNode:
		g.Entities[op.NodeName] = op.NewNode
	case OpDelNode:
		delete(g.Entities, op.NodeName)
	case OpUpdNode:
		g.Entities[op.NodeName] = op.NewNode
	case OpAddEdge:
		g.Edges = append(g.Edges, *op.NewEdge)
		idx := len(g.Edges) - 1
		g.adjOut[op.NewEdge.Source] = append(g.adjOut[op.NewEdge.Source], idx)
		g.adjIn[op.NewEdge.Target] = append(g.adjIn[op.NewEdge.Target], idx)
	case OpDelEdge:
		if op.EdgeIdx >= 0 && op.EdgeIdx < len(g.Edges) {
			g.Edges[op.EdgeIdx] = Edge{Source: "\x00DEL", Target: "\x00DEL", Type: "\x00DEL"}
			g.rebuildAdj()
		}
	case OpUpdEdge:
		if op.EdgeIdx >= 0 && op.EdgeIdx < len(g.Edges) {
			g.Edges[op.EdgeIdx] = *op.NewEdge
			g.rebuildAdj()
		}
	}
}

func (app *App) revertOp(op Operation) {
	g := app.getGraph()
	switch op.Kind {
	case OpAddNode:
		delete(g.Entities, op.NodeName)
	case OpDelNode:
		g.Entities[op.NodeName] = op.OldNode
	case OpUpdNode:
		g.Entities[op.NodeName] = op.OldNode
	case OpAddEdge:
		last := len(g.Edges) - 1
		if last >= 0 {
			g.Edges = g.Edges[:last]
			g.rebuildAdj()
		}
	case OpDelEdge:
		if op.EdgeIdx >= 0 && op.EdgeIdx < len(g.Edges) {
			g.Edges[op.EdgeIdx] = *op.OldEdge
			g.rebuildAdj()
		}
	case OpUpdEdge:
		if op.EdgeIdx >= 0 && op.EdgeIdx < len(g.Edges) {
			g.Edges[op.EdgeIdx] = *op.OldEdge
			g.rebuildAdj()
		}
	}
}

// ── evaluation metrics (ported from evaluate_graph_metrics.py) ──

type EvalReport struct {
	Timestamp               string              `json:"timestamp"`
	TotalEntities           int                 `json:"total_entities"`
	TotalEdges              int                 `json:"total_edges"`
	ConnectedNodes          int                 `json:"connected_nodes"`
	IsolatedNodes           int                 `json:"isolated_nodes"`
	IsolatedRate            float64             `json:"isolated_rate"`
	EdgeInfoDensity         float64             `json:"edge_info_density"`
	NetworkDensity          float64             `json:"network_density"`
	LargestComponentSize    int                 `json:"largest_component_size"`
	LargestComponentRatio   float64             `json:"largest_component_ratio"`
	ComponentCount          int                 `json:"component_count"`
	SingletonCount          int                 `json:"singleton_count"`
	AvgDegree               float64             `json:"avg_degree"`
	MedianDegree            int                 `json:"median_degree"`
	MaxDegree               int                 `json:"max_degree"`
	EntityTypeDist          []TypeCount         `json:"entity_type_distribution"`
	EdgeTypeDist            []TypeCount         `json:"edge_type_distribution"`
	IsolatedByType          []TypeCount         `json:"isolated_by_type"`
	TopConnectedNodes       []NodeDegree        `json:"top_connected_nodes"`
	WeakIsolatedNodes       int                 `json:"weak_isolated_nodes"`
	WeakIsolatedRate        float64             `json:"weak_isolated_rate"`
}

type TypeCount struct {
	Type  string `json:"type"`
	Count int    `json:"count"`
}

type NodeDegree struct {
	Name   string `json:"name"`
	Type   string `json:"type"`
	Degree int    `json:"degree"`
}

func (app *App) evaluate() *EvalReport {
	g := app.getGraph()
	g.mu.RLock()
	defer g.mu.RUnlock()

	report := &EvalReport{
		Timestamp:     time.Now().Format(time.RFC3339),
		TotalEntities: len(g.Entities),
	}

	liveEdges := make([]Edge, 0, len(g.Edges))
	for _, e := range g.Edges {
		if e.Source != "\x00DEL" {
			liveEdges = append(liveEdges, e)
		}
	}
	report.TotalEdges = len(liveEdges)

	names := make(map[string]bool, len(g.Entities))
	for n := range g.Entities {
		names[n] = true
	}

	// degree for all edges
	degAll := make(map[string]int)
	adjAll := make(map[string]map[string]bool)
	for _, e := range liveEdges {
		if adjAll[e.Source] == nil {
			adjAll[e.Source] = make(map[string]bool)
		}
		if adjAll[e.Target] == nil {
			adjAll[e.Target] = make(map[string]bool)
		}
		adjAll[e.Source][e.Target] = true
		adjAll[e.Target][e.Source] = true
	}
	for n := range names {
		degAll[n] = len(adjAll[n])
	}

	// degree excluding weak edges
	weakTypes := map[string]bool{"belongs_to_domain": true, "belongs_to_header": true}
	adjStrong := make(map[string]map[string]bool)
	for _, e := range liveEdges {
		if weakTypes[e.Type] {
			continue
		}
		if adjStrong[e.Source] == nil {
			adjStrong[e.Source] = make(map[string]bool)
		}
		if adjStrong[e.Target] == nil {
			adjStrong[e.Target] = make(map[string]bool)
		}
		adjStrong[e.Source][e.Target] = true
		adjStrong[e.Target][e.Source] = true
	}

	isolated := 0
	weakIsolated := 0
	for n := range names {
		if degAll[n] == 0 {
			isolated++
		}
		if len(adjStrong[n]) == 0 {
			weakIsolated++
		}
	}
	report.ConnectedNodes = len(names) - isolated
	report.IsolatedNodes = isolated
	report.IsolatedRate = float64(isolated) / math.Max(float64(len(names)), 1)
	report.WeakIsolatedNodes = weakIsolated
	report.WeakIsolatedRate = float64(weakIsolated) / math.Max(float64(len(names)), 1)

	n := float64(len(names))
	report.EdgeInfoDensity = float64(len(liveEdges)) / math.Max(n, 1)
	report.NetworkDensity = float64(len(liveEdges)) / math.Max(n*(n-1), 1)

	// connected components via BFS
	visited := make(map[string]bool)
	var compSizes []int
	for node := range names {
		if visited[node] {
			continue
		}
		queue := []string{node}
		visited[node] = true
		size := 0
		for len(queue) > 0 {
			cur := queue[0]
			queue = queue[1:]
			size++
			for nb := range adjAll[cur] {
				if names[nb] && !visited[nb] {
					visited[nb] = true
					queue = append(queue, nb)
				}
			}
		}
		compSizes = append(compSizes, size)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(compSizes)))

	report.ComponentCount = len(compSizes)
	if len(compSizes) > 0 {
		report.LargestComponentSize = compSizes[0]
		report.LargestComponentRatio = float64(compSizes[0]) / math.Max(n, 1)
	}
	singletons := 0
	for _, s := range compSizes {
		if s == 1 {
			singletons++
		}
	}
	report.SingletonCount = singletons

	// degree stats
	degrees := make([]int, 0, len(names))
	maxDeg := 0
	sumDeg := 0
	for _, d := range degAll {
		degrees = append(degrees, d)
		sumDeg += d
		if d > maxDeg {
			maxDeg = d
		}
	}
	sort.Ints(degrees)
	report.AvgDegree = float64(sumDeg) / math.Max(float64(len(degrees)), 1)
	if len(degrees) > 0 {
		report.MedianDegree = degrees[len(degrees)/2]
	}
	report.MaxDegree = maxDeg

	// entity type distribution
	etCounts := make(map[string]int)
	for _, info := range g.Entities {
		etCounts[info.Type]++
	}
	report.EntityTypeDist = mapToSortedCounts(etCounts)

	// edge type distribution
	edgeCounts := make(map[string]int)
	for _, e := range liveEdges {
		edgeCounts[e.Type]++
	}
	report.EdgeTypeDist = mapToSortedCounts(edgeCounts)

	// isolated by type
	isoCounts := make(map[string]int)
	for n := range names {
		if len(adjStrong[n]) == 0 {
			t := "unknown"
			if info, ok := g.Entities[n]; ok {
				t = info.Type
			}
			isoCounts[t]++
		}
	}
	report.IsolatedByType = mapToSortedCounts(isoCounts)

	// top connected
	type nd struct {
		name string
		deg  int
	}
	var nds []nd
	for n, d := range degAll {
		nds = append(nds, nd{n, d})
	}
	sort.Slice(nds, func(i, j int) bool { return nds[i].deg > nds[j].deg })
	top := 20
	if len(nds) < top {
		top = len(nds)
	}
	for i := 0; i < top; i++ {
		t := "unknown"
		if info, ok := g.Entities[nds[i].name]; ok {
			t = info.Type
		}
		report.TopConnectedNodes = append(report.TopConnectedNodes, NodeDegree{
			Name:   nds[i].name,
			Type:   t,
			Degree: nds[i].deg,
		})
	}

	return report
}

func mapToSortedCounts(m map[string]int) []TypeCount {
	var result []TypeCount
	for k, v := range m {
		result = append(result, TypeCount{k, v})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Count > result[j].Count })
	return result
}

// ── HTTP handlers ──

func (app *App) handleGetGraph(w http.ResponseWriter, r *http.Request) {
	g := app.getGraph()
	g.mu.RLock()
	defer g.mu.RUnlock()

	type SigmaNode struct {
		Key   string            `json:"key"`
		Attrs map[string]interface{} `json:"attributes"`
	}
	type SigmaEdge struct {
		Source string            `json:"source"`
		Target string            `json:"target"`
		Attrs  map[string]interface{} `json:"attributes"`
	}

	nodes := make([]SigmaNode, 0, len(g.Entities))
	for name, info := range g.Entities {
		nodes = append(nodes, SigmaNode{
			Key: name,
			Attrs: map[string]interface{}{
				"label":       name,
				"type":        info.Type,
				"file":        info.File,
				"description": info.Desc,
				"degree":      g.Degree(name),
			},
		})
	}

	edges := make([]SigmaEdge, 0, len(g.Edges))
	for _, e := range g.Edges {
		if e.Source == "\x00DEL" {
			continue
		}
		// Only include edges where both endpoints exist
		if _, ok := g.Entities[e.Source]; !ok {
			continue
		}
		if _, ok := g.Entities[e.Target]; !ok {
			continue
		}
		edges = append(edges, SigmaEdge{
			Source: e.Source,
			Target: e.Target,
			Attrs: map[string]interface{}{
				"type": e.Type,
			},
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"nodes": nodes,
		"edges": edges,
	})
}

func (app *App) handleGetNode(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	if name == "" {
		http.Error(w, "missing ?name=", 400)
		return
	}
	g := app.getGraph()
	g.mu.RLock()
	info, ok := g.Entities[name]
	if !ok {
		g.mu.RUnlock()
		http.Error(w, "node not found", 404)
		return
	}
	infoVal := *info
	deg := g.Degree(name)
	neighbors := g.Neighbors(name)
	neighborList := make([]string, 0, len(neighbors))
	for n := range neighbors {
		neighborList = append(neighborList, n)
	}
	sort.Strings(neighborList)

	edgesOut := make([]Edge, 0)
	edgesIn := make([]Edge, 0)
	for _, i := range g.adjOut[name] {
		if g.Edges[i].Source != "\x00DEL" {
			edgesOut = append(edgesOut, g.Edges[i])
		}
	}
	for _, i := range g.adjIn[name] {
		if g.Edges[i].Source != "\x00DEL" {
			edgesIn = append(edgesIn, g.Edges[i])
		}
	}
	g.mu.RUnlock()

	ocr := app.buildOCRInfo(name, &infoVal)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"name":      name,
		"info":      &infoVal,
		"degree":    deg,
		"neighbors": neighborList,
		"edges_out": edgesOut,
		"edges_in":  edgesIn,
		"ocr":       ocr,
	})
}

func (app *App) handleGetNodeContext(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	if name == "" {
		http.Error(w, "missing ?name=", 400)
		return
	}
	g := app.getGraph()
	g.mu.RLock()
	info, ok := g.Entities[name]
	if !ok {
		g.mu.RUnlock()
		http.Error(w, "node not found", 404)
		return
	}
	infoVal := *info
	deg := g.Degree(name)
	neighbors := g.Neighbors(name)
	neighborList := make([]string, 0, len(neighbors))
	for n := range neighbors {
		neighborList = append(neighborList, n)
	}
	sort.Strings(neighborList)
	edgesOut := make([]Edge, 0)
	edgesIn := make([]Edge, 0)
	for _, i := range g.adjOut[name] {
		if g.Edges[i].Source != "\x00DEL" {
			edgesOut = append(edgesOut, g.Edges[i])
		}
	}
	for _, i := range g.adjIn[name] {
		if g.Edges[i].Source != "\x00DEL" {
			edgesIn = append(edgesIn, g.Edges[i])
		}
	}
	g.mu.RUnlock()
	ocr := app.buildOCRInfo(name, &infoVal)

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("# %s\n\n", name))
	sb.WriteString(fmt.Sprintf("**类型**: %s\n", infoVal.Type))
	sb.WriteString(fmt.Sprintf("**度数**: %d\n", deg))
	if infoVal.Desc != "" {
		sb.WriteString(fmt.Sprintf("**描述**: %s\n", infoVal.Desc))
	}
	if infoVal.File != "" {
		sb.WriteString(fmt.Sprintf("**来源文件**: %s\n", infoVal.File))
	}
	sb.WriteString("\n## 出边\n")
	for _, e := range edgesOut {
		sb.WriteString(fmt.Sprintf("- %s --[%s]--> %s\n", e.Source, e.Type, e.Target))
	}
	sb.WriteString("\n## 入边\n")
	for _, e := range edgesIn {
		sb.WriteString(fmt.Sprintf("- %s --[%s]--> %s\n", e.Source, e.Type, e.Target))
	}
	sb.WriteString("\n## 邻居\n")
	sb.WriteString(strings.Join(neighborList, ", "))
	sb.WriteString("\n")
	if ctx, ok := ocr["context"].(string); ok && ctx != "" {
		sb.WriteString("\n## OCR 原文上下文\n```\n")
		sb.WriteString(ctx)
		sb.WriteString("\n```\n")
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Write([]byte(sb.String()))
}

func (app *App) buildOCRInfo(entityName string, info *EntityInfo) map[string]interface{} {
	out := map[string]interface{}{
		"json_file":   "",
		"stem":        "",
		"source_line": 0,
		"has_p_txt":   false,
		"has_t_txt":   false,
		"p_filename":  "",
		"t_filename":  "",
		"context":     "",
		"ocr_dir_ok":  app.ocrDir != "",
	}
	if info == nil || info.File == "" {
		return out
	}
	jsonBase := filepath.Base(info.File)
	stem := strings.TrimSuffix(jsonBase, ".json")
	out["json_file"] = jsonBase
	out["stem"] = stem
	if stem == "" || !ocrBaseStemRe.MatchString(stem) {
		return out
	}
	sl := readEntitySourceLine(app.dataDir, jsonBase, entityName)
	out["source_line"] = sl
	if app.ocrDir == "" {
		return out
	}
	if pPath, pName, pOk := safeOCRFile(app.ocrDir, stem, "p"); pOk {
		out["has_p_txt"] = true
		out["p_filename"] = pName
		if sl > 0 {
			out["context"] = readOCRLineContext(pPath, sl, 2, 5)
		}
	}
	if _, tName, tOk := safeOCRFile(app.ocrDir, stem, "t"); tOk {
		out["has_t_txt"] = true
		out["t_filename"] = tName
	}
	if out["context"] == "" && sl > 0 {
		if tPath, _, tOk := safeOCRFile(app.ocrDir, stem, "t"); tOk {
			out["context"] = readOCRLineContext(tPath, sl, 2, 5)
		}
	}
	return out
}

func (app *App) handleOCRDownload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if app.ocrDir == "" {
		http.Error(w, "OCR directory not configured", http.StatusNotFound)
		return
	}
	base := r.URL.Query().Get("base")
	variant := r.URL.Query().Get("variant")
	path, fn, ok := safeOCRFile(app.ocrDir, base, variant)
	if !ok {
		http.Error(w, "file not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, strings.ReplaceAll(fn, `"`, ``)))
	http.ServeFile(w, r, path)
}

func (app *App) handleSearch(w http.ResponseWriter, r *http.Request) {
	q := strings.ToLower(r.URL.Query().Get("q"))
	if q == "" {
		json.NewEncoder(w).Encode([]string{})
		return
	}
	g := app.getGraph()
	g.mu.RLock()
	defer g.mu.RUnlock()

	var results []map[string]interface{}
	limit := 50
	for name, info := range g.Entities {
		if strings.Contains(strings.ToLower(name), q) {
			results = append(results, map[string]interface{}{
				"name":   name,
				"type":   info.Type,
				"degree": g.Degree(name),
			})
			if len(results) >= limit {
				break
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

func (app *App) handleCreateNode(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name string `json:"name"`
		Type string `json:"type"`
		Desc string `json:"description"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	if req.Name == "" {
		http.Error(w, "name required", 400)
		return
	}

	g := app.getGraph()
	g.mu.Lock()
	defer g.mu.Unlock()

	if _, exists := g.Entities[req.Name]; exists {
		http.Error(w, "node already exists", 409)
		return
	}

	info := &EntityInfo{Name: req.Name, Type: req.Type, Desc: req.Desc}
	ops := []Operation{{Kind: OpAddNode, NodeName: req.Name, NewNode: info}}
	app.applyOp(ops[0])
	app.history.Push(ops)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (app *App) handleUpdateNode(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name    string `json:"name"`
		Type    string `json:"type"`
		Desc    string `json:"description"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	g := app.getGraph()
	g.mu.Lock()
	defer g.mu.Unlock()

	old, ok := g.Entities[req.Name]
	if !ok {
		http.Error(w, "node not found", 404)
		return
	}

	oldCopy := *old
	newInfo := &EntityInfo{
		ID:   old.ID,
		File: old.File,
		Name: old.Name,
		Type: req.Type,
		Desc: req.Desc,
	}

	ops := []Operation{{Kind: OpUpdNode, NodeName: req.Name, OldNode: &oldCopy, NewNode: newInfo}}
	app.applyOp(ops[0])
	app.history.Push(ops)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (app *App) handleDeleteNode(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	g := app.getGraph()
	g.mu.Lock()
	defer g.mu.Unlock()

	old, ok := g.Entities[req.Name]
	if !ok {
		http.Error(w, "node not found", 404)
		return
	}

	oldCopy := *old
	ops := []Operation{{Kind: OpDelNode, NodeName: req.Name, OldNode: &oldCopy}}
	app.applyOp(ops[0])
	app.history.Push(ops)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (app *App) handleCreateEdge(w http.ResponseWriter, r *http.Request) {
	var req Edge
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	if req.Source == "" || req.Target == "" {
		http.Error(w, "source and target required", 400)
		return
	}
	if req.Type == "" {
		req.Type = "related_to"
	}

	g := app.getGraph()
	g.mu.Lock()
	defer g.mu.Unlock()

	edge := req
	ops := []Operation{{Kind: OpAddEdge, NewEdge: &edge}}
	app.applyOp(ops[0])
	app.history.Push(ops)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (app *App) handleUpdateEdge(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Index   int    `json:"index"`
		Source  string `json:"source"`
		Target  string `json:"target"`
		Type    string `json:"type"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	g := app.getGraph()
	g.mu.Lock()
	defer g.mu.Unlock()

	if req.Index < 0 || req.Index >= len(g.Edges) {
		http.Error(w, "invalid edge index", 400)
		return
	}

	old := g.Edges[req.Index]
	newEdge := Edge{Source: req.Source, Target: req.Target, Type: req.Type}
	ops := []Operation{{Kind: OpUpdEdge, EdgeIdx: req.Index, OldEdge: &old, NewEdge: &newEdge}}
	app.applyOp(ops[0])
	app.history.Push(ops)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (app *App) handleDeleteEdge(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Index int `json:"index"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	g := app.getGraph()
	g.mu.Lock()
	defer g.mu.Unlock()

	if req.Index < 0 || req.Index >= len(g.Edges) {
		http.Error(w, "invalid edge index", 400)
		return
	}

	old := g.Edges[req.Index]
	ops := []Operation{{Kind: OpDelEdge, EdgeIdx: req.Index, OldEdge: &old}}
	app.applyOp(ops[0])
	app.history.Push(ops)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (app *App) handleUndo(w http.ResponseWriter, r *http.Request) {
	ops := app.history.PopUndo()
	if ops == nil {
		http.Error(w, "nothing to undo", 400)
		return
	}

	g := app.getGraph()
	g.mu.Lock()
	defer g.mu.Unlock()

	for i := len(ops) - 1; i >= 0; i-- {
		app.revertOp(ops[i])
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (app *App) handleRedo(w http.ResponseWriter, r *http.Request) {
	ops := app.history.PopRedo()
	if ops == nil {
		http.Error(w, "nothing to redo", 400)
		return
	}

	g := app.getGraph()
	g.mu.Lock()
	defer g.mu.Unlock()

	for _, op := range ops {
		app.applyOp(op)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (app *App) handleHistoryStatus(w http.ResponseWriter, r *http.Request) {
	undo, redo := app.history.Sizes()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]int{"undo": undo, "redo": redo})
}

// ── branches ──

func (app *App) handleSaveBranch(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	if req.Name == "" {
		http.Error(w, "name required", 400)
		return
	}

	g := app.getGraph()
	g.mu.RLock()

	entsCopy := make(map[string]*EntityInfo, len(g.Entities))
	for k, v := range g.Entities {
		cp := *v
		entsCopy[k] = &cp
	}
	edgesCopy := make([]Edge, 0, len(g.Edges))
	for _, e := range g.Edges {
		if e.Source != "\x00DEL" {
			edgesCopy = append(edgesCopy, e)
		}
	}
	g.mu.RUnlock()

	snap := &BranchSnapshot{
		Name:      req.Name,
		CreatedAt: time.Now().Format(time.RFC3339),
		Entities:  entsCopy,
		Edges:     edgesCopy,
	}

	app.branchMu.Lock()
	app.branches[req.Name] = snap
	app.branchMu.Unlock()

	// persist to disk
	branchDir := filepath.Join(app.dataDir, "_branches")
	os.MkdirAll(branchDir, 0755)
	data, _ := json.MarshalIndent(snap, "", "  ")
	os.WriteFile(filepath.Join(branchDir, req.Name+".json"), data, 0644)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (app *App) handleLoadBranch(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	app.branchMu.RLock()
	snap, ok := app.branches[req.Name]
	app.branchMu.RUnlock()

	if !ok {
		http.Error(w, "branch not found", 404)
		return
	}

	g := app.getGraph()
	g.mu.Lock()
	g.Entities = make(map[string]*EntityInfo, len(snap.Entities))
	for k, v := range snap.Entities {
		cp := *v
		g.Entities[k] = &cp
	}
	g.Edges = make([]Edge, len(snap.Edges))
	copy(g.Edges, snap.Edges)
	g.rebuildAdj()
	g.mu.Unlock()

	app.history = NewHistory()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (app *App) handleListBranches(w http.ResponseWriter, r *http.Request) {
	app.branchMu.RLock()
	defer app.branchMu.RUnlock()

	var list []map[string]string
	for _, snap := range app.branches {
		list = append(list, map[string]string{
			"name":       snap.Name,
			"created_at": snap.CreatedAt,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(list)
}

// ── stats (lightweight counts for stats bar) ──

func (app *App) handleStats(w http.ResponseWriter, r *http.Request) {
	g := app.getGraph()
	g.mu.RLock()
	defer g.mu.RUnlock()
	liveEdges := 0
	for _, e := range g.Edges {
		if e.Source != "\x00DEL" {
			liveEdges++
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"total_entities": len(g.Entities),
		"total_edges":    liveEdges,
	})
}

// ── evaluate ──

func (app *App) handleEvaluate(w http.ResponseWriter, r *http.Request) {
	report := app.evaluate()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(report)
}

// ── hot reload from disk (pipeline / external edits) ──

func (app *App) reloadBranchesFromDisk() {
	app.branchMu.Lock()
	defer app.branchMu.Unlock()
	app.branches = make(map[string]*BranchSnapshot)
	branchDir := filepath.Join(app.dataDir, "_branches")
	entries, err := os.ReadDir(branchDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".json") {
			data, _ := os.ReadFile(filepath.Join(branchDir, e.Name()))
			var snap BranchSnapshot
			if json.Unmarshal(data, &snap) == nil {
				app.branches[snap.Name] = &snap
			}
		}
	}
}

func (app *App) reloadGraphFromDisk() {
	newG, err := loadGraph(app.dataDir)
	if err != nil {
		log.Printf("Auto-reload: skipped (%v)", err)
		return
	}
	app.graphPtr.Store(newG)
	app.history = NewHistory()
	app.reloadBranchesFromDisk()
	log.Printf("Auto-reload: graph from disk (%d entities, %d edges)", len(newG.Entities), len(newG.Edges))
}

func startGraphAutoReload(app *App, interval time.Duration) {
	idxPath := filepath.Join(app.dataDir, "global_entity_index.json")
	edgePath := filepath.Join(app.dataDir, "global_edges.json")
	sti, err1 := os.Stat(idxPath)
	ste, err2 := os.Stat(edgePath)
	if err1 != nil || err2 != nil {
		log.Printf("Auto-reload: cannot stat data files, watcher disabled")
		return
	}
	lastIdx, lastEdge := sti.ModTime(), ste.ModTime()
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for range t.C {
			sti, err1 := os.Stat(idxPath)
			ste, err2 := os.Stat(edgePath)
			if err1 != nil || err2 != nil {
				continue
			}
			idxMT, edgeMT := sti.ModTime(), ste.ModTime()
			if idxMT.Equal(lastIdx) && edgeMT.Equal(lastEdge) {
				continue
			}
			if time.Now().UnixNano() < app.reloadSkipUntil.Load() {
				continue
			}
			lastIdx, lastEdge = idxMT, edgeMT
			app.reloadGraphFromDisk()
		}
	}()
	log.Printf("Auto-reload: polling %s + %s every %v", filepath.Base(idxPath), filepath.Base(edgePath), interval)
}

// ── save ──

func (app *App) handleSave(w http.ResponseWriter, r *http.Request) {
	g := app.getGraph()
	g.mu.RLock()

	// Save entities
	entIdx := map[string]interface{}{
		"_schema":                "global_entity_index_v4.0_refined",
		"_generated_at":         time.Now().Format(time.RFC3339),
		"total_unique_entities": len(g.Entities),
		"entities":              g.Entities,
	}
	entData, _ := json.MarshalIndent(entIdx, "", "  ")

	// Save edges (filter deleted)
	liveEdges := make([]Edge, 0, len(g.Edges))
	for _, e := range g.Edges {
		if e.Source != "\x00DEL" {
			liveEdges = append(liveEdges, e)
		}
	}
	edgeObj := map[string]interface{}{
		"_generated_at": time.Now().Format(time.RFC3339),
		"total_edges":   len(liveEdges),
		"edges":         liveEdges,
	}
	edgeData, _ := json.MarshalIndent(edgeObj, "", "  ")
	g.mu.RUnlock()

	if err := os.WriteFile(filepath.Join(app.dataDir, "global_entity_index.json"), entData, 0644); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	if err := os.WriteFile(filepath.Join(app.dataDir, "global_edges.json"), edgeData, 0644); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	// 避免保存后立即触发自动重载（内容与内存一致）
	app.reloadSkipUntil.Store(time.Now().Add(4 * time.Second).UnixNano())

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "saved"})
}

// ── System memory API ──
// getSystemMemory: Windows impl in memory_windows.go, stub in memory_other.go

func (app *App) handleSystemMemory(w http.ResponseWriter, r *http.Request) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	goAllocMB := ms.Alloc / (1024 * 1024)
	goSysMB := ms.Sys / (1024 * 1024)

	totalMB, availMB, usedPct, sysErr := getSystemMemory()

	resp := map[string]interface{}{
		"go_alloc_mb":      goAllocMB,
		"go_sys_mb":        goSysMB,
		"go_num_gc":        ms.NumGC,
		"system_total_mb":  totalMB,
		"system_avail_mb":  availMB,
		"system_used_pct":  usedPct,
		"system_error":     sysErr != nil,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// ── CORS middleware ──

func cors(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == "OPTIONS" {
			w.WriteHeader(200)
			return
		}
		h.ServeHTTP(w, r)
	})
}

func openBrowser(url string) {
	var cmd string
	var args []string
	switch runtime.GOOS {
	case "windows":
		cmd = "cmd"
		args = []string{"/c", "start", url}
	case "darwin":
		cmd = "open"
		args = []string{url}
	default:
		cmd = "xdg-open"
		args = []string{url}
	}
	exec.Command(cmd, args...).Start()
}

func main() {
	port := flag.Int("port", 10086, "HTTP port")
	dataDir := flag.String("data", "", "Path to json_output_v4 directory")
	ocrDirFlag := flag.String("ocr", "", "Path to OCR_raw (default: <parent of data>/OCR_raw)")
	noBrowser := flag.Bool("no-browser", false, "Don't open browser automatically")
	autoReload := flag.Bool("auto-reload", false, "Reload graph when global_entity_index.json / global_edges.json change on disk")
	reloadInterval := flag.Duration("reload-interval", 2*time.Second, "How often to check data files for changes")
	flag.Parse()

	if *dataDir == "" {
		// Try to find json_output_v4 relative to executable or cwd
		candidates := []string{
			"json_output_v4",
			"../json_output_v4",
			filepath.Join("..", "json_output_v4"),
		}
		for _, c := range candidates {
			if _, err := os.Stat(filepath.Join(c, "global_entity_index.json")); err == nil {
				*dataDir = c
				break
			}
		}
		if *dataDir == "" {
			log.Fatal("Cannot find json_output_v4 directory. Use --data flag.")
		}
	}

	absData, _ := filepath.Abs(*dataDir)
	log.Printf("Data directory: %s", absData)

	absOCR := ""
	if *ocrDirFlag != "" {
		absOCR, _ = filepath.Abs(*ocrDirFlag)
	} else {
		absOCR = filepath.Join(filepath.Dir(absData), "OCR_raw")
	}
	if st, err := os.Stat(absOCR); err != nil || !st.IsDir() {
		log.Printf("OCR_raw not available at %s — source downloads/snippets disabled (use --ocr)", absOCR)
		absOCR = ""
	} else {
		log.Printf("OCR directory: %s", absOCR)
	}

	graph, err := loadGraph(absData)
	if err != nil {
		log.Fatalf("Failed to load graph: %v", err)
	}

	app := &App{
		history:  NewHistory(),
		branches: make(map[string]*BranchSnapshot),
		dataDir:  absData,
		ocrDir:   absOCR,
	}
	app.graphPtr.Store(graph)

	// load existing branches
	branchDir := filepath.Join(absData, "_branches")
	if entries, err := os.ReadDir(branchDir); err == nil {
		for _, e := range entries {
			if strings.HasSuffix(e.Name(), ".json") {
				data, _ := os.ReadFile(filepath.Join(branchDir, e.Name()))
				var snap BranchSnapshot
				if json.Unmarshal(data, &snap) == nil {
					app.branches[snap.Name] = &snap
				}
			}
		}
	}

	if *autoReload {
		startGraphAutoReload(app, *reloadInterval)
	}

	mux := http.NewServeMux()

	// API routes
	mux.HandleFunc("/api/graph", app.handleGetGraph)
	mux.HandleFunc("/api/node", app.handleGetNode)
	mux.HandleFunc("/api/node/context", app.handleGetNodeContext)
	mux.HandleFunc("/api/search", app.handleSearch)
	mux.HandleFunc("/api/node/create", app.handleCreateNode)
	mux.HandleFunc("/api/node/update", app.handleUpdateNode)
	mux.HandleFunc("/api/node/delete", app.handleDeleteNode)
	mux.HandleFunc("/api/edge/create", app.handleCreateEdge)
	mux.HandleFunc("/api/edge/update", app.handleUpdateEdge)
	mux.HandleFunc("/api/edge/delete", app.handleDeleteEdge)
	mux.HandleFunc("/api/undo", app.handleUndo)
	mux.HandleFunc("/api/redo", app.handleRedo)
	mux.HandleFunc("/api/history", app.handleHistoryStatus)
	mux.HandleFunc("/api/branch/save", app.handleSaveBranch)
	mux.HandleFunc("/api/branch/load", app.handleLoadBranch)
	mux.HandleFunc("/api/branches", app.handleListBranches)
	mux.HandleFunc("/api/stats", app.handleStats)
	mux.HandleFunc("/api/evaluate", app.handleEvaluate)
	mux.HandleFunc("/api/save", app.handleSave)
	mux.HandleFunc("/api/system/memory", app.handleSystemMemory)
	mux.HandleFunc("/api/ocr/download", app.handleOCRDownload)

	// Static files
	staticSub, _ := fs.Sub(staticFS, "static")
	mux.Handle("/", http.FileServer(http.FS(staticSub)))

	addr := fmt.Sprintf(":%d", *port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	actualPort := ln.Addr().(*net.TCPAddr).Port
	url := fmt.Sprintf("http://localhost:%d", actualPort)

	log.Printf("Starting server at %s", url)
	if !*noBrowser {
		go func() {
			time.Sleep(500 * time.Millisecond)
			openBrowser(url)
		}()
	}

	log.Fatal(http.Serve(ln, cors(mux)))
}
