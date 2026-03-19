// MCP 服务器：将 Windows API 知识图谱暴露给大模型
// 需先启动 graph_viewer，本服务通过 HTTP 轮询获取图谱数据
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

var graphURL string

func init() {
	flag.StringVar(&graphURL, "graph-url", "http://localhost:10086", "graph_viewer HTTP 地址")
}

func apiGet(path string) ([]byte, error) {
	resp, err := http.Get(graphURL + path)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API %s: %s", resp.Status, string(body))
	}
	return io.ReadAll(resp.Body)
}

func main() {
	flag.Parse()

	s := server.NewMCPServer("WinAPI-KG", "1.0.0",
		server.WithToolCapabilities(true),
	)

	// 获取完整图谱
	s.AddTool(
		mcp.NewTool("kg_get_graph",
			mcp.WithDescription("获取知识图谱的节点和边，用于轮询分析关系建立是否合适"),
			mcp.WithString("format", mcp.Description("返回格式: full(默认) 或 summary")),
		),
		handleGetGraph,
	)

	// 获取单个节点详情（含邻居、边、OCR 上下文）
	s.AddTool(
		mcp.NewTool("kg_get_node",
			mcp.WithDescription("获取节点详情，含类型、描述、出边、入边、邻居、OCR 原文上下文"),
			mcp.WithString("name", mcp.Required(), mcp.Description("节点名称（实体 ID）")),
		),
		handleGetNode,
	)

	// 搜索节点
	s.AddTool(
		mcp.NewTool("kg_search",
			mcp.WithDescription("按名称搜索节点"),
			mcp.WithString("q", mcp.Required(), mcp.Description("搜索关键词")),
		),
		handleSearch,
	)

	// 获取节点 LLM 上下文（用于生成示例代码等）
	s.AddTool(
		mcp.NewTool("kg_get_node_context",
			mcp.WithDescription("获取节点的完整上下文，供大模型做解读、生成示例代码"),
			mcp.WithString("name", mcp.Required(), mcp.Description("节点名称")),
		),
		handleGetNodeContext,
	)

	// 评估图谱指标
	s.AddTool(
		mcp.NewTool("kg_evaluate",
			mcp.WithDescription("评估图谱指标：孤立率、边类型分布、度数统计等"),
		),
		handleEvaluate,
	)

	if err := server.ServeStdio(s); err != nil {
		fmt.Fprintf(os.Stderr, "MCP server error: %v\n", err)
		os.Exit(1)
	}
}

func handleGetGraph(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	data, err := apiGet("/api/graph")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(string(data)), nil
}

func handleGetNode(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	name, err := req.RequireString("name")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	data, err := apiGet("/api/node?name=" + strings.ReplaceAll(name, " ", "%20"))
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(string(data)), nil
}

func handleGetNodeContext(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	name, err := req.RequireString("name")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	data, err := apiGet("/api/node/context?name=" + strings.ReplaceAll(name, " ", "%20"))
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(string(data)), nil
}

func handleSearch(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	q, err := req.RequireString("q")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	data, err := apiGet("/api/search?q=" + strings.ReplaceAll(q, " ", "%20"))
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(string(data)), nil
}

func handleEvaluate(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	data, err := apiGet("/api/evaluate")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(string(data)), nil
}
