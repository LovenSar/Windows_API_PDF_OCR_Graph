#!/usr/bin/env python3
"""
知识图谱导出工具 — 支持 Neo4j CSV / GraphML / GEXF / JSON-LD 格式

用法:
  python export_graph.py --format neo4j      # Neo4j LOAD CSV 格式
  python export_graph.py --format graphml    # GraphML (Gephi/yEd)
  python export_graph.py --format gexf       # GEXF (Gephi)
  python export_graph.py --format all        # 全部格式
"""

import csv
import json
import os
import glob
import argparse
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime, timezone

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(THIS_DIR, "json_output_v4")
EXPORT_DIR = os.path.join(THIS_DIR, "exports")


def load_graph():
    """Load entities and edges from json_output_v4."""
    files = sorted(
        f for f in glob.glob(os.path.join(OUT_DIR, "*.json"))
        if not os.path.basename(f).startswith("_")
        and not os.path.basename(f).startswith("global")
    )
    entities = {}
    for fp in files:
        with open(fp, "r", encoding="utf-8") as f:
            doc = json.load(f)
        source_file = os.path.basename(fp)
        for ent in doc.get("entities", []):
            eid = ent.get("id", "").strip()
            name = ent.get("name", "").strip()
            if not eid or not name:
                continue
            entities[name] = {
                "id": eid,
                "name": name,
                "entity_type": str(ent.get("entity_type", "unknown")).strip().lower(),
                "description": str(ent.get("description", ""))[:500],
                "confidence": ent.get("confidence", 0.5),
                "source_file": source_file,
            }

    edges_path = os.path.join(OUT_DIR, "global_edges.json")
    edges = []
    if os.path.exists(edges_path):
        with open(edges_path, "r", encoding="utf-8") as f:
            edge_doc = json.load(f)
        for e in edge_doc.get("edges", []):
            s = e.get("source", "").strip()
            t = e.get("target", "").strip()
            et = e.get("type", "related_to").strip()
            if s and t:
                edges.append({"source": s, "target": t, "type": et})

    return entities, edges


def export_neo4j(entities, edges):
    """Export as Neo4j LOAD CSV format (nodes.csv + edges.csv)."""
    neo4j_dir = os.path.join(EXPORT_DIR, "neo4j")
    os.makedirs(neo4j_dir, exist_ok=True)

    nodes_path = os.path.join(neo4j_dir, "nodes.csv")
    with open(nodes_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow([":ID", "name", "entity_type:LABEL", "description", "confidence:float", "source_file"])
        for name, ent in sorted(entities.items()):
            w.writerow([
                ent["id"], ent["name"], ent["entity_type"],
                ent["description"].replace("\n", " ")[:200],
                ent["confidence"], ent["source_file"],
            ])

    edges_path = os.path.join(neo4j_dir, "edges.csv")
    entity_names = set(entities.keys())
    with open(edges_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow([":START_ID", ":END_ID", ":TYPE"])
        for e in edges:
            src_ent = entities.get(e["source"])
            tgt_ent = entities.get(e["target"])
            if src_ent and tgt_ent:
                w.writerow([src_ent["id"], tgt_ent["id"], e["type"]])

    # Cypher import script
    cypher_path = os.path.join(neo4j_dir, "import.cypher")
    with open(cypher_path, "w", encoding="utf-8") as f:
        f.write("""\
// Load nodes
LOAD CSV WITH HEADERS FROM 'file:///nodes.csv' AS row
CREATE (n:Entity {
  id: row[':ID'],
  name: row.name,
  description: row.description,
  confidence: toFloat(row['confidence:float']),
  source_file: row.source_file
})
SET n:`${row['entity_type:LABEL']}`;

// Create index
CREATE INDEX entity_name IF NOT EXISTS FOR (n:Entity) ON (n.name);
CREATE INDEX entity_id IF NOT EXISTS FOR (n:Entity) ON (n.id);

// Load edges
LOAD CSV WITH HEADERS FROM 'file:///edges.csv' AS row
MATCH (a:Entity {id: row[':START_ID']})
MATCH (b:Entity {id: row[':END_ID']})
CALL apoc.create.relationship(a, row[':TYPE'], {}, b) YIELD rel
RETURN count(rel);
""")

    print(f"  Neo4j CSV: {nodes_path}")
    print(f"             {edges_path}")
    print(f"             {cypher_path}")
    return nodes_path, edges_path


def export_graphml(entities, edges):
    """Export as GraphML format."""
    os.makedirs(EXPORT_DIR, exist_ok=True)

    ns = "http://graphml.graphstruct.org/xmlns"
    root = ET.Element("graphml", xmlns=ns)

    ET.SubElement(root, "key", id="d_name", attrib={"for": "node", "attr.name": "name", "attr.type": "string"})
    ET.SubElement(root, "key", id="d_type", attrib={"for": "node", "attr.name": "entity_type", "attr.type": "string"})
    ET.SubElement(root, "key", id="d_desc", attrib={"for": "node", "attr.name": "description", "attr.type": "string"})
    ET.SubElement(root, "key", id="d_conf", attrib={"for": "node", "attr.name": "confidence", "attr.type": "double"})
    ET.SubElement(root, "key", id="e_type", attrib={"for": "edge", "attr.name": "edge_type", "attr.type": "string"})

    graph = ET.SubElement(root, "graph", id="WindowsAPIKG", edgedefault="directed")

    name_to_idx = {}
    for i, (name, ent) in enumerate(sorted(entities.items())):
        nid = f"n{i}"
        name_to_idx[name] = nid
        node = ET.SubElement(graph, "node", id=nid)
        d = ET.SubElement(node, "data", key="d_name")
        d.text = ent["name"]
        d = ET.SubElement(node, "data", key="d_type")
        d.text = ent["entity_type"]
        d = ET.SubElement(node, "data", key="d_desc")
        d.text = ent["description"][:200]
        d = ET.SubElement(node, "data", key="d_conf")
        d.text = str(ent["confidence"])

    eidx = 0
    for e in edges:
        src_id = name_to_idx.get(e["source"])
        tgt_id = name_to_idx.get(e["target"])
        if src_id and tgt_id:
            edge_el = ET.SubElement(graph, "edge", id=f"e{eidx}", source=src_id, target=tgt_id)
            d = ET.SubElement(edge_el, "data", key="e_type")
            d.text = e["type"]
            eidx += 1

    path = os.path.join(EXPORT_DIR, "windows_api_kg.graphml")
    tree = ET.ElementTree(root)
    ET.indent(tree, space="  ")
    tree.write(path, encoding="unicode", xml_declaration=True)
    print(f"  GraphML: {path} ({eidx} edges)")
    return path


def export_gexf(entities, edges):
    """Export as GEXF format (Gephi native)."""
    os.makedirs(EXPORT_DIR, exist_ok=True)

    root = ET.Element("gexf", xmlns="http://gexf.net/1.3",
                       version="1.3")
    meta = ET.SubElement(root, "meta", lastmodifieddate=datetime.now().strftime("%Y-%m-%d"))
    ET.SubElement(meta, "creator").text = "Windows API KG Pipeline"
    ET.SubElement(meta, "description").text = "Windows API Knowledge Graph"

    graph = ET.SubElement(root, "graph", defaultedgetype="directed", mode="static")

    attrs_node = ET.SubElement(graph, "attributes", **{"class": "node"})
    ET.SubElement(attrs_node, "attribute", id="0", title="entity_type", type="string")
    ET.SubElement(attrs_node, "attribute", id="1", title="confidence", type="float")

    attrs_edge = ET.SubElement(graph, "attributes", **{"class": "edge"})
    ET.SubElement(attrs_edge, "attribute", id="0", title="edge_type", type="string")

    nodes_el = ET.SubElement(graph, "nodes")
    name_to_idx = {}
    for i, (name, ent) in enumerate(sorted(entities.items())):
        name_to_idx[name] = str(i)
        node = ET.SubElement(nodes_el, "node", id=str(i), label=ent["name"])
        avs = ET.SubElement(node, "attvalues")
        ET.SubElement(avs, "attvalue", **{"for": "0", "value": ent["entity_type"]})
        ET.SubElement(avs, "attvalue", **{"for": "1", "value": str(ent["confidence"])})

    edges_el = ET.SubElement(graph, "edges")
    eidx = 0
    for e in edges:
        src_id = name_to_idx.get(e["source"])
        tgt_id = name_to_idx.get(e["target"])
        if src_id and tgt_id:
            edge_el = ET.SubElement(edges_el, "edge", id=str(eidx), source=src_id, target=tgt_id)
            avs = ET.SubElement(edge_el, "attvalues")
            ET.SubElement(avs, "attvalue", **{"for": "0", "value": e["type"]})
            eidx += 1

    path = os.path.join(EXPORT_DIR, "windows_api_kg.gexf")
    tree = ET.ElementTree(root)
    ET.indent(tree, space="  ")
    tree.write(path, encoding="unicode", xml_declaration=True)
    print(f"  GEXF:    {path} ({eidx} edges)")
    return path


def main():
    parser = argparse.ArgumentParser(description="知识图谱导出工具")
    parser.add_argument("--format", choices=["neo4j", "graphml", "gexf", "all"], default="all")
    args = parser.parse_args()

    print("加载图谱数据...")
    entities, edges = load_graph()
    print(f"  实体: {len(entities)}, 边: {len(edges)}\n")

    fmt = args.format
    if fmt in ("neo4j", "all"):
        print("[Neo4j CSV]")
        export_neo4j(entities, edges)
    if fmt in ("graphml", "all"):
        print("[GraphML]")
        export_graphml(entities, edges)
    if fmt in ("gexf", "all"):
        print("[GEXF]")
        export_gexf(entities, edges)

    print("\n导出完成。")


if __name__ == "__main__":
    main()
