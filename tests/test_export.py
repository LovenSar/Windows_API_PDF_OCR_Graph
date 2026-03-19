"""Tests for export_graph.py — format validation."""

import sys, os, json, csv, tempfile
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from export_graph import export_neo4j, export_graphml, export_gexf

SAMPLE_ENTITIES = {
    "CreateFile": {
        "id": "windows::CreateFile",
        "name": "CreateFile",
        "entity_type": "function",
        "description": "Creates or opens a file.",
        "confidence": 0.9,
        "source_file": "test.json",
    },
    "OVERLAPPED": {
        "id": "windows::OVERLAPPED",
        "name": "OVERLAPPED",
        "entity_type": "structure",
        "description": "Contains information used in asynchronous I/O.",
        "confidence": 0.85,
        "source_file": "test.json",
    },
}

SAMPLE_EDGES = [
    {"source": "CreateFile", "target": "OVERLAPPED", "type": "uses_type"},
]


class TestNeo4jExport:
    def test_creates_csv_files(self, tmp_path, monkeypatch):
        import export_graph
        monkeypatch.setattr(export_graph, "EXPORT_DIR", str(tmp_path))
        nodes_path, edges_path = export_neo4j(SAMPLE_ENTITIES, SAMPLE_EDGES)
        assert os.path.exists(nodes_path)
        assert os.path.exists(edges_path)

        with open(nodes_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            assert ":ID" in header
            rows = list(reader)
            assert len(rows) == 2

        with open(edges_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            assert ":START_ID" in header
            rows = list(reader)
            assert len(rows) == 1


class TestGraphMLExport:
    def test_creates_valid_xml(self, tmp_path, monkeypatch):
        import export_graph
        monkeypatch.setattr(export_graph, "EXPORT_DIR", str(tmp_path))
        path = export_graphml(SAMPLE_ENTITIES, SAMPLE_EDGES)
        assert os.path.exists(path)
        import xml.etree.ElementTree as ET
        tree = ET.parse(path)
        root = tree.getroot()
        ns = "{http://graphml.graphstruct.org/xmlns}"
        graph = root.find(f"{ns}graph")
        nodes = graph.findall(f"{ns}node")
        edges = graph.findall(f"{ns}edge")
        assert len(nodes) == 2
        assert len(edges) == 1


class TestGEXFExport:
    def test_creates_valid_xml(self, tmp_path, monkeypatch):
        import export_graph
        monkeypatch.setattr(export_graph, "EXPORT_DIR", str(tmp_path))
        path = export_gexf(SAMPLE_ENTITIES, SAMPLE_EDGES)
        assert os.path.exists(path)
        import xml.etree.ElementTree as ET
        tree = ET.parse(path)
        root = tree.getroot()
        assert "gexf" in root.tag
