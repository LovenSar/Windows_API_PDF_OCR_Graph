#!/usr/bin/env python3
"""Fail-closed document governance checks. No skip flags. Exit 0 or nonzero."""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
REQUIRED_FIELDS = (
    "schema_version",
    "component_id",
    "internal_version",
    "updated",
    "owner",
    "role",
    "core_files",
    "public_entry",
    "upstream",
    "downstream",
    "config_entry",
    "outputs",
)
REGISTERED_DIRS = (
    "docs",
    "tools",
)
COMPOSITION_IDS = (
    "docs",
    "tools",
)
ROOT_COMPONENT_ID = "winapi_graph"
SUPPORTED_SCHEMA = "1"
OWNER_RE = re.compile(r"^P[1-5](,P[1-5])*$")
VERSION_RE = re.compile(r"^\d+\.\d+\.\d+$")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
PRODUCT_RE = re.compile(
    r"Windows_API_PDF_OCR_Graph v(\d+\.\d+\.\d+) \((\d{4}-\d{2}-\d{2})\)"
)
PRODUCT_LINE = "Windows_API_PDF_OCR_Graph vX.Y.Z (YYYY-MM-DD)"
CHANGELOG_HEADING_RE = re.compile(
    r"^#{1,6}\s+.*(Recent|Added source files|Model routing updates)",
    re.IGNORECASE,
)
TOP_LEVEL_NAMES = {
    "AGENTS.md",
    "README.md",
    "CHANGELOG.md",
    "docs",
    "tools",
    "scripts",
    "graph_viewer",
    "pipeline.py",
    "pipeline_lib",
    "example.graphy.json",
    "tests",
    "OCR_raw",
    "gt_templates",
    "entity_aliases.json",
}

errors: list[str] = []


def err(msg: str) -> None:
    errors.append(msg)


def posix(path: Path) -> str:
    return path.as_posix()


def parse_tables(text: str) -> list[list[list[str]]]:
    tables: list[list[list[str]]] = []
    current: list[list[str]] | None = None
    for raw in text.splitlines():
        line = raw.strip()
        if line.startswith("|") and line.endswith("|"):
            cells = [c.strip() for c in line.strip("|").split("|")]
            if all(set(c) <= set("-:") and c for c in cells):
                continue
            if current is None:
                current = []
                tables.append(current)
            current.append(cells)
        else:
            current = None
    return tables


def heading_table_block(text: str, heading: str) -> str:
    """Return the heading plus immediately following markdown tables only."""
    pattern = re.compile(rf"^## {re.escape(heading)}\s*$", re.MULTILINE)
    match = pattern.search(text)
    if not match:
        return ""
    rest = text[match.end() :]
    collected: list[str] = []
    saw_table = False
    for line in rest.splitlines(keepends=True):
        stripped = line.strip()
        if not stripped:
            collected.append(line)
            continue
        if stripped.startswith("|"):
            saw_table = True
            collected.append(line)
            continue
        break
    if not saw_table:
        return ""
    return text[match.start() : match.end()] + "".join(collected)


def parse_facts(text: str) -> dict[str, str] | None:
    body = heading_table_block(text, "组件事实")
    if not body:
        return None
    for table in parse_tables(body):
        if len(table) < 2 or table[0][:2] != ["字段", "值"]:
            continue
        facts: dict[str, str] = {}
        for row in table[1:]:
            if len(row) >= 2:
                facts[row[0]] = row[1]
        return facts
    return None


def parse_interfaces(text: str) -> list[tuple[str, str, str, str]]:
    body = heading_table_block(text, "公开接口")
    rows: list[tuple[str, str, str, str]] = []
    if not body:
        return rows
    for table in parse_tables(body):
        if not table or len(table[0]) < 4:
            continue
        header = table[0]
        if header[0] != "接口" or "owner 路径" not in header[3]:
            continue
        for row in table[1:]:
            if len(row) >= 4:
                rows.append((row[0], row[1], row[2], row[3]))
    return rows


def split_csv(value: str) -> list[str]:
    if not value or value.strip() == "-":
        return []
    return [part.strip() for part in value.split(",") if part.strip()]


def looks_like_repo_path(value: str) -> bool:
    value = value.strip()
    if not value or value == "-":
        return False
    if "://" in value:
        return False
    if value.startswith("llm-aav-mcp-framework"):
        return False
    if " " in value and "/" not in value:
        return False
    first = value.split("/")[0]
    return first in TOP_LEVEL_NAMES


def path_exists(rel: str, base: Path | None = None) -> bool:
    rel = rel.replace("\\", "/").rstrip("/")
    root = base or ROOT
    return (root / rel).exists()


def git_output(args: list[str]) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if result.returncode != 0:
        return ""
    return result.stdout


def git_changed_files() -> set[str]:
    names: set[str] = set()
    for args in (
        ["diff", "--name-only", "HEAD"],
        ["diff", "--name-only", "--cached", "HEAD"],
        ["ls-files", "--others", "--exclude-standard"],
    ):
        for line in git_output(args).splitlines():
            item = line.strip().replace("\\", "/")
            if item:
                names.add(item)
    return names


def git_show(rel: str) -> str | None:
    result = subprocess.run(
        ["git", "show", f"HEAD:{rel}"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if result.returncode != 0:
        return None
    return result.stdout


def find_agents_files() -> list[Path]:
    files: list[Path] = []
    skip_dir_names = {"_temp", "node_modules", ".git", ".next", "__pycache__", "target"}

    def allow_dir(rel: str) -> bool:
        if not rel:
            return True
        return not any(part in skip_dir_names for part in rel.split("/"))

    stack = [ROOT]
    while stack:
        current = stack.pop()
        try:
            entries = list(current.iterdir())
        except OSError:
            continue
        for entry in entries:
            rel = posix(entry.relative_to(ROOT))
            if entry.is_dir():
                if allow_dir(rel):
                    stack.append(entry)
            elif entry.name == "AGENTS.md":
                files.append(entry)
    uniq = {posix(path.relative_to(ROOT)): path for path in files}
    return [uniq[key] for key in sorted(uniq)]


def component_dir(agents_path: Path) -> str:
    rel = posix(agents_path.relative_to(ROOT))
    if rel == "AGENTS.md":
        return ""
    return str(Path(rel).parent).replace("\\", "/")


def normalize_doc(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def strip_governed_tables(text: str) -> str:
    out = text
    for heading in ("组件事实", "公开接口"):
        block = heading_table_block(out, heading)
        if not block:
            continue
        out = out.replace(block, "\n", 1)
    return normalize_doc(out)


def parse_semver(value: str) -> tuple[int, int, int] | None:
    if not VERSION_RE.match(value):
        return None
    major, minor, patch = value.split(".")
    return int(major), int(minor), int(patch)


def public_keys(ifaces: list[tuple[str, str, str, str]]) -> set[tuple[str, str]]:
    return {(name, kind) for name, kind, stability, _owner in ifaces if stability == "public"}


def load_all() -> dict[str, dict]:
    loaded: dict[str, dict] = {}
    for path in find_agents_files():
        rel = posix(path.relative_to(ROOT))
        text = path.read_text(encoding="utf-8")
        loaded[rel] = {
            "path": path,
            "rel": rel,
            "dir": component_dir(path),
            "text": text,
            "facts": parse_facts(text),
            "ifaces": parse_interfaces(text),
        }
    return loaded


def registered_from_rules(text: str) -> list[str]:
    match = re.search(
        r"### 1\.5 受管目录\r?\n+(.*?)(?=\r?\n## |\r?\n### |\Z)",
        text,
        re.S,
    )
    if not match:
        err("docs/DEVELOPMENT_RULES.md missing section 1.5 registered directory list")
        return []
    return re.findall(r"^- `([^`]+)/`\s*$", match.group(1), re.M)


def check_registered(loaded: dict[str, dict]) -> None:
    rules = (ROOT / "docs" / "DEVELOPMENT_RULES.md").read_text(encoding="utf-8")
    listed = registered_from_rules(rules)
    if listed and set(listed) != set(REGISTERED_DIRS):
        err(
            "registered directory set mismatch between check.py and "
            f"docs/DEVELOPMENT_RULES.md: {sorted(set(listed))} vs {list(REGISTERED_DIRS)}"
        )
    for rel_dir in REGISTERED_DIRS:
        marker = f"`{rel_dir}/`"
        if marker not in rules:
            err(f"registered dir {rel_dir}/ missing from docs/DEVELOPMENT_RULES.md")
        agents = f"{rel_dir}/AGENTS.md"
        if agents not in loaded:
            err(f"registered dir missing AGENTS.md: {agents}")
    if "AGENTS.md" not in loaded:
        err("repository root AGENTS.md is missing")
    tracked = git_output(["ls-files", "AGENTS.md", "**/AGENTS.md"])
    for rel in tracked.splitlines():
        item = rel.strip().replace("\\", "/")
        if not item:
            continue
        if not path_exists(item):
            err(f"tracked AGENTS.md deleted: {item}")
        elif item not in loaded:
            err(f"tracked AGENTS.md skipped by checker: {item}")


def fact_field_errors(rel: str, facts: dict[str, str] | None) -> list[str]:
    """Return field-level fact table errors. Does not use git or ROOT."""
    out: list[str] = []
    if facts is None:
        out.append(f"{rel}: missing component facts table")
        return out
    for field in REQUIRED_FIELDS:
        if field not in facts or facts[field] == "":
            out.append(f"{rel}: missing field {field}")
    schema = facts.get("schema_version", "")
    if schema != SUPPORTED_SCHEMA:
        out.append(f"{rel}: unsupported schema_version {schema!r}")
    version = facts.get("internal_version", "")
    if version and not VERSION_RE.match(version):
        out.append(f"{rel}: invalid internal_version {version!r}")
    updated = facts.get("updated", "")
    if updated and not DATE_RE.match(updated):
        out.append(f"{rel}: invalid updated {updated!r}")
    owner = facts.get("owner", "")
    if owner:
        if "–" in owner or "—" in owner or "-" in owner:
            out.append(f"{rel}: owner must not use a range: {owner!r}")
        elif not OWNER_RE.match(owner):
            out.append(f"{rel}: invalid owner {owner!r}")
    elif "owner" in facts:
        out.append(f"{rel}: invalid owner {owner!r}")
    return out


def version_policy_errors(rel: str, old_text: str | None, new_text: str) -> list[str]:
    """Return version bump errors from two AGENTS.md snapshots. No git."""
    facts = parse_facts(new_text) or {}
    out: list[str] = []
    if is_governance_baseline(old_text, facts):
        return out
    if old_text is None:
        return out
    old_facts = parse_facts(old_text) or {}
    old_ver = parse_semver(old_facts.get("internal_version", ""))
    new_ver = parse_semver(facts.get("internal_version", ""))
    if old_ver is None or new_ver is None:
        return out
    old_pub = public_keys(parse_interfaces(old_text))
    new_pub = public_keys(parse_interfaces(new_text))
    added = new_pub - old_pub
    removed = old_pub - new_pub
    bump = version_bump(old_ver, new_ver)
    if not added and not removed and bump in {"major", "minor"}:
        out.append(
            f"{rel}: public interfaces unchanged but {bump} bump "
            f"{old_facts.get('internal_version')} -> {facts.get('internal_version')}"
        )
    if removed and bump != "major":
        out.append(f"{rel}: public interface removed/renamed but version did not rise MAJOR")
    if added and bump not in {"minor", "major"}:
        out.append(f"{rel}: public interface added but version did not rise at least MINOR")
    return out


def readme_matrix_errors(
    text: str,
    by_id: dict[str, dict],
    *,
    composition_ids: tuple[str, ...] = COMPOSITION_IDS,
    root_component_id: str = ROOT_COMPONENT_ID,
    product_re: re.Pattern[str] = PRODUCT_RE,
    product_line: str = PRODUCT_LINE,
    path_exists_fn=None,
) -> list[str]:
    """Return README composition matrix errors. path_exists_fn defaults to path_exists."""
    exists = path_exists if path_exists_fn is None else path_exists_fn
    out: list[str] = []
    if not product_re.search(text):
        out.append(f"README.md missing product version line {product_line}")
    found: dict[str, tuple[str, str]] = {}
    for table in parse_tables(text):
        if not table:
            continue
        header = [h.strip() for h in table[0]]
        if header[:3] != ["component_id", "path", "internal_version"]:
            continue
        if "pin" in header:
            out.append("README matrix must not include pin column")
            continue
        for row in table[1:]:
            if len(row) >= 3:
                found[row[0]] = (row[1].rstrip("/"), row[2])
    if set(found) != set(composition_ids):
        out.append(
            f"README matrix component_id set mismatch: {sorted(found)} vs {list(composition_ids)}"
        )
    if root_component_id in found:
        out.append(f"README matrix must not include {root_component_id}")
    for cid in composition_ids:
        if cid not in found:
            continue
        path, version = found[cid]
        item = by_id.get(cid)
        if not item:
            out.append(f"README matrix {cid} has no AGENTS component")
            continue
        expected_dir = item["dir"]
        if path != expected_dir:
            out.append(f"README matrix {cid} path {path} != {expected_dir}")
        actual = item["facts"]["internal_version"]
        if version != actual:
            out.append(f"README matrix {cid} version {version} != {actual}")
        if not exists(path):
            out.append(f"README matrix path missing: {path}")
    return out


def check_facts(loaded: dict[str, dict]) -> dict[str, dict]:
    by_id: dict[str, dict] = {}
    for rel, item in loaded.items():
        facts = item["facts"]
        for msg in fact_field_errors(rel, facts):
            err(msg)
        if facts is None:
            continue
        cid = facts.get("component_id", "")
        if not cid:
            continue
        if cid in by_id:
            err(f"component_id conflict: {cid} in {by_id[cid]['rel']} and {rel}")
        by_id[cid] = item
        item["cid"] = cid
    root = loaded.get("AGENTS.md")
    if root and root.get("facts") and root["facts"].get("component_id") != ROOT_COMPONENT_ID:
        err(f"root AGENTS.md component_id must be {ROOT_COMPONENT_ID}")
    return by_id


def check_paths(item: dict) -> None:
    rel = item["rel"]
    facts = item["facts"] or {}
    for file_rel in split_csv(facts.get("core_files", "")):
        if "\\" in file_rel:
            err(f"{rel}: core_files must use / not backslash: {file_rel}")
        if not path_exists(file_rel):
            err(f"{rel}: core_files missing {file_rel}")
    for name, _kind, _stab, owner_path in item["ifaces"]:
        if "\\" in owner_path:
            err(f"{rel}: owner path must use / not backslash: {owner_path}")
        if not path_exists(owner_path):
            err(f"{rel}: owner path missing for {name}: {owner_path}")
    entry = facts.get("public_entry", "")
    for part in split_csv(entry):
        if looks_like_repo_path(part) and not path_exists(part):
            err(f"{rel}: public_entry path missing {part}")


def check_readme(by_id: dict[str, dict]) -> None:
    text = (ROOT / "README.md").read_text(encoding="utf-8")
    for msg in readme_matrix_errors(text, by_id):
        err(msg)


def check_unique_owners(loaded: dict[str, dict]) -> None:
    owners: dict[str, str] = {}
    sources: dict[str, str] = {}
    for rel, item in loaded.items():
        for name, _kind, _stab, owner_path in item["ifaces"]:
            if name in owners and owners[name] != owner_path:
                err(
                    f"unique owner conflict for {name}: "
                    f"{sources[name]} -> {owners[name]} vs {rel} -> {owner_path}"
                )
            else:
                owners[name] = owner_path
                sources[name] = rel


def owning_agents(rel_file: str, loaded: dict[str, dict]) -> str | None:
    path = Path(rel_file)
    candidates = [""] if rel_file in ("AGENTS.md", "README.md", "CHANGELOG.md") else []
    if not candidates:
        for directory in [path.parent, *path.parents]:
            as_posix = posix(directory)
            if as_posix in (".", ""):
                candidates.append("")
                break
            candidates.append(as_posix)
    for directory in candidates:
        agents_rel = "AGENTS.md" if directory == "" else f"{directory}/AGENTS.md"
        if agents_rel in loaded:
            return agents_rel
    return None


def facts_only_agents_change(rel: str, new_text: str) -> bool:
    old = git_show(rel)
    if old is None:
        return strip_governed_tables(new_text) == ""
    return strip_governed_tables(old) == strip_governed_tables(new_text)


def check_changelog_debt(loaded: dict[str, dict], changed: set[str]) -> None:
    triggered: set[str] = set()
    for rel_file in changed:
        if rel_file.endswith("/AGENTS.md") or rel_file == "AGENTS.md":
            if rel_file in loaded and facts_only_agents_change(rel_file, loaded[rel_file]["text"]):
                continue
        owner = owning_agents(rel_file, loaded)
        if owner:
            triggered.add(owner)
    for agents_rel in triggered:
        text = loaded[agents_rel]["text"]
        for line in text.splitlines():
            if CHANGELOG_HEADING_RE.search(line):
                err(f"{agents_rel}: changelog heading must be fused: {line.strip()}")


def parent_agents(item: dict, loaded: dict[str, dict]) -> list[dict]:
    directory = item["dir"]
    if directory == "":
        return []
    parents: list[dict] = []
    current = Path(directory)
    while True:
        current = current.parent
        rel = "AGENTS.md" if str(current) in (".", "") else posix(current / "AGENTS.md")
        if str(current) == "." or posix(current) == "":
            if "AGENTS.md" in loaded:
                parents.append(loaded["AGENTS.md"])
            break
        if rel in loaded:
            parents.append(loaded[rel])
        if posix(current) == "":
            break
    return parents


def references_component(parent: dict, child: dict) -> bool:
    cid = child.get("cid")
    if not cid:
        return False
    pfacts = parent["facts"] or {}
    if cid in split_csv(pfacts.get("upstream", "")) or cid in split_csv(pfacts.get("downstream", "")):
        return True
    child_dir = child["dir"]
    if child_dir and child_dir in parent["text"]:
        return True
    for _name, _kind, _stab, owner_path in parent["ifaces"]:
        if child_dir and (owner_path == child_dir or owner_path.startswith(child_dir + "/")):
            return True
    return False


def version_summaries(parent: dict, cid: str) -> list[str]:
    found: list[str] = []
    for table in parse_tables(parent["text"]):
        if not table:
            continue
        header = [h.lower() for h in table[0]]
        if "component_id" not in header or "internal_version" not in header:
            continue
        id_idx = header.index("component_id")
        ver_idx = header.index("internal_version")
        for row in table[1:]:
            if len(row) > max(id_idx, ver_idx) and row[id_idx] == cid:
                found.append(row[ver_idx])
    return found


def child_tracked_changed(old_text: str | None, new_item: dict) -> bool:
    if old_text is None:
        return False
    old_facts = parse_facts(old_text) or {}
    new_facts = new_item["facts"] or {}
    for field in ("internal_version", "public_entry", "upstream", "downstream"):
        if old_facts.get(field, "") != new_facts.get(field, ""):
            return True
    old_pub = public_keys(parse_interfaces(old_text))
    new_pub = public_keys(new_item["ifaces"])
    return old_pub != new_pub


def check_propagation(loaded: dict[str, dict], by_id: dict[str, dict]) -> None:
    for rel, item in loaded.items():
        if not item.get("facts"):
            continue
        old_text = git_show(rel)
        if not child_tracked_changed(old_text, item):
            continue
        cid = item["cid"]
        for parent in parent_agents(item, loaded):
            if not references_component(parent, item):
                continue
            pfacts = parent["facts"] or {}
            if cid in split_csv(pfacts.get("upstream", "")) or cid in split_csv(
                pfacts.get("downstream", "")
            ):
                child_still = cid in by_id
                if not child_still:
                    err(f"{parent['rel']}: stale component_id reference {cid}")
            child_dir = item["dir"]
            for _name, _kind, _stab, owner_path in parent["ifaces"]:
                if child_dir and (
                    owner_path == child_dir or owner_path.startswith(child_dir + "/")
                ):
                    if not path_exists(owner_path):
                        err(f"{parent['rel']}: stale owner path into {cid}: {owner_path}")
            for summary in version_summaries(parent, cid):
                actual = item["facts"]["internal_version"]
                if summary != actual:
                    err(
                        f"{parent['rel']}: version summary for {cid} is {summary}, "
                        f"component is {actual}"
                    )


def is_governance_baseline(old_text: str | None, new_facts: dict[str, str]) -> bool:
    if new_facts.get("internal_version") != "0.1.0":
        return False
    if old_text is None:
        return True
    old_facts = parse_facts(old_text)
    return old_facts is None or "internal_version" not in old_facts


def version_bump(old: tuple[int, int, int], new: tuple[int, int, int]) -> str:
    if new[0] != old[0]:
        return "major"
    if new[1] != old[1]:
        return "minor"
    if new[2] != old[2]:
        return "patch"
    return "none"


def check_versions(loaded: dict[str, dict]) -> None:
    for rel, item in loaded.items():
        for msg in version_policy_errors(rel, git_show(rel), item["text"]):
            err(msg)


def main() -> int:
    if not (ROOT / "docs" / "DEVELOPMENT_RULES.md").is_file():
        print("docs/DEVELOPMENT_RULES.md missing", file=sys.stderr)
        return 1
    loaded = load_all()
    check_registered(loaded)
    by_id = check_facts(loaded)
    for item in loaded.values():
        if item["facts"]:
            check_paths(item)
    check_readme(by_id)
    check_unique_owners(loaded)
    changed = git_changed_files()
    check_changelog_debt(loaded, changed)
    check_propagation(loaded, by_id)
    check_versions(loaded)
    if errors:
        print("agents_doccheck failed:", file=sys.stderr)
        for item in errors:
            print(f"- {item}", file=sys.stderr)
        return 1
    print("agents_doccheck: ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
