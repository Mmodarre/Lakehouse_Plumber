"""Check curated field help and flag documentation changes for content review."""

import argparse
import hashlib
import json
import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
CATALOG_ROOT = ROOT / "src/lhp/schemas/help"
KINDS = ("project", "pipeline_config", "job_config", "flowgroup", "template")
MANIFEST = CATALOG_ROOT / "source-review.json"


def load_catalogs() -> dict:
    """Read only the named catalogs, excluding the review manifest."""
    return {
        kind: json.loads((CATALOG_ROOT / f"{kind}.json").read_text()) for kind in KINDS
    }


def source_manifest(catalogs: dict) -> dict:
    """Record the exact source revisions reviewed for each group of entries."""
    by_source: dict[str, list[str]] = {}
    for catalog in catalogs.values():
        for entry in catalog["entries"]:
            for source in entry["sources"]:
                by_source.setdefault(source["file"], []).append(entry["id"])
    return {
        "version": 1,
        "sources": {
            path: {
                "sha256": hashlib.sha256((ROOT / path).read_bytes()).hexdigest(),
                "entryIds": sorted(set(ids)),
            }
            for path, ids in sorted(by_source.items())
        },
    }


def catalog_issues(catalogs: dict) -> list[str]:
    """Validate bindings and examples without interpreting their prose."""
    issues: list[str] = []
    ids: set[str] = set()
    bindings: set[str] = set()
    related: list[tuple[str, str]] = []
    for kind, catalog in catalogs.items():
        if catalog.get("version") != 1 or not isinstance(catalog.get("entries"), list):
            issues.append(f"{kind}: expected version 1 and entries list")
            continue
        for entry in catalog["entries"]:
            eid = entry.get("id", "")
            if not eid or eid in ids:
                issues.append(f"{kind}: missing or duplicate id {eid!r}")
            ids.add(eid)
            if (
                not isinstance(entry.get("summary"), str)
                or not entry["summary"].strip()
            ):
                issues.append(f"{eid}: missing summary")
            if not entry.get("sources") or not entry.get("bindings"):
                issues.append(f"{eid}: sources and bindings are required")
            for field in ("details", "constraints", "relatedHelpIds"):
                if field in entry and (
                    not isinstance(entry[field], list)
                    or any(
                        not isinstance(v, str) or not v.strip() for v in entry[field]
                    )
                ):
                    issues.append(f"{eid}: {field} must contain nonempty strings")
            for choice in entry.get("choices", []):
                if not isinstance(choice.get("value"), str) or not choice.get(
                    "explanation"
                ):
                    issues.append(f"{eid}: malformed choice")
            for binding in entry.get("bindings", []):
                path = binding.get("path")
                if not isinstance(path, list) or any(
                    not isinstance(v, (str, int)) or isinstance(v, bool) for v in path
                ):
                    issues.append(f"{eid}: invalid binding path")
                subtype = binding.get("subtype")
                if subtype is not None and not re.fullmatch(
                    r"(?:load|transform|write|test):[a-z_]+", subtype
                ):
                    issues.append(f"{eid}: invalid subtype {subtype}")
                key = json.dumps([kind, path, subtype])
                if key in bindings:
                    issues.append(f"{eid}: duplicate contextual binding {key}")
                bindings.add(key)
            for source in entry.get("sources", []):
                path = ROOT / source["file"]
                if not path.is_relative_to(ROOT) or not path.is_file():
                    issues.append(f"{eid}: missing source {source['file']}")
                    continue
                anchor = source.get("anchor")
                if anchor:
                    text = path.read_text()
                    headings = [
                        re.sub(r"[^\w -]", "", h).strip().lower().replace(" ", "-")
                        for h in re.findall(r"^#{1,6}\s+(.+)$", text, re.MULTILINE)
                    ]
                    if f".. _{anchor}:" not in text and anchor not in headings:
                        issues.append(f"{eid}: unknown source anchor {anchor}")
            for example in entry.get("examples", []):
                try:
                    if not example.get("label") or not isinstance(
                        example.get("yaml"), str
                    ):
                        raise ValueError("example needs label and YAML string")
                    parsed = yaml.safe_load(example["yaml"])
                    if parsed is None:
                        raise ValueError("empty YAML example")
                except (ValueError, yaml.YAMLError) as exc:
                    issues.append(f"{eid}: invalid example: {exc}")
            related.extend((eid, target) for target in entry.get("relatedHelpIds", []))
    issues.extend(
        f"{eid}: unknown related help {target}"
        for eid, target in related
        if target not in ids
    )
    return issues


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--refresh-sources",
        action="store_true",
        help="Record sources after reviewing affected help entries",
    )
    args = parser.parse_args()
    catalogs = load_catalogs()
    issues = catalog_issues(catalogs)
    if issues:
        print("\n".join(issues))
        return 1
    current = source_manifest(catalogs)
    if args.refresh_sources:
        MANIFEST.write_text(json.dumps(current, indent=2) + "\n")
    elif not MANIFEST.exists() or json.loads(MANIFEST.read_text()) != current:
        previous = (
            json.loads(MANIFEST.read_text()).get("sources", {})
            if MANIFEST.exists()
            else {}
        )
        changed = [
            p for p, info in current["sources"].items() if previous.get(p) != info
        ]
        print("Help source review required: " + ", ".join(changed))
        print("Review affected entries, then run with --refresh-sources.")
        return 1
    print(
        f"Field help: {sum(len(c['entries']) for c in catalogs.values())} entries checked across {len(catalogs)} catalogs."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
