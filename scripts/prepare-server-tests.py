#!/usr/bin/env python3
"""Expose suite-owned white-box tests to Go without modifying the candidate."""

import argparse
import json
from pathlib import Path


def prepare(server, sources, output):
    server = server.resolve(strict=True)
    sources = sources.resolve(strict=True)
    if "module github.com/liran/sink\n" not in (server / "go.mod").read_text():
        raise ValueError("--server must point to a Sink source checkout")
    replacements = {}
    for source in sorted(sources.rglob("*.go")):
        relative = source.relative_to(sources)
        if source.is_symlink():
            raise ValueError(f"suite source must not be a symlink: {relative}")
        if not source.name.endswith("_test.go"):
            raise ValueError(f"overlay may only add tests: {relative}")
        target = server / relative
        if target.exists():
            raise ValueError(f"candidate still owns a migrated file: {relative}")
        if relative.parts[0] == "internal" and not target.parent.is_dir():
            raise ValueError(f"candidate package is missing: {relative.parent}")
        replacements[str(target)] = str(source)
    if not replacements:
        raise ValueError("suite contains no server tests")
    output.parent.mkdir(parents=True, exist_ok=True)
    overlay = {"Replace": replacements}
    output.write_text(json.dumps(overlay, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    sources = Path(__file__).resolve().parents[1] / "server-tests/testdata"
    prepare(args.server, sources, args.output)


if __name__ == "__main__":
    main()
