"""Read-only view of the COVESA VSS tree (``vss.json`` release artifact)."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

DEFAULT_VSS_JSON = Path("sources/vss/vss.json")


@dataclass(frozen=True)
class VssLeaf:
    path: str
    type: str  # sensor | actuator | attribute
    datatype: str
    unit: str


class VssCatalog:
    def __init__(self, leaves: dict[str, VssLeaf]) -> None:
        self._leaves = leaves

    @classmethod
    def load(cls, path: Path = DEFAULT_VSS_JSON) -> VssCatalog:
        tree: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
        leaves: dict[str, VssLeaf] = {}

        def walk(name: str, node: dict[str, Any]) -> None:
            if node.get("type") in ("sensor", "actuator", "attribute"):
                leaves[name] = VssLeaf(
                    name, node["type"], node.get("datatype", ""), node.get("unit", "")
                )
            for child, sub in node.get("children", {}).items():
                walk(f"{name}.{child}", sub)

        for root, node in tree.items():
            walk(root, node)
        return cls(leaves)

    def __contains__(self, path: object) -> bool:
        return path in self._leaves

    def __len__(self) -> int:
        return len(self._leaves)

    def leaf(self, path: str) -> VssLeaf:
        try:
            return self._leaves[path]
        except KeyError:
            raise KeyError(f"{path!r} is not a leaf of the VSS tree") from None
