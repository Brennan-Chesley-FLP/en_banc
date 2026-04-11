"""Extract the SQLMesh DAG and output a Mermaid flowchart."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def generate_mermaid(sqlmesh_path: str = "sql_processing") -> str:
    from sqlmesh import Context

    ctx = Context(paths=sqlmesh_path)
    dag = ctx.dag

    lines = ["graph LR"]

    # Collect all edges from the DAG
    edges: list[tuple[str, str]] = []
    all_nodes: set[str] = set()

    for model_fqn in dag.sorted:
        all_nodes.add(model_fqn)
        for upstream in dag.upstream(model_fqn):
            edges.append((upstream, model_fqn))
            all_nodes.add(upstream)

    # Build short name mapping: "analytics"."schema"."table" -> schema.table
    def short_name(fqn: str) -> str:
        parts = fqn.replace('"', "").split(".")
        if len(parts) >= 3:
            return f"{parts[-2]}.{parts[-1]}"
        return fqn.replace('"', "")

    def node_id(fqn: str) -> str:
        return short_name(fqn).replace(".", "_").replace("-", "_")

    # Group nodes by schema for subgraphs
    schemas: dict[str, list[str]] = {}
    for node in sorted(all_nodes):
        name = short_name(node)
        schema = name.split(".")[0] if "." in name else "_root"
        schemas.setdefault(schema, []).append(node)

    for schema, nodes in sorted(schemas.items()):
        lines.append(f"    subgraph {schema}")
        for node in sorted(nodes):
            nid = node_id(node)
            label = short_name(node).split(".")[-1]
            lines.append(f"        {nid}[{label}]")
        lines.append("    end")

    lines.append("")

    for upstream, downstream in sorted(edges):
        lines.append(f"    {node_id(upstream)} --> {node_id(downstream)}")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--path",
        default="sql_processing",
        help="Path to the SQLMesh project (default: sql_processing)",
    )
    parser.add_argument(
        "--output",
        "-o",
        help="Output file (default: stdout)",
    )
    args = parser.parse_args()

    mermaid = generate_mermaid(args.path)

    if args.output:
        Path(args.output).write_text(mermaid + "\n")
        print(f"Wrote Mermaid diagram to {args.output}", file=sys.stderr)
    else:
        print(mermaid)


if __name__ == "__main__":
    main()
