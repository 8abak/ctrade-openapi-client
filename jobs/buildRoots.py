# jobs/buildRoots.py
"""
Scan backend/main.py and backend/db.py to produce an overview of:
- HTTP routes (FastAPI-style decorators)
- DB helper functions

Output: docs/routes-and-db.txt
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import List, Tuple


ROUTE_METHODS = {"get", "post", "put", "delete", "patch"}


def get_project_root() -> Path:
    # jobs/ -> project root
    return Path(__file__).resolve().parents[1]


def find_routes(file_path: Path) -> List[Tuple[str, str, str, str]]:
    """
    Return list of (file_name, function_name, HTTP_METHOD, path).
    """
    source = file_path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(file_path))

    routes: List[Tuple[str, str, str, str]] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            for dec in node.decorator_list:
                # Look for decorators like @app.get("/path") or @router.post("/path")
                if isinstance(dec, ast.Call) and isinstance(dec.func, ast.Attribute):
                    method = dec.func.attr.lower()
                    if method in ROUTE_METHODS:
                        http_method = method.upper()
                        route_path = "(dynamic)"

                        if dec.args:
                            first_arg = dec.args[0]
                            if isinstance(first_arg, ast.Constant) and isinstance(
                                first_arg.value, str
                            ):
                                route_path = first_arg.value

                        routes.append(
                            (
                                file_path.name,
                                node.name,
                                http_method,
                                route_path,
                            )
                        )

    routes.sort(key=lambda x: (x[0], x[3], x[2], x[1]))
    return routes


def find_db_functions(file_path: Path) -> List[Tuple[str, str, str]]:
    """
    Return list of (file_name, function_name, signature_string) for db helpers.

    We approximate signature as comma-separated argument names, ignoring defaults.
    """
    source = file_path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(file_path))

    funcs: List[Tuple[str, str, str]] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            # Only top-level functions (no nested defs, no class methods)
            if not isinstance(getattr(node, "parent", None), ast.FunctionDef):
                # Build a simple signature: arg1, arg2, ...
                arg_names = []
                for arg in node.args.args:
                    # Optionally skip "self" if you ever have methods in db.py
                    if arg.arg == "self":
                        continue
                    arg_names.append(arg.arg)

                sig = ", ".join(arg_names)
                funcs.append((file_path.name, node.name, sig))

    funcs.sort(key=lambda x: (x[0], x[1]))
    return funcs


def attach_parents(tree: ast.AST) -> None:
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            setattr(child, "parent", node)


def build_routes_and_db_text() -> str:
    root = get_project_root()
    backend_dir = root / "backend"
    main_path = backend_dir / "main.py"
    db_path = backend_dir / "db.py"

    if not main_path.exists():
        raise FileNotFoundError(f"backend/main.py not found at {main_path}")
    if not db_path.exists():
        raise FileNotFoundError(f"backend/db.py not found at {db_path}")

    # Re-parse with parents attached for db.py (if we ever care about nesting)
    db_source = db_path.read_text(encoding="utf-8")
    db_tree = ast.parse(db_source, filename=str(db_path))
    attach_parents(db_tree)

    routes = find_routes(main_path)
    db_funcs = find_db_functions(db_path)  # uses db_tree indirectly

    lines = []
    lines.append("=== ROUTES & DB HELPERS OVERVIEW (LATEST) ===")
    lines.append("")

    # Routes section
    lines.append("=== HTTP ROUTES (backend/main.py) ===")
    if not routes:
        lines.append("No routes detected.")
    else:
        header = "file | method | path | handler"
        lines.append(header)
        lines.append("-" * len(header))
        for file_name, func_name, http_method, route_path in routes:
            lines.append(
                f"{file_name} | {http_method:<6} | {route_path:<30} | {func_name}"
            )
    lines.append("")

    # DB functions section
    lines.append("=== DB FUNCTIONS (backend/db.py) ===")
    if not db_funcs:
        lines.append("No DB helper functions detected.")
    else:
        header = "file | function | args"
        lines.append(header)
        lines.append("-" * len(header))
        for file_name, func_name, sig in db_funcs:
            lines.append(f"{file_name} | {func_name:<20} | ({sig})")

    lines.append("")
    return "\n".join(lines)


def main() -> None:
    root = get_project_root()
    docs_dir = root / "docs"
    docs_dir.mkdir(exist_ok=True)

    out_path = docs_dir / "routes-and-db.txt"
    text = build_routes_and_db_text()
    out_path.write_text(text, encoding="utf-8")
    print(f"Wrote routes/DB overview to {out_path}")


if __name__ == "__main__":
    main()
