# jobs/buildSchema.py
"""
Generate a snapshot of the current PostgreSQL schema and write it to docs/db-schema.txt.

Relies on:
- db.get_conn()  -> returns a psycopg2 connection to the `trading` database.
- Project structure: this file is in jobs/, docs/ is at project root.
"""

from pathlib import Path
from textwrap import indent

from db import get_conn  # adjust if your helper name/path is slightly different


SCHEMA_COLUMNS_SQL = """
SELECT
    c.table_name,
    c.ordinal_position AS col_position,
    c.column_name,
    c.data_type,
    COALESCE(c.character_maximum_length::text, '') AS char_max_len,
    COALESCE(c.numeric_precision::text, '') AS numeric_precision,
    COALESCE(c.numeric_scale::text, '') AS numeric_scale,
    c.is_nullable,
    COALESCE(c.column_default, '') AS column_default
FROM information_schema.columns c
JOIN information_schema.tables t
  ON c.table_schema = t.table_schema
 AND c.table_name   = t.table_name
WHERE c.table_schema = 'public'
  AND t.table_type   = 'BASE TABLE'
ORDER BY c.table_name, c.ordinal_position;
"""

SCHEMA_INDEXES_SQL = """
SELECT
    t.relname AS table_name,
    i.relname AS index_name,
    idx.indisprimary AS is_primary,
    idx.indisunique  AS is_unique,
    pg_get_indexdef(idx.indexrelid) AS index_def
FROM pg_class t
JOIN pg_namespace ns
  ON ns.oid = t.relnamespace
JOIN pg_index idx
  ON t.oid = idx.indrelid
JOIN pg_class i
  ON i.oid = idx.indexrelid
WHERE ns.nspname = 'public'
  AND t.relkind = 'r'
ORDER BY t.relname, i.relname;
"""

SCHEMA_CONSTRAINTS_SQL = """
SELECT
    tc.table_name,
    tc.constraint_name,
    tc.constraint_type,
    COALESCE(kcu.column_name, '') AS column_name,
    COALESCE(ccu.table_name, '') AS foreign_table_name,
    COALESCE(ccu.column_name, '') AS foreign_column_name
FROM information_schema.table_constraints AS tc
LEFT JOIN information_schema.key_column_usage AS kcu
   ON tc.constraint_name = kcu.constraint_name
  AND tc.table_schema = kcu.table_schema
LEFT JOIN information_schema.constraint_column_usage AS ccu
   ON tc.constraint_name = ccu.constraint_name
  AND tc.table_schema = ccu.table_schema
WHERE tc.table_schema = 'public'
ORDER BY tc.table_name, tc.constraint_name, kcu.column_name;
"""


def get_project_root() -> Path:
    # jobs/ -> project root
    return Path(__file__).resolve().parents[1]


def build_schema_text() -> str:
    lines = []

    with get_conn() as conn:
        with conn.cursor() as cur:
            # Header & timestamp
            cur.execute("SELECT now()")
            (now_val,) = cur.fetchone()
            lines.append("=== DATABASE SCHEMA SNAPSHOT (LATEST) ===")
            lines.append(f"Generated at: {now_val}")
            lines.append("Schema: public")
            lines.append("")

            # Columns
            lines.append("=== TABLES & COLUMNS ===")
            cur.execute(SCHEMA_COLUMNS_SQL)
            rows = cur.fetchall()
            current_table = None
            for (
                table_name,
                col_pos,
                col_name,
                data_type,
                char_max_len,
                numeric_precision,
                numeric_scale,
                is_nullable,
                column_default,
            ) in rows:
                if table_name != current_table:
                    lines.append("")
                    lines.append(f"Table: {table_name}")
                    lines.append("-" * (7 + len(table_name)))
                    header = (
                        "pos | column_name | data_type | len/prec/scale "
                        "| nullable | default"
                    )
                    lines.append("  " + header)
                    lines.append("  " + "-" * len(header))
                    current_table = table_name

                length_info = ""
                if char_max_len:
                    length_info = char_max_len
                elif numeric_precision:
                    length_info = numeric_precision
                    if numeric_scale:
                        length_info += f"/{numeric_scale}"

                line = (
                    f"{col_pos:>3} | {col_name:<12} | {data_type:<10} | "
                    f"{length_info:<13} | {is_nullable:<8} | {column_default}"
                )
                lines.append("  " + line)

            lines.append("")
            lines.append("=== INDEXES ===")
            cur.execute(SCHEMA_INDEXES_SQL)
            idx_rows = cur.fetchall()
            current_table = None
            for table_name, index_name, is_primary, is_unique, index_def in idx_rows:
                if table_name != current_table:
                    lines.append("")
                    lines.append(f"Table: {table_name}")
                    lines.append("-" * (7 + len(table_name)))
                    current_table = table_name

                flags = []
                if is_primary:
                    flags.append("PRIMARY")
                if is_unique:
                    flags.append("UNIQUE")
                flags_str = ", ".join(flags) if flags else "normal"

                lines.append(f"  Index: {index_name} [{flags_str}]")
                lines.append("  " + index_def)

            lines.append("")
            lines.append("=== CONSTRAINTS ===")
            cur.execute(SCHEMA_CONSTRAINTS_SQL)
            cons_rows = cur.fetchall()
            current_table = None
            for (
                table_name,
                constraint_name,
                constraint_type,
                column_name,
                foreign_table_name,
                foreign_column_name,
            ) in cons_rows:
                if table_name != current_table:
                    lines.append("")
                    lines.append(f"Table: {table_name}")
                    lines.append("-" * (7 + len(table_name)))
                    current_table = table_name

                extra = ""
                if foreign_table_name and foreign_column_name:
                    extra = f" -> {foreign_table_name}({foreign_column_name})"

                lines.append(
                    f"  {constraint_type:<12} {constraint_name}"
                    + (f" on {column_name}" if column_name else "")
                    + extra
                )

    return "\n".join(lines) + "\n"


def main() -> None:
    root = get_project_root()
    docs_dir = root / "docs"
    docs_dir.mkdir(exist_ok=True)

    out_path = docs_dir / "db-schema.txt"
    text = build_schema_text()
    out_path.write_text(text, encoding="utf-8")
    print(f"Wrote latest schema to {out_path}")


if __name__ == "__main__":
    main()
