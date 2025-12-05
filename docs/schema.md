<!-- docs/schema.md -->

# Schema Documentation Protocol — Segmeling / datavis.au

The canonical and current database schema for the `trading` PostgreSQL database is stored in:

- `docs/db-schema.txt`

This file is not edited by hand.  
It is regenerated automatically after each deployment or migration using:

    python -m jobs.buildSchema

The generated file includes:

- A timestamp for when the snapshot was taken.  
- All tables and columns in the `public` schema.  
- Data types, lengths, numeric precision and scale.  
- Nullability and default values.  
- Index definitions.  
- Table constraints, including foreign keys.

---

## Rules for Custom GPTs

1. Before generating or modifying any SQL, the GPT must read `docs/db-schema.txt`.  
2. The GPT must not assume that a table or column exists unless it appears in `docs/db-schema.txt`.  
3. The GPT must not rename or drop tables, columns, or constraints unless:
   - There is a clear GitHub issue describing the change, and  
   - The user explicitly approves the modification.

4. If the GPT proposes a schema change (new table, new column, new index), it must also:
   - Propose a migration script or change script.  
   - Propose an entry to add to `docs/decisions.md` describing the change.

---

## Rules for Developers

- Do not manually edit `docs/db-schema.txt`.  
- When running migrations locally, regenerate the schema snapshot with:

      python -m jobs.buildSchema

  and review the output for correctness.

- If schema differences appear between environments, use the snapshots to diagnose and reconcile them.

---

This protocol ensures that the database schema, codebase, and AI tools remain aligned at all times.
