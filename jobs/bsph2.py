import psycopg2
from psycopg2.extras import execute_values

DB_NAME = "trading"
DB_USER = "babak"
DB_PASSWORD = "babak33044"  # already working in bsph1
DB_HOST = "localhost"
DB_PORT = 5432


BATCH_SIZE = 10_000
UPDATE_BATCH_SIZE = 50_000


def update_vel_len(conn):
    """
    Fill vel_len for each velocity group.
    Uses streaming cursor ordered by id, relying on vel_grp increasing when category changes.
    """
    print("Step 1: Computing vel_len...")

    cur = conn.cursor()
    stream = conn.cursor(name="vel_len_stream", withhold=True)
    stream.itersize = BATCH_SIZE

    stream.execute("""
        SELECT id, vel_grp
        FROM segments
        ORDER BY id
    """)

    updates = []  # (vel_len, id)
    group_ids = []
    current_grp = None

    total_groups = 0

    while True:
        rows = stream.fetchmany(BATCH_SIZE)
        if not rows:
            break

        for seg_id, grp in rows:
            # Skip rows that don't have a group (shouldn't happen for vel)
            if grp is None:
                continue

            if current_grp is None:
                # first group
                current_grp = grp
                group_ids = [seg_id]
            elif grp == current_grp:
                group_ids.append(seg_id)
            else:
                # group ended
                group_len = len(group_ids)
                for gid in group_ids:
                    updates.append((group_len, gid))
                total_groups += 1

                # start new group
                current_grp = grp
                group_ids = [seg_id]

            if len(updates) >= UPDATE_BATCH_SIZE:
                execute_values(
                    cur,
                    """
                    UPDATE segments AS s
                    SET vel_len = v.len
                    FROM (VALUES %s) AS v(len, id)
                    WHERE s.id = v.id
                    """,
                    updates,
                    page_size=UPDATE_BATCH_SIZE,
                )
                conn.commit()
                print(f"  vel_len: updated groups so far: {total_groups}")
                updates.clear()

    # flush last group
    if current_grp is not None and group_ids:
        group_len = len(group_ids)
        for gid in group_ids:
            updates.append((group_len, gid))
        total_groups += 1

    if updates:
        execute_values(
            cur,
            """
            UPDATE segments AS s
            SET vel_len = v.len
            FROM (VALUES %s) AS v(len, id)
            WHERE s.id = v.id
            """,
            updates,
            page_size=UPDATE_BATCH_SIZE,
        )
        conn.commit()

    stream.close()
    cur.close()
    print(f"Step 1 done: vel_len computed for {total_groups} groups.")


def update_generic_len(conn, col_grp, col_len, label):
    """
    Generic function to fill *_len (kal_len, mom_len) based on *_grp.
    We assume group ids are contiguous segments in id-order.
    """
    print(f"Step 2: Computing {label}_len...")

    cur = conn.cursor()
    stream = conn.cursor(name=f"{label}_len_stream", withhold=True)
    stream.itersize = BATCH_SIZE

    query = f"""
        SELECT id, {col_grp}
        FROM segments
        ORDER BY id
    """
    stream.execute(query)

    updates = []  # (len, id)
    group_ids = []
    current_grp = None
    total_groups = 0

    while True:
        rows = stream.fetchmany(BATCH_SIZE)
        if not rows:
            break

        for seg_id, grp in rows:
            if grp is None:
                continue

            if current_grp is None:
                current_grp = grp
                group_ids = [seg_id]
            elif grp == current_grp:
                group_ids.append(seg_id)
            else:
                # group ended
                g_len = len(group_ids)
                for gid in group_ids:
                    updates.append((g_len, gid))
                total_groups += 1

                current_grp = grp
                group_ids = [seg_id]

            if len(updates) >= UPDATE_BATCH_SIZE:
                execute_values(
                    cur,
                    f"""
                    UPDATE segments AS s
                    SET {col_len} = v.len
                    FROM (VALUES %s) AS v(len, id)
                    WHERE s.id = v.id
                    """,
                    updates,
                    page_size=UPDATE_BATCH_SIZE,
                )
                conn.commit()
                print(f"  {label}_len: updated groups so far: {total_groups}")
                updates.clear()

    # flush last group
    if current_grp is not None and group_ids:
        g_len = len(group_ids)
        for gid in group_ids:
            updates.append((g_len, gid))
        total_groups += 1

    if updates:
        execute_values(
            cur,
            f"""
            UPDATE segments AS s
            SET {col_len} = v.len
            FROM (VALUES %s) AS v(len, id)
            WHERE s.id = v.id
            """,
            updates,
            page_size=UPDATE_BATCH_SIZE,
        )
        conn.commit()

    stream.close()
    cur.close()
    print(f"Step 2 done: {label}_len computed for {total_groups} groups.")


def compute_vol_thresholds(conn):
    """
    Compute approximate 33% and 66% percentiles of vol_val to define regimes.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT
            percentile_cont(0.33) WITHIN GROUP (ORDER BY vol_val),
            percentile_cont(0.66) WITHIN GROUP (ORDER BY vol_val)
        FROM segments
        WHERE vol_val IS NOT NULL;
    """)
    q1, q2 = cur.fetchone()
    cur.close()
    print(f"Step 3: vol_val percentiles -> q1={q1}, q2={q2}")
    return q1, q2


def update_vol_segments(conn, q1, q2):
    """
    Set vol_cat, vol_grp, vol_pos, vol_len based on vol_val and percentiles.
    """
    print("Step 4: Computing vol_cat, vol_grp, vol_pos, vol_len...")

    cur = conn.cursor()
    stream = conn.cursor(name="vol_stream", withhold=True)
    stream.itersize = BATCH_SIZE

    # We process in id order so that segments follow time
    stream.execute("""
        SELECT id, vol_val
        FROM segments
        ORDER BY id
    """)

    updates = []  # (vol_cat, vol_grp, vol_pos, vol_len, id)

    prev_cat = None
    vol_grp = 0
    group_buffer = []  # list of (id, cat, pos)
    total_groups = 0

    while True:
        rows = stream.fetchmany(BATCH_SIZE)
        if not rows:
            break

        for seg_id, vol_val in rows:
            if vol_val is None:
                # We'll leave all vol_* as NULL for these rows
                # If we had an open group and then hit a NULL, we consider group ended
                if group_buffer:
                    g_len = len(group_buffer)
                    for (gid, cat, pos) in group_buffer:
                        updates.append((cat, vol_grp, pos, g_len, gid))
                    total_groups += 1
                    group_buffer = []
                    prev_cat = None
                continue

            # classify vol_val into 1,2,3
            if vol_val < q1:
                cat = 1    # compression
            elif vol_val < q2:
                cat = 2    # normal
            else:
                cat = 3    # expansion

            if prev_cat is None:
                # first non-NULL
                vol_grp += 1
                pos = 0
                group_buffer = [(seg_id, cat, pos)]
                prev_cat = cat
            else:
                if cat == prev_cat:
                    pos = group_buffer[-1][2] + 1  # previous pos +1
                    group_buffer.append((seg_id, cat, pos))
                else:
                    # finalize old group
                    g_len = len(group_buffer)
                    for (gid, old_cat, old_pos) in group_buffer:
                        updates.append((old_cat, vol_grp, old_pos, g_len, gid))
                    total_groups += 1

                    # start new group
                    vol_grp += 1
                    pos = 0
                    group_buffer = [(seg_id, cat, pos)]
                    prev_cat = cat

            if len(updates) >= UPDATE_BATCH_SIZE:
                execute_values(
                    cur,
                    """
                    UPDATE segments AS s
                    SET vol_cat = v.cat,
                        vol_grp = v.grp,
                        vol_pos = v.pos,
                        vol_len = v.len
                    FROM (VALUES %s) AS v(cat, grp, pos, len, id)
                    WHERE s.id = v.id
                    """,
                    updates,
                    page_size=UPDATE_BATCH_SIZE,
                )
                conn.commit()
                print(f"  vol_*: updated groups so far: {total_groups}")
                updates.clear()

    # finalize last open group
    if group_buffer:
        g_len = len(group_buffer)
        for (gid, cat, pos) in group_buffer:
            updates.append((cat, vol_grp, pos, g_len, gid))
        total_groups += 1

    if updates:
        execute_values(
            cur,
            """
            UPDATE segments AS s
            SET vol_cat = v.cat,
                vol_grp = v.grp,
                vol_pos = v.pos,
                vol_len = v.len
            FROM (VALUES %s) AS v(cat, grp, pos, len, id)
            WHERE s.id = v.id
            """,
            updates,
            page_size=UPDATE_BATCH_SIZE,
        )
        conn.commit()

    stream.close()
    cur.close()
    print(f"Step 4 done: vol_* computed for {total_groups} groups.")


def main():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )

    conn.autocommit = False

    # 1) vel_len
    update_vel_len(conn)

    # 2) kal_len and mom_len
    update_generic_len(conn, col_grp="kal_grp", col_len="kal_len", label="kal")
    update_generic_len(conn, col_grp="mom_grp", col_len="mom_len", label="mom")

    # 3) vol thresholds
    q1, q2 = compute_vol_thresholds(conn)

    # 4) vol segments
    update_vol_segments(conn, q1, q2)

    conn.close()
    print("Phase 2 enrichment completed.")


if __name__ == "__main__":
    main()
