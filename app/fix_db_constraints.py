#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

DSN = os.getenv("DB_DSN")


def exists_constraint(cur, conname: str) -> bool:
    cur.execute("SELECT 1 FROM pg_constraint WHERE conname = %s", (conname,))
    return cur.fetchone() is not None


def count_duplicates(cur, table: str, cols: list[str]) -> int:
    cols_sql = ", ".join(cols)
    cur.execute(
        f"""
        SELECT COALESCE(sum(cnt) ,0) FROM (
            SELECT COUNT(*) - 1 AS cnt
            FROM {table}
            GROUP BY {cols_sql}
            HAVING COUNT(*) > 1
        ) t
        """
    )
    row = cur.fetchone()
    return int(row[0] or 0)


def dedupe_by_ctid(cur, table: str, cols: list[str]) -> int:
    cols_sql = " AND ".join([f"a.{c} = b.{c}" for c in cols])
    cur.execute(
        f"""
        WITH d AS (
            DELETE FROM {table} a
            USING {table} b
            WHERE a.ctid < b.ctid AND {cols_sql}
            RETURNING 1
        )
        SELECT COUNT(*) FROM d
        """
    )
    return int(cur.fetchone()[0])


def set_not_null(cur, table: str, col: str):
    cur.execute(
        f"""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = '{table}' AND column_name = '{col}' AND is_nullable = 'YES'
            ) THEN
                EXECUTE 'ALTER TABLE {table} ALTER COLUMN {col} SET NOT NULL';
            END IF;
        END$$;
        """
    )


def main():
    conn = psycopg2.connect(DSN)
    cur = conn.cursor()

    # 1) fixtures: PK(id)
    print("[fixtures] verificando duplicatas por id...")
    dups = count_duplicates(cur, "fixtures", ["id"])
    if dups > 0:
        removed = dedupe_by_ctid(cur, "fixtures", ["id"])
        print(f"   removidos {removed} duplicados")
    set_not_null(cur, "fixtures", "id")
    if not exists_constraint(cur, "fixtures_pkey"):
        print("[fixtures] adicionando PRIMARY KEY (id)...")
        cur.execute("ALTER TABLE fixtures ADD CONSTRAINT fixtures_pkey PRIMARY KEY (id)")

    # 2) events: PK(id)
    print("[events] verificando duplicatas por id...")
    dups = count_duplicates(cur, "events", ["id"])
    if dups > 0:
        removed = dedupe_by_ctid(cur, "events", ["id"])
        print(f"   removidos {removed} duplicados")
    set_not_null(cur, "events", "id")
    if not exists_constraint(cur, "events_pkey"):
        print("[events] adicionando PRIMARY KEY (id)...")
        cur.execute("ALTER TABLE events ADD CONSTRAINT events_pkey PRIMARY KEY (id)")

    # 3) fixture_statistics: UNIQUE (fixture_id, type_id, participant_id)
    print("[fixture_statistics] verificando duplicatas por (fixture_id, type_id, participant_id)...")
    dups = count_duplicates(cur, "fixture_statistics", ["fixture_id", "type_id", "participant_id"])
    if dups > 0:
        removed = dedupe_by_ctid(cur, "fixture_statistics", ["fixture_id", "type_id", "participant_id"])
        print(f"   removidos {removed} duplicados")
    if not exists_constraint(cur, "fixture_statistics_unique"):
        print("[fixture_statistics] adicionando UNIQUE (fixture_id, type_id, participant_id)...")
        cur.execute(
            "ALTER TABLE fixture_statistics ADD CONSTRAINT fixture_statistics_unique UNIQUE (fixture_id, type_id, participant_id)"
        )

    # 4) fixture_participants: UNIQUE (fixture_id, team_id)
    print("[fixture_participants] verificando duplicatas por (fixture_id, team_id)...")
    dups = count_duplicates(cur, "fixture_participants", ["fixture_id", "team_id"])
    if dups > 0:
        removed = dedupe_by_ctid(cur, "fixture_participants", ["fixture_id", "team_id"])
        print(f"   removidos {removed} duplicados")
    if not exists_constraint(cur, "fixture_participants_unique"):
        print("[fixture_participants] adicionando UNIQUE (fixture_id, team_id)...")
        cur.execute(
            "ALTER TABLE fixture_participants ADD CONSTRAINT fixture_participants_unique UNIQUE (fixture_id, team_id)"
        )

    conn.commit()
    conn.close()
    print("✅ Constraints aplicadas com sucesso")


if __name__ == "__main__":
    main()
