#!/usr/bin/env python3
"""
Party Switching Detection
=========================
Captures a snapshot of each voter's current party registration and detects
changes from previous snapshots.

Table: voter_party_snapshot
  StateVoterId  VARCHAR(20)
  snapshot_date DATE
  party         VARCHAR(30)
  PRIMARY KEY (StateVoterId, snapshot_date)

Columns added to voter_file:
  prior_party       VARCHAR(5)    -- previous party before most recent switch
  party_change_date DATE          -- date the switch was detected
  is_party_switcher TINYINT(1)    -- 1 if voter has ever switched

Designed for periodic runs. Each run:
  1. Inserts/updates today's snapshot from voter_file.OfficialParty
  2. Compares to the previous snapshot date
  3. Flags voters whose party changed between snapshots

Called by: python main.py party-snapshot
"""

import os, sys, time, argparse
import pymysql
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.db import get_conn

VOTER_TABLE = "voter_file"

SNAPSHOT_TABLE = "voter_party_snapshot"

SWITCHER_COLUMNS = [
    ("prior_party",       "VARCHAR(30) DEFAULT NULL"),
    ("party_change_date", "DATE        DEFAULT NULL"),
    ("is_party_switcher", "TINYINT(1)  DEFAULT 0"),
]

CREATE_SNAPSHOT = f"""
CREATE TABLE IF NOT EXISTS {SNAPSHOT_TABLE} (
    StateVoterId  VARCHAR(20)  NOT NULL,
    snapshot_date DATE         NOT NULL,
    party         VARCHAR(30)   DEFAULT NULL,
    PRIMARY KEY (StateVoterId, snapshot_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""


def connect():
    return get_conn(database="nys_voter_tagging", autocommit=True)


def ensure_columns(cur, columns):
    added = 0
    for col_name, col_def in columns:
        cur.execute("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME   = %s
              AND COLUMN_NAME  = %s
        """, (VOTER_TABLE, col_name))
        if cur.fetchone()[0] == 0:
            cur.execute(f"ALTER TABLE {VOTER_TABLE} ADD COLUMN {col_name} {col_def}")
            added += 1
    return added


def main():
    parser = argparse.ArgumentParser(description="Party switching snapshot")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--debug",   "-d", action="store_true")
    parser.add_argument("--quiet",   "-q", action="store_true")
    parser.parse_args()

    print("=" * 60)
    print("  NYS Voter Tagging - Party Snapshot & Switching Detection")
    print("=" * 60)

    conn = connect()
    cur  = conn.cursor()

    # Step 1: Create snapshot table + voter_file columns
    print("\n[1] Ensuring snapshot table and columns...")
    cur.execute(CREATE_SNAPSHOT)
    # Widen party column if table pre-existed with VARCHAR(5)
    cur.execute("ALTER TABLE voter_party_snapshot MODIFY COLUMN party VARCHAR(30) DEFAULT NULL")
    cur.execute("ALTER TABLE voter_file MODIFY COLUMN prior_party VARCHAR(30) DEFAULT NULL")
    added = ensure_columns(cur, SWITCHER_COLUMNS)
    if added:
        print(f"  Added {added} column(s) to voter_file")

    # Step 2: Check existing snapshots
    cur.execute(f"SELECT DISTINCT snapshot_date FROM {SNAPSHOT_TABLE} ORDER BY snapshot_date DESC LIMIT 5")
    existing_dates = [r[0] for r in cur.fetchall()]
    if existing_dates:
        print(f"  Existing snapshots: {', '.join(str(d) for d in existing_dates)}")
    else:
        print("  No previous snapshots found (first run)")

    # Step 3: Insert today's snapshot
    print("\n[2] Capturing today's party snapshot...")
    t = time.time()
    cur.execute(f"""
        INSERT INTO {SNAPSHOT_TABLE} (StateVoterId, snapshot_date, party)
        SELECT StateVoterId, CURDATE(), OfficialParty
        FROM {VOTER_TABLE}
        WHERE StateVoterId IS NOT NULL
        ON DUPLICATE KEY UPDATE party = VALUES(party)
    """)
    elapsed = time.time() - t
    print(f"  -> {cur.rowcount:,} rows inserted/updated ({elapsed:.1f}s)")

    # Step 4: Detect party changes vs previous snapshot
    if not existing_dates:
        print("\n[3] First snapshot - no comparison available yet.")
        print("  Run again after the next voter file load to detect switches.")
    else:
        # Find the most recent snapshot date that isn't today
        cur.execute(f"""
            SELECT MAX(snapshot_date) FROM {SNAPSHOT_TABLE}
            WHERE snapshot_date < CURDATE()
        """)
        prev_date = cur.fetchone()[0]

        if prev_date is None:
            print("\n[3] No previous snapshot to compare (only today's exists).")
        else:
            print(f"\n[3] Detecting party switches (comparing to {prev_date})...")
            t = time.time()

            # Find voters whose party changed between prev_date and today
            cur.execute(f"""
                UPDATE {VOTER_TABLE} v
                JOIN {SNAPSHOT_TABLE} prev ON v.StateVoterId = prev.StateVoterId
                                          AND prev.snapshot_date = %s
                JOIN {SNAPSHOT_TABLE} curr ON v.StateVoterId = curr.StateVoterId
                                          AND curr.snapshot_date = CURDATE()
                SET v.prior_party       = prev.party,
                    v.party_change_date = CURDATE(),
                    v.is_party_switcher = 1
                WHERE prev.party != curr.party
                  AND prev.party IS NOT NULL
                  AND curr.party IS NOT NULL
            """, (prev_date,))
            elapsed = time.time() - t
            print(f"  -> {cur.rowcount:,} party switches detected ({elapsed:.1f}s)")

    # Summary
    print("\n" + "=" * 60)
    print("  Party Snapshot Summary")
    print("=" * 60)

    cur.execute(f"SELECT COUNT(DISTINCT snapshot_date) FROM {SNAPSHOT_TABLE}")
    snap_count = cur.fetchone()[0]
    print(f"\n  Total snapshots:         {int(snap_count):>10,}")

    cur.execute(f"SELECT COUNT(*) FROM {VOTER_TABLE} WHERE is_party_switcher = 1")
    switchers = cur.fetchone()[0]
    print(f"  Total party switchers:   {int(switchers):>10,}")

    if switchers:
        cur.execute(f"""
            SELECT prior_party, OfficialParty, COUNT(*) AS cnt
            FROM {VOTER_TABLE}
            WHERE is_party_switcher = 1
            GROUP BY prior_party, OfficialParty
            ORDER BY cnt DESC
            LIMIT 10
        """)
        rows = cur.fetchall()
        print(f"\n  Top Party Switches:")
        print(f"  {'From':<8} {'To':<8} {'Count':>10}")
        print(f"  {'-'*28}")
        for from_p, to_p, cnt in rows:
            print(f"  {(from_p or '?'):<8} {(to_p or '?'):<8} {int(cnt):>10,}")

    print()
    cur.close()
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
