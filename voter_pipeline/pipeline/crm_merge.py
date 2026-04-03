#!/usr/bin/env python3
"""
Shared CRM contact merge logic.

Every data-source loader (HubSpot, Campaign Monitor, CSV imports, etc.)
converts its records into a standardised dict, then calls
``upsert_contacts()`` from this module.

Standard contact dict
---------------------
{
    "email":      "primary@example.com",        # REQUIRED - dedup key
    "emails":     ["primary@example.com", ...],  # all known emails (incl. primary)
    "first_name": "John",
    "last_name":  "Doe",
    "mobile":     "5551112222",                  # dedicated mobile/cell
    "phones":     ["5551234567", "5559876543"],
    "address":    "123 Main St",
    "city":       "Albany",
    "state":      "NY",
    "zip":        "12207",
    "company":    "Acme Inc",
}

Merge rules
-----------
- ``email_1`` is the UNIQUE dedup anchor (lowercased, stripped)
- **Emails** — append new values to next empty slot (up to 5); primary stays
  in slot 1 (``_fill_slots``)
- **Phones** — most-recently-synced number takes ``phone_1``; older numbers
  cascade to later slots (``_prepend_slots``)
- **Mobile** — most-recent non-null value wins (overwrite)
- **Name / company** — fill blanks only (COALESCE behaviour)
- **Address** — most-recent record always wins (overwrite)
- **Sources** — comma-separated list of every source that contributed data

Performance
-----------
``upsert_contacts`` operates in batches of ``UPSERT_BATCH`` contacts:
  - ONE ``SELECT … IN (…)`` per batch instead of N individual SELECTs
  - ONE ``executemany INSERT IGNORE`` for all new contacts in the batch
  - ONE ``executemany UPDATE`` for all existing contacts in the batch
  = ~3 queries per UPSERT_BATCH contacts  (was 2×N)

``tag_cm_membership`` operates in batches of ``TAG_BATCH`` emails:
  - ONE ``SELECT … IN (…)`` per batch
  - ONE ``executemany UPDATE`` per batch
  = ~2 queries per TAG_BATCH emails  (was 2×N)
"""

import re

# ---------------------------------------------------------------------------
# Batch sizes — tunable
# ---------------------------------------------------------------------------
UPSERT_BATCH = 500   # contacts per batch in upsert_contacts
TAG_BATCH    = 1000  # emails per batch in tag_cm_membership

# ---------------------------------------------------------------------------
# Name cleaning (matches voter_file JOIN pattern)
# ---------------------------------------------------------------------------
_NON_ALPHA = re.compile(r"[^A-Z]")


def clean_name(s):
    """UPPER + strip non-alpha — matches SQL REGEXP_REPLACE(UPPER(...), '[^A-Z]', '')."""
    if not s:
        return None
    cleaned = _NON_ALPHA.sub("", s.upper())
    return cleaned if cleaned else None


# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------
_NON_DIGIT = re.compile(r"\D")


def _normalize_phone(p):
    """Strip to digits, return None if fewer than 7 digits."""
    if not p:
        return None
    digits = _NON_DIGIT.sub("", str(p))
    return digits if len(digits) >= 7 else None


def _normalize_email(e):
    """Lowercase, strip, require @ and dot."""
    if not e:
        return None
    e = str(e).strip().lower()
    if "@" in e and "." in e:
        return e
    return None


# ---------------------------------------------------------------------------
# Contacts DDL — unified, source-agnostic schema
# ---------------------------------------------------------------------------
CONTACTS_DDL = """
CREATE TABLE IF NOT EXISTS contacts (
    id           INT AUTO_INCREMENT PRIMARY KEY,
    email_1      VARCHAR(255) NOT NULL,
    email_2      VARCHAR(255) DEFAULT NULL,
    email_3      VARCHAR(255) DEFAULT NULL,
    email_4      VARCHAR(255) DEFAULT NULL,
    email_5      VARCHAR(255) DEFAULT NULL,
    first_name   VARCHAR(100) DEFAULT NULL,
    last_name    VARCHAR(100) DEFAULT NULL,
    mobile       VARCHAR(50)  DEFAULT NULL,
    phone_1      VARCHAR(50)  DEFAULT NULL,
    phone_2      VARCHAR(50)  DEFAULT NULL,
    phone_3      VARCHAR(50)  DEFAULT NULL,
    phone_4      VARCHAR(50)  DEFAULT NULL,
    phone_5      VARCHAR(50)  DEFAULT NULL,
    address      VARCHAR(255) DEFAULT NULL,
    city         VARCHAR(100) DEFAULT NULL,
    state        VARCHAR(50)  DEFAULT NULL,
    zip          VARCHAR(20)  DEFAULT NULL,
    zip5         VARCHAR(5)   DEFAULT NULL,
    company      VARCHAR(255) DEFAULT NULL,
    sources      VARCHAR(500)  DEFAULT NULL,
    cm_lists     VARCHAR(1000) DEFAULT NULL,
    cm_segments  VARCHAR(1000) DEFAULT NULL,
    clean_first  VARCHAR(100)  DEFAULT NULL,
    clean_last   VARCHAR(100) DEFAULT NULL,
    created_at   DATETIME     DEFAULT CURRENT_TIMESTAMP,
    updated_at   DATETIME     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE KEY  uq_email1     (email_1),
    INDEX       idx_name_zip  (clean_last(50), clean_first(50), zip5),
    INDEX       idx_email2    (email_2),
    INDEX       idx_mobile    (mobile),
    INDEX       idx_phone1    (phone_1),
    INDEX       idx_phone2    (phone_2),
    INDEX       idx_state     (state(10))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
"""


# ---------------------------------------------------------------------------
# Slot-filling helpers
# ---------------------------------------------------------------------------
def _fill_slots(existing_vals, new_vals, max_slots=5):
    """Merge *new_vals* into *existing_vals* (list of up to *max_slots*).

    Returns a new list of length *max_slots*.  Existing non-None values are
    preserved in their original position.  New values that are not already
    present are appended to the first empty slot.

    Used for **emails** where the original primary should stay in slot 1.
    """
    slots = list(existing_vals) + [None] * max_slots
    slots = slots[:max_slots]

    present = {v.lower() for v in slots if v}
    for val in new_vals:
        if val and val.lower() not in present:
            for i in range(max_slots):
                if slots[i] is None:
                    slots[i] = val
                    present.add(val.lower())
                    break
    return slots


def _prepend_slots(existing_vals, new_vals, max_slots=5):
    """Put *new_vals* first, then existing non-duplicate values.

    Returns a new list of length *max_slots*.  Most-recent-first ordering:
    new values take slot 1, 2, …; existing values that aren't duplicates
    shift to later slots.  Capped at *max_slots*.

    Used for **phones** where the most-recently-synced number should be
    ``phone_1`` and older numbers cascade to ``phone_2``, ``phone_3``, etc.
    """
    result = []
    seen = set()

    for val in new_vals:
        if val and val.lower() not in seen and len(result) < max_slots:
            result.append(val)
            seen.add(val.lower())

    for val in existing_vals:
        if val and val.lower() not in seen and len(result) < max_slots:
            result.append(val)
            seen.add(val.lower())

    while len(result) < max_slots:
        result.append(None)

    return result


def _append_source(existing_sources, new_tag):
    """Add *new_tag* to the comma-separated *existing_sources* if not present."""
    if not existing_sources:
        return new_tag
    tags = [t.strip() for t in existing_sources.split(",")]
    if new_tag not in tags:
        tags.append(new_tag)
    return ",".join(tags)


def _append_tag(existing, new_tag, sep="|"):
    """Add *new_tag* to *existing* (separated by *sep*) if not already present."""
    if not existing:
        return new_tag
    tags = [t.strip() for t in existing.split(sep)]
    if new_tag not in tags:
        tags.append(new_tag)
    return sep.join(tags)


# ---------------------------------------------------------------------------
# Bulk membership tagging  (BATCH-OPTIMISED)
# ---------------------------------------------------------------------------
def tag_cm_membership(cur, emails, column, tag_value):
    """Append *tag_value* to *column* for all contacts matching *emails*.

    Processes in batches of TAG_BATCH:
      - ONE ``SELECT … IN (…)`` per batch
      - ONE ``executemany UPDATE`` per batch
    = O(N / TAG_BATCH) queries  (was O(N))

    Parameters
    ----------
    cur : pymysql cursor
    emails : list[str]
    column : ``"cm_lists"`` or ``"cm_segments"``
    tag_value : str — the list or segment name to append

    Returns
    -------
    int : number of rows updated
    """
    if column not in ("cm_lists", "cm_segments"):
        raise ValueError(f"Invalid column: {column}")

    norm = [e.strip().lower() for e in emails if e]
    updated = 0

    for i in range(0, len(norm), TAG_BATCH):
        batch = norm[i : i + TAG_BATCH]
        if not batch:
            continue

        ph = ",".join(["%s"] * len(batch))
        cur.execute(
            f"SELECT id, {column} FROM contacts WHERE email_1 IN ({ph})",
            batch,
        )
        rows = cur.fetchall()

        updates = []
        for row_id, existing in rows:
            new_val = _append_tag(existing, tag_value)
            if new_val != existing:
                updates.append((new_val, row_id))

        if updates:
            cur.executemany(
                f"UPDATE contacts SET {column} = %s WHERE id = %s",
                updates,
            )
            updated += len(updates)

    return updated


# ---------------------------------------------------------------------------
# Internal: normalise one contact dict into typed fields
# ---------------------------------------------------------------------------
def _prepare(c):
    """Return a normalised tuple of fields from a standard contact dict."""
    primary_email = _normalize_email(c.get("email"))
    if not primary_email:
        return None

    seen_e = {primary_email}
    all_emails = [primary_email]
    for e in c.get("emails", []):
        ne = _normalize_email(e)
        if ne and ne not in seen_e:
            all_emails.append(ne)
            seen_e.add(ne)

    mobile = _normalize_phone(c.get("mobile"))
    all_phones = []
    for p in c.get("phones", []):
        np = _normalize_phone(p)
        if np and np != mobile:
            all_phones.append(np)

    first_name = (c.get("first_name") or "").strip()[:100] or None
    last_name  = (c.get("last_name")  or "").strip()[:100] or None
    address    = (c.get("address")    or "").strip()[:255] or None
    city       = (c.get("city")       or "").strip()[:100] or None
    state      = (c.get("state")      or "").strip()[:50]  or None
    zipval     = (c.get("zip")        or "").strip()[:20]  or None
    zip5       = zipval[:5] if zipval else None
    company    = (c.get("company")    or "").strip()[:255] or None
    cm_lists   = c.get("cm_lists") or None

    return (primary_email, all_emails, mobile, all_phones,
            first_name, last_name, address, city, state, zipval, zip5,
            company, cm_lists)


# ---------------------------------------------------------------------------
# Core upsert  (BATCH-OPTIMISED)
# ---------------------------------------------------------------------------
def upsert_contacts(cur, contacts, source_tag):
    """Merge a batch of standardised contact dicts into the unified table.

    Processes in batches of UPSERT_BATCH:
      - ONE ``SELECT … IN (…)`` per batch to fetch all existing records
      - ONE ``executemany INSERT IGNORE`` for new contacts
      - ONE ``executemany UPDATE`` for existing contacts
    = ~3 queries per UPSERT_BATCH contacts  (was 2×N)

    Parameters
    ----------
    cur : pymysql cursor
    contacts : list[dict]  — standardised contact dicts (see module docstring)
    source_tag : str       — e.g. ``"hs_jh"`` or ``"cm_politika"``

    Returns
    -------
    (inserted, updated) : tuple[int, int]
    """
    total_inserted = 0
    total_updated  = 0

    for batch_start in range(0, len(contacts), UPSERT_BATCH):
        batch = contacts[batch_start : batch_start + UPSERT_BATCH]
        ins, upd = _upsert_batch(cur, batch, source_tag)
        total_inserted += ins
        total_updated  += upd

    return total_inserted, total_updated


def _upsert_batch(cur, contacts, source_tag):
    """Process one UPSERT_BATCH of contacts against the DB."""

    # 1. Normalise + deduplicate within this batch
    #    (same email can appear in multiple HubSpot lists in the same page)
    by_email = {}
    for c in contacts:
        prep = _prepare(c)
        if prep is None:
            continue
        email = prep[0]
        if email not in by_email:
            by_email[email] = prep
        else:
            # Merge duplicate within batch: extend email/phone lists,
            # keep first non-null name, take most-recent address
            ex = by_email[email]
            merged_emails = list(dict.fromkeys(ex[1] + prep[1]))  # preserve order, dedup
            merged_phones = list(dict.fromkeys(ex[3] + prep[3]))
            by_email[email] = (
                email,
                merged_emails,
                ex[2] or prep[2],      # mobile: first wins
                merged_phones,
                ex[4] or prep[4],      # first_name: first wins
                ex[5] or prep[5],      # last_name:  first wins
                prep[6] or ex[6],      # address:    most-recent wins
                prep[7] or ex[7],
                prep[8] or ex[8],
                prep[9] or ex[9],
                prep[10] or ex[10],
                ex[11] or prep[11],    # company:    first wins
                ex[12] or prep[12],    # cm_lists:   first wins
            )

    if not by_email:
        return 0, 0

    email_list = list(by_email.keys())

    # 2. Fetch all existing records in ONE query
    ph = ",".join(["%s"] * len(email_list))
    cur.execute(
        f"SELECT id, email_1, email_2, email_3, email_4, email_5,"
        f"       first_name, last_name, mobile,"
        f"       phone_1, phone_2, phone_3, phone_4, phone_5,"
        f"       address, city, state, zip, zip5,"
        f"       company, sources, cm_lists, cm_segments"
        f" FROM contacts WHERE email_1 IN ({ph})",
        email_list,
    )
    existing_map = {row[1]: row for row in cur.fetchall()}

    # 3. Build INSERT rows (new) and UPDATE rows (existing)
    insert_rows = []
    update_rows = []

    for email, prep in by_email.items():
        (_, all_emails, mobile, all_phones,
         first_name, last_name, address, city, state, zipval, zip5,
         company, cm_lists_val) = prep

        if email not in existing_map:
            # ── NEW contact ──────────────────────────────────────────────
            email_slots = (all_emails + [None] * 5)[:5]
            phone_slots = (all_phones + [None] * 5)[:5]
            insert_rows.append((
                email_slots[0], email_slots[1], email_slots[2],
                email_slots[3], email_slots[4],
                first_name, last_name, mobile,
                phone_slots[0], phone_slots[1], phone_slots[2],
                phone_slots[3], phone_slots[4],
                address, city, state, zipval, zip5,
                company, source_tag, cm_lists_val,
                clean_name(first_name), clean_name(last_name),
            ))

        else:
            # ── EXISTING contact — apply merge rules ─────────────────────
            row = existing_map[email]
            row_id    = row[0]
            ex_emails = list(row[1:6])
            ex_fn     = row[6];   ex_ln    = row[7];  ex_mobile = row[8]
            ex_phones = list(row[9:14])
            ex_addr   = row[14];  ex_city  = row[15]; ex_state = row[16]
            ex_zip    = row[17];  ex_zip5  = row[18]; ex_comp  = row[19]
            ex_src    = row[20];  ex_cm_l  = row[21]; ex_cm_s  = row[22]

            new_emails = _fill_slots(ex_emails, all_emails, 5)
            new_phones = _prepend_slots(ex_phones, all_phones, 5)
            new_fn     = ex_fn    or first_name
            new_ln     = ex_ln    or last_name
            new_mobile = mobile   or ex_mobile
            new_comp   = ex_comp  or company
            new_addr   = address  or ex_addr
            new_city   = city     or ex_city
            new_state  = state    or ex_state
            new_zip    = zipval   or ex_zip
            new_zip5   = zip5     or ex_zip5
            new_src    = _append_source(ex_src, source_tag)
            new_cm_l   = _append_tag(ex_cm_l, cm_lists_val) if cm_lists_val else ex_cm_l

            update_rows.append((
                new_emails[1], new_emails[2], new_emails[3], new_emails[4],
                new_fn, new_ln, new_mobile,
                new_phones[0], new_phones[1], new_phones[2],
                new_phones[3], new_phones[4],
                new_addr, new_city, new_state, new_zip, new_zip5,
                new_comp, new_src, new_cm_l, ex_cm_s,
                clean_name(new_fn), clean_name(new_ln),
                row_id,
            ))

    # 4. Batch INSERT new contacts
    inserted = 0
    if insert_rows:
        cur.executemany("""
            INSERT IGNORE INTO contacts (
                email_1, email_2, email_3, email_4, email_5,
                first_name, last_name, mobile,
                phone_1, phone_2, phone_3, phone_4, phone_5,
                address, city, state, zip, zip5,
                company, sources, cm_lists,
                clean_first, clean_last
            ) VALUES (
                %s,%s,%s,%s,%s,
                %s,%s,%s,
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,
                %s,%s,%s,
                %s,%s
            )
        """, insert_rows)
        inserted = cur.rowcount

    # 5. Batch UPDATE existing contacts
    updated = 0
    if update_rows:
        cur.executemany("""
            UPDATE contacts SET
                email_2=%s, email_3=%s, email_4=%s, email_5=%s,
                first_name=%s, last_name=%s, mobile=%s,
                phone_1=%s, phone_2=%s, phone_3=%s, phone_4=%s, phone_5=%s,
                address=%s, city=%s, state=%s, zip=%s, zip5=%s,
                company=%s, sources=%s, cm_lists=%s, cm_segments=%s,
                clean_first=%s, clean_last=%s
            WHERE id=%s
        """, update_rows)
        updated = len(update_rows)

    return inserted, updated


# ---------------------------------------------------------------------------
# Voter ↔ Contact linkage
# ---------------------------------------------------------------------------
def enrich_voter_crm(conn):
    """Pre-compute crm_email, crm_phone, crm_mobile on voter_file from CRM contacts.

    Direction: iterate voter_file rows, probe contacts idx_name_zip
    (indexed → O(log N) per probe).  Result is cached in the
    ``crm_email``, ``crm_phone``, ``crm_mobile`` columns so subsequent runs
    skip already-matched rows.
    """
    with conn.cursor() as cur:
        try:
            cur.execute("SELECT COUNT(*) FROM crm_unified.contacts")
            cnt = cur.fetchone()[0]
        except Exception:
            print("  CRM enrich: skipped (no crm_unified.contacts)")
            return
        if cnt == 0:
            print("  CRM enrich: skipped (contacts table empty)")
            return

        cur.execute("""
            SELECT COLUMN_NAME FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = 'nys_voter_tagging'
              AND TABLE_NAME = 'voter_file'
              AND COLUMN_NAME IN ('crm_email', 'crm_phone', 'crm_mobile')
        """)
        existing = {r[0] for r in cur.fetchall()}
        for col, sqltype in [("crm_email",  "VARCHAR(255)"),
                              ("crm_phone",  "VARCHAR(50)"),
                              ("crm_mobile", "VARCHAR(50)")]:
            if col not in existing:
                cur.execute(
                    f"ALTER TABLE nys_voter_tagging.voter_file "
                    f"ADD COLUMN {col} {sqltype} DEFAULT NULL"
                )

        cur.execute("""
            UPDATE nys_voter_tagging.voter_file v
            JOIN crm_unified.contacts c
              ON c.clean_last  = v.clean_last
             AND c.clean_first = v.clean_first
             AND c.zip5        = SUBSTRING(v.PrimaryZip, 1, 5)
            SET v.crm_email  = c.email_1,
                v.crm_phone  = c.phone_1,
                v.crm_mobile = c.mobile
            WHERE v.crm_email IS NULL
              AND v.clean_last IS NOT NULL
        """)
        new_matched = cur.rowcount
        conn.commit()

        cur.execute("SELECT COUNT(*) FROM nys_voter_tagging.voter_file WHERE crm_email IS NOT NULL")
        total = cur.fetchone()[0]
        if new_matched:
            print(f"  CRM enrich: {new_matched:,} new matches ({total:,} total voters with CRM data)")
        else:
            print(f"  CRM enrich: 0 new matches ({total:,} voters already have CRM data)")


# Backward-compat alias
enrich_voter_email = enrich_voter_crm
