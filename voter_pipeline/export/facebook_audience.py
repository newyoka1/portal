#!/usr/bin/env python3
"""
export/facebook_audience.py
============================
Export a named voter audience to a Facebook Custom Audience via the
Meta Marketing API (no third-party SDK required — uses requests).

PII normalization and SHA-256 hashing follows Facebook's official spec:
  https://developers.facebook.com/docs/marketing-api/audiences/guides/custom-audiences

Requires in .env:
    FB_ACCESS_TOKEN=<long-lived System User token with ads_management permission>
    FB_AD_ACCOUNT_ID=<numeric Ad Account ID — without the "act_" prefix>

Usage:
    python export/facebook_audience.py --list-audiences
    python export/facebook_audience.py --audience NYS_HARD_DEM
    python export/facebook_audience.py --audience NYS_SWING --ld 63
    python export/facebook_audience.py --audience NYS_HARD_GOP --dry-run
    python export/facebook_audience.py --audience NYS_HARD_GOP --fb-audience-id 1234567890
    python export/facebook_audience.py --audience NYS_HARD_GOP --fb-audience-id 1234567890 --replace
"""

import argparse
import hashlib
import math
import os
import random
import re
import sys
import time
from datetime import date, datetime

import requests
from dotenv import load_dotenv

# ── Path setup ────────────────────────────────────────────────────────────────
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)
load_dotenv(os.path.join(BASE, '.env'))

from utils.db import get_conn

# ── Facebook Marketing API constants ─────────────────────────────────────────
FB_API_VERSION = 'v19.0'
FB_BASE        = f'https://graph.facebook.com/{FB_API_VERSION}'
BATCH_SIZE     = 10_000   # Facebook maximum rows per request
MAX_RETRIES    = 3
RETRY_DELAY    = 5        # seconds between retries

# Multi-key schema sent to Facebook.
# PHONE may appear twice: slot 0 = Mobile, slot 1 = Landline.
# Facebook uses any combination of these fields to match against its user graph.
SCHEMA = [
    'EXTERN_ID',  # StateVoterId — plain (not hashed); enables lookalike seeding
    'EMAIL',      # crm_email (CRM-enriched)
    'PHONE',      # Mobile
    'PHONE',      # Landline (second phone slot)
    'FN',         # FirstName
    'LN',         # LastName
    'ZIP',        # PrimaryZip (5-digit)
    'ST',         # PrimaryState (2-letter, lowercase)
    'CT',         # PrimaryCity (alpha-only, lowercase)
    'DOBY',       # Year of birth
    'DOBM',       # Month of birth (zero-padded)
    'DOBD',       # Day of birth (zero-padded)
    'GEN',        # Gender (m / f)
    'COUNTRY',    # Always 'us'
]


# ── PII normalization + SHA-256 hashing ──────────────────────────────────────
# Facebook spec: normalize first, then SHA-256 hex-digest.
# Missing / invalid fields must be sent as '' (empty string, not hashed).

def _sha256(s: str) -> str:
    return hashlib.sha256(s.encode('utf-8')).hexdigest()


def _norm_email(v) -> str:
    if not v:
        return ''
    v = str(v).strip().lower()
    return _sha256(v) if ('@' in v and '.' in v) else ''


def _norm_phone(v) -> str:
    """Normalize to US E.164 (11 digits, leading 1), then hash."""
    if not v:
        return ''
    digits = re.sub(r'\D', '', str(v))
    if len(digits) == 10:
        digits = '1' + digits
    if len(digits) == 11 and digits[0] == '1':
        return _sha256(digits)
    return ''


def _norm_name(v) -> str:
    """Lowercase alpha-only name hash (removes spaces, punctuation, suffixes)."""
    if not v:
        return ''
    cleaned = re.sub(r'[^a-z]', '', str(v).strip().lower())
    return _sha256(cleaned) if cleaned else ''


def _norm_zip(v) -> str:
    if not v:
        return ''
    z = re.sub(r'\D', '', str(v))[:5]
    return _sha256(z) if len(z) == 5 else ''


def _norm_state(v) -> str:
    if not v:
        return ''
    cleaned = re.sub(r'[^a-z]', '', str(v).strip().lower())[:2]
    return _sha256(cleaned) if len(cleaned) == 2 else ''


def _norm_city(v) -> str:
    if not v:
        return ''
    cleaned = re.sub(r'[^a-z]', '', str(v).strip().lower())
    return _sha256(cleaned) if cleaned else ''


def _norm_dob_year(v) -> str:
    if not v:
        return ''
    if isinstance(v, date):
        return _sha256(str(v.year))
    return ''


def _norm_dob_month(v) -> str:
    if not v:
        return ''
    if isinstance(v, date):
        return _sha256(f'{v.month:02d}')
    return ''


def _norm_dob_day(v) -> str:
    if not v:
        return ''
    if isinstance(v, date):
        return _sha256(f'{v.day:02d}')
    return ''


def _norm_gender(v) -> str:
    if not v:
        return ''
    g = str(v).strip().upper()[:1]
    return _sha256(g.lower()) if g in ('M', 'F') else ''


# Pre-compute constant hash (country is always 'us')
_COUNTRY_HASH = _sha256('us')


def build_row(row: dict) -> list:
    """
    Convert a voter_file row dict into a list of hashed values matching SCHEMA order.
    Empty string '' is used for missing/invalid fields (Facebook ignores them).
    """
    return [
        str(row['StateVoterId']),             # EXTERN_ID — plain, not hashed
        _norm_email(row.get('crm_email')),
        _norm_phone(row.get('Mobile')),
        _norm_phone(row.get('Landline')),
        _norm_name(row.get('FirstName')),
        _norm_name(row.get('LastName')),
        _norm_zip(row.get('PrimaryZip')),
        _norm_state(row.get('PrimaryState')),
        _norm_city(row.get('PrimaryCity')),
        _norm_dob_year(row.get('DOB')),
        _norm_dob_month(row.get('DOB')),
        _norm_dob_day(row.get('DOB')),
        _norm_gender(row.get('Gender')),
        _COUNTRY_HASH,
    ]


# ── Facebook API client ───────────────────────────────────────────────────────

class FBClient:
    """Thin wrapper around Meta Marketing API using requests."""

    def __init__(self, access_token: str, ad_account_id: str):
        self.token      = access_token
        self.account_id = ad_account_id   # numeric only, without 'act_'
        self._session   = requests.Session()

    def _get(self, path: str, params: dict | None = None) -> dict:
        r = self._session.get(
            f'{FB_BASE}{path}',
            params={**(params or {}), 'access_token': self.token},
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        if 'error' in data:
            raise RuntimeError(f"Facebook API error: {data['error']}")
        return data

    def _post(self, path: str, payload: dict) -> dict:
        payload = {**payload, 'access_token': self.token}
        last_exc = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = self._session.post(f'{FB_BASE}{path}', json=payload, timeout=120)
                r.raise_for_status()
                data = r.json()
                if 'error' in data:
                    raise RuntimeError(f"Facebook API error: {data['error']}")
                return data
            except (requests.RequestException, RuntimeError) as exc:
                last_exc = exc
                if attempt < MAX_RETRIES:
                    wait = RETRY_DELAY * attempt
                    print(f'    [retry {attempt}/{MAX_RETRIES}] {exc} — waiting {wait}s...')
                    time.sleep(wait)
        raise last_exc  # type: ignore[misc]

    def list_audiences(self) -> list[dict]:
        data = self._get(
            f'/act_{self.account_id}/customaudiences',
            params={'fields': 'id,name,approximate_count_upper_bound,description,time_updated'},
        )
        return data.get('data', [])

    def create_audience(self, name: str, description: str = '') -> str:
        """Create a new Custom Audience and return its ID."""
        data = self._post(
            f'/act_{self.account_id}/customaudiences',
            {
                'name': name,
                'subtype': 'CUSTOM',
                'description': description,
                'customer_file_source': 'USER_PROVIDED_ONLY',
            },
        )
        return data['id']

    def add_users(self, audience_id: str, rows: list) -> dict:
        """Append a batch of hashed rows to a Custom Audience."""
        return self._post(
            f'/{audience_id}/users',
            {'payload': {'schema': SCHEMA, 'data': rows}},
        )

    def replace_users_batch(
        self,
        audience_id: str,
        rows: list,
        session_id: int,
        batch_seq: int,
        total_rows: int,
        is_last: bool,
    ) -> dict:
        """Send one batch as part of an atomic usersreplace session."""
        return self._post(
            f'/{audience_id}/usersreplace',
            {
                'session': {
                    'session_id':             session_id,
                    'batch_seq':              batch_seq,
                    'last_batch_in_session':  is_last,
                    'estimated_num_total':    total_rows,
                },
                'payload': {'schema': SCHEMA, 'data': rows},
            },
        )


# ── Database helpers ──────────────────────────────────────────────────────────

def list_audience_names(conn) -> list[tuple]:
    """Return [(audience_name, count), ...] from voter_audience_bridge."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT audience, COUNT(*) AS cnt
            FROM nys_voter_tagging.voter_audience_bridge
            GROUP BY audience
            ORDER BY audience
        """)
        return cur.fetchall()


def count_voters(conn, audience_name: str, district_filter: tuple | None) -> int:
    """Fast COUNT for the audience (used to track replace-session progress)."""
    where_parts = ['vab.audience = %s']
    params      = [audience_name]
    if district_filter:
        col, val = district_filter
        where_parts.append(f'vf.`{col}` = %s')
        params.append(val)
    sql = f"""
        SELECT COUNT(*)
        FROM nys_voter_tagging.voter_file vf
        INNER JOIN nys_voter_tagging.voter_audience_bridge vab
            ON vf.StateVoterId = vab.StateVoterId
        WHERE {' AND '.join(where_parts)}
    """
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()[0]


def stream_voters(conn, audience_name: str, district_filter: tuple | None):
    """
    Yield voter dicts one row at a time using SSCursor (server-side streaming).
    This avoids buffering millions of rows in Python memory.
    """
    import pymysql.cursors
    where_parts = ['vab.audience = %s']
    params      = [audience_name]
    if district_filter:
        col, val = district_filter
        where_parts.append(f'vf.`{col}` = %s')
        params.append(val)

    sql = f"""
        SELECT
            vf.StateVoterId,
            vf.crm_email,
            vf.Mobile,
            vf.Landline,
            vf.FirstName,
            vf.LastName,
            vf.PrimaryZip,
            vf.PrimaryState,
            vf.PrimaryCity,
            vf.DOB,
            vf.Gender
        FROM nys_voter_tagging.voter_file vf
        INNER JOIN nys_voter_tagging.voter_audience_bridge vab
            ON vf.StateVoterId = vab.StateVoterId
        WHERE {' AND '.join(where_parts)}
    """
    cursor  = conn.cursor(pymysql.cursors.SSCursor)
    cursor.execute(sql, params)
    columns = [d[0] for d in cursor.description]
    for row in cursor:
        yield dict(zip(columns, row))
    cursor.close()


# ── Argument parsing ──────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description='Export a voter audience to a Facebook Custom Audience',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python export/facebook_audience.py --list-audiences
  python export/facebook_audience.py --audience NYS_HARD_DEM
  python export/facebook_audience.py --audience NYS_SWING --ld 63
  python export/facebook_audience.py --audience NYS_HARD_GOP --dry-run
  python export/facebook_audience.py --audience NYS_HARD_GOP --fb-audience-id 1234567890
  python export/facebook_audience.py --audience NYS_HARD_GOP --fb-audience-id 1234567890 --replace
        """,
    )

    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        '--audience', metavar='NAME',
        help='Audience name from voter_audience_bridge (e.g. NYS_HARD_DEM)',
    )
    mode.add_argument(
        '--list-audiences', action='store_true',
        help='List all available audience names with voter counts',
    )

    dist = p.add_mutually_exclusive_group()
    dist.add_argument('--ld',     metavar='NUM',  help='Filter to Assembly/Legislative District')
    dist.add_argument('--sd',     metavar='NUM',  help='Filter to State Senate District')
    dist.add_argument('--cd',     metavar='NUM',  help='Filter to Congressional District')
    dist.add_argument('--county', metavar='NAME', help='Filter to county (e.g. Nassau)')

    p.add_argument(
        '--fb-audience-id', metavar='ID',
        help='Existing Facebook Custom Audience ID to update (default: create new)',
    )
    p.add_argument(
        '--replace', action='store_true',
        help='Atomically replace all audience members (requires --fb-audience-id). '
             'Default: append to existing audience.',
    )
    p.add_argument(
        '--audience-name', metavar='NAME', dest='fb_audience_name',
        help='Override the name for the new Facebook Custom Audience',
    )
    p.add_argument(
        '--dry-run', action='store_true',
        help='Query and hash records but do not upload anything to Facebook',
    )
    p.add_argument('--verbose', '-v', action='store_true', help='Show per-batch progress')
    return p.parse_args()


def get_district_filter(args) -> tuple | None:
    if args.ld:     return ('LDName',     args.ld)
    if args.sd:     return ('SDName',     args.sd)
    if args.cd:     return ('CDName',     args.cd)
    if args.county: return ('CountyName', args.county)
    return None


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()

    token      = os.getenv('FB_ACCESS_TOKEN', '').strip()
    account_id = os.getenv('FB_AD_ACCOUNT_ID', '').strip()

    if not args.list_audiences and not args.dry_run:
        if not token:
            sys.exit('ERROR: FB_ACCESS_TOKEN not set in .env\n'
                     '  See: https://developers.facebook.com/docs/marketing-api/overview/authentication')
        if not account_id:
            sys.exit('ERROR: FB_AD_ACCOUNT_ID not set in .env\n'
                     '  Find it at: https://business.facebook.com → Settings → Ad Accounts')

    if args.replace and not args.fb_audience_id:
        sys.exit('ERROR: --replace requires --fb-audience-id')

    conn = get_conn('nys_voter_tagging', autocommit=True)

    # ── List audiences ────────────────────────────────────────────────────────
    if args.list_audiences:
        rows = list_audience_names(conn)
        print(f'\n  {"Audience":<45}  {"Voters":>10}')
        print('  ' + '─' * 58)
        for name, cnt in rows:
            print(f'  {name:<45}  {cnt:>10,}')
        print(f'\n  {len(rows)} audience(s) total.\n')
        conn.close()
        return

    # ── Build labels ──────────────────────────────────────────────────────────
    district_filter = get_district_filter(args)
    district_label  = ''
    if district_filter:
        col, val = district_filter
        prefix = {'LDName': 'LD', 'SDName': 'SD', 'CDName': 'CD', 'CountyName': ''}.get(col, '')
        district_label = f' — {prefix}{val}'

    fb_name = (
        args.fb_audience_name
        or f'{args.audience}{district_label} [{datetime.now():%Y-%m-%d}]'
    )

    print(f'\nAudience   : {args.audience}{district_label}')
    if not args.dry_run:
        if args.fb_audience_id:
            mode_label = 'replace' if args.replace else 'append'
            print(f'FB audience: {args.fb_audience_id} ({mode_label})')
        else:
            print(f'FB name    : {fb_name} (new audience)')
    else:
        print('Mode       : DRY RUN — no data will be sent to Facebook')

    # ── Stream → hash → batch ─────────────────────────────────────────────────
    print('\nFetching and hashing voters...')
    t0          = time.time()
    total_rows  = 0
    batch_buf   : list = []
    fb          = FBClient(token, account_id) if not args.dry_run else None  # type: ignore[assignment]

    # For replace mode, collect total row count so the session protocol knows
    # how many records to expect in total.
    if args.replace:
        print('  Counting rows for replace session...')
        total_for_replace = count_voters(conn, args.audience, district_filter)
        num_batches_est   = math.ceil(total_for_replace / BATCH_SIZE)
        print(f'  {total_for_replace:,} voters → ~{num_batches_est} batches')
        replace_session_id = random.randint(10**9, 10**18)
        batch_seq          = 0
    else:
        total_for_replace  = 0
        replace_session_id = 0
        batch_seq          = 0

    audience_id: str | None = None

    # Create the FB audience before uploading (append / new-audience modes only)
    if not args.dry_run and not args.replace:
        if args.fb_audience_id:
            audience_id = args.fb_audience_id
        else:
            print('\nCreating Facebook Custom Audience...')
            description = (
                f'NYS Voter Pipeline | {args.audience}{district_label} | '
                f'{datetime.now():%Y-%m-%d}'
            )
            audience_id = fb.create_audience(fb_name, description)
            print(f'  Created audience ID : {audience_id}')
            print(f'  (Save this and use --fb-audience-id {audience_id} on future runs)\n')
        print(f'Uploading to audience {audience_id}...')

    def _flush_batch(buf: list, is_last: bool = False):
        """Upload one batch; handles both append and replace session modes."""
        nonlocal batch_seq, audience_id
        if args.dry_run or not buf:
            return
        batch_seq += 1

        if args.replace:
            aid = args.fb_audience_id
            result = fb.replace_users_batch(
                aid, buf,
                session_id=replace_session_id,
                batch_seq=batch_seq,
                total_rows=total_for_replace,
                is_last=is_last,
            )
        else:
            result = fb.add_users(audience_id, buf)  # type: ignore[arg-type]

        recv = result.get('num_received', '?')
        if args.verbose or is_last:
            print(f'  Batch {batch_seq}: {len(buf):,} sent, {recv} received '
                  f'({time.time()-t0:.0f}s elapsed)')

    # For replace mode, we need to know total batches before sending.
    # Collect all batches first if replacing (allows accurate last-batch detection).
    # For append mode, flush each batch immediately to minimize memory use.
    if args.replace:
        all_replace_batches: list[list] = []

        for record in stream_voters(conn, args.audience, district_filter):
            batch_buf.append(build_row(record))
            total_rows += 1
            if len(batch_buf) == BATCH_SIZE:
                all_replace_batches.append(batch_buf)
                batch_buf = []
        if batch_buf:
            all_replace_batches.append(batch_buf)

        conn.close()
        print(f'  Hashed {total_rows:,} voters ({time.time()-t0:.1f}s)')

        if total_rows == 0:
            print(f'\nNo voters found for "{args.audience}"{district_label}.')
            return

        if args.dry_run:
            print(f'\nDRY RUN complete — {total_rows:,} records ready, nothing uploaded.\n')
            return

        print(f'\nReplacing audience {args.fb_audience_id} '
              f'({len(all_replace_batches)} batches, {total_rows:,} rows)...')
        for i, b in enumerate(all_replace_batches, 1):
            is_last = (i == len(all_replace_batches))
            batch_seq += 1
            result = fb.replace_users_batch(
                args.fb_audience_id, b,
                session_id=replace_session_id,
                batch_seq=i,
                total_rows=total_rows,
                is_last=is_last,
            )
            recv = result.get('num_received', '?')
            print(f'  Batch {i}/{len(all_replace_batches)}: {len(b):,} sent, {recv} received')

    else:
        # Append / new-audience mode — stream directly, upload batch by batch
        for record in stream_voters(conn, args.audience, district_filter):
            batch_buf.append(build_row(record))
            total_rows += 1
            if len(batch_buf) == BATCH_SIZE:
                _flush_batch(batch_buf)
                batch_buf = []
        _flush_batch(batch_buf, is_last=True)   # final partial batch
        conn.close()

        if total_rows == 0:
            print(f'\nNo voters found for "{args.audience}"{district_label}.')
            print('Run: python export/facebook_audience.py --list-audiences')
            return

        if args.dry_run:
            print(f'\nDRY RUN complete — {total_rows:,} records ready, nothing uploaded.\n')
            return

    # ── Summary ───────────────────────────────────────────────────────────────
    final_audience_id = args.fb_audience_id or audience_id
    elapsed = time.time() - t0

    print(f'\n{"─"*60}')
    print(f'  Facebook Custom Audience ID : {final_audience_id}')
    print(f'  Audience name               : {fb_name}')
    print(f'  Total records uploaded      : {total_rows:,}')
    print(f'  Total time                  : {elapsed:.1f}s')
    print(f'{"─"*60}')
    print()
    print('  To refresh this audience again:')
    print(f'    python export/facebook_audience.py \\')
    print(f'      --audience {args.audience} \\')
    print(f'      --fb-audience-id {final_audience_id} --replace')
    print()


if __name__ == '__main__':
    main()
