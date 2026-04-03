#!/usr/bin/env python3
"""
export/facebook_donor_audience.py
==================================
Interactive script to build and upload Facebook Custom Audiences from:
  - BOE (NY state campaign finance) donors
  - FEC (federal campaign finance) donors
  - NYC CFB donors
  - Issue / voter segment audiences

You will be prompted step-by-step for:
  1. Which Facebook Ad Account to upload to
  2. Audience source (donors by type, or voter segment)
  3. Geographic filter  (zip, LD, SD, CD, county, or none)
  4. Minimum donation amount  (donor modes only)
  5. Create new audience, append to existing, or replace existing

Credentials: reads META_ACCESS_TOKEN and META_BUSINESS_IDS from either
  - voter_pipeline/.env        (add them here to override)
  - portal .env / portal settings DB  (used as automatic fallback)
"""

import hashlib
import os
import random
import re
import sys
import time
from datetime import date, datetime

import requests
from dotenv import load_dotenv

# ── Path / credential setup ───────────────────────────────────────────────────
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)

# Load voter pipeline .env first, then receipt-automation .env as fallback
# (load_dotenv won't override vars that are already set)
load_dotenv(os.path.join(BASE, '.env'))
_receipt_env = os.path.join(BASE, '..', 'facebook-receipt-automation', '.env')
if os.path.exists(_receipt_env):
    load_dotenv(_receipt_env)

from utils.db import get_conn  # noqa: E402  (after sys.path insert)

# ── Meta API constants ────────────────────────────────────────────────────────
META_API_VERSION = os.getenv('META_API_VERSION', 'v21.0')
META_BASE        = f'https://graph.facebook.com/{META_API_VERSION}'
BATCH_SIZE       = 10_000
MAX_RETRIES      = 3
RETRY_DELAY      = 5

# Schema sent to Facebook — order matters; PHONE appears twice (Mobile / Landline)
SCHEMA = [
    'EXTERN_ID',  # StateVoterId — plain (not hashed); enables lookalike seeding
    'EMAIL',      # crm_email
    'PHONE',      # Mobile
    'PHONE',      # Landline
    'FN',         # FirstName
    'LN',         # LastName
    'ZIP',        # PrimaryZip (5-digit)
    'ST',         # PrimaryState
    'CT',         # PrimaryCity
    'DOBY',       # Year of birth
    'DOBM',       # Month of birth (zero-padded)
    'DOBD',       # Day of birth (zero-padded)
    'GEN',        # Gender (m/f)
    'COUNTRY',    # Always 'us'
]

# ── PII normalization + SHA-256 hashing ──────────────────────────────────────

def _sha256(s: str) -> str:
    return hashlib.sha256(s.encode('utf-8')).hexdigest()

def _norm_email(v) -> str:
    if not v: return ''
    v = str(v).strip().lower()
    return _sha256(v) if ('@' in v and '.' in v) else ''

def _norm_phone(v) -> str:
    if not v: return ''
    d = re.sub(r'\D', '', str(v))
    if len(d) == 10: d = '1' + d
    return _sha256(d) if (len(d) == 11 and d[0] == '1') else ''

def _norm_name(v) -> str:
    if not v: return ''
    c = re.sub(r'[^a-z]', '', str(v).strip().lower())
    return _sha256(c) if c else ''

def _norm_zip(v) -> str:
    if not v: return ''
    z = re.sub(r'\D', '', str(v))[:5]
    return _sha256(z) if len(z) == 5 else ''

def _norm_state(v) -> str:
    if not v: return ''
    c = re.sub(r'[^a-z]', '', str(v).strip().lower())[:2]
    return _sha256(c) if len(c) == 2 else ''

def _norm_city(v) -> str:
    if not v: return ''
    c = re.sub(r'[^a-z]', '', str(v).strip().lower())
    return _sha256(c) if c else ''

def _norm_dob_year(v) -> str:
    return _sha256(str(v.year)) if isinstance(v, date) else ''

def _norm_dob_month(v) -> str:
    return _sha256(f'{v.month:02d}') if isinstance(v, date) else ''

def _norm_dob_day(v) -> str:
    return _sha256(f'{v.day:02d}') if isinstance(v, date) else ''

def _norm_gender(v) -> str:
    if not v: return ''
    g = str(v).strip().upper()[:1]
    return _sha256(g.lower()) if g in ('M', 'F') else ''

_COUNTRY_HASH = _sha256('us')

def build_row(row: dict) -> list:
    return [
        str(row['StateVoterId']),
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


# ── Meta API client ───────────────────────────────────────────────────────────

class MetaClient:
    def __init__(self, token: str):
        self.token   = token
        self._session = requests.Session()

    def _get(self, path: str, params: dict | None = None) -> dict:
        r = self._session.get(
            f'{META_BASE}/{path}',
            params={**(params or {}), 'access_token': self.token},
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        if 'error' in data:
            raise RuntimeError(f"Meta API error: {data['error'].get('message', data['error'])}")
        return data

    def _get_paginated(self, path: str, params: dict | None = None) -> list:
        results, data = [], self._get(path, params)
        results.extend(data.get('data', []))
        while 'paging' in data and 'next' in data['paging']:
            r = self._session.get(data['paging']['next'],
                                  params={'access_token': self.token}, timeout=30)
            r.raise_for_status()
            data = r.json()
            results.extend(data.get('data', []))
        return results

    def _post(self, path: str, payload: dict) -> dict:
        payload = {**payload, 'access_token': self.token}
        last_exc = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = self._session.post(f'{META_BASE}/{path}', json=payload, timeout=120)
                if not r.ok:
                    try:
                        err = r.json().get('error', {})
                        msg = err.get('error_user_msg') or err.get('message') or r.text[:300]
                    except Exception:
                        msg = r.text[:300]
                    raise RuntimeError(f"Meta API {r.status_code}: {msg}")
                data = r.json()
                if 'error' in data:
                    raise RuntimeError(f"Meta API error: {data['error'].get('message', data['error'])}")
                return data
            except (requests.RequestException, RuntimeError) as exc:
                last_exc = exc
                if attempt < MAX_RETRIES:
                    wait = RETRY_DELAY * attempt
                    print(f'    [retry {attempt}/{MAX_RETRIES}] {exc} — waiting {wait}s...')
                    time.sleep(wait)
        raise last_exc  # type: ignore[misc]

    def get_all_ad_accounts(self, business_ids: list[str]) -> list[dict]:
        accounts: dict[str, dict] = {}
        for bid in business_ids:
            for endpoint in (f'{bid}/owned_ad_accounts', f'{bid}/client_ad_accounts'):
                try:
                    rows = self._get_paginated(endpoint,
                        {'fields': 'id,name,account_id,currency,business_name'})
                    for a in rows:
                        accounts[a['id']] = a
                except requests.HTTPError:
                    pass
        return list(accounts.values())

    def get_my_adaccounts(self) -> list[dict]:
        """Return ad accounts accessible to this token via /me/adaccounts."""
        try:
            return self._get_paginated('me/adaccounts',
                                       {'fields': 'id,name,account_id,currency'})
        except Exception:
            return []

    def get_all_custom_audiences(self, account_id: str) -> dict[str, str]:
        """Return {audience_name: audience_id} for every Custom Audience in this ad account."""
        rows = self._get_paginated(
            f'act_{account_id}/customaudiences',
            {'fields': 'id,name'},
        )
        return {a['name']: a['id'] for a in rows}

    def create_audience(self, account_id: str, name: str, description: str = '') -> str:
        data = self._post(f'act_{account_id}/customaudiences', {
            'name': name,
            'subtype': 'CUSTOM',
            'description': description,
            'customer_file_source': 'USER_PROVIDED_ONLY',
        })
        return data['id']

    def add_users(self, audience_id: str, rows: list) -> dict:
        return self._post(f'{audience_id}/users',
                          {'payload': {'schema': SCHEMA, 'data': rows}})

    def replace_users_batch(self, audience_id: str, rows: list,
                            session_id: int, batch_seq: int,
                            total_rows: int) -> dict:
        return self._post(f'{audience_id}/usersreplace', {
            'session': {
                'session_id': session_id,
                'batch_seq': batch_seq,
                'estimated_num_total': total_rows,
            },
            'payload': {'schema': SCHEMA, 'data': rows},
        })


# ── SQL query builders ────────────────────────────────────────────────────────

SELECT_COLS = """
    vf.StateVoterId, vf.crm_email, vf.Mobile, vf.Landline,
    vf.FirstName, vf.LastName, vf.PrimaryZip, vf.PrimaryState,
    vf.PrimaryCity, vf.DOB, vf.Gender
"""

# Voter-table-only columns: no CRM email, use PrimaryPhone (BOE registration
# phone — 100% coverage) instead of Landline (phone-append, ~74% coverage).
SELECT_COLS_VOTER = """
    vf.StateVoterId, NULL AS crm_email, vf.Mobile, vf.PrimaryPhone AS Landline,
    vf.FirstName, vf.LastName, vf.PrimaryZip, vf.PrimaryState,
    vf.PrimaryCity, vf.DOB, vf.Gender
"""

def _geo_clause(geo: tuple | None) -> tuple[str, list]:
    """Return (WHERE fragment, params) for the geographic filter, or ('1=1', [])."""
    if not geo:
        return '1=1', []
    col, val = geo
    return f'vf.`{col}` = %s', [val]


def _dist_prefix(geo: tuple | None) -> str:
    """Return short district label for use at the start of FB audience names.

    Examples: ('SDName', '23') -> 'SD 23'
              ('LDName', '63') -> 'LD 63'
              ('CDName', '1')  -> 'CD 1'
              ('CountyName', 'Queens') -> 'Queens'
              None             -> ''
    """
    if not geo:
        return ''
    col, val = geo
    short = {'SDName': 'SD', 'LDName': 'LD', 'CDName': 'CD'}.get(col)
    return f'{short} {val}' if short else str(val)


def build_donor_sql(sources: list[str], geo: tuple | None,
                    min_amount: float) -> tuple[str, list]:
    """
    Build a UNION query for the requested donor sources.
    sources: subset of ['boe', 'fec', 'cfb']
    Returns (sql, params).
    """
    geo_where, geo_params = _geo_clause(geo)
    parts, params = [], []

    if 'boe' in sources:
        parts.append(f"""
            SELECT {SELECT_COLS}
            FROM nys_voter_tagging.voter_file vf
            INNER JOIN boe_donors.boe_donor_summary bds
                ON vf.StateVoterId = bds.StateVoterId
            WHERE {geo_where} AND bds.total_amt >= %s
        """)
        params += geo_params + [min_amount]

    if 'fec' in sources:
        parts.append(f"""
            SELECT {SELECT_COLS}
            FROM nys_voter_tagging.voter_file vf
            WHERE {geo_where}
              AND vf.is_national_donor = 1
              AND COALESCE(vf.national_total_amount, 0) >= %s
        """)
        params += geo_params + [min_amount]

    if 'cfb' in sources:
        parts.append(f"""
            SELECT {SELECT_COLS}
            FROM nys_voter_tagging.voter_file vf
            INNER JOIN cfb_donors.cfb_donor_summary cds
                ON vf.StateVoterId = cds.StateVoterId
            WHERE {geo_where} AND cds.total_amt >= %s
        """)
        params += geo_params + [min_amount]

    # UNION deduplicates voters who appear in multiple donor databases
    sql = '\nUNION\n'.join(parts)
    return sql, params


def build_audience_sql(audience_name: str | None, geo: tuple | None) -> tuple[str, list]:
    """audience_name=None means all audiences (any voter in voter_audience_bridge)."""
    geo_where, geo_params = _geo_clause(geo)
    if audience_name is None:
        sql = f"""
            SELECT DISTINCT {SELECT_COLS}
            FROM nys_voter_tagging.voter_file vf
            INNER JOIN nys_voter_tagging.voter_audience_bridge vab
                ON vf.StateVoterId = vab.StateVoterId
            WHERE {geo_where}
        """
        return sql, geo_params
    sql = f"""
        SELECT {SELECT_COLS}
        FROM nys_voter_tagging.voter_file vf
        INNER JOIN nys_voter_tagging.voter_audience_bridge vab
            ON vf.StateVoterId = vab.StateVoterId
        WHERE vab.audience = %s AND {geo_where}
    """
    return sql, [audience_name] + geo_params


def count_sql(query_sql: str, params: list, conn) -> int:
    wrapped = f'SELECT COUNT(*) FROM ({query_sql}) AS _sub'
    with conn.cursor() as cur:
        cur.execute(wrapped, params)
        return cur.fetchone()[0]


def stream_sql(query_sql: str, params: list, conn):
    import pymysql.cursors
    cursor = conn.cursor(pymysql.cursors.SSCursor)
    cursor.execute(query_sql, params)
    columns = [d[0] for d in cursor.description]
    for row in cursor:
        yield dict(zip(columns, row))
    cursor.close()


# ── Interactive prompts ───────────────────────────────────────────────────────

def _divider(title: str = ''):
    print()
    print('=' * 62)
    if title:
        print(f'  {title}')
        print('=' * 62)

def _pick(prompt: str, options: list[str], allow_multiple: bool = False):
    """
    Print a numbered menu and return the 0-based index (or list of indices).
    allow_multiple: accept comma-separated input like "1,3"
    """
    for i, opt in enumerate(options, 1):
        print(f'    [{i}] {opt}')
    print()
    while True:
        raw = input(f'  {prompt}: ').strip()
        if not raw:
            continue
        try:
            if allow_multiple:
                indices = [int(x.strip()) - 1 for x in raw.split(',')]
                if all(0 <= i < len(options) for i in indices):
                    return indices
            else:
                idx = int(raw) - 1
                if 0 <= idx < len(options):
                    return idx
        except ValueError:
            pass
        print(f'  Please enter a number between 1 and {len(options)}.')


def prompt_account(meta: MetaClient, business_ids: list[str]) -> dict:
    _divider('FACEBOOK AD ACCOUNT')
    print('  Fetching ad accounts from Meta Business Manager(s)...')
    accounts = meta.get_all_ad_accounts(business_ids)
    if not accounts:
        sys.exit(
            '\nERROR: No ad accounts found.\n'
            '  Check META_BUSINESS_IDS in .env and ensure the token has ads_management permission.'
        )
    accounts.sort(key=lambda a: a.get('name', ''))
    labels = [
        f"{a.get('name', 'Unnamed')}  (ID: {a.get('account_id') or a['id'].replace('act_', '')})"
        for a in accounts
    ]
    print()
    idx = _pick('Select account', labels)
    return accounts[idx]


def prompt_source() -> tuple[str, list[str] | str]:
    """
    Returns ('donors', ['boe','fec','cfb',...]) or ('audience', audience_name).
    """
    _divider('AUDIENCE SOURCE')
    source_opts = [
        'BOE donors       (NY state campaign finance)',
        'FEC donors       (federal campaign finance)',
        'CFB donors       (NYC Campaign Finance Board)',
        'All donors       (BOE + FEC + CFB combined)',
        'Issue / segment audience  (voter_audience_bridge)',
    ]
    idx = _pick('Select source', source_opts)

    if idx == 0:   return 'donors', ['boe']
    if idx == 1:   return 'donors', ['fec']
    if idx == 2:   return 'donors', ['cfb']
    if idx == 3:   return 'donors', ['boe', 'fec', 'cfb']

    # Issue audience — show numbered list from DB
    conn = get_conn('nys_voter_tagging', autocommit=True)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT audience, COUNT(*) AS cnt
            FROM nys_voter_tagging.voter_audience_bridge
            GROUP BY audience ORDER BY audience
        """)
        rows = cur.fetchall()
    conn.close()
    if not rows:
        sys.exit('\nERROR: No audiences found in voter_audience_bridge. Run pipeline first.')
    print()
    # Prepend "All audiences" — selecting this creates one separate FB audience per segment
    labels = [f'{"[ All audiences — one FB audience per segment ]":<48} {len(rows):>10,} segments']
    labels += [f'{name:<48} {cnt:>10,} voters' for name, cnt in rows]
    aud_idx = _pick('Select audience', labels)
    if aud_idx == 0:
        return 'audience', None          # None → bulk: one FB audience per segment
    return 'audience', rows[aud_idx - 1][0]


def prompt_geo() -> tuple[str | None, str | None]:
    """Returns (column_name, value) or (None, None) for no filter."""
    _divider('GEOGRAPHIC FILTER')
    opts = [
        'Zip code',
        'Legislative District (LD / Assembly)',
        'State Senate District (SD)',
        'Congressional District (CD)',
        'County',
        'No filter — statewide',
    ]
    idx = _pick('Select filter', opts)
    if idx == 5:
        return None, None

    col_map = {
        0: ('PrimaryZip',  'Enter zip code'),
        1: ('LDName',      'Enter LD number'),
        2: ('SDName',      'Enter SD number'),
        3: ('CDName',      'Enter CD number'),
        4: ('CountyName',  'Enter county name'),
    }
    col, prompt = col_map[idx]
    print()
    val = input(f'  {prompt}: ').strip()
    return col, val


def prompt_min_amount() -> float:
    print()
    raw = input('  Minimum total donation amount ($ — press Enter for none): ').strip()
    if not raw:
        return 0.0
    try:
        return float(raw.replace(',', '').replace('$', ''))
    except ValueError:
        return 0.0


def prompt_audience_mode(account_name: str, default_name: str = '') -> tuple[str, str | None, str]:
    """
    Returns ('new', None, fb_name), ('append', audience_id, fb_name),
            or ('replace', audience_id, fb_name).
    default_name: pre-filled suggestion shown to the user (e.g. 'Politika-NYS_HARD_DEM').
    """
    if not default_name:
        default_name = f'Voter Export {datetime.now():%Y-%m-%d}'

    _divider('FACEBOOK CUSTOM AUDIENCE')
    opts = [
        'Create new audience',
        'Append to existing audience',
        'Replace existing audience  (atomic swap of all members)',
    ]
    idx = _pick('Select mode', opts)

    if idx == 0:
        print()
        raw = input(f'  Audience name [{default_name}]: ').strip()
        return 'new', None, (raw or default_name)

    print()
    aud_id = input('  Enter existing Facebook Custom Audience ID: ').strip()
    raw = input(f'  Audience name (for reference) [{default_name}]: ').strip()
    mode = 'append' if idx == 1 else 'replace'
    return mode, aud_id, (raw or default_name)


# ── Upload ────────────────────────────────────────────────────────────────────

def upload(meta: MetaClient, account_id: str, audience_id: str | None,
           fb_name: str, mode: str, description: str,
           sql: str, params: list, conn) -> str:
    """
    Stream voters from `sql`, hash, and upload to Facebook.
    Returns the final audience ID.
    """
    t0 = time.time()

    if mode == 'replace':
        # Need total count for the session protocol
        print('\n  Counting rows for replace session...')
        total = count_sql(sql, params, conn)
        print(f'  {total:,} voters found.')
        session_id = random.randint(10**9, 10**18)
        batch_seq  = 0

        # Collect all batches for progress display
        print('  Hashing records...')
        batches, buf = [], []
        for row in stream_sql(sql, params, conn):
            buf.append(build_row(row))
            if len(buf) == BATCH_SIZE:
                batches.append(buf); buf = []
        if buf:
            batches.append(buf)

        print(f'  Replacing audience {audience_id} '
              f'({len(batches)} batch(es), {total:,} rows)...')
        for i, batch in enumerate(batches, 1):
            result  = meta.replace_users_batch(
                audience_id, batch,      # type: ignore[arg-type]
                session_id=session_id, batch_seq=i,
                total_rows=total,
            )
            print(f'    Batch {i}/{len(batches)}: {len(batch):,} sent  '
                  f'({result.get("num_received","?")} received)')
        return audience_id  # type: ignore[return-value]

    # ── New or append ─────────────────────────────────────────────────────────
    if mode == 'new':
        print(f'\n  Creating Facebook Custom Audience: "{fb_name}"...')
        audience_id = meta.create_audience(account_id, fb_name, description)
        print(f'  Created audience ID : {audience_id}')
        print(f'  (Save this ID — use it with --replace on future runs)\n')
    else:
        print(f'\n  Appending to audience {audience_id}...')

    total_rows, batch_num, buf = 0, 0, []

    def flush(b):
        nonlocal batch_num
        if not b: return
        batch_num += 1
        result = meta.add_users(audience_id, b)
        print(f'    Batch {batch_num}: {len(b):,} sent  '
              f'({result.get("num_received","?")} received)  '
              f'({time.time()-t0:.0f}s)')

    for row in stream_sql(sql, params, conn):
        buf.append(build_row(row))
        total_rows += 1
        if len(buf) == BATCH_SIZE:
            flush(buf); buf = []
    flush(buf)

    return audience_id  # type: ignore[return-value]


# ── Bulk audience upload ──────────────────────────────────────────────────────

def _fast_count_audience(conn, audience_name: str, geo: tuple | None) -> int:
    """
    Count voters for a single named audience.
    - Statewide: counts directly on voter_audience_bridge (indexed, instant).
    - Geo-filtered: joins voter_file using the district index.
    Avoids the slow COUNT(SELECT DISTINCT ...) subquery pattern.
    """
    if not geo:
        with conn.cursor() as cur:
            cur.execute(
                'SELECT COUNT(*) FROM nys_voter_tagging.voter_audience_bridge WHERE audience = %s',
                [audience_name],
            )
            return cur.fetchone()[0]
    geo_col, geo_val = geo
    sql = f"""
        SELECT COUNT(*)
        FROM nys_voter_tagging.voter_file vf
        INNER JOIN nys_voter_tagging.voter_audience_bridge vab
            ON vf.StateVoterId = vab.StateVoterId
        WHERE vab.audience = %s AND vf.`{geo_col}` = %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, [audience_name, geo_val])
        return cur.fetchone()[0]


def _upload_all_audiences(meta: MetaClient, account_id: str, account_name: str,
                          geo: tuple | None, cli) -> None:
    """
    For each audience in voter_audience_bridge, create a separate Facebook
    Custom Audience named "Politika-{audience_name}".
    The geographic filter (if any) is applied to every audience.
    """
    conn = get_conn('nys_voter_tagging', autocommit=True)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT audience, COUNT(*) AS cnt
            FROM nys_voter_tagging.voter_audience_bridge
            GROUP BY audience ORDER BY audience
        """)
        all_audiences = cur.fetchall()

    # Apply audience filter if provided
    _filter = getattr(cli, 'audience_filter', None)
    if _filter:
        _allowed = {n.strip() for n in _filter.split(',') if n.strip()}
        all_audiences = [(n, c) for n, c in all_audiences if n in _allowed]

    if not all_audiences:
        conn.close()
        print('\n  No audiences found in voter_audience_bridge. Run pipeline first.')
        return

    geo_label = f'{geo[0]} = {geo[1]}' if geo else 'Statewide'
    prefix_label = getattr(cli, 'audience_prefix', 'Politika') or 'Politika'

    _divider('BULK AUDIENCE UPLOAD')
    print(f'  Will create {len(all_audiences)} separate Facebook Custom Audiences:')
    print(f'  Filter      : {geo_label}')
    print(f'  Account     : {account_name}  (act_{account_id})')
    print()
    # For statewide, show pre-fetched bridge counts (fast).
    # For geo-filtered, show "?" — we'll count per-audience during the loop.
    for aud_name, cnt in all_audiences:
        cnt_label = f'{cnt:>10,} voters' if not geo else f'{"(counting...)":>10}'
        _disp = aud_name.removesuffix('.csv').removesuffix('.CSV')
        print(f'    {prefix_label}-{_disp:<42}  {cnt_label}')
    print()

    if cli.dry_run:
        print('  DRY RUN — no data will be sent to Facebook.\n')
        conn.close()
        return

    if not getattr(cli, 'yes', False):
        try:
            ans = input('  Create / update all audiences? [y/N]: ').strip().lower()
        except (EOFError, KeyboardInterrupt):
            ans = 'n'
        if ans not in ('y', 'yes'):
            print('\n  Upload cancelled.')
            conn.close()
            return

    # ── Fetch existing audiences once (one API call for the whole account) ────
    print('\n  Checking for existing audiences in this ad account...')
    existing = meta.get_all_custom_audiences(account_id)
    print(f'  Found {len(existing):,} existing Custom Audience(s) in account.\n')

    # ── Loop: one upload per audience ─────────────────────────────────────────
    t_total  = time.time()
    created  : list[tuple[str, str, int, str]] = []   # (fb_name, audience_id, count, action)
    skipped  : list[str] = []

    for aud_name, bridge_cnt in all_audiences:
        prefix  = getattr(cli, 'audience_prefix', 'Politika') or 'Politika'
        # Per-audience custom names override prefix-based default
        _names_b64 = getattr(cli, 'audience_names_b64', None)
        if _names_b64:
            import base64 as _b64, json as _json
            try:
                _names_map = _json.loads(_b64.b64decode(_names_b64).decode())
            except Exception:
                _names_map = {}
        else:
            _names_map = {}
        _clean_name = aud_name.removesuffix('.csv').removesuffix('.CSV')
        _dp = _dist_prefix(geo)
        fb_name = _names_map.get(aud_name) or _names_map.get(_clean_name) or (f'{_dp}-{prefix}-{_clean_name}' if _dp else f'{prefix}-{_clean_name}')
        sql, params = build_audience_sql(aud_name, geo)

        print(f'\n{"─"*62}')
        print(f'  {fb_name}')
        print(f'{"─"*62}')

        # Use fast direct count (no slow subquery wrapping)
        count = _fast_count_audience(conn, aud_name, geo)
        if count == 0:
            print(f'  No voters found — skipping.')
            skipped.append(fb_name)
            continue

        description = (
            f'NYS Voter Pipeline | {aud_name} | {geo_label} | '
            f'{count:,} records | {datetime.now():%Y-%m-%d}'
        )

        # Auto-detect: replace if the audience already exists, otherwise create new
        existing_id = existing.get(fb_name)
        if existing_id:
            print(f'  {count:,} voters — replacing existing (ID: {existing_id})...')
            action = 'replaced'
            final_id = upload(
                meta, account_id,
                audience_id=existing_id,
                fb_name=fb_name,
                mode='replace',
                description=description,
                sql=sql, params=params, conn=conn,
            )
        else:
            print(f'  {count:,} voters — creating new...')
            action = 'created'
            final_id = upload(
                meta, account_id,
                audience_id=None,
                fb_name=fb_name,
                mode='new',
                description=description,
                sql=sql, params=params, conn=conn,
            )
        created.append((fb_name, final_id, count, action))

    conn.close()

    # ── Bulk summary ──────────────────────────────────────────────────────────
    elapsed = time.time() - t_total
    print()
    print('═' * 62)
    print(f'  Bulk upload complete  ({elapsed:.0f}s total)')
    print(f'  Created  : {len(created)}   Skipped: {len(skipped)}')
    print()
    for fb_name, aud_id, cnt, action in created:
        print(f'  [{action:<8}]  {fb_name:<46}  {cnt:>8,}  ID: {aud_id}')
    if skipped:
        print()
        for s in skipped:
            print(f'  [skipped — no voters]  {s}')
    print('═' * 62)
    print()


# ── Party-based upload ───────────────────────────────────────────────────────

# Parties with fewer than this many voters are merged into a single "Other" audience.
PARTY_MIN_COUNT = 1_000


def _upload_by_party(meta: 'MetaClient', account_id: str, account_name: str,
                     geo: tuple | None, cli) -> None:
    """
    For each OfficialParty value in voter_file, create/replace a Facebook
    Custom Audience named "Politika-Party-{party}".
    Parties below PARTY_MIN_COUNT voters are merged into a single Party-Other audience.
    Geographic filter (if any) is applied to every party.
    """
    conn = get_conn('nys_voter_tagging', autocommit=True)
    geo_clause, geo_params = _geo_clause(geo)
    geo_label = f'{geo[0]}={geo[1]}' if geo else 'Statewide'

    with conn.cursor() as cur:
        cur.execute(
            f'SELECT OfficialParty, COUNT(*) AS cnt '
            f'FROM nys_voter_tagging.voter_file vf '
            f'WHERE ({geo_clause}) AND OfficialParty IS NOT NULL AND OfficialParty != "" '
            f'GROUP BY OfficialParty ORDER BY cnt DESC',
            geo_params,
        )
        all_parties = [(r[0], int(r[1])) for r in cur.fetchall()]

    # Split: parties >= PARTY_MIN_COUNT get their own audience; smaller ones merge into Other
    main_parties  = [(p, c) for p, c in all_parties if c >= PARTY_MIN_COUNT]
    minor_parties = [(p, c) for p, c in all_parties if c <  PARTY_MIN_COUNT]
    other_count   = sum(c for _, c in minor_parties)

    _pfx_base = getattr(cli, 'party_prefix', None) or getattr(cli, 'audience_prefix', 'Politika') or 'Politika'
    _dp_disp  = _dist_prefix(geo)
    pfx_disp  = f'{_dp_disp}-{_pfx_base}' if _dp_disp else _pfx_base

    n_audiences = len(main_parties) + (1 if minor_parties else 0)
    print(f'\n\n{"\u2550"*62}')
    print(f'  PUSH BY PARTY  \u2014  {n_audiences} audiences  ({geo_label})')
    print(f'  Account: {account_name}  (act_{account_id})')
    print(f'{"\u2550"*62}')
    for party, cnt in main_parties:
        print(f'  {pfx_disp}-Party-{party:<26}  {cnt:>10,} voters')
    if minor_parties:
        print(f'  {pfx_disp}-Party-Other{"":22}  {other_count:>10,} voters')
        for p, c in minor_parties:
            print(f'      \u2514 {p:<30}  {c:>10,}')
    print()

    if cli.dry_run:
        print('  DRY RUN \u2014 no data will be sent to Facebook.\n')
        conn.close()
        return

    existing = meta.get_all_custom_audiences(account_id)
    t_total  = time.time()

    # ── Main parties — one audience each ─────────────────────────────────────
    for party, cnt in main_parties:
        prefix  = getattr(cli, 'party_prefix', None) or getattr(cli, 'audience_prefix', 'Politika') or 'Politika'
        _dp = _dist_prefix(geo)
        fb_name = f'{_dp}-{prefix}-Party-{party}' if _dp else f'{prefix}-Party-{party}'
        sql = (
            f'SELECT {SELECT_COLS_VOTER} '
            f'FROM nys_voter_tagging.voter_file vf '
            f'WHERE vf.OfficialParty = %s AND ({geo_clause})'
        )
        params = [party] + geo_params
        description = (
            f'NYS Voter Pipeline | Party: {party} | {geo_label} | '
            f'{cnt:,} records | {datetime.now():%Y-%m-%d}'
        )
        existing_id = existing.get(fb_name)
        mode        = 'replace' if existing_id else 'new'

        print(f'\n{"\u2500"*62}')
        print(f'  {fb_name}  ({cnt:,} voters) \u2014 {"replacing" if existing_id else "creating new"}')
        print(f'{"\u2500"*62}')
        if cnt == 0:
            print('  No voters \u2014 skipping.')
            continue

        final_id = upload(
            meta, account_id,
            audience_id=existing_id, fb_name=fb_name, mode=mode,
            description=description, sql=sql, params=params, conn=conn,
        )
        print(f'  Audience ID: {final_id}')

    # ── Minor parties — merged into one "Other" audience ─────────────────────
    if minor_parties and other_count > 0:
        prefix  = getattr(cli, 'party_prefix', None) or getattr(cli, 'audience_prefix', 'Politika') or 'Politika'
        _dp = _dist_prefix(geo)
        fb_name = f'{_dp}-{prefix}-Party-Other' if _dp else f'{prefix}-Party-Other'
        minor_names  = [p for p, _ in minor_parties]
        placeholders = ', '.join(['%s'] * len(minor_names))
        sql = (
            f'SELECT {SELECT_COLS_VOTER} '
            f'FROM nys_voter_tagging.voter_file vf '
            f'WHERE vf.OfficialParty IN ({placeholders}) AND ({geo_clause})'
        )
        params = minor_names + geo_params
        party_list  = ', '.join(minor_names)
        description = (
            f'NYS Voter Pipeline | Party: Other ({party_list}) | {geo_label} | '
            f'{other_count:,} records | {datetime.now():%Y-%m-%d}'
        )
        existing_id = existing.get(fb_name)
        mode        = 'replace' if existing_id else 'new'

        print(f'\n{"\u2500"*62}')
        print(f'  {fb_name}  ({other_count:,} voters \u2014 {len(minor_parties)} minor parties merged)')
        print(f'{"\u2500"*62}')

        final_id = upload(
            meta, account_id,
            audience_id=existing_id, fb_name=fb_name, mode=mode,
            description=description, sql=sql, params=params, conn=conn,
        )
        print(f'  Audience ID: {final_id}')

    conn.close()
    elapsed = time.time() - t_total
    print(f'\n  Party push complete  ({elapsed:.0f}s total).\n')


# ── Donor segment definitions ────────────────────────────────────────────────

DONOR_SEGMENTS = [
    {
        'key':   'reg_rep',
        'label': 'Registered Republican donors',
        'where': (
            "vf.OfficialParty = 'Republican' "
            "AND (COALESCE(vf.boe_total_amt,0) + COALESCE(vf.national_total_amount,0)"
            " + COALESCE(vf.cfb_total_amt,0)) > 0"
        ),
    },
    {
        'key':   'reg_dem',
        'label': 'Registered Democrat donors',
        'where': (
            "vf.OfficialParty = 'Democrat' "
            "AND (COALESCE(vf.boe_total_amt,0) + COALESCE(vf.national_total_amount,0)"
            " + COALESCE(vf.cfb_total_amt,0)) > 0"
        ),
    },
    {
        'key':   'gave_rep',
        'label': 'Donated to Republicans',
        'where': (
            "COALESCE(vf.boe_total_R_amt,0) > 0 "
            "OR COALESCE(vf.national_republican_amount,0) > 0"
        ),
    },
    {
        'key':   'gave_unk',
        'label': 'Donated to unknown/independent',
        'where': (
            "COALESCE(vf.boe_total_U_amt,0) > 0 "
            "OR COALESCE(vf.national_unknown_amount,0) > 0 "
            "OR COALESCE(vf.national_independent_amount,0) > 0"
        ),
    },
    {
        'key':   'gave_dem',
        'label': 'Donated to Democrats',
        'where': (
            "COALESCE(vf.boe_total_D_amt,0) > 0 "
            "OR COALESCE(vf.national_democratic_amount,0) > 0"
        ),
    },
]


def _upload_donor_segments(meta, account_id, account_name, geo, cli):
    """
    Create/replace a Facebook Custom Audience for each donor segment.

    Segment keys (pass via --donor-segments key1,key2 or omit for all 5):
      reg_rep  — Registered Republican + any donation total > 0
      reg_dem  — Registered Democrat   + any donation total > 0
      gave_rep — Donated to Republican candidates (BOE R or FEC R)
      gave_unk — Donated to unknown/independent candidates
      gave_dem — Donated to Democrat candidates (BOE D or FEC D)

    Custom audience names via --donor-names-b64 (base64 JSON {key: fb_name}).
    """
    _seg_filter = getattr(cli, 'donor_segments', None)
    if _seg_filter:
        _allowed = {k.strip() for k in _seg_filter.split(',') if k.strip()}
        segments = [s for s in DONOR_SEGMENTS if s['key'] in _allowed]
    else:
        segments = list(DONOR_SEGMENTS)

    if not segments:
        print('  No donor segments to push.')
        return

    _names_b64 = getattr(cli, 'donor_names_b64', None)
    if _names_b64:
        import base64 as _b64, json as _json
        try:
            _names_map = _json.loads(_b64.b64decode(_names_b64).decode())
        except Exception:
            _names_map = {}
    else:
        _names_map = {}

    prefix     = getattr(cli, 'audience_prefix', 'Politika') or 'Politika'
    geo_clause, geo_params = _geo_clause(geo)
    geo_label  = f'{geo[0]}={geo[1]}' if geo else 'Statewide'

    print(f'\n\n{"=" * 62}')
    print(f'  PUSH DONOR SEGMENTS  --  {len(segments)} segment(s)  ({geo_label})')
    print(f'  Account: {account_name}  (act_{account_id})')
    print(f'{"=" * 62}')

    conn = get_conn('nys_voter_tagging', autocommit=True)

    # Pre-count every segment (one query each)
    counts = {}
    for seg in segments:
        _dp = _dist_prefix(geo)
        fb_name = _names_map.get(seg['key']) or (f'{_dp}-{prefix}-{seg["label"]}' if _dp else f'{prefix}-{seg["label"]}')
        sql_cnt = (
            f'SELECT COUNT(*) FROM nys_voter_tagging.voter_file vf '
            f'WHERE ({seg["where"]}) AND ({geo_clause})'
        )
        with conn.cursor() as cur:
            cur.execute(sql_cnt, geo_params)
            counts[seg['key']] = cur.fetchone()[0]
        print(f'  {fb_name:<48}  {counts[seg["key"]]:>10,} voters')
    print()

    if cli.dry_run:
        print('  DRY RUN -- no data will be sent to Facebook.\n')
        conn.close()
        return

    if not getattr(cli, 'yes', False):
        try:
            ans = input('  Create / update all donor segment audiences? [y/N]: ').strip().lower()
        except (EOFError, KeyboardInterrupt):
            ans = 'n'
        if ans not in ('y', 'yes'):
            print('\n  Upload cancelled.')
            conn.close()
            return

    print('\n  Checking for existing audiences in this ad account...')
    existing = meta.get_all_custom_audiences(account_id)
    print(f'  Found {len(existing):,} existing Custom Audience(s) in account.\n')

    t_total = time.time()
    created = []
    skipped = []

    for seg in segments:
        _dp = _dist_prefix(geo)
        fb_name = _names_map.get(seg['key']) or (f'{_dp}-{prefix}-{seg["label"]}' if _dp else f'{prefix}-{seg["label"]}')
        count   = counts[seg['key']]

        print(f'\n{"-" * 62}')
        print(f'  {fb_name}')
        print(f'{"-" * 62}')

        if count == 0:
            print('  No voters found -- skipping.')
            skipped.append(fb_name)
            continue

        sql    = (
            f'SELECT {SELECT_COLS_VOTER} FROM nys_voter_tagging.voter_file vf '
            f'WHERE ({seg["where"]}) AND ({geo_clause})'
        )
        params = list(geo_params)

        description = (
            f'NYS Voter Pipeline | {seg["label"]} | {geo_label} | '
            f'{count:,} records | {datetime.now():%Y-%m-%d}'
        )

        existing_id = existing.get(fb_name)
        if existing_id:
            print(f'  {count:,} voters -- replacing existing (ID: {existing_id})...')
            action = 'replaced'
            final_id = upload(
                meta, account_id,
                audience_id=existing_id,
                fb_name=fb_name,
                mode='replace',
                description=description,
                sql=sql, params=params, conn=conn,
            )
        else:
            print(f'  {count:,} voters -- creating new...')
            action = 'created'
            final_id = upload(
                meta, account_id,
                audience_id=None,
                fb_name=fb_name,
                mode='new',
                description=description,
                sql=sql, params=params, conn=conn,
            )
        created.append((fb_name, final_id, count, action))

    conn.close()

    elapsed = time.time() - t_total
    print()
    print('=' * 62)
    print(f'  Donor segment push complete  ({elapsed:.0f}s total)')
    print(f'  Created/updated: {len(created)}   Skipped: {len(skipped)}')
    print()
    for fb_name, aud_id, cnt, action in created:
        print(f'  [{action:<8}]  {fb_name:<48}  {cnt:>8,}  ID: {aud_id}')
    if skipped:
        print()
        for s in skipped:
            print(f'  [skipped -- no voters]  {s}')
    print('=' * 62)
    print()


# ── Argument parsing (optional — all flags have interactive fallbacks) ────────

def _parse_args():
    import argparse
    p = argparse.ArgumentParser(add_help=False)
    p.add_argument('--audience',        metavar='NAME', default=None)
    p.add_argument('--list-audiences',  action='store_true')
    p.add_argument('--ld',        metavar='NUM',  default=None)
    p.add_argument('--sd',        metavar='NUM',  default=None)
    p.add_argument('--cd',        metavar='NUM',  default=None)
    p.add_argument('--county',    metavar='NAME', default=None)
    p.add_argument('--statewide', action='store_true', default=False,
                   help='No geographic filter (all of NY State)')
    p.add_argument('--fb-audience-id',  metavar='ID',   default=None, dest='fb_audience_id')
    p.add_argument('--replace',         action='store_true')
    p.add_argument('--audience-name',   metavar='NAME', default=None, dest='fb_audience_name')
    p.add_argument('--dry-run',         action='store_true', dest='dry_run')
    p.add_argument('--fb-account',      metavar='NAME',      default='', dest='fb_account',
                   help='Named FB account suffix (FB_ACCESS_TOKEN_NAME / FB_AD_ACCOUNT_ID_NAME)')
    p.add_argument('--fb-ad-account-id', metavar='ID', default=None, dest='fb_ad_account_id',
                   help='Ad Account ID to use directly (bypasses env var / business-ID lookup)')
    p.add_argument('--sheets', metavar='SHEETS', default=None,
                   help='Comma-separated push targets: audiences,party (mirrors export sheet types)')
    p.add_argument('--yes', action='store_true',
                   help='Skip interactive confirmations (for portal automation)')
    p.add_argument('--audience-prefix', metavar='PREFIX', default='Politika',
                   dest='audience_prefix',
                   help='Prefix for FB audience names, e.g. "LD63" → "LD63-HardDems"')
    p.add_argument('--audience-filter', metavar='NAMES', default=None,
                   dest='audience_filter',
                   help='Comma-separated audience names to include (no .csv); all if omitted')
    p.add_argument('--audience-names-b64', metavar='B64', default=None,
                   dest='audience_names_b64',
                   help='Base64-encoded JSON mapping {audience_key: fb_audience_name}')
    p.add_argument('--party-prefix', metavar='PREFIX', default=None,
                   dest='party_prefix',
                   help='Prefix for party audiences, e.g. "LD63" -> "LD63-Party-DEM"')
    p.add_argument('--donor-segments', metavar='KEYS', default=None,
                   dest='donor_segments',
                   help='Comma-separated donor segment keys: '
                        'reg_rep,reg_dem,gave_rep,gave_unk,gave_dem (all if omitted)')
    p.add_argument('--donor-names-b64', metavar='B64', default=None,
                   dest='donor_names_b64',
                   help='Base64-encoded JSON {segment_key: fb_audience_name}')
    return p.parse_known_args()[0]


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    cli = _parse_args()

    token        = os.getenv('META_ACCESS_TOKEN', '').strip()
    business_ids = [b.strip() for b in os.getenv('META_BUSINESS_IDS', '').split(',') if b.strip()]

    # --fb-account: select a named credential pair from env vars
    _sfx = ('_' + cli.fb_account.strip()) if getattr(cli, 'fb_account', '').strip() else ''
    _tok_override = os.getenv(f'META_ACCESS_TOKEN{_sfx}',
                    os.getenv(f'FB_ACCESS_TOKEN{_sfx}', '')).strip()
    if _tok_override:
        token = _tok_override
    _bids_sfx = os.getenv(f'META_BUSINESS_IDS{_sfx}', '').strip()
    if _bids_sfx:
        business_ids = [b.strip() for b in _bids_sfx.split(',') if b.strip()]

    if not token:
        sys.exit(
            '\nERROR: META_ACCESS_TOKEN not set.\n'
            '  Add META_ACCESS_TOKEN to voter_pipeline/.env  OR\n'
            '  set it in the portal Settings page (Meta section).\n'
            '  Get a System User token from: Meta Business Manager → Settings → System Users'
        )
    _has_direct_account = bool(
        getattr(cli, 'fb_ad_account_id', None) or
        os.getenv(f'FB_AD_ACCOUNT_ID{_sfx}', '').strip() or
        os.getenv('FB_AD_ACCOUNT_ID', '').strip()
    )
    if not business_ids and not _has_direct_account:
        sys.exit(
            '\nERROR: META_BUSINESS_IDS not set and no ad account ID configured.\n'
            '  Either set META_BUSINESS_IDS in .env / Settings, OR\n'
            '  set FB_AD_ACCOUNT_ID in Settings, OR pass --fb-ad-account-id <ID>.\n'
            '  Find Business IDs at: https://business.facebook.com → Settings → Business Info'
        )

    meta = MetaClient(token)

    print()
    print('╔══════════════════════════════════════════════════════════════╗')
    print('║          FACEBOOK AUDIENCE EXPORT                           ║')
    print('╚══════════════════════════════════════════════════════════════╝')

    # ── --list-audiences shortcut ─────────────────────────────────────────────
    if cli.list_audiences:
        conn = get_conn('nys_voter_tagging', autocommit=True)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT audience, COUNT(*) AS cnt
                FROM nys_voter_tagging.voter_audience_bridge
                GROUP BY audience ORDER BY audience
            """)
            rows = cur.fetchall()
        conn.close()
        print(f'\n  {"Audience":<45}  {"Voters":>10}')
        print('  ' + '─' * 58)
        for name, cnt in rows:
            print(f'  {name:<45}  {cnt:>10,}')
        print(f'\n  {len(rows)} audience(s) total.\n')
        return

    # ── Step 1: pick ad account ───────────────────────────────────────────────
    _direct_id = getattr(cli, 'fb_ad_account_id', None)
    _aid_env   = os.getenv(f'FB_AD_ACCOUNT_ID{_sfx}', '').strip()
    if not _aid_env:
        _aid_env = os.getenv('FB_AD_ACCOUNT_ID', '').strip()

    if _direct_id:
        account_id   = _direct_id.strip().lstrip('act_')
        account_name = f'act_{account_id}'
        print(f'\n  Account: act_{account_id}  [from --fb-ad-account-id]')
    elif _aid_env:
        account_id   = _aid_env
        account_name = cli.fb_account.strip() if _sfx else 'Default'
        print(f'\n  Account: {account_name}  (act_{account_id})  [from settings]')
    else:
        account      = prompt_account(meta, business_ids)
        account_id   = (account.get('account_id') or account['id'].replace('act_', ''))
        account_name = account.get('name', account_id)
        print(f'\n  Selected: {account_name}  (act_{account_id})')

    # ── --sheets shortcut: route to bulk upload functions ────────────────────
    if getattr(cli, 'sheets', None):
        _sheets   = [s.strip() for s in cli.sheets.split(',') if s.strip()]
        if getattr(cli, 'statewide', False):
            _geo2 = None  # explicit statewide — no filter
        else:
            _geo_col2 = (
                'LDName'     if cli.ld     else
                'SDName'     if cli.sd     else
                'CDName'     if cli.cd     else
                'CountyName' if cli.county else None
            )
            _geo_val2 = cli.ld or cli.sd or cli.cd or cli.county or None
            _geo2 = (_geo_col2, _geo_val2) if _geo_col2 else None

        if 'audiences' in _sheets:
            _upload_all_audiences(meta, account_id, account_name, _geo2, cli)
        if 'party' in _sheets:
            _upload_by_party(meta, account_id, account_name, _geo2, cli)
        if 'donors' in _sheets:
            _upload_donor_segments(meta, account_id, account_name, _geo2, cli)
        return

    # ── Step 2: audience source ───────────────────────────────────────────────
    # If --audience was passed on the CLI, skip the interactive source prompt
    if cli.audience:
        source_type, source_detail = 'audience', cli.audience
    else:
        source_type, source_detail = prompt_source()

    # ── Step 3: geographic filter ─────────────────────────────────────────────
    # If any district/geo flag was passed on the CLI, use it directly
    cli_geo_col = (
        'LDName'     if cli.ld     else
        'SDName'     if cli.sd     else
        'CDName'     if cli.cd     else
        'CountyName' if cli.county else
        'PrimaryZip' if False      else None   # zip not a CLI flag here
    )
    cli_geo_val = cli.ld or cli.sd or cli.cd or cli.county or None

    if getattr(cli, 'statewide', False):
        geo_col, geo_val = None, None   # explicit statewide
    elif cli_geo_col:
        geo_col, geo_val = cli_geo_col, cli_geo_val
    else:
        geo_col, geo_val = prompt_geo()
    geo = (geo_col, geo_val) if geo_col else None

    # ── Step 4: min amount (donors only) ─────────────────────────────────────
    min_amount = 0.0
    if source_type == 'donors':
        min_amount = prompt_min_amount()

    # ── Bulk path: "All audiences" → one FB Custom Audience per segment ───────
    # Each audience is created separately, named "Politika-{audience_name}".
    # This branch handles the entire flow and returns early.
    if source_type == 'audience' and source_detail is None:
        _upload_all_audiences(meta, account_id, account_name, geo, cli)
        return

    # ── Step 5: audience mode ─────────────────────────────────────────────────
    # Build a default FB audience name: issue/segment audiences get "Politika-" prefix
    if source_type == 'audience':
        raw_name = source_detail if source_detail else 'All-Audiences'
        auto_name = f'Politika-{raw_name}'
    else:
        # Donors: use source label, e.g. "Politika-BOE-donors"
        donor_tag = ('All-Donors' if len(source_detail) == 3
                     else source_detail[0].upper() + '-donors')
        auto_name = f'Politika-{donor_tag}'

    # CLI --audience-name overrides the auto-generated name
    default_fb_name = cli.fb_audience_name or auto_name

    # If --fb-audience-id was passed on CLI, use it directly
    if cli.fb_audience_id:
        mode        = 'replace' if cli.replace else 'append'
        audience_id = cli.fb_audience_id
        fb_name     = default_fb_name
    else:
        # Auto-detect: if an audience with this name already exists, replace it
        print(f'\n  Checking for existing audience "{default_fb_name}"...')
        existing_map = meta.get_all_custom_audiences(account_id)
        existing_id  = existing_map.get(default_fb_name)
        if existing_id:
            print(f'  Found existing audience (ID: {existing_id}) — will replace automatically.')
            mode, audience_id, fb_name = 'replace', existing_id, default_fb_name
        else:
            print(f'  No existing audience found — will create new.')
            mode, audience_id, fb_name = prompt_audience_mode(account_name, default_fb_name)

    # ── Build SQL ─────────────────────────────────────────────────────────────
    if source_type == 'donors':
        sql, params = build_donor_sql(source_detail, geo, min_amount)  # type: ignore[arg-type]
        source_label = (
            'All donors (BOE + FEC + CFB)' if len(source_detail) == 3
            else source_detail[0].upper() + ' donors'
        )
    else:
        sql, params = build_audience_sql(source_detail, geo)  # type: ignore[arg-type]
        source_label = 'All audiences (combined)' if source_detail is None else f'Audience: {source_detail}'

    geo_label = f'{geo_col} = {geo_val}' if geo else 'Statewide'

    # ── Count + confirm ───────────────────────────────────────────────────────
    _divider('CONFIRM UPLOAD')
    conn = get_conn('nys_voter_tagging', autocommit=True)

    print('  Counting matching voters...')
    try:
        # For a single named audience (statewide), use the fast bridge count.
        # For donors or geo-filtered queries, fall back to the subquery count.
        if source_type == 'audience' and source_detail and not geo:
            count = _fast_count_audience(conn, source_detail, None)
        else:
            count = count_sql(sql, params, conn)
    except Exception as exc:
        conn.close()
        sys.exit(f'\nERROR counting voters: {exc}\n'
                 '  Make sure the donor pipeline has been run (python main.py donors).')

    if count == 0:
        conn.close()
        print(f'\n  No voters found for: {source_label}, {geo_label}')
        print('  Check that the pipeline and donor enrichment have been run.')
        return

    print()
    print(f'  Source      : {source_label}')
    print(f'  Filter      : {geo_label}')
    if source_type == 'donors' and min_amount > 0:
        print(f'  Min amount  : ${min_amount:,.2f}')
    print(f'  Voters found: {count:,}')
    print(f'  FB account  : {account_name}  (act_{account_id})')
    print(f'  Audience    : {fb_name}  ({mode})')
    print()

    if cli.dry_run:
        print('  DRY RUN — no data will be sent to Facebook.\n')
        conn.close()
        return

    try:
        ans = input('  Proceed? [y/N]: ').strip().lower()
    except (EOFError, KeyboardInterrupt):
        ans = 'n'

    if ans not in ('y', 'yes'):
        print('\n  Upload cancelled.')
        conn.close()
        return

    # ── Upload ────────────────────────────────────────────────────────────────
    description = (
        f'NYS Voter Pipeline | {source_label} | {geo_label} | '
        f'{count:,} records | {datetime.now():%Y-%m-%d}'
    )
    t0 = time.time()
    print()

    final_id = upload(
        meta, account_id, audience_id, fb_name, mode,
        description, sql, params, conn,
    )
    conn.close()

    elapsed = time.time() - t0
    print()
    print('═' * 62)
    print(f'  Facebook Custom Audience ID : {final_id}')
    print(f'  Account                     : {account_name}')
    print(f'  Records uploaded            : {count:,}')
    print(f'  Elapsed                     : {elapsed:.1f}s')
    print('═' * 62)
    print()
    print('  To refresh this audience again, run:')
    print('    python main.py fb-audiences')
    print(f'  and choose: Replace existing → ID {final_id}')
    print()


if __name__ == '__main__':
    main()
