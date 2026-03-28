"""
Facebook receipt downloader using Playwright.

Uses a saved browser session (cookies) to navigate to each ad account's
billing page and click the download button on every transaction row.

First run:  launches a visible browser so you can log in.
            Saves session to fb_session.json for headless reuse.
Later runs: loads fb_session.json and runs fully headless.
"""

import logging
import time
from datetime import datetime
from pathlib import Path

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

from src.config import RECEIPT_DOWNLOAD_DIR, META_BUSINESS_IDS

logger = logging.getLogger(__name__)

SESSION_FILE = Path("fb_session.json")
FB_BILLING_URL = (
    "https://business.facebook.com/billing_hub/payment_activity"
    "?asset_id={account_id}"
    "&business_id={business_id}"
    "&placement=ads_manager"
    "&payment_account_id={account_id}"
    "&date={start_ts}_{end_ts}"
)

ACTION_SELECTOR = (
    "td:last-child a, "
    "td:last-child button, "
    "td:last-child [role='button']"
)


def _ensure_session(playwright) -> dict:
    """
    Return storage_state dict.
    If fb_session.json exists load it; otherwise open a visible browser
    for the user to log in and save the session.
    """
    if SESSION_FILE.exists():
        logger.info("Loading saved Facebook session from %s", SESSION_FILE)
        import json
        with open(SESSION_FILE) as f:
            return json.load(f)

    logger.info("No saved session found -- opening browser for login...")
    browser = playwright.chromium.launch(headless=False, slow_mo=50)
    ctx = browser.new_context()
    page = ctx.new_page()
    page.goto("https://www.facebook.com/login")

    print("\n" + "=" * 60)
    print("  Please log into Facebook in the browser window.")
    print("  Once you are fully logged in, come back here and")
    print("  press ENTER to continue.")
    print("=" * 60 + "\n")
    input("Press ENTER after logging in > ")

    state = ctx.storage_state(path=str(SESSION_FILE))
    browser.close()
    logger.info("Session saved to %s", SESSION_FILE)
    return state


def _log_clickable_elements(page, account_id: str) -> None:
    """
    Dump the text/aria-label of every visible clickable element to the log.
    This is critical for debugging when the 'See more' selector doesn't match —
    it tells us exactly what Facebook has rendered on the page.
    """
    try:
        elements = page.evaluate("""() => {
            const results = [];
            const all = document.querySelectorAll(
                '[role="button"], button, a, [tabindex="0"]'
            );
            for (const el of all) {
                if (el.offsetParent !== null) {
                    const text = (el.innerText || el.textContent || '').trim().substring(0, 80);
                    const aria = el.getAttribute('aria-label') || '';
                    const testid = el.getAttribute('data-testid') || '';
                    if (text || aria) {
                        results.push(
                            el.tagName + '[role=' + (el.getAttribute('role') || '-') + '] '
                            + 'text="' + text + '" '
                            + 'aria="' + aria + '" '
                            + 'testid="' + testid + '"'
                        );
                    }
                }
            }
            // Return last 40 elements (near the bottom of the page)
            return results.slice(-40);
        }""")
        # Encode to ASCII, replacing non-ASCII chars, so Windows cp1252 console
        # doesn't crash the log handler when button text contains Unicode/emoji
        safe_lines = [
            e.encode("ascii", errors="replace").decode("ascii") for e in elements
        ]
        logger.info(
            "Visible clickable elements for %s (%d total shown):\n  %s",
            account_id, len(safe_lines), "\n  ".join(safe_lines)
        )
    except Exception as e:
        logger.warning("Could not log clickable elements for %s: %s", account_id, e)


def _find_and_click_show_more(page) -> bool:
    """
    Try every known strategy to find and click a Facebook 'See more' pagination
    button. Returns True if a button was found and clicked, False otherwise.
    """
    # ── Strategy 1: CSS :has-text() selectors ──────────────────────────────────
    CSS_SELECTORS = [
        "div[role='button']:has-text('See more')",
        "div[role='button']:has-text('Show more')",
        "div[role='button']:has-text('Load more')",
        "div[role='button']:has-text('See More')",
        "span[role='button']:has-text('See more')",
        "span[role='button']:has-text('See More')",
        "button:has-text('See more')",
        "button:has-text('Show more')",
        "button:has-text('Load more')",
        "a:has-text('See more')",
        "a:has-text('See More')",
        "[data-testid*='see_more']",
        "[data-testid*='show_more']",
        "[aria-label*='See more']",
        "[aria-label*='Show more']",
        "[aria-label*='Load more']",
        # Facebook sometimes uses these class patterns
        "[class*='seeMore']",
        "[class*='see-more']",
        "[class*='loadMore']",
        "[class*='load-more']",
    ]
    for selector in CSS_SELECTORS:
        try:
            elems = page.query_selector_all(selector)
            for el in elems:
                if el.is_visible():
                    el.scroll_into_view_if_needed()
                    time.sleep(0.3)
                    el.click()
                    logger.info("Clicked pagination button via CSS: %s", selector)
                    return True
        except Exception:
            pass

    # ── Strategy 2: JavaScript — exact text match on all clickable elements ────
    clicked = page.evaluate("""() => {
        const keywords = ['see more', 'show more', 'load more', 'see all'];
        const all = document.querySelectorAll(
            'div[role="button"], span[role="button"], button, a, [tabindex="0"]'
        );
        for (const el of all) {
            const text = (el.innerText || el.textContent || '').trim().toLowerCase();
            if (keywords.some(k => text === k) && el.offsetParent !== null) {
                el.scrollIntoView({block: 'center'});
                el.click();
                return true;
            }
        }
        return false;
    }""")
    if clicked:
        logger.info("Clicked pagination button via JS exact-text scan")
        return True

    # ── Strategy 3: JavaScript — partial text match near the table footer ──────
    clicked = page.evaluate("""() => {
        const keywords = ['more', 'load', 'next'];
        // Look specifically near table/billing container footers
        const containers = document.querySelectorAll('table, [role="table"], [class*="billing"], [class*="payment"]');
        for (const container of containers) {
            const siblings = [];
            let el = container.nextElementSibling;
            for (let i = 0; i < 5 && el; i++, el = el.nextElementSibling) {
                siblings.push(...el.querySelectorAll('[role="button"], button, a'));
            }
            for (const btn of siblings) {
                const text = (btn.innerText || btn.textContent || '').trim().toLowerCase();
                if (keywords.some(k => text.includes(k)) && btn.offsetParent !== null) {
                    btn.scrollIntoView({block: 'center'});
                    btn.click();
                    return true;
                }
            }
        }
        return false;
    }""")
    if clicked:
        logger.info("Clicked pagination button via JS table-footer scan")
        return True

    return False


def _load_all_rows(page, account_id: str, dest_dir: Path) -> None:
    """
    Load all transaction rows by:
      1. Scrolling incrementally — handles infinite-scroll / lazy rendering.
      2. When scrolling stops adding rows, tries 'See more' pagination buttons.
      3. Repeats until the table is stable.

    On the first pass we also dump all visible button texts to the log so we
    can debug selector mismatches if pagination fails.
    """
    prev_count = -1
    stable_rounds = 0
    total_clicks = 0

    # Save a screenshot before we start so we can inspect the initial state
    try:
        shot = dest_dir / f"debug_before_scroll_{account_id}.png"
        page.screenshot(path=str(shot))
        logger.info("Saved pre-scroll screenshot: %s", shot)
    except Exception:
        pass

    for round_num in range(80):  # safety cap — handles up to ~800 rows
        # Scroll to bottom to trigger any lazy / infinite-scroll loading
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(0.8)

        cur_count = len(page.query_selector_all(ACTION_SELECTOR))

        # First round: dump all clickable elements so we know what's on the page
        if round_num == 0:
            logger.info(
                "After initial scroll: %d download row(s) visible for %s",
                cur_count, account_id,
            )
            _log_clickable_elements(page, account_id)

        if cur_count > prev_count:
            if prev_count >= 0:
                logger.info(
                    "Scroll round %d: rows %d -> %d for %s",
                    round_num + 1, prev_count, cur_count, account_id,
                )
            stable_rounds = 0
        else:
            stable_rounds += 1

        # After 2 stable rounds (scrolling adds nothing), try the button
        if stable_rounds >= 2:
            clicked = _find_and_click_show_more(page)
            if clicked:
                total_clicks += 1
                time.sleep(2.0)
                new_count = len(page.query_selector_all(ACTION_SELECTOR))
                logger.info(
                    "Pagination click %d: rows %d -> %d for %s",
                    total_clicks, cur_count, new_count, account_id,
                )
                if new_count > cur_count:
                    stable_rounds = 0
                    prev_count = new_count
                    continue
                else:
                    # Button clicked but no new rows — dump current state and stop
                    logger.info(
                        "Pagination button had no effect — table fully loaded: "
                        "%d rows for %s", cur_count, account_id,
                    )
                    _log_clickable_elements(page, account_id)
                    break
            else:
                logger.info(
                    "No pagination button found — table fully loaded: "
                    "%d rows for %s", cur_count, account_id,
                )
                break

        prev_count = cur_count

    # Scroll back to top so all download anchors are in DOM and clickable
    page.evaluate("window.scrollTo(0, 0)")
    time.sleep(0.5)


def download_receipts_for_account(
    account_id: str,
    business_id: str,
    start_date: datetime,
    end_date: datetime,
    base_dir: Path | None = None,
) -> list[Path]:
    """
    Download all receipt PDFs for one ad account over the given period.
    Returns a list of Paths to the downloaded PDFs.

    base_dir: root folder for this run (e.g. INVOICES/2026-03-10_2026-03-17/).
              Defaults to RECEIPT_DOWNLOAD_DIR if not supplied.
    """
    # Give a 5-day buffer before start_date so billing events that cover
    # the period boundary are not missed
    from datetime import timedelta
    buffered_start = start_date - timedelta(days=5)
    start_ts = int(buffered_start.timestamp())
    end_ts = int(end_date.timestamp())
    url = FB_BILLING_URL.format(
        account_id=account_id,
        business_id=business_id,
        start_ts=start_ts,
        end_ts=end_ts,
    )

    safe_account = account_id.replace("act_", "")
    root = base_dir if base_dir is not None else RECEIPT_DOWNLOAD_DIR
    dest_dir = root / safe_account
    dest_dir.mkdir(parents=True, exist_ok=True)

    downloaded: list[Path] = []

    with sync_playwright() as p:
        storage_state = _ensure_session(p)
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            storage_state=storage_state,
            accept_downloads=True,
        )
        page = ctx.new_page()

        logger.info("Navigating to billing page for account %s", account_id)
        page.goto(url, wait_until="networkidle", timeout=60_000)

        # Wait for the transactions table to appear
        try:
            page.wait_for_selector(
                "[data-testid='billing-payment-activity-table'], "
                "table, "
                ".billing-transactions",
                timeout=20_000,
            )
        except PWTimeout:
            logger.warning(
                "Transactions table not found for %s -- page may be empty", account_id
            )
            browser.close()
            return downloaded

        # Wait for at least one clickable element in the Action column
        try:
            page.wait_for_selector(ACTION_SELECTOR, timeout=25_000)
        except PWTimeout:
            debug_path = dest_dir / f"debug_no_actions_{account_id}.png"
            page.screenshot(path=str(debug_path))
            logger.warning(
                "No action elements appeared within 25s for %s -- "
                "saved debug screenshot to %s",
                account_id, debug_path,
            )
            browser.close()
            return downloaded

        # Scroll + paginate until all rows are loaded
        _load_all_rows(page, account_id=safe_account, dest_dir=dest_dir)

        # Collect every download element (FB uses <a> tags)
        download_buttons = page.query_selector_all(ACTION_SELECTOR)
        logger.info(
            "Found %d download button(s) for account %s",
            len(download_buttons), account_id,
        )

        for i, btn in enumerate(download_buttons):
            try:
                with page.expect_download(timeout=30_000) as dl_info:
                    btn.click()
                download = dl_info.value
                filename = download.suggested_filename or f"receipt_{i + 1}.pdf"
                if not filename.lower().endswith(".pdf"):
                    filename += ".pdf"
                dest_path = dest_dir / filename
                download.save_as(str(dest_path))
                logger.info("Downloaded receipt -> %s", dest_path)
                downloaded.append(dest_path)
                time.sleep(1)
            except PWTimeout:
                logger.warning(
                    "Download timed out for button %d on account %s", i + 1, account_id
                )
            except Exception as e:
                logger.error(
                    "Error downloading button %d on account %s: %s", i + 1, account_id, e
                )

        browser.close()

    return downloaded


def download_receipts_for_all_accounts(
    account_ids: list[str],
    start_date: datetime,
    end_date: datetime,
    business_id: str | None = None,
) -> dict[str, list[Path]]:
    """
    Download receipts for multiple ad accounts.
    Returns mapping of account_id -> list of PDF paths.
    """
    bid = business_id or (META_BUSINESS_IDS[0] if META_BUSINESS_IDS else "")
    results: dict[str, list[Path]] = {}

    for account_id in account_ids:
        safe_id = account_id.replace("act_", "")
        paths = download_receipts_for_account(safe_id, bid, start_date, end_date)
        results[account_id] = paths

    return results
