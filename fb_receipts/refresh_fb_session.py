"""
Refreshes the saved Facebook browser session (fb_session.json).

Opens a visible browser window — log in to Facebook, then wait.
The script detects login automatically and saves the session.

Run via the portal Settings tab or directly:
    python refresh_fb_session.py
"""

import sys
from pathlib import Path
from playwright.sync_api import sync_playwright

SESSION_FILE = Path("fb_session.json")


def main():
    if SESSION_FILE.exists():
        SESSION_FILE.unlink()
        print("Deleted old session.")

    print("Opening browser — please log in to Facebook...")
    print("The window will close automatically once login is detected.")
    sys.stdout.flush()

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False, slow_mo=50)
        ctx     = browser.new_context()
        page    = ctx.new_page()

        page.goto("https://www.facebook.com/login")

        # Wait up to 5 minutes for the user to finish logging in.
        # Login is complete when the URL no longer contains "login".
        try:
            page.wait_for_url(
                lambda url: "login" not in url and "facebook.com" in url,
                timeout=300_000,
            )
        except Exception:
            print("Timed out waiting for login. Please try again.")
            browser.close()
            sys.exit(1)

        # Give Facebook a moment to settle after redirect
        page.wait_for_timeout(2_000)

        ctx.storage_state(path=str(SESSION_FILE))
        browser.close()

    print(f"Session saved to {SESSION_FILE}")
    print("You can close this window.")


if __name__ == "__main__":
    main()
