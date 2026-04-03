"""
Sanitize marketing-email HTML for fast, safe rendering in an iframe.

Strategy: keep the original HTML structure intact (preserves appearance
across HubSpot, Mailchimp, Campaign Monitor, etc.) but surgically remove
elements that cause slow loads or security risks:
  - Tracking pixels (1x1 images)
  - <script> tags
  - External <link> stylesheets
  - Known tracker image patterns
  - Add loading="lazy" to all images
"""
import re


def sanitize_email_html(raw_html: str) -> str:
    """
    Clean raw marketing-email HTML for fast iframe rendering.
    Keeps original layout, strips tracking junk, lazy-loads images.
    """
    if not raw_html:
        return ""

    html = raw_html

    # 1. Remove <script> blocks entirely
    html = re.sub(r"<script[^>]*>.*?</script>", "", html, flags=re.I | re.S)

    # 2. Remove external <link> stylesheets (keep <style> blocks — they're inline)
    html = re.sub(r"<link[^>]*stylesheet[^>]*/?>", "", html, flags=re.I)

    # 3. Remove tracking pixels: <img> with width="1" or height="1"
    html = re.sub(
        r'<img[^>]*(?:width\s*=\s*["\']?[01]["\']?|height\s*=\s*["\']?[01]["\']?)[^>]*/?>',
        "", html, flags=re.I
    )

    # 4. Remove common invisible tracker images (beacon, open-tracking, etc.)
    html = re.sub(
        r'<img[^>]*src\s*=\s*["\'][^"\']*(?:beacon|track|open|wf-|\.gif\?)[^"\']*["\'][^>]*/?>',
        "", html, flags=re.I
    )

    # 5. Add loading="lazy" and decoding="async" to remaining images for non-blocking load
    def _lazy_img(m):
        tag = m.group(0)
        if "loading=" in tag.lower():
            return tag  # already has it
        # Insert before the closing > or />
        tag = re.sub(r"\s*/?\s*>$", ' loading="lazy" decoding="async">', tag.rstrip())
        return tag

    html = re.sub(r"<img\b[^>]*?>", _lazy_img, html, flags=re.I | re.S)

    # 6. Remove HTML comments (often contain MSO conditionals that bloat size)
    html = re.sub(r"<!--.*?-->", "", html, flags=re.S)

    # 7. Remove <meta> and <title> tags (unnecessary in iframe context)
    html = re.sub(r"<meta[^>]*/?>", "", html, flags=re.I)
    html = re.sub(r"<title[^>]*>.*?</title>", "", html, flags=re.I | re.S)

    # 8. Collapse excessive whitespace (saves bytes)
    html = re.sub(r"\n\s*\n", "\n", html)

    return html
