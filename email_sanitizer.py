"""
Sanitize messy marketing-email HTML into clean, fast-loading HTML.

Strips tracking pixels, HubSpot/Mailchimp wrapper tables, inline styles,
and rebuilds the email as simple, readable HTML with minimal markup.
"""
import re
from html.parser import HTMLParser


class _EmailCleaner(HTMLParser):
    """Single-pass HTML parser that extracts content into clean markup."""

    # Tags whose content we skip entirely
    SKIP_TAGS = {"style", "script", "head", "title", "meta", "link"}
    # Tags we keep structurally
    BLOCK_TAGS = {"p", "div", "h1", "h2", "h3", "h4", "h5", "h6",
                  "li", "ul", "ol", "blockquote", "br", "hr", "pre"}
    INLINE_TAGS = {"a", "b", "strong", "i", "em", "u", "span", "sub", "sup"}

    def __init__(self):
        super().__init__()
        self._skip_depth = 0
        self._output = []
        self._images = []
        self._links = []
        self._in_link = None  # current <a> href

    def handle_starttag(self, tag, attrs):
        attrs_d = dict(attrs)
        tag = tag.lower()

        if tag in self.SKIP_TAGS:
            self._skip_depth += 1
            return
        if self._skip_depth:
            return

        # Images — keep real content images, skip tracking pixels
        if tag == "img":
            src = attrs_d.get("src", "")
            width = attrs_d.get("width", "")
            height = attrs_d.get("height", "")
            # Skip 1x1 tracking pixels
            if width in ("1", "0") or height in ("1", "0"):
                return
            # Skip common tracker patterns
            if not src or "track" in src.lower() or "open" in src.lower() or "beacon" in src.lower():
                return
            self._images.append(src)
            # Preserve alt text, constrain width
            alt = attrs_d.get("alt", "")
            self._output.append(
                f'<img src="{_esc(src)}" alt="{_esc(alt)}" '
                f'style="max-width:100%;height:auto;display:block;margin:8px 0;">'
            )
            return

        # Links
        if tag == "a":
            href = attrs_d.get("href", "")
            # Skip unsubscribe/manage/tracking-only links (and their content)
            if any(kw in href.lower() for kw in ("unsubscribe", "manage_preferences", "subscription", "manage-preferences")):
                self._in_link = "SKIP"
                return
            self._in_link = href
            self._output.append(f'<a href="{_esc(href)}" style="color:#0066cc;">')
            return

        # Block elements — emit as <p> or <br> for spacing
        if tag in self.BLOCK_TAGS:
            if tag == "br":
                self._output.append("<br>")
            elif tag == "hr":
                self._output.append('<hr style="border:none;border-top:1px solid #e0e0e0;margin:16px 0;">')
            elif tag in ("h1", "h2", "h3", "h4", "h5", "h6"):
                self._output.append(f"<{tag} style='margin:16px 0 8px;'>")
            elif tag in ("ul", "ol", "li"):
                self._output.append(f"<{tag}>")
            else:
                self._output.append("<p>")
            return

        # Inline formatting
        if tag in ("b", "strong"):
            self._output.append("<strong>")
        elif tag in ("i", "em"):
            self._output.append("<em>")

    def handle_endtag(self, tag):
        tag = tag.lower()
        if tag in self.SKIP_TAGS:
            self._skip_depth = max(0, self._skip_depth - 1)
            return
        if self._skip_depth:
            return

        if tag == "a":
            if self._in_link is not None:
                self._output.append("</a>")
            self._in_link = None
        elif tag in ("h1", "h2", "h3", "h4", "h5", "h6"):
            self._output.append(f"</{tag}>")
        elif tag in ("ul", "ol", "li"):
            self._output.append(f"</{tag}>")
        elif tag in self.BLOCK_TAGS and tag not in ("br", "hr"):
            self._output.append("</p>")
        elif tag in ("b", "strong"):
            self._output.append("</strong>")
        elif tag in ("i", "em"):
            self._output.append("</em>")

    # Common footer phrases to strip
    _FOOTER_PHRASES = ("paid for by", "unsubscribe", "manage preferences",
                       "view in browser", "view this email", "email preferences",
                       "update your preferences", "opt out")

    def handle_data(self, data):
        if self._skip_depth:
            return
        if self._in_link == "SKIP":
            return  # suppress text inside unsubscribe/manage links
        text = data.strip()
        if not text:
            return
        # Skip common footer boilerplate
        if any(phrase in text.lower() for phrase in self._FOOTER_PHRASES):
            return
        self._output.append(_esc(text) + " ")

    def get_clean_html(self) -> str:
        raw = "".join(self._output)
        # Collapse whitespace
        raw = re.sub(r"\s+", " ", raw)
        # Remove empty paragraphs
        raw = re.sub(r"<p>\s*</p>", "", raw)
        # Remove duplicate <br> runs
        raw = re.sub(r"(<br>\s*){3,}", "<br><br>", raw)
        return raw.strip()


def _esc(text: str) -> str:
    """Minimal HTML escaping."""
    return (text
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;"))


def sanitize_email_html(raw_html: str) -> str:
    """
    Take raw marketing-email HTML and return clean, fast-rendering HTML.
    The result is wrapped in a minimal document with system fonts.
    """
    if not raw_html:
        return ""

    parser = _EmailCleaner()
    try:
        parser.feed(raw_html)
    except Exception:
        # If parsing fails, fall back to text extraction
        text = re.sub(r"<[^>]+>", " ", raw_html)
        text = re.sub(r"\s+", " ", text).strip()
        return f"<p>{_esc(text)}</p>"

    inner = parser.get_clean_html()

    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
       color: #1d1d1f; line-height: 1.6; padding: 16px; margin: 0; font-size: 15px; }}
p {{ margin: 0 0 12px; }}
img {{ border-radius: 6px; }}
a {{ color: #0066cc; }}
h1,h2,h3 {{ color: #1d1d1f; }}
hr {{ border: none; border-top: 1px solid #e0e0e0; margin: 16px 0; }}
</style></head><body>{inner}</body></html>"""
