"""
Meta Marketing API client for fetching ad account invoices and receipts.

Walks: Business Manager -> Ad Accounts -> Invoices/Transactions
Downloads invoice PDFs to local storage.
"""

import logging
import requests
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from src.config import META_ACCESS_TOKEN, META_BUSINESS_IDS, META_BASE_URL, RECEIPT_DOWNLOAD_DIR

logger = logging.getLogger(__name__)


class MetaClient:
    def __init__(self, access_token: str = META_ACCESS_TOKEN):
        self.access_token = access_token
        self.session = requests.Session()
        self.session.params = {"access_token": self.access_token}

    def _get(self, endpoint: str, params: dict | None = None, _silent_codes: tuple = ()) -> dict:
        url = f"{META_BASE_URL}/{endpoint}"
        resp = self.session.get(url, params=params or {})
        if not resp.ok:
            # Suppress ERROR logging for known/expected permission failures
            # (e.g. pages_read_engagement required for post attachments).
            # The caller's except block handles these gracefully.
            if resp.status_code not in _silent_codes:
                logger.error("Meta API error %s for %s: %s", resp.status_code, url, resp.text)
        resp.raise_for_status()
        return resp.json()

    def _get_paginated(self, endpoint: str, params: dict | None = None) -> list:
        results = []
        data = self._get(endpoint, params)
        results.extend(data.get("data", []))
        while "paging" in data and "next" in data["paging"]:
            resp = self.session.get(data["paging"]["next"])
            resp.raise_for_status()
            data = resp.json()
            results.extend(data.get("data", []))
        return results

    def get_ad_accounts(self, business_id: str) -> list[dict]:
        """Get all ad accounts owned by or shared with a business."""
        owned = self._get_paginated(
            f"{business_id}/owned_ad_accounts",
            {"fields": "id,name,account_id,currency,business_name,account_status"},
        )
        client = self._get_paginated(
            f"{business_id}/client_ad_accounts",
            {"fields": "id,name,account_id,currency,business_name,account_status"},
        )
        all_accounts = {a["id"]: a for a in owned + client}
        logger.info(
            "Business %s: found %d ad accounts", business_id, len(all_accounts)
        )
        return list(all_accounts.values())

    def get_all_ad_accounts(self) -> list[dict]:
        """Get ad accounts across all configured business managers."""
        accounts = []
        for bid in META_BUSINESS_IDS:
            try:
                accounts.extend(self.get_ad_accounts(bid))
            except requests.HTTPError as e:
                logger.error("Failed to fetch ad accounts for business %s: %s", bid, e)
        return accounts

    def get_spend(
        self,
        ad_account_id: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> list[dict]:
        """
        Fetch spend data for an ad account via the Insights API.
        Works for all account types (credit card, prepay, and invoiced).
        Returns one record per day with spend > 0.
        """
        if not ad_account_id.startswith("act_"):
            ad_account_id = f"act_{ad_account_id}"

        params = {
            "fields": "account_name,spend,impressions,clicks,date_start,date_stop",
            "time_range": f'{{"since":"{start_date.strftime("%Y-%m-%d")}","until":"{end_date.strftime("%Y-%m-%d")}"}}'
            if start_date and end_date
            else None,
            "time_increment": 1,
            "level": "account",
        }
        params = {k: v for k, v in params.items() if v is not None}

        try:
            results = self._get_paginated(f"{ad_account_id}/insights", params)
            # Filter to days that actually had spend
            results = [r for r in results if float(r.get("spend", 0)) > 0]
            logger.info(
                "Ad account %s: found %d days with spend", ad_account_id, len(results)
            )
            return results
        except requests.HTTPError as e:
            logger.warning("Could not fetch insights for %s: %s", ad_account_id, e)
            return []

    def get_ad_images(
        self,
        ad_account_id: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        max_images: int = 0,  # 0 = no limit
        base_dir: Optional[Path] = None,
    ) -> list[Path]:
        """
        Download thumbnail images for ads that actually ran during the billing period.

        Strategy:
          1. Use the Insights API (level=ad) split into 90-day chunks to find ad IDs
             that had impressions — Meta's ad-level insights degrade beyond ~90 days.
          2. Fetch creative thumbnails for exactly those ad IDs (deduplicated).
          3. Fall back to most-recent ads if Insights returns nothing.

        base_dir: root folder for this run (e.g. INVOICES/2026-03-10_2026-03-17/).
                  Defaults to RECEIPT_DOWNLOAD_DIR if not supplied.
        """
        if not ad_account_id.startswith("act_"):
            ad_account_id = f"act_{ad_account_id}"

        safe_account = ad_account_id.replace("act_", "")
        root = base_dir if base_dir is not None else RECEIPT_DOWNLOAD_DIR
        img_dir = root / safe_account / "images"
        img_dir.mkdir(parents=True, exist_ok=True)

        # ── Step 1: find ad IDs with spend in the period via Insights ────────
        # Split into 90-day chunks — Meta's level=ad insights become unreliable
        # for single requests spanning more than ~90 days.
        ad_ids: list[str] = []
        if start_date and end_date:
            chunks: list[tuple[datetime, datetime]] = []
            chunk_start = start_date
            while chunk_start < end_date:
                chunk_end = min(chunk_start + timedelta(days=90), end_date)
                chunks.append((chunk_start, chunk_end))
                chunk_start = chunk_end + timedelta(days=1)

            logger.info(
                "Fetching ad images across %d x 90-day chunk(s) for %s",
                len(chunks), ad_account_id,
            )

            for cs, ce in chunks:
                try:
                    insight_params = {
                        "fields": "ad_id,ad_name",
                        "level": "ad",
                        "time_range": (
                            f'{{"since":"{cs.strftime("%Y-%m-%d")}",'
                            f'"until":"{ce.strftime("%Y-%m-%d")}"}}'
                        ),
                    }
                    rows = self._get_paginated(f"{ad_account_id}/insights", insight_params)
                    chunk_ids = [r["ad_id"] for r in rows if r.get("ad_id")]
                    logger.info(
                        "  Chunk %s to %s: %d ad ID(s)",
                        cs.strftime("%Y-%m-%d"), ce.strftime("%Y-%m-%d"), len(chunk_ids),
                    )
                    ad_ids.extend(chunk_ids)
                except requests.HTTPError as e:
                    logger.warning(
                        "Could not fetch insights chunk %s-%s for %s: %s",
                        cs.strftime("%Y-%m-%d"), ce.strftime("%Y-%m-%d"), ad_account_id, e,
                    )

            # Deduplicate while preserving chronological order
            seen: set[str] = set()
            unique_ids: list[str] = []
            for aid in ad_ids:
                if aid not in seen:
                    seen.add(aid)
                    unique_ids.append(aid)
            ad_ids = unique_ids
            logger.info(
                "Total unique ad IDs with spend in period for %s: %d",
                ad_account_id, len(ad_ids),
            )

        # ── Step 2: fetch creatives for those specific ad IDs ────────────────
        # image_hash lets us call /{account}/adimages for the true full-res URL
        # when object_story_spec is absent (common for Ads-Manager-only creatives).
        CREATIVE_FIELDS = (
            "name,"
            "creative{"
            "image_url,"
            "thumbnail_url,"
            "image_hash,"
            "video_id,"
            "effective_object_story_id,"
            "object_story_spec{link_data{picture,image_hash},photo_data{url,image_hash},video_data{image_url,video_id}}"
            "}"
        )

        ads: list[dict] = []
        if ad_ids:
            for ad_id in dict.fromkeys(ad_ids):  # preserves order, dedupes
                try:
                    ad_data = self._get(ad_id, {"fields": CREATIVE_FIELDS})
                    ads.append(ad_data)
                except requests.HTTPError as e:
                    logger.warning("Could not fetch creative for ad %s: %s", ad_id, e)
        else:
            # Fallback: no Insights data — fetch most recent ads for this account
            logger.info(
                "No Insights ad IDs found for %s — falling back to recent ads", ad_account_id
            )
            try:
                ads = self._get_paginated(
                    f"{ad_account_id}/ads",
                    {"fields": CREATIVE_FIELDS},
                )
            except requests.HTTPError as e:
                logger.warning("Could not fetch ads for %s: %s", ad_account_id, e)
                return []

        # ── Step 3: download images, deduplicating by URL ─────────────────────
        # Resolution priority (highest first):
        #   1. object_story_spec.link_data.picture  — original uploaded image (1200px+)
        #   2. object_story_spec.photo_data.url     — photo ads
        #   3. object_story_spec.video_data.image_url — video ad cover frame
        #   4. adimages API via image_hash          — full-res for Ads Manager creatives
        #   5. image_url                            — rendered creative (~600px)
        #   6. thumbnail_url                        — last resort, low-res preview only

        seen_urls: set[str] = set()
        downloaded: list[Path] = []

        for ad in ads:
            if max_images and len(downloaded) >= max_images:  # 0 = no limit
                break

            creative = ad.get("creative") or {}
            spec = creative.get("object_story_spec") or {}
            link = spec.get("link_data") or {}
            photo = spec.get("photo_data") or {}
            video = spec.get("video_data") or {}

            url = (
                link.get("picture")
                or photo.get("url")
                or video.get("image_url")
            )

            # If object_story_spec didn't give us a URL, try the adimages endpoint
            # using the creative's image_hash — this always returns the full-res original
            if not url:
                image_hash = creative.get("image_hash") or link.get("image_hash")
                if image_hash:
                    try:
                        img_data = self._get(
                            f"{ad_account_id}/adimages",
                            {"hashes": f'["{image_hash}"]', "fields": "url,width,height"},
                        )
                        entries = img_data.get("data", [])
                        if entries:
                            url = entries[0].get("url")
                            w = entries[0].get("width", "?")
                            h = entries[0].get("height", "?")
                            logger.info(
                                "Resolved full-res via adimages hash: %sx%s for ad %s",
                                w, h, ad.get("id", "?"),
                            )
                    except Exception as e:
                        logger.warning(
                            "adimages hash lookup failed for ad %s: %s", ad.get("id"), e
                        )

            # For video ads: fetch the video cover thumbnail at full resolution.
            # video_id is available on the creative for ads-manager video creatives.
            # Also check object_story_spec.video_data.video_id for boosted posts.
            if not url:
                video_id = (
                    creative.get("video_id")
                    or (video.get("video_id") if video else None)
                )
                if video_id:
                    try:
                        thumb_data = self._get(
                            f"{video_id}/thumbnails",
                            {"fields": "uri,width,height"},
                        )
                        thumbs = thumb_data.get("data", [])
                        if thumbs:
                            # Pick the largest thumbnail by pixel area
                            best = max(
                                thumbs,
                                key=lambda t: (t.get("width") or 0) * (t.get("height") or 0),
                            )
                            url = best.get("uri")
                            logger.info(
                                "Resolved video thumbnail %sx%s for video %s",
                                best.get("width", "?"), best.get("height", "?"), video_id,
                            )
                    except Exception as e:
                        logger.warning(
                            "Video thumbnail lookup failed for video %s: %s", video_id, e
                        )

            # Last API fallback: request a square (1:1) rendered creative thumbnail.
            # Square avoids the horizontal-strip problem that happens with width-only
            # requests for video creatives, and matches the most common FB ad format.
            if not url:
                creative_id = creative.get("id")
                if creative_id:
                    try:
                        cr_data = self._get(
                            creative_id,
                            {
                                "fields": "thumbnail_url",
                                "thumbnail_width": 1080,
                                "thumbnail_height": 1080,
                            },
                        )
                        url = cr_data.get("thumbnail_url")
                        if url:
                            logger.info(
                                "Resolved 1080x1080 square thumbnail for creative %s",
                                creative_id,
                            )
                    except Exception as e:
                        logger.warning(
                            "Creative thumbnail lookup failed for %s: %s", creative_id, e
                        )

            # Absolute last resort — use image_url (rendered preview) or thumbnail
            if not url:
                url = creative.get("image_url") or creative.get("thumbnail_url")

            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            try:
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()
                content_type = resp.headers.get("content-type", "")
                if "jpeg" in content_type or "jpg" in content_type:
                    ext = ".jpg"
                elif "png" in content_type:
                    ext = ".png"
                elif "gif" in content_type:
                    ext = ".gif"
                elif "webp" in content_type:
                    ext = ".webp"
                else:
                    ext = ".jpg"  # safe default for Facebook images
                safe_name = "".join(
                    c if c.isalnum() else "_" for c in ad.get("name", "ad")
                )[:40]
                dest = img_dir / f"{safe_name}{ext}"
                dest.write_bytes(resp.content)
                downloaded.append(dest)
                logger.info(
                    "Downloaded ad image (%d KB): %s",
                    len(resp.content) // 1024, dest,
                )
            except Exception as e:
                logger.warning("Could not download ad image for ad %s: %s", ad.get("id"), e)

        logger.info(
            "Ad account %s: downloaded %d ad image(s) for period", ad_account_id, len(downloaded)
        )
        return downloaded

    def download_invoice_pdf(
        self, ad_account_id: str, invoice_id: str
    ) -> Path | None:
        """
        Download an invoice PDF from Meta.

        Meta provides a download URL at /{invoice_id}?fields=download_uri
        """
        try:
            data = self._get(invoice_id, {"fields": "download_uri"})
            download_url = data.get("download_uri")
            if not download_url:
                logger.warning("No download URI for invoice %s", invoice_id)
                return None

            resp = self.session.get(download_url)
            resp.raise_for_status()

            safe_account = ad_account_id.replace("act_", "")
            dest = RECEIPT_DOWNLOAD_DIR / safe_account
            dest.mkdir(parents=True, exist_ok=True)

            filepath = dest / f"{invoice_id}.pdf"
            filepath.write_bytes(resp.content)
            logger.info("Downloaded invoice %s -> %s", invoice_id, filepath)
            return filepath

        except Exception as e:
            logger.error("Failed to download invoice %s: %s", invoice_id, e)
            return None

    def get_transactions(
        self,
        ad_account_id: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> list[dict]:
        """
        Fetch billing transactions (individual charges/payments) for an ad account.
        Returns one record per billing event — matches Facebook's Payment Activity view.

        Each transaction has: id, time, amount, status (paid/failed), payment method info.
        """
        if not ad_account_id.startswith("act_"):
            ad_account_id = f"act_{ad_account_id}"

        params = {
            "fields": "id,time,amount,currency,status,payment_option",
        }
        if start_date and end_date:
            params["time_range"] = (
                f'{{"since":"{start_date.strftime("%Y-%m-%d")}",'
                f'"until":"{end_date.strftime("%Y-%m-%d")}"}}'
            )

        try:
            results = self._get_paginated(f"{ad_account_id}/transactions", params)
            logger.info("Ad account %s: found %d transactions", ad_account_id, len(results))
            return results
        except requests.HTTPError as e:
            logger.warning("Could not fetch transactions for %s: %s", ad_account_id, e)
            return []

    def get_campaign_spend(
        self,
        ad_account_id: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> list[dict]:
        """
        Fetch campaign-level spend breakdown for the period.
        Returns one record per campaign with spend > 0.
        """
        if not ad_account_id.startswith("act_"):
            ad_account_id = f"act_{ad_account_id}"

        params = {
            "fields": "campaign_name,campaign_id,spend,impressions,clicks,date_start,date_stop",
            "level": "campaign",
        }
        if start_date and end_date:
            params["time_range"] = (
                f'{{"since":"{start_date.strftime("%Y-%m-%d")}",'
                f'"until":"{end_date.strftime("%Y-%m-%d")}"}}'
            )

        try:
            results = self._get_paginated(f"{ad_account_id}/insights", params)
            results = [r for r in results if float(r.get("spend", 0)) > 0]
            logger.info("Ad account %s: %d campaigns with spend", ad_account_id, len(results))
            return results
        except requests.HTTPError as e:
            logger.warning("Could not fetch campaign insights for %s: %s", ad_account_id, e)
            return []

    def fetch_receipts_for_account(
        self,
        ad_account_id: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> list[dict]:
        """
        Fetch all receipt data for an ad account.
        Returns daily spend rows + campaign breakdown + billing transactions.
        """
        if start_date is None:
            start_date = datetime.now() - timedelta(days=35)
        if end_date is None:
            end_date = datetime.now()

        spend_rows = self.get_spend(ad_account_id, start_date, end_date)
        campaigns = self.get_campaign_spend(ad_account_id, start_date, end_date)

        receipts = []
        for row in spend_rows:
            receipts.append({
                "type": "spend",
                "ad_account_id": ad_account_id,
                "invoice_id": f"{row.get('date_start')}_{row.get('date_stop')}",
                "amount": row.get("spend"),
                "currency": "USD",
                "date": row.get("date_start"),
                "status": "charged",
                "impressions": row.get("impressions"),
                "clicks": row.get("clicks"),
                "pdf_path": None,
                "raw": row,
            })

        return receipts
