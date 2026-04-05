"""
Video-to-animated-GIF generator for email composer.
Downloads a short clip from YouTube/Vimeo/Facebook and converts to GIF.
Requires: yt-dlp, ffmpeg
"""
import logging
import os
import re
import subprocess
import uuid

logger = logging.getLogger(__name__)

UPLOAD_DIR = os.path.join("static", "uploads")
GIF_SECONDS = 4        # duration of the animated GIF
GIF_WIDTH = 400         # pixel width (height auto)
GIF_FPS = 10            # frames per second


def parse_video_url(url: str) -> dict | None:
    """
    Parse a video URL and return {platform, video_id, url}.
    Supports YouTube, Vimeo, and Facebook.
    """
    url = url.strip()

    # YouTube: various formats
    yt = (
        re.match(r'(?:https?://)?(?:www\.)?youtube\.com/watch\?v=([a-zA-Z0-9_-]{11})', url)
        or re.match(r'(?:https?://)?youtu\.be/([a-zA-Z0-9_-]{11})', url)
        or re.match(r'(?:https?://)?(?:www\.)?youtube\.com/embed/([a-zA-Z0-9_-]{11})', url)
        or re.match(r'(?:https?://)?(?:www\.)?youtube\.com/shorts/([a-zA-Z0-9_-]{11})', url)
    )
    if yt:
        vid = yt.group(1)
        return {
            "platform": "youtube",
            "video_id": vid,
            "url": f"https://www.youtube.com/watch?v={vid}",
            "thumbnail": f"https://img.youtube.com/vi/{vid}/hqdefault.jpg",
        }

    # Vimeo
    vm = re.match(r'(?:https?://)?(?:www\.)?vimeo\.com/(\d+)', url)
    if vm:
        vid = vm.group(1)
        return {
            "platform": "vimeo",
            "video_id": vid,
            "url": f"https://vimeo.com/{vid}",
            "thumbnail": None,  # fetched via oEmbed
        }

    # Facebook video
    fb = re.match(r'(?:https?://)?(?:www\.)?facebook\.com/.+/videos/(\d+)', url)
    if not fb:
        fb = re.match(r'(?:https?://)?(?:www\.)?fb\.watch/([a-zA-Z0-9_-]+)', url)
    if fb:
        return {
            "platform": "facebook",
            "video_id": fb.group(1),
            "url": url,
            "thumbnail": None,
        }

    # Generic — try yt-dlp on anything
    if url.startswith("http"):
        return {
            "platform": "other",
            "video_id": uuid.uuid4().hex[:8],
            "url": url,
            "thumbnail": None,
        }

    return None


def get_static_thumbnail(info: dict) -> str | None:
    """Return a static thumbnail URL (no download needed) for YouTube."""
    if info["platform"] == "youtube":
        return info["thumbnail"]
    return None


def generate_gif(video_url: str, output_name: str | None = None) -> dict:
    """
    Download first few seconds of a video and convert to animated GIF.
    Returns {"ok": True, "gif_url": "/static/uploads/xxx.gif", "video_url": "..."}
    or {"ok": False, "error": "..."}.
    """
    os.makedirs(UPLOAD_DIR, exist_ok=True)

    fname = output_name or uuid.uuid4().hex[:12]
    tmp_video = os.path.join(UPLOAD_DIR, f"{fname}_tmp.mp4")
    palette_path = os.path.join(UPLOAD_DIR, f"{fname}_palette.png")
    gif_path = os.path.join(UPLOAD_DIR, f"{fname}.gif")

    try:
        # Step 1: Download first N seconds with yt-dlp
        logger.info("Downloading video clip: %s", video_url)
        dl_cmd = [
            "yt-dlp",
            "--no-playlist",
            "--format", "worst[ext=mp4]/worst",   # smallest format for speed
            "--download-sections", f"*0-{GIF_SECONDS}",
            "--force-keyframes-at-cuts",
            "--output", tmp_video,
            "--no-warnings",
            "--quiet",
            video_url,
        ]
        result = subprocess.run(dl_cmd, capture_output=True, text=True, timeout=60)
        if result.returncode != 0:
            # Fallback: try without --download-sections (older yt-dlp or unsupported)
            dl_cmd2 = [
                "yt-dlp",
                "--no-playlist",
                "--format", "worst[ext=mp4]/worst",
                "--output", tmp_video,
                "--no-warnings",
                "--quiet",
                "--external-downloader-args", f"ffmpeg:-t {GIF_SECONDS}",
                video_url,
            ]
            result2 = subprocess.run(dl_cmd2, capture_output=True, text=True, timeout=60)
            if result2.returncode != 0:
                return {"ok": False, "error": f"Download failed: {result.stderr[:200]}"}

        if not os.path.exists(tmp_video):
            # yt-dlp might add format suffix — find the file
            import glob
            candidates = glob.glob(os.path.join(UPLOAD_DIR, f"{fname}_tmp*"))
            if candidates:
                tmp_video = candidates[0]
            else:
                return {"ok": False, "error": "Downloaded video file not found"}

        # Step 2: Generate palette for high-quality GIF
        logger.info("Generating GIF palette...")
        palette_cmd = [
            "ffmpeg", "-y",
            "-t", str(GIF_SECONDS),
            "-i", tmp_video,
            "-vf", f"fps={GIF_FPS},scale={GIF_WIDTH}:-1:flags=lanczos,palettegen=stats_mode=diff",
            palette_path,
        ]
        subprocess.run(palette_cmd, capture_output=True, timeout=30)

        # Step 3: Generate GIF with palette
        logger.info("Converting to animated GIF...")
        gif_cmd = [
            "ffmpeg", "-y",
            "-t", str(GIF_SECONDS),
            "-i", tmp_video,
            "-i", palette_path,
            "-lavfi", f"fps={GIF_FPS},scale={GIF_WIDTH}:-1:flags=lanczos[x];[x][1:v]paletteuse=dither=bayer:bayer_scale=3",
            gif_path,
        ]
        result = subprocess.run(gif_cmd, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            return {"ok": False, "error": f"GIF conversion failed: {result.stderr[:200]}"}

        # Check file size — if over 2MB, reduce quality
        gif_size = os.path.getsize(gif_path)
        if gif_size > 2 * 1024 * 1024:
            logger.info("GIF too large (%d bytes), reducing...", gif_size)
            reduced_cmd = [
                "ffmpeg", "-y",
                "-t", str(GIF_SECONDS),
                "-i", tmp_video,
                "-vf", f"fps={GIF_FPS // 2},scale={GIF_WIDTH - 80}:-1:flags=lanczos",
                gif_path,
            ]
            subprocess.run(reduced_cmd, capture_output=True, timeout=30)

        gif_url = f"/static/uploads/{fname}.gif"
        logger.info("GIF generated: %s (%d bytes)", gif_url, os.path.getsize(gif_path))
        return {"ok": True, "gif_url": gif_url}

    except subprocess.TimeoutExpired:
        return {"ok": False, "error": "Video download timed out"}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}
    finally:
        # Cleanup temp files
        for f in [tmp_video, palette_path]:
            try:
                os.remove(f)
            except OSError:
                pass
        # Also clean up any yt-dlp suffixed variants
        import glob
        for f in glob.glob(os.path.join(UPLOAD_DIR, f"{fname}_tmp*")):
            try:
                os.remove(f)
            except OSError:
                pass
