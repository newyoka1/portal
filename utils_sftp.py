"""Shared SFTP upload utility — uploads files to WP Engine."""
import os
import logging
import stat as _stat

logger = logging.getLogger(__name__)

def _cfg(key, default=""):
    """Read from portal DB settings, fall back to env var."""
    try:
        from portal_config import get_setting
        return get_setting(key, default)
    except Exception:
        return os.getenv(key, default)

# How many files to keep per remote directory (oldest deleted first)
MAX_REMOTE_FILES = int(os.getenv("SFTP_MAX_FILES", "20"))


def sftp_upload(local_path: str, remote_dir: str = "exports",
                cleanup: bool = True, max_files: int | None = None) -> str | None:
    """Upload a file via SFTP. Returns public URL or None on failure.

    After upload, removes the oldest files in remote_dir if the count
    exceeds max_files (default: MAX_REMOTE_FILES).  Also deletes the
    local file when running on Railway (ephemeral disk).
    """
    host = _cfg("SFTP_HOST")
    port = int(_cfg("SFTP_PORT", "2222"))
    user = _cfg("SFTP_USER")
    pw   = _cfg("SFTP_PASS")
    base_url = _cfg("SFTP_BASE_URL", "https://politikanyc.com")

    if not host or not user or not pw:
        logger.warning("SFTP not configured — skipping upload")
        return None
    try:
        import paramiko
        filename = os.path.basename(local_path)
        transport = paramiko.Transport((host, port))
        transport.connect(username=user, password=pw)
        sftp = paramiko.SFTPClient.from_transport(transport)
        try:
            sftp.mkdir(remote_dir)
        except IOError:
            pass
        sftp.put(local_path, f"{remote_dir}/{filename}")
        url = f"{base_url}/{remote_dir}/{filename}"
        logger.info("Uploaded %s → %s", filename, url)

        # Clean up old files on the remote
        if cleanup:
            _prune_remote(sftp, remote_dir, max_files or MAX_REMOTE_FILES)

        sftp.close()
        transport.close()

        # Clean up local temp file on Railway (ephemeral disk)
        _remove_local(local_path)

        return url
    except Exception as e:
        logger.warning("SFTP upload failed: %s", e)
        return None


def _prune_remote(sftp, remote_dir: str, keep: int):
    """Delete oldest files in remote_dir, keeping only the newest `keep` files."""
    try:
        entries = sftp.listdir_attr(remote_dir)
        # Only regular files (skip directories)
        files = [e for e in entries if _stat.S_ISREG(e.st_mode or 0)]
        if len(files) <= keep:
            return
        # Sort by mtime ascending (oldest first)
        files.sort(key=lambda e: e.st_mtime or 0)
        to_delete = files[: len(files) - keep]
        for f in to_delete:
            path = f"{remote_dir}/{f.filename}"
            try:
                sftp.remove(path)
                logger.info("Pruned old remote file: %s", path)
            except Exception:
                pass
    except Exception as e:
        logger.debug("Remote prune skipped: %s", e)


def _remove_local(path: str):
    """Remove local file if running on Railway (PORT env var set)."""
    if not os.getenv("PORT"):
        return  # local dev — keep the file
    try:
        os.remove(path)
        logger.debug("Cleaned up local temp file: %s", path)
    except OSError:
        pass
