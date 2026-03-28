"""Shared SFTP upload utility — uploads files to WP Engine."""
import os
import logging

logger = logging.getLogger(__name__)

# SFTP config — from env vars or DB settings (fb_ad_approval uses DB)
SFTP_HOST = os.getenv("SFTP_HOST", "")
SFTP_PORT = int(os.getenv("SFTP_PORT", "2222"))
SFTP_USER = os.getenv("SFTP_USER", "")
SFTP_PASS = os.getenv("SFTP_PASS", "")
SFTP_BASE_URL = os.getenv("SFTP_BASE_URL", "https://politikanyc.com")


def sftp_upload(local_path: str, remote_dir: str = "exports") -> str | None:
    """Upload a file via SFTP. Returns public URL or None on failure."""
    if not SFTP_HOST or not SFTP_USER or not SFTP_PASS:
        logger.warning("SFTP not configured — skipping upload")
        return None
    try:
        import paramiko
        filename = os.path.basename(local_path)
        transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
        transport.connect(username=SFTP_USER, password=SFTP_PASS)
        sftp = paramiko.SFTPClient.from_transport(transport)
        try:
            sftp.mkdir(remote_dir)
        except IOError:
            pass
        sftp.put(local_path, f"{remote_dir}/{filename}")
        sftp.close()
        transport.close()
        url = f"{SFTP_BASE_URL}/{remote_dir}/{filename}"
        logger.info("Uploaded %s → %s", filename, url)
        return url
    except Exception as e:
        logger.warning("SFTP upload failed: %s", e)
        return None
