import io
import os
import tarfile
from pathlib import Path


def load_dotenv():
    """Load ~/.env into os.environ (skip existing keys)."""
    env_path = Path.home() / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, _, value = line.partition("=")
        key, value = key.strip(), value.strip()
        if key and key not in os.environ:
            os.environ[key] = value


def build_tar_archive(files: dict) -> bytes:
    """Build a gzip tar archive from a filename->content dict."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for name, content in files.items():
            safe = name.lstrip("/")
            if ".." in safe.split("/"):
                raise ValueError(f"path traversal in filename: {name}")
            data = content.encode() if isinstance(content, str) else content
            info = tarfile.TarInfo(name=safe)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return buf.getvalue()
