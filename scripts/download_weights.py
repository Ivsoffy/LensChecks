#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API_BASE = "https://cloud-api.yandex.net/v1/disk/public/resources"
DEFAULT_TIMEOUT = (10, 120)  # connect, read
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
DOWNLOAD_JOBS = [
    (
        REPO_ROOT / "scripts" / "weights_codes.json",
        REPO_ROOT / "src" / "modules" / "module_2",
    ),
    (
        REPO_ROOT / "scripts" / "weights_grades.json",
        REPO_ROOT / "src" / "modules" / "module_4",
    ),
]


class DownloadError(RuntimeError):
    pass


@dataclass
class RemoteItem:
    path: str  # relative path inside public folder
    type: str  # "file" or "dir"
    size: Optional[int]
    md5: Optional[str]
    name: str


def build_public_key_candidates(public_key: str) -> List[str]:
    """
    Build a short list of plausible Yandex public_key values to maximize compatibility
    between various shared-link hostnames and raw key formats.
    """
    raw = (public_key or "").strip()
    if not raw:
        return []

    candidates: List[str] = []

    def add(value: Optional[str]) -> None:
        if not value:
            return
        v = value.strip()
        if v and v not in candidates:
            candidates.append(v)

    add(raw)

    try:
        parsed = urlparse(raw)
    except Exception:
        parsed = None

    token: Optional[str] = None
    if parsed and parsed.scheme in {"http", "https"} and parsed.netloc:
        path_parts = [p for p in parsed.path.split("/") if p]
        if len(path_parts) >= 2 and path_parts[0] in {"d", "i"}:
            token = path_parts[1]
            add(token)
            add(f"https://disk.yandex.ru/{path_parts[0]}/{token}")
            add(f"https://yadi.sk/{path_parts[0]}/{token}")

    if not token and "/" not in raw and "://" not in raw:
        add(f"https://disk.yandex.ru/d/{raw}")
        add(f"https://yadi.sk/d/{raw}")

    return candidates


def build_session() -> requests.Session:
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "HEAD"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)

    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(
        {
            "User-Agent": "weights-downloader/1.0",
            "Accept": "application/json",
        }
    )
    return session


def md5_file(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    h = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_file(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def format_bytes(num_bytes: float) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(max(num_bytes, 0.0))
    unit_idx = 0
    while value >= 1024 and unit_idx < len(units) - 1:
        value /= 1024.0
        unit_idx += 1
    if unit_idx == 0:
        return f"{int(value)}{units[unit_idx]}"
    return f"{value:.1f}{units[unit_idx]}"


def format_eta(seconds: float) -> str:
    sec = int(max(seconds, 0))
    minutes, sec = divmod(sec, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours:d}:{minutes:02d}:{sec:02d}"
    return f"{minutes:02d}:{sec:02d}"


def shorten_label(label: str, max_len: int = 42) -> str:
    if len(label) <= max_len:
        return label
    head = max_len // 2 - 2
    tail = max_len - head - 3
    return f"{label[:head]}...{label[-tail:]}"


def atomic_download(
    session: requests.Session,
    url: str,
    dst: Path,
    expected_size: Optional[int] = None,
    chunk_size: int = 8 * 1024 * 1024,
    label: Optional[str] = None,
) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(dst.suffix + ".part")
    progress_label = shorten_label(label or str(dst.name))

    with session.get(url, stream=True, timeout=DEFAULT_TIMEOUT) as r:
        if r.status_code != 200:
            raise DownloadError(f"Download failed for {dst}: HTTP {r.status_code}")

        total_size = expected_size
        if total_size is None:
            try:
                content_length = r.headers.get("Content-Length")
                if content_length:
                    total_size = int(content_length)
            except Exception:
                total_size = None

        downloaded = 0
        started_at = time.time()
        last_drawn_at = 0.0

        def draw_progress(force: bool = False) -> None:
            nonlocal last_drawn_at
            now = time.time()
            if not force and (now - last_drawn_at) < 0.2:
                return
            elapsed = max(now - started_at, 1e-6)
            speed = downloaded / elapsed

            if total_size and total_size > 0:
                ratio = min(downloaded / total_size, 1.0)
                bar_width = 24
                filled = int(bar_width * ratio)
                bar = "#" * filled + "-" * (bar_width - filled)
                eta = (total_size - downloaded) / speed if speed > 0 else 0.0
                line = (
                    f"\r[PROG] {progress_label} [{bar}] {ratio * 100:6.2f}% "
                    f"{format_bytes(downloaded)}/{format_bytes(total_size)} "
                    f"{format_bytes(speed)}/s ETA {format_eta(eta)}"
                )
            else:
                line = (
                    f"\r[PROG] {progress_label} "
                    f"{format_bytes(downloaded)} {format_bytes(speed)}/s"
                )

            print(line, end="", flush=True)
            last_drawn_at = now

        with tmp.open("wb") as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    draw_progress()
        draw_progress(force=True)
        print()

    if expected_size is not None:
        actual_size = tmp.stat().st_size
        if actual_size != expected_size:
            tmp.unlink(missing_ok=True)
            raise DownloadError(
                f"Size mismatch for {dst}: expected={expected_size}, actual={actual_size}"
            )

    os.replace(tmp, dst)


class YandexDiskPublicDownloader:
    def __init__(
        self,
        public_key: str,
        target_dir: Path,
        include_ext: Optional[Iterable[str]] = None,
    ):
        self.original_public_key = public_key
        self.public_key_candidates = build_public_key_candidates(public_key)
        if not self.public_key_candidates:
            raise DownloadError("public_key is empty.")
        self.public_key = self.public_key_candidates[0]
        self.resolved_public_key: Optional[str] = None
        self.target_dir = target_dir
        self.include_ext = {ext.lower() for ext in include_ext} if include_ext else None
        self.session = build_session()
        self.state: Dict[str, Any] = {"files": {}}

    def _request_json(self, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
        r = self.session.get(url, params=params, timeout=DEFAULT_TIMEOUT)
        if r.status_code != 200:
            raise DownloadError(
                f"API request failed: {url} params={params} status={r.status_code} body={r.text[:500]}"
            )
        return r.json()

    def get_root_meta(self) -> Dict[str, Any]:
        last_error: Optional[Exception] = None
        for candidate in self.public_key_candidates:
            try:
                meta = self._request_json(
                    API_BASE,
                    {
                        "public_key": candidate,
                        "limit": 1000,
                    },
                )
                self.public_key = candidate
                self.resolved_public_key = candidate
                return meta
            except Exception as e:
                last_error = e
                continue

        tried = ", ".join(repr(x) for x in self.public_key_candidates)
        raise DownloadError(
            "Unable to resolve public Yandex Disk resource. "
            f"Tried public_key variants: {tried}. "
            "The link may be invalid, deleted, or not public. "
            f"Last error: {last_error}"
        )

    def list_dir(self, rel_path: Optional[str] = None) -> List[RemoteItem]:
        """
        Р’РѕР·РІСЂР°С‰Р°РµС‚ СЃРѕРґРµСЂР¶РёРјРѕРµ РґРёСЂРµРєС‚РѕСЂРёРё С‡РµСЂРµР· РїР°РіРёРЅР°С†РёСЋ.
        rel_path=None РѕР·РЅР°С‡Р°РµС‚ РєРѕСЂРµРЅСЊ РїСѓР±Р»РёС‡РЅРѕР№ РїР°РїРєРё.
        """
        items: List[RemoteItem] = []
        offset = 0
        limit = 1000

        while True:
            rel_dir = rel_path.lstrip("/") if rel_path else None
            params: Dict[str, Any] = {
                "public_key": self.public_key,
                "limit": limit,
                "offset": offset,
            }
            if rel_dir:
                # For public/resources listing, nested paths must be passed with
                # a leading slash (e.g. "/subdir/file").
                params["path"] = "/" + rel_dir

            meta = self._request_json(API_BASE, params)
            embedded = meta.get("_embedded", {})
            batch = embedded.get("items", [])

            for x in batch:
                raw_path = x.get("path") or x.get("name")
                item_rel_path = self._normalize_rel_path(raw_path, current_dir=rel_dir)
                items.append(
                    RemoteItem(
                        path=item_rel_path,
                        type=x["type"],
                        size=x.get("size"),
                        md5=x.get("md5"),
                        name=x["name"],
                    )
                )

            total = embedded.get("total", len(batch))
            offset += len(batch)
            if offset >= total or not batch:
                break

        return items

    def _normalize_rel_path(self, api_path: str, current_dir: Optional[str]) -> str:
        """
        Normalize API path values to a relative path inside the public root.
        """
        p = api_path.replace("\\", "/")

        if p.startswith("disk:/"):
            p = p[len("disk:/") :]

        p = p.lstrip("/")

        if current_dir:
            current_dir_posix = str(PurePosixPath(current_dir)).lstrip("/")
            current_name = PurePosixPath(current_dir_posix).name

            if p == current_name:
                return current_dir_posix

            if p == current_dir_posix or p.startswith(current_dir_posix + "/"):
                return p

            return str(PurePosixPath(current_dir_posix) / p)

        return p

    def walk_files(self, rel_path: Optional[str] = None) -> List[RemoteItem]:
        files: List[RemoteItem] = []
        for item in self.list_dir(rel_path):
            if item.type == "dir":
                files.extend(self.walk_files(item.path))
            elif item.type == "file":
                if self._should_include(item.path):
                    files.append(item)
        return files

    def _should_include(self, rel_path: str) -> bool:
        if self.include_ext is None:
            return True
        return Path(rel_path).suffix.lower() in self.include_ext

    def get_download_href(self, rel_path: str) -> str:
        # Yandex download endpoint expects an absolute-like path inside the public
        # resource (leading slash). Keep a fallback for older behavior.
        path_candidates: List[str] = []
        normalized = "/" + rel_path.lstrip("/")
        path_candidates.append(normalized)
        if rel_path not in path_candidates:
            path_candidates.append(rel_path)

        last_error: Optional[Exception] = None
        for candidate_path in path_candidates:
            try:
                payload = self._request_json(
                    f"{API_BASE}/download",
                    {
                        "public_key": self.public_key,
                        "path": candidate_path,
                    },
                )
                href = payload.get("href")
                if href:
                    return href
                last_error = DownloadError(
                    f"No download href returned for {rel_path} with path={candidate_path}"
                )
            except Exception as e:
                last_error = e

        raise DownloadError(
            f"Unable to resolve download href for {rel_path}. "
            f"Tried path variants: {path_candidates}. Last error: {last_error}"
        )

    def is_up_to_date(self, item: RemoteItem, local_path: Path) -> bool:
        if not local_path.exists() or not local_path.is_file():
            return False

        if item.size is not None and local_path.stat().st_size != item.size:
            return False

        key = item.path
        entry = self.state.get("files", {}).get(key)

        # Р•СЃР»Рё СЂР°РЅСЊС€Рµ md5 СѓР¶Рµ СЃРІРµСЂСЏР»Рё Рё СЂР°Р·РјРµСЂ СЃРѕРІРїР°РґР°РµС‚ вЂ” РїСЂРѕРїСѓСЃРєР°РµРј.
        if entry:
            if (
                entry.get("size") == item.size
                and entry.get("md5") == item.md5
                and entry.get("local_sha256")
            ):
                return True

        # Р•СЃР»Рё remote md5 РµСЃС‚СЊ вЂ” РјРѕР¶РЅРѕ СЃРІРµСЂРёС‚СЊ Р»РѕРєР°Р»СЊРЅС‹Р№ md5.
        if item.md5:
            try:
                return md5_file(local_path) == item.md5
            except Exception:
                return False

        # Р•СЃР»Рё md5 РЅРµС‚, Р° СЂР°Р·РјРµСЂ СЃРѕРІРїР°РґР°РµС‚ вЂ” СЃС‡РёС‚Р°РµРј С„Р°Р№Р» Р°РєС‚СѓР°Р»СЊРЅС‹Рј.
        return True

    def sync(self, delete_extraneous: bool = False) -> None:
        root_meta = self.get_root_meta()
        root_type = root_meta.get("type")
        if root_type != "dir":
            raise DownloadError(
                "public_key must point to a public folder, not a single file."
            )

        remote_files = self.walk_files()
        remote_paths = set()

        for item in remote_files:
            remote_paths.add(item.path)
            local_path = self.target_dir / Path(item.path)

            if self.is_up_to_date(item, local_path):
                print(f"[SKIP] {item.path}")
                continue

            print(f"[DOWN] {item.path}")
            href = self.get_download_href(item.path)
            atomic_download(
                self.session,
                href,
                local_path,
                expected_size=item.size,
                label=item.path,
            )

            self.state["files"][item.path] = {
                "size": item.size,
                "md5": item.md5,
                "local_sha256": sha256_file(local_path),
                "updated_at": int(time.time()),
            }

        if delete_extraneous:
            self._delete_extra_files(remote_paths)

        print("[DONE] weights synchronized")

    def _delete_extra_files(self, remote_paths: set[str]) -> None:
        self.target_dir.mkdir(parents=True, exist_ok=True)

        for local_path in self.target_dir.rglob("*"):
            if not local_path.is_file():
                continue
            if (
                local_path.name == ".yadisk_manifest.json"
                or local_path.suffix == ".part"
            ):
                continue
            rel_path = local_path.relative_to(self.target_dir).as_posix()
            if rel_path not in remote_paths:
                print(f"[DEL ] {rel_path}")
                local_path.unlink(missing_ok=True)

        # РџРѕС‡РёСЃС‚РёРј РїСѓСЃС‚С‹Рµ РїР°РїРєРё
        for root, dirs, files in os.walk(self.target_dir, topdown=False):
            if Path(root) == self.target_dir:
                continue
            if not dirs and not files:
                Path(root).rmdir()


def load_manifest(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download weights sequentially: "
            "scripts/weights_codes.json -> src/module_2, "
            "then scripts/weights_grades.json -> src/module_4."
        )
    )
    parser.add_argument(
        "--delete-extraneous",
        action="store_true",
        help="Delete local files that no longer exist remotely.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        total_jobs = len(DOWNLOAD_JOBS)
        for idx, (manifest_path, target_dir) in enumerate(DOWNLOAD_JOBS, start=1):
            print(f"[STEP] {idx}/{total_jobs} {manifest_path} -> {target_dir}")

            if not manifest_path.exists():
                raise DownloadError(f"Manifest file does not exist: {manifest_path}")

            manifest = load_manifest(manifest_path)
            public_key = manifest.get("public_key")
            if not public_key:
                raise DownloadError(
                    f"public_key is required in manifest: {manifest_path}"
                )

            include_ext = manifest.get("include_ext")

            downloader = YandexDiskPublicDownloader(
                public_key=public_key,
                target_dir=target_dir,
                include_ext=include_ext,
            )
            downloader.sync(delete_extraneous=args.delete_extraneous)

        return 0
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        return 130
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
