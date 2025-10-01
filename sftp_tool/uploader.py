"""Core uploading logic for the SFTP tool."""

from __future__ import annotations

import csv
import fnmatch
import hashlib
import json
import logging
import os
import posixpath
import queue
import random
import shutil
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from argparse import Namespace

import paramiko

from .config import Config, FilePair, FileResult, Stats

def setup_logger(log_file: str) -> logging.Logger:
    logger = logging.getLogger("sftp_uploader")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger

# ====================== 토큰 버킷(전역/파일) ======================
class TokenBucket:
    """간단한 토큰버킷. 단위=바이트. capacity가 버스트 허용량."""
    def __init__(self, rate_bps: int, capacity: Optional[int] = None):
        self.rate = max(0, rate_bps)
        self.capacity = capacity if capacity is not None else self.rate
        self.tokens = float(self.capacity)
        self.lock = threading.Lock()
        self.last = time.time()

    def consume(self, n: int):
        """n 바이트 송신 전 호출. 충분한 토큰 없으면 슬립."""
        if self.rate <= 0:
            return
        with self.lock:
            now = time.time()
            elapsed = now - self.last
            self.last = now
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            need = float(n)
            if self.tokens >= need:
                self.tokens -= need
                return
            # 부족한 만큼 충전될 때까지 슬립
            remain = need - self.tokens
            self.tokens = 0.0
            sleep_time = remain / self.rate
        time.sleep(sleep_time)

# ====================== SFTP 연결/세션 풀 ======================
def connect_sftp(cfg: Config) -> Tuple[paramiko.SSHClient, paramiko.SFTPClient]:
    client = paramiko.SSHClient()
    if cfg.strict_host_key_checking:
        client.load_system_host_keys()
    else:
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    client.connect(
        hostname=cfg.host,
        port=cfg.port,
        username=cfg.username,
        password=cfg.password,
        key_filename=(cfg.key_path if cfg.key_path and Path(cfg.key_path).expanduser().exists() else None),
        timeout=cfg.timeout,
        allow_agent=True,
        look_for_keys=True,
    )

    # 서버 정책으로 SFTP 채널 튜닝이 지정된 경우 from_transport 사용
    if cfg.sftp_window_size or cfg.sftp_max_packet_size:
        t = client.get_transport()
        sftp = paramiko.SFTPClient.from_transport(
            t,
            window_size=cfg.sftp_window_size,
            max_packet_size=cfg.sftp_max_packet_size
        )  # 윈도/패킷 크기는 전송 속도에 영향 가능. :contentReference[oaicite:1]{index=1}
    else:
        sftp = client.open_sftp()
    return client, sftp

class SFTPSessionPool:
    def __init__(self, cfg: Config, size: int, logger: logging.Logger):
        self.cfg = cfg
        self.size = max(1, size)
        self.logger = logger
        self.q: "queue.LifoQueue[Tuple[paramiko.SSHClient,paramiko.SFTPClient]]" = queue.LifoQueue()
        for _ in range(self.size):
            self.q.put_nowait(self._create())

    def _create(self):
        return connect_sftp(self.cfg)

    def acquire(self):
        return self.q.get()

    def release(self, sess):
        self.q.put(sess)

    def invalidate(self, sess):
        try:
            client, sftp = sess
            try: sftp.close()
            except Exception: pass
            client.close()
        except Exception: pass
        try:
            self.q.put_nowait(self._create())
        except Exception as e:
            self.logger.warning(f"[POOL] 새 세션 생성 실패: {e}")

    def close(self):
        while not self.q.empty():
            try:
                client, sftp = self.q.get_nowait()
                try: sftp.close()
                except Exception: pass
                client.close()
            except Exception:
                pass

# ====================== SFTP 유틸 ======================
def sftp_exists(sftp: paramiko.SFTPClient, path: str) -> bool:
    try:
        sftp.stat(path)
        return True
    except Exception:
        return False

def sftp_mkdirs(sftp: paramiko.SFTPClient, remote_dir: str) -> None:
    remote_dir = posixpath.normpath(remote_dir)
    if remote_dir in ("", "/"):
        return
    parts = remote_dir.strip("/").split("/")
    path = "/"
    for p in parts:
        path = posixpath.join(path, p)
        try:
            sftp.stat(path)
        except Exception:
            sftp.mkdir(path)

def touch_remote_file(sftp: paramiko.SFTPClient, remote_path: str, mode: int = 0o644) -> None:
    sftp_mkdirs(sftp, posixpath.dirname(remote_path))
    with sftp.open(remote_path, "w") as f:
        f.write("")
    sftp.chmod(remote_path, mode)

# ====================== 해시 ======================
def _hash_obj(name: str):
    name = name.lower()
    if name not in ("md5", "sha1", "sha256"):
        raise ValueError(f"Unsupported hash algo: {name}")
    return hashlib.new(name)

def hash_local(path: Path, algo: str = "sha256", max_bytes: int = 0) -> str:
    h = _hash_obj(algo)
    remain = max_bytes if max_bytes and max_bytes > 0 else float("inf")
    with path.open("rb") as f:
        while remain > 0:
            chunk = f.read(min(1024 * 1024, int(remain)))
            if not chunk: break
            h.update(chunk)
            remain -= len(chunk)
    return h.hexdigest()

def hash_remote(sftp: paramiko.SFTPClient, remote_path: str, algo: str = "sha256", max_bytes: int = 0) -> str:
    h = _hash_obj(algo)
    remain = max_bytes if max_bytes and max_bytes > 0 else float("inf")
    with sftp.open(remote_path, "rb") as f:
        while remain > 0:
            chunk = f.read(min(1024 * 1024, int(remain)))
            if not chunk: break
            h.update(chunk)
            remain -= len(chunk)
    return h.hexdigest()

# ====================== 진행률(전체) ======================
class GlobalProgress:
    def __init__(self, total_bytes: int, interval: float, logger: logging.Logger):
        self.total = max(0, total_bytes)
        self.done = 0
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.interval = interval
        self.logger = logger
        self.last_ts = time.time()
        self.last_done = 0
        self.thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self.thread.start()

    def add(self, n: int):
        if n <= 0: return
        with self.lock:
            self.done += n

    def _run(self):
        while not self.stop.wait(self.interval):
            with self.lock:
                done = self.done
            pct = (done / self.total * 100.0) if self.total else 100.0
            now = time.time()
            delta_b = done - self.last_done
            delta_t = now - self.last_ts if now > self.last_ts else 1e-6
            speed = delta_b / delta_t / (1024 * 1024)  # MB/s
            self.last_done, self.last_ts = done, now
            self.logger.info(f"[TOTAL] {done}/{self.total} bytes ({pct:.1f}%) - {speed:.2f} MB/s")

    def finish(self):
        self.stop.set()
        self.thread.join(timeout=2)

# ====================== 파일 수집/필터 ======================
def should_exclude_by_ext(path: Path, exts: set) -> bool:
    return path.suffix.lower() in exts

def match_any_glob(name: str, patterns: List[str]) -> bool:
    return any(fnmatch.fnmatch(name, pat) for pat in patterns)

def rel_remote_path(cfg: Config, base: Path, path_under_base: Path) -> str:
    rel = path_under_base.as_posix()
    if cfg.include_root_name:
        return posixpath.join(cfg.remote_root.rstrip("/"), base.name, rel)
    else:
        return posixpath.join(cfg.remote_root.rstrip("/"), rel)

def gather_files(cfg: Config, logger: logging.Logger) -> Tuple[List[FilePair], Stats, Dict[str, List[str]], List[Path]]:
    stats = Stats()
    pairs: List[FilePair] = []
    lists = {"excluded_by_ext": [], "meta_missing": [], "glob_excluded": [], "size_excluded": []}

    # 처리할 base 목록
    bases = cfg.local_dirs if cfg.local_dirs else [(cfg.local_root / d).resolve() for d in cfg.order]

    for base in bases:
        if not base.exists():
            logger.warning(f"[SKIP] 존재하지 않는 디렉토리: {base}")
            continue
        for root, _, files in os.walk(base):
            root_path = Path(root)
            for fname in files:
                fpath = (root_path / fname).resolve()
                stats.scanned += 1

                # .meta 자체는 대상 아님
                if fpath.suffix.lower() == ".meta":
                    continue

                # 확장자 제외
                if should_exclude_by_ext(fpath, cfg.exclude_exts):
                    stats.excluded_by_ext += 1
                    lists["excluded_by_ext"].append(str(fpath))
                    continue

                # 와일드카드 포함/제외
                if cfg.include_globs and not match_any_glob(fpath.name, cfg.include_globs):
                    stats.filtered_by_glob += 1
                    lists["glob_excluded"].append(str(fpath))
                    continue
                if cfg.exclude_globs and match_any_glob(fpath.name, cfg.exclude_globs):
                    stats.filtered_by_glob += 1
                    lists["glob_excluded"].append(str(fpath))
                    continue

                # 크기 제한
                size = fpath.stat().st_size
                if (cfg.min_size and size < cfg.min_size) or (cfg.max_size and size > cfg.max_size):
                    stats.filtered_by_size += 1
                    lists["size_excluded"].append(str(fpath))
                    continue

                # .meta 필요
                if cfg.require_meta:
                    if not Path(str(fpath) + ".meta").exists():
                        stats.skipped_meta_missing += 1
                        lists["meta_missing"].append(str(fpath))
                        continue

                mtime = fpath.stat().st_mtime
                rel_under_base = fpath.relative_to(base)
                remote = rel_remote_path(cfg, base, rel_under_base)
                pairs.append(FilePair(base=base, local=fpath, remote=remote, size=size, mtime=mtime))
                stats.selected += 1
                stats.bytes_total += size

    # 최신 mtime 필터
    if cfg.newest_scope in ("per_base", "global") and cfg.newest_count > 0:
        if cfg.newest_scope == "global":
            pairs.sort(key=lambda p: p.mtime, reverse=True)
            kept = pairs[: cfg.newest_count]
            removed = {id(x) for x in pairs[cfg.newest_count:]}
        else:
            kept = []
            removed_ids = set()
            for base in bases:
                pick = [p for p in pairs if p.base == base]
                pick.sort(key=lambda p: p.mtime, reverse=True)
                kept.extend(pick[: cfg.newest_count])
                for x in pick[cfg.newest_count:]:
                    removed_ids.add(id(x))
            removed = removed_ids
        stats.filtered_by_newest = len(removed)
        pairs = [p for p in pairs if id(p) not in removed]

    return pairs, stats, lists, bases

# ====================== 격리 보관 ======================
def find_base_for(path: Path, bases: List[Path]) -> Optional[Path]:
    best = None
    for b in bases:
        try:
            path.relative_to(b)
            if best is None or len(str(b)) > len(str(best)):
                best = b
        except ValueError:
            continue
    return best

def quarantine_copy(local_file: str, bases: List[Path], subdir_name: str, logger: logging.Logger):
    try:
        p = Path(local_file)
        base = find_base_for(p, bases)
        target_root = (base if base else p.parent)
        quarantine_dir = target_root / subdir_name
        rel = p.name if base is None else p.relative_to(base)
        dst = quarantine_dir / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        if p.exists():
            shutil.copy2(p, dst)
            logger.info(f"[QUARANTINE] {p} -> {dst}")
    except Exception as e:
        logger.warning(f"[QUARANTINE] 실패: {local_file} :: {e}")

# ====================== 업로드(재시도/속도제한/부분재개) ======================
def backoff_sleep(attempt_idx: int, base: float, max_delay: float, jitter: float):
    delay = min(max_delay, base * (2 ** max(0, attempt_idx - 1)))
    delay = delay + random.random() * (jitter * max(0.0, delay))
    time.sleep(delay)

def upload_streaming(
    sftp: paramiko.SFTPClient,
    cfg: Config,
    gp: GlobalProgress,
    file: FilePair,
    global_limiter: Optional[TokenBucket],
    per_file_limiter: Optional[TokenBucket],
    logger: logging.Logger,
) -> None:
    """부분 재개 포함 스트리밍 업로드."""
    sftp_mkdirs(sftp, posixpath.dirname(file.remote))

    local_size = file.size
    remote_size = 0
    exists = False
    try:
        st = sftp.stat(file.remote)
        remote_size = int(getattr(st, "st_size", 0))
        exists = True
    except Exception:
        exists = False

    # SKIP 규칙
    if exists:
        if cfg.skip_if_exists == "size_equal" and remote_size == local_size:
            logger.info(f"[SKIP=size_equal] {file.local} -> {file.remote}")
            gp.add(local_size)
            return
        if cfg.skip_if_exists == "mtime_ge":
            # 원격 mtime >= 로컬 mtime 이면 스킵
            try:
                r_mtime = float(getattr(st, "st_mtime", 0.0))
                if r_mtime >= file.mtime:
                    logger.info(f"[SKIP=mtime_ge] {file.local} -> {file.remote}")
                    gp.add(local_size)
                    return
            except Exception:
                pass

    # 부분 재개
    start_off = 0
    mode = "wb"
    if exists and cfg.resume_enabled and remote_size < local_size:
        start_off = remote_size
        mode = "ab"  # append 모드: 항상 파일 끝에 이어붙임. (seek은 다음 write에 의해 무시) :contentReference[oaicite:2]{index=2}
        logger.info(f"[RESUME] {file.local} -> {file.remote} (offset {start_off})")
    elif exists and cfg.resume_enabled and remote_size > local_size:
        # 원격이 더 크면 덮어씀
        logger.info(f"[RESUME] remote larger than local; overwrite: {file.local} -> {file.remote}")
        mode = "wb"
        start_off = 0

    # 스트리밍 업로드
    sent = start_off
    total = local_size
    chunk = max(4096, int(cfg.upload_chunk_size))
    with file.local.open("rb") as lfd:
        if start_off:
            lfd.seek(start_off, 0)
        with sftp.open(file.remote, mode) as rfd:
            while True:
                buf = lfd.read(chunk)
                if not buf:
                    break
                n = len(buf)
                # 속도 제한(전역 -> 파일별)
                if global_limiter:
                    global_limiter.consume(n)
                if per_file_limiter:
                    per_file_limiter.consume(n)
                rfd.write(buf)
                sent += n
                gp.add(n)

    # confirm(stat) 검증 (Paramiko put의 confirm 동작과 동일 개념) :contentReference[oaicite:3]{index=3}
    if cfg.confirm_put:
        r = sftp.stat(file.remote)
        rsize = int(getattr(r, "st_size", 0))
        if rsize != local_size:
            raise IOError(f"size mismatch after upload: remote={rsize}, local={local_size}")

    # .meta 동반 업로드
    if cfg.require_meta:
        meta_local = Path(str(file.local) + ".meta")
        if meta_local.exists():
            meta_remote = file.remote + ".meta"
            with meta_local.open("rb") as lfd:
                with sftp.open(meta_remote, "wb") as rfd:
                    while True:
                        buf = lfd.read(chunk)
                        if not buf:
                            break
                        n = len(buf)
                        if global_limiter:
                            global_limiter.consume(n)
                        if per_file_limiter:
                            per_file_limiter.consume(n)
                        rfd.write(buf)

    # 해시 검증 (옵션, 부분 재개 포함)
    if cfg.verify_hash and file.size > 0:
        lh = hash_local(file.local, cfg.hash_algo, cfg.hash_max_bytes)
        rh = hash_remote(sftp, file.remote, cfg.hash_algo, cfg.hash_max_bytes)
        if lh != rh:
            raise ValueError(f"hash mismatch ({cfg.hash_algo}): local={lh} remote={rh}")

def upload_with_retries(
    cfg: Config,
    pool: SFTPSessionPool,
    gp: GlobalProgress,
    file: FilePair,
    global_limiter: Optional[TokenBucket],
    logger: logging.Logger,
) -> FileResult:
    attempts = 0
    start_all = time.time()
    last_err = None
    per_file_limiter = TokenBucket(cfg.rate_limit_per_file_bps) if cfg.rate_limit_per_file_bps > 0 else None

    while attempts < max(1, cfg.retries):
        attempts += 1
        client, sftp = pool.acquire()
        t0 = time.time()
        try:
            if cfg.progress:
                # Paramiko put의 콜백 형태(func(transferred, total))와 동일 형태를 맞춰 사용 :contentReference[oaicite:4]{index=4}
                pass  # 파일별 진행률은 GlobalProgress로 충분히 커버(원하면 세부 로그 콜백 추가 가능)

            upload_streaming(sftp, cfg, gp, file, global_limiter, per_file_limiter, logger)

            dur = time.time() - t0
            speed = (file.size / (1024 * 1024)) / dur if dur > 0 else 0.0
            pool.release((client, sftp))
            return FileResult(
                local=str(file.local), remote=file.remote, size=file.size,
                duration_sec=dur, speed_mb_s=speed, attempts=attempts,
                ok=True, verified=cfg.verify_hash or None, error=None
            )
        except Exception as e:
            last_err = str(e)
            pool.invalidate((client, sftp))
            if attempts >= cfg.retries:
                break
            time.sleep(0.05)
            backoff_sleep(attempts, cfg.backoff_base, cfg.backoff_max, cfg.backoff_jitter)

    dur_tot = time.time() - start_all
    speed_avg = (file.size / (1024 * 1024)) / dur_tot if dur_tot > 0 else 0.0
    return FileResult(
        local=str(file.local), remote=file.remote, size=file.size,
        duration_sec=dur_tot, speed_mb_s=speed_avg, attempts=attempts,
        ok=False, verified=False if cfg.verify_hash else None, error=last_err
    )

# ====================== 보고서/알림 ======================
def write_csv(path: Path, rows: List[Dict[str, Any]], header_order: Optional[List[str]] = None):
    import csv
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        with path.open("w", encoding="utf-8", newline="") as f:
            if header_order:
                csv.DictWriter(f, fieldnames=header_order).writeheader()
        return
    keys = header_order or list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(rows)

def write_json(path: Path, data: Any):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def try_write_excel(xlsx_path: Path, tables: Dict[str, List[Dict[str, Any]]], logger: logging.Logger):
    try:
        import pandas as pd  # type: ignore
        with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
            for sheet, rows in tables.items():
                pd.DataFrame(rows).to_excel(writer, sheet_name=(sheet[:31] or "Sheet"), index=False)
        logger.info(f"[REPORT] Excel 생성: {xlsx_path}")
    except Exception as e:
        logger.info(f"[REPORT] Excel 생략(pandas/openpyxl 미설치 또는 실패): {e}")

def notify_slack(webhook_url: str, text: str, logger: logging.Logger) -> None:
    if not webhook_url:
        return
    data = json.dumps({"text": text}).encode("utf-8")
    req = urllib.request.Request(webhook_url, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            _ = resp.read()
        logger.info("[ALERT] Slack 전송 완료")
    except urllib.error.URLError as e:
        logger.warning(f"[ALERT] Slack 전송 실패: {e}")

def notify_email(cfg: Config, subject: str, body: str, attachments: Optional[List[Path]], logger: logging.Logger) -> None:
    if not (cfg.smtp_server and cfg.email_from and cfg.email_to):
        return
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = cfg.email_from
    msg["To"] = ", ".join(cfg.email_to or [])
    msg.set_content(body)

    for p in attachments or []:
        try:
            with p.open("rb") as f:
                data = f.read()
            msg.add_attachment(data, maintype="application", subtype="octet-stream", filename=p.name)
        except Exception as e:
            logger.warning(f"[ALERT] 첨부 실패: {p} :: {e}")

    try:
        if cfg.smtp_use_tls:
            with smtplib.SMTP(cfg.smtp_server, cfg.smtp_port, timeout=15) as s:
                s.starttls()
                if cfg.smtp_user:
                    s.login(cfg.smtp_user, cfg.smtp_pass or "")
                s.send_message(msg)
        else:
            with smtplib.SMTP(cfg.smtp_server, cfg.smtp_port, timeout=15) as s:
                if cfg.smtp_user:
                    s.login(cfg.smtp_user, cfg.smtp_pass or "")
                s.send_message(msg)
        logger.info("[ALERT] 이메일 전송 완료")
    except Exception as e:
        logger.warning(f"[ALERT] 이메일 전송 실패: {e}")

# ====================== 정책/설정 ======================
def apply_server_policy(cfg: Config) -> Config:
    pol = cfg.server_policies.get(cfg.host) if isinstance(cfg.server_policies, dict) else None
    if not pol:
        return cfg
    # 허용된 키만 반영
    def ov(key, cast=None):
        if key in pol and pol[key] is not None:
            val = pol[key]
            return cast(val) if cast else val
        return None

    if (v := ov("max_workers", int)) is not None: cfg.max_workers = v
    if (v := ov("rate_limit_bps", int)) is not None: cfg.rate_limit_bps = v
    if (v := ov("rate_limit_per_file_bps", int)) is not None: cfg.rate_limit_per_file_bps = v
    if (v := ov("retries", int)) is not None: cfg.retries = v
    if (v := ov("verify_hash", bool)) is not None: cfg.verify_hash = v
    if (v := ov("confirm_put", bool)) is not None: cfg.confirm_put = v
    if (v := ov("upload_chunk_size", int)) is not None: cfg.upload_chunk_size = v
    if (v := ov("skip_if_exists", str)) is not None: cfg.skip_if_exists = v
    if (v := ov("sftp_window_size", lambda x: int(x) if x is not None else None)) is not None: cfg.sftp_window_size = v
    if (v := ov("sftp_max_packet_size", lambda x: int(x) if x is not None else None)) is not None: cfg.sftp_max_packet_size = v
    return cfg

def _ci_get(mapping: Dict[str, Any], key: str, default: Any = None) -> Any:
    if not isinstance(mapping, dict):
        return default
    key_lower = key.lower()
    for k, v in mapping.items():
        if isinstance(k, str) and k.lower() == key_lower:
            return v
    return default


def parse_elasticsearch_config(e: Env, cfg: Config) -> Optional[ElasticsearchConfig]:
    raw = e.get_json("ELASTICSEARCH", None)
    if not raw:
        raw = e.data.get("ELASTICSEARCH")
    if not isinstance(raw, dict):
        return None

    base_url = _ci_get(raw, "base_url")
    kibana_url = _ci_get(raw, "kibana_url")
    api_user = _ci_get(raw, "api_user") or _ci_get(raw, "username")
    api_pass = _ci_get(raw, "api_pass") or _ci_get(raw, "password")
    verify_ssl = bool(_ci_get(raw, "verify_ssl", True))
    timeout_val = _ci_get(raw, "timeout", 10.0)
    try:
        timeout = float(timeout_val)
    except Exception:
        timeout = 10.0
    default_index = _ci_get(raw, "default_index")

    nodes_cfg: List[ElasticsearchNode] = []
    nodes_raw = _ci_get(raw, "nodes", [])
    if isinstance(nodes_raw, (list, tuple)):
        for item in nodes_raw:
            if not isinstance(item, dict):
                continue
            host = _ci_get(item, "host")
            if not host:
                continue
            name = str(_ci_get(item, "name", host))
            ssh_port_val = _ci_get(item, "ssh_port", 22)
            try:
                ssh_port = int(ssh_port_val)
            except Exception:
                ssh_port = 22
            ssh_user = _ci_get(item, "ssh_user") or cfg.username
            ssh_key_path = _ci_get(item, "ssh_key_path") or cfg.key_path
            ssh_password = _ci_get(item, "ssh_password") or cfg.password
            nodes_cfg.append(ElasticsearchNode(
                name=name,
                host=str(host),
                ssh_port=ssh_port,
                ssh_user=str(ssh_user) if ssh_user else None,
                ssh_key_path=str(ssh_key_path) if ssh_key_path else None,
                ssh_password=str(ssh_password) if ssh_password else None,
            ))

    if not (base_url or kibana_url or nodes_cfg):
        return None

    return ElasticsearchConfig(
        base_url=str(base_url) if base_url else None,
        kibana_url=str(kibana_url) if kibana_url else None,
        api_user=str(api_user) if api_user else None,
        api_pass=str(api_pass) if api_pass else None,
        verify_ssl=verify_ssl,
        timeout=timeout,
        default_index=str(default_index) if default_index else None,
        nodes=nodes_cfg,
    )


def load_config(env_path: Path, args: argparse.Namespace) -> Config:
    e = Env(env_path, profile=getattr(args, "profile", None))

    def arg_over(k_env, default, cast=lambda x: x):
        v = getattr(args, k_env, None)
        if v is None:
            v = e.get(k_env.upper(), default)
        return cast(v) if (v is not None and v != "") else default

    # 알림 이메일 수신자
    email_to_val = e.get("EMAIL_TO", "")
    email_to_env = [x.strip() for x in (email_to_val.replace(";", ",").split(",")) if x.strip()] if email_to_val else []

    cfg = Config(
        # 접속/경로
        host=str(e.get("SFTP_HOST", "")),
        port=e.get_int("SFTP_PORT", 22),
        username=str(e.get("SFTP_USERNAME", "")),
        password=(e.get("SFTP_PASSWORD", None)),
        key_path=e.get("SFTP_KEY_PATH", None),
        remote_root=str(e.get("REMOTE_ROOT", "/")).rstrip("/") + "/",
        local_root=Path(str(e.get("LOCAL_ROOT", "."))).resolve(),
        order=e.get_list("ORDER"),
        local_dirs=None,  # CLI에서만 직접 지정 가능

        # 스캔/필터
        exclude_exts=set([x.strip().lower() for x in e.get_list("EXCLUDE_EXTS")]),
        include_globs=e.get_list("FILTER_INCLUDE_GLOBS"),
        exclude_globs=e.get_list("FILTER_EXCLUDE_GLOBS"),
        min_size=e.get_int("FILTER_MIN_SIZE", 0),
        max_size=e.get_int("FILTER_MAX_SIZE", 0),
        newest_scope=str(e.get("FILTER_NEWEST_SCOPE", "off")).lower(),
        newest_count=e.get_int("FILTER_NEWEST_COUNT", 1),

        # 업로드 정책
        require_meta=e.get_bool("REQUIRE_META", True),
        confirm_put=e.get_bool("CONFIRM_PUT", True),
        include_root_name=e.get_bool("INCLUDE_ROOT_NAME", True),
        skip_if_exists=str(e.get("SKIP_IF_EXISTS", "size_equal")).lower(),
        resume_enabled=e.get_bool("RESUME_ENABLED", True),
        upload_chunk_size=e.get_int("UPLOAD_CHUNK_SIZE", 32768),

        # 속도 제한
        rate_limit_bps=e.get_int("RATE_LIMIT_BPS", 0),
        rate_limit_per_file_bps=e.get_int("RATE_LIMIT_PER_FILE_BPS", 0),

        # 동시성/재시도
        max_workers=e.get_int("MAX_WORKERS", 5),
        retries=e.get_int("RETRIES", 3),
        backoff_base=e.get_float("BACKOFF_BASE", 1.0),
        backoff_max=e.get_float("BACKOFF_MAX", 10.0),
        backoff_jitter=e.get_float("BACKOFF_JITTER", 0.3),

        # 검증/진행률/로그
        verify_hash=e.get_bool("VERIFY_HASH", True),
        hash_algo=str(e.get("HASH_ALGO", "sha256")).lower(),
        hash_max_bytes=e.get_int("HASH_MAX_BYTES", 209_715_200),
        progress=e.get_bool("PROGRESS", True),
        progress_print_interval=e.get_float("PROGRESS_PRINT_INTERVAL", 0.5),
        log_file=str(e.get("LOG_FILE", "sftp_upload.log")),
        timeout=e.get_int("TIMEOUT", 30),
        strict_host_key_checking=e.get_bool("STRICT_HOST_KEY_CHECKING", False),

        # 마커/격리
        create_end_touch=e.get_bool("CREATE_END_TOUCH", False),
        touch_only=e.get_bool("TOUCH_ONLY", False),
        end_touch_name=str(e.get("END_TOUCH_NAME", "end.touch")),
        quarantine_excluded=e.get_bool("QUARANTINE_EXCLUDED", True),
        quarantine_failed=e.get_bool("QUARANTINE_FAILED", True),

        # 알림
        slack_webhook_url=e.get("SLACK_WEBHOOK_URL", None),
        smtp_server=e.get("SMTP_SERVER", None),
        smtp_port=e.get_int("SMTP_PORT", 587),
        smtp_user=e.get("SMTP_USER", None),
        smtp_pass=e.get("SMTP_PASS", None),
        smtp_use_tls=e.get_bool("SMTP_USE_TLS", True),
        email_from=e.get("EMAIL_FROM", None),
        email_to=email_to_env,

        # 서버별
        server_policies=e.get_json("SERVER_POLICIES", {}) or {},
        sftp_window_size=None,
        sftp_max_packet_size=None,
        profile_name=e.profile or (args.profile or "default"),
        config_path=str(env_path),
        elasticsearch=None,
    )

    # CLI override: --password 프롬프트, --local-dirs 직접 지정, --email-to 추가 등
    if getattr(args, "password", None) == "__PROMPT__":
        cfg.password = getpass.getpass("SFTP Password: ")
    if getattr(args, "smtp_pass", None) == "__PROMPT__":
        cfg.smtp_pass = getpass.getpass("SMTP Password: ")

    if args.local_dirs:
        dirs = []
        for d in args.local_dirs:
            p = Path(d)
            if not p.is_absolute():
                p = (cfg.local_root / p).resolve()
            else:
                p = p.resolve()
            dirs.append(p)
        cfg.local_dirs = dirs

    if args.email_to:
        # CLI 수신자를 env 목록 뒤에 추가
        tails = [x.strip() for x in args.email_to.replace(";", ",").split(",") if x.strip()]
        cfg.email_to = cfg.email_to + tails

    # 서버별 정책 적용
    cfg = apply_server_policy(cfg)
    cfg.elasticsearch = parse_elasticsearch_config(e, cfg)
    return cfg

# ====================== CLI 파서 ======================


def run_upload(cfg: Config, args: Namespace, logger: logging.Logger) -> int:
    if cfg.touch_only:
        try:
            client, sftp = connect_sftp(cfg)
            try:
                remote_touch = posixpath.join(cfg.remote_root.rstrip("/"), cfg.end_touch_name)
                touch_remote_file(sftp, remote_touch)
                logger.info(f"[TOUCH] created: {remote_touch}")
            finally:
                try:
                    sftp.close()
                except Exception:
                    pass
                client.close()
            return 0
        except Exception as exc:
            logger.error(f"[ERROR] touch-only 실패: {exc}")
            return 2

    pairs, stats, pre_lists, bases = gather_files(cfg, logger)
    logger.info(
        f"[DISCOVER] 스캔:{stats.scanned} 선택:{stats.selected} 제외(확장자):{stats.excluded_by_ext} "
        f"글롭필터:{stats.filtered_by_glob} 크기필터:{stats.filtered_by_size} 최신필터:{stats.filtered_by_newest} "
        f"메타누락:{stats.skipped_meta_missing} 총용량:{stats.bytes_total} bytes"
    )

    if cfg.quarantine_excluded:
        for f in pre_lists.get("excluded_by_ext", []) + pre_lists.get("glob_excluded", []) + pre_lists.get("size_excluded", []):
            quarantine_copy(f, bases, "exclude", logger)
        for f in pre_lists.get("meta_missing", []):
            quarantine_copy(f, bases, "exclude", logger)

    out_dir = Path("reports") / datetime.now().strftime("%Y%m%d%H%M")
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.dry_run:
        write_json(out_dir / "prelists.json", pre_lists)
        write_json(out_dir / "summary.json", {
            "scanned": stats.scanned,
            "selected": stats.selected,
            "excluded_by_ext": stats.excluded_by_ext,
            "filtered_by_glob": stats.filtered_by_glob,
            "filtered_by_size": stats.filtered_by_size,
            "filtered_by_newest": stats.filtered_by_newest,
            "skipped_meta_missing": stats.skipped_meta_missing,
            "bytes_total": stats.bytes_total,
        })
        (out_dir / "candidates.txt").write_text("\n".join(str(p.local) for p in pairs), encoding="utf-8")
        logger.info(f"[DRY-RUN] 후보/업로드 목록 생성: {out_dir}")
        return 0

    gp = GlobalProgress(stats.bytes_total, cfg.progress_print_interval, logger)
    gp.start()

    global_limiter = TokenBucket(cfg.rate_limit_bps) if cfg.rate_limit_bps > 0 else None

    pool = SFTPSessionPool(cfg, cfg.max_workers, logger)
    file_results: List[FileResult] = []
    success_files: List[str] = []
    failed_files: List[str] = []

    t_global = time.time()
    try:
        with ThreadPoolExecutor(max_workers=cfg.max_workers) as executor:
            futures = [executor.submit(upload_with_retries, cfg, pool, gp, pair, global_limiter, logger) for pair in pairs]
            for fut in as_completed(futures):
                res = fut.result()
                file_results.append(res)
                stats.duration_total_sec += res.duration_sec
                stats.bytes_uploaded += res.size if res.ok else 0
                if res.ok:
                    stats.uploaded_ok += 1
                    success_files.append(res.local)
                else:
                    stats.uploaded_fail += 1
                    failed_files.append(res.local)
    finally:
        gp.finish()
        pool.close()

    if cfg.quarantine_failed:
        for path_failed in failed_files:
            quarantine_copy(path_failed, bases, "failure", logger)

    if cfg.create_end_touch:
        try:
            client, sftp = connect_sftp(cfg)
            try:
                remote_touch = posixpath.join(cfg.remote_root.rstrip("/"), cfg.end_touch_name)
                touch_remote_file(sftp, remote_touch)
                logger.info(f"[TOUCH] created: {remote_touch}")
            finally:
                try:
                    sftp.close()
                except Exception:
                    pass
                client.close()
        except Exception as exc:
            logger.error(f"[ERROR] end.touch 생성 실패: {exc}")

    rows_success = [dict(local=r.local, remote=r.remote, size=r.size,
                         duration_sec=round(r.duration_sec, 3), speed_mb_s=round(r.speed_mb_s, 3),
                         attempts=r.attempts, verified=r.verified) for r in file_results if r.ok]
    rows_failed = [dict(local=r.local, remote=r.remote, size=r.size,
                        duration_sec=round(r.duration_sec, 3), attempts=r.attempts, error=r.error)
                   for r in file_results if not r.ok]
    rows_skipped = [{"local": f, "reason": "excluded_or_glob_or_size_or_meta"} for f in
                    pre_lists.get("excluded_by_ext", []) + pre_lists.get("glob_excluded", []) +
                    pre_lists.get("size_excluded", []) + pre_lists.get("meta_missing", [])]

    write_csv(out_dir / "success.csv", rows_success,
              header_order=["local", "remote", "size", "duration_sec", "speed_mb_s", "attempts", "verified"])
    write_csv(out_dir / "failed.csv", rows_failed,
              header_order=["local", "remote", "size", "duration_sec", "attempts", "error"])
    write_csv(out_dir / "skipped.csv", rows_skipped, header_order=["local", "reason"])

    summary = {
        "scanned": stats.scanned,
        "selected": stats.selected,
        "excluded_by_ext": stats.excluded_by_ext,
        "filtered_by_glob": stats.filtered_by_glob,
        "filtered_by_size": stats.filtered_by_size,
        "filtered_by_newest": stats.filtered_by_newest,
        "skipped_meta_missing": stats.skipped_meta_missing,
        "uploaded_ok": stats.uploaded_ok,
        "uploaded_fail": stats.uploaded_fail,
        "bytes_total": stats.bytes_total,
        "bytes_uploaded": stats.bytes_uploaded,
        "duration_total_sec": round(stats.duration_total_sec, 3),
        "overall_elapsed_sec": round(time.time() - t_global, 3),
        "profile_name": cfg.profile_name,
        "config_path": cfg.config_path,
        "remote_root": cfg.remote_root,
        "host": cfg.host,
    }
    write_json(out_dir / "summary.json", summary)
    write_json(out_dir / "summary.json", summary)
    (out_dir / "success.txt").write_text("\n".join(success_files), encoding="utf-8")
    (out_dir / "failed.txt").write_text("\n".join(failed_files), encoding="utf-8")

    try_write_excel(out_dir / "report.xlsx", {
        "summary": [summary],
        "success": rows_success,
        "failed": rows_failed,
        "skipped": rows_skipped
    }, logger)

    print("\n=== Upload Summary ===")
    for key, value in summary.items():
        print(f"{key}: {value}")
    print(f"\nReport path: {out_dir.resolve()}")

    alert_text = (
        f"SFTP upload finished\n"
        f"- OK: {stats.uploaded_ok}\n"
        f"- FAIL: {stats.uploaded_fail}\n"
        f"- Skipped: {stats.excluded_by_ext + stats.filtered_by_glob + stats.filtered_by_size + stats.skipped_meta_missing}\n"
        f"- Bytes: {stats.bytes_uploaded}/{stats.bytes_total}\n"
        f"- Elapsed: {summary['overall_elapsed_sec']}s\n"
        f"- Remote: {cfg.remote_root}\n"
        f"- Host: {cfg.host}\n"
        f"- Profile: {cfg.profile_name} ({cfg.config_path})\n"
        f"- Report: {out_dir.resolve()}"
    )
    if cfg.slack_webhook_url:
        notify_slack(cfg.slack_webhook_url, alert_text, logger)
    if cfg.smtp_server and cfg.email_from and cfg.email_to:
        attachments = [out_dir / "summary.json", out_dir / "success.csv", out_dir / "failed.csv", out_dir / "skipped.csv"]
        notify_email(cfg, subject="[SFTP] 업로드 결과 보고", body=alert_text, attachments=attachments, logger=logger)

    return 0


__all__ = [
    "setup_logger",
    "TokenBucket",
    "connect_sftp",
    "SFTPSessionPool",
    "gather_files",
    "run_upload",
]
