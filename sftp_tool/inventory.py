"""Inventory scanning and reporting for remote SFTP files."""

from __future__ import annotations

import csv
import json
import posixpath
import sqlite3
import stat
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import logging

FILE_PATH_FIELD = '\ud30c\uc77c\ud328\uc2a4'  # Column header for file path (Korean text)
EXCLUSION_FIELD = '\uc81c\uc678\uc5ec\ubd80'  # Column header for exclusion flag (Korean text)
DATA_TYPE_FIELD = '\ub370\uc774\ud130 \uc720\ud615 \uba85'  # Metadata column for data-type name (Korean text)
FILE_NAME_FIELD = '\ud30c\uc77c\uba85'  # Metadata column for original file name (Korean text)
PHYSICAL_NAME_FIELD = '\ubb3c\ub9ac\ud30c\uc77c\uba85'  # Metadata column for physical file name (Korean text)
PLATFORM_KEYWORD = '\uacbd\uae30\uad50\uc721_\ub514\uc9c0\ud2f0\ub110\ud50c\ub7ab\ud3fc_'  # Keyword that marks platform files
EXCLUDE_TYPE_NAMES = {'\uc6a9\uc5b4\uc0ac\uc804', 'Q&A'}  # Data-type names that default to exclusion
Q_A_KEYWORD = 'Q_A'

from .config import Config, InventorySettings, PostgresConfig
from .pgmeta import fetch_metadata
from .uploader import connect_sftp, hash_remote


@dataclass
class InventoryEntry:
    path: str
    name: str
    size: int
    mtime: int
    sha256: Optional[str]


@dataclass
class InventoryScanResult:
    run_id: int
    total_files: int
    total_bytes: int
    duration_sec: float


@dataclass
class InventoryReportResult:
    run_id: int
    output_dir: Path
    summary: Dict[str, int]


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA foreign_keys = ON;')
    return conn


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS inventory_runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            profile TEXT,
            remote_root TEXT,
            hash_mode TEXT,
            max_hash_bytes INTEGER,
            total_files INTEGER DEFAULT 0,
            total_bytes INTEGER DEFAULT 0,
            added INTEGER DEFAULT 0,
            modified INTEGER DEFAULT 0,
            renamed INTEGER DEFAULT 0,
            deleted INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS inventory_files (
            run_id INTEGER NOT NULL,
            path TEXT NOT NULL,
            name TEXT NOT NULL,
            size INTEGER NOT NULL,
            mtime INTEGER NOT NULL,
            sha256 TEXT,
            PRIMARY KEY (run_id, path),
            FOREIGN KEY (run_id) REFERENCES inventory_runs(run_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS inventory_latest (
            path TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            size INTEGER NOT NULL,
            mtime INTEGER NOT NULL,
            sha256 TEXT,
            last_run_id INTEGER NOT NULL,
            last_seen_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS inventory_changes (
            change_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            change_type TEXT NOT NULL,
            path TEXT NOT NULL,
            old_path TEXT,
            size_before INTEGER,
            size_after INTEGER,
            hash_before TEXT,
            hash_after TEXT,
            seen_before TEXT,
            seen_after TEXT,
            FOREIGN KEY (run_id) REFERENCES inventory_runs(run_id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_inventory_changes_run ON inventory_changes(run_id);
        """
    )


def _start_run(conn: sqlite3.Connection, cfg: Config, inv: InventorySettings) -> int:
    started_at = datetime.utcnow().isoformat(timespec='seconds') + 'Z'
    cur = conn.execute(
        """
        INSERT INTO inventory_runs (started_at, profile, remote_root, hash_mode, max_hash_bytes)
        VALUES (?, ?, ?, ?, ?)
        """,
        (started_at, cfg.profile_name, inv.remote_root, inv.hash_mode, inv.max_hash_bytes),
    )
    return int(cur.lastrowid)


def _finish_run(conn: sqlite3.Connection, run_id: int, summary: Dict[str, int]) -> None:
    finished_at = datetime.utcnow().isoformat(timespec='seconds') + 'Z'
    conn.execute(
        """
        UPDATE inventory_runs
           SET finished_at = ?,
               total_files = ?,
               total_bytes = ?,
               added = ?,
               modified = ?,
               renamed = ?,
               deleted = ?
         WHERE run_id = ?
        """,
        (
            finished_at,
            summary.get('total_files', 0),
            summary.get('total_bytes', 0),
            summary.get('added', 0),
            summary.get('modified', 0),
            summary.get('renamed', 0),
            summary.get('deleted', 0),
            run_id,
        ),
    )


def _load_latest(conn: sqlite3.Connection, case_insensitive: bool) -> Dict[str, sqlite3.Row]:
    latest: Dict[str, sqlite3.Row] = {}
    transform = (lambda p: p.lower()) if case_insensitive else (lambda p: p)
    for row in conn.execute("SELECT * FROM inventory_latest"):
        latest[transform(row['path'])] = row
    return latest


def scan_and_record(cfg: Config, inv: InventorySettings, logger: logging.Logger) -> InventoryScanResult:
    conn = _connect(inv.db_path)
    _ensure_schema(conn)
    run_id = _start_run(conn, cfg, inv)
    previous_map = _load_latest(conn, inv.case_insensitive)
    prev_by_hash: Dict[str, List[sqlite3.Row]] = {}
    for row in previous_map.values():
        sha = row['sha256']
        if sha:
            prev_by_hash.setdefault(sha, []).append(row)

    transform = (lambda p: p.lower()) if inv.case_insensitive else (lambda p: p)

    entries: List[InventoryEntry] = []
    total_bytes = 0
    start_time = time.time()

    client, sftp = connect_sftp(cfg)
    try:
        stack = [inv.remote_root.rstrip('/') or '/']
        visited = set()
        while stack:
            current = stack.pop()
            if current in visited:
                continue
            visited.add(current)
            try:
                attrs = sftp.listdir_attr(current)
            except FileNotFoundError:
                logger.warning("[INVENTORY] directory missing: %s", current)
                continue
            for attr in attrs:
                name = attr.filename
                if name in {'.', '..'}:
                    continue
                remote_path = posixpath.join(current, name)
                if stat.S_ISDIR(attr.st_mode):
                    stack.append(remote_path)
                    continue
                if not stat.S_ISREG(attr.st_mode):
                    continue
                size = int(attr.st_size)
                mtime = int(attr.st_mtime)
                key = transform(remote_path)
                previous_row = previous_map.get(key)
                sha = None
                if inv.hash_mode != 'off':
                    reuse = False
                    if inv.hash_mode == 'changed' and previous_row is not None:
                        if previous_row['size'] == size and previous_row['mtime'] == mtime and previous_row['sha256']:
                            sha = previous_row['sha256']
                            reuse = True
                    if not reuse:
                        try:
                            sha = hash_remote(sftp, remote_path, cfg.hash_algo, inv.max_hash_bytes)
                        except FileNotFoundError:
                            logger.warning("[INVENTORY] file vanished before hashing: %s", remote_path)
                if sha is None and previous_row is not None:
                    sha = previous_row['sha256']
                entries.append(InventoryEntry(remote_path, name, size, mtime, sha))
                total_bytes += size
    finally:
        try:
            sftp.close()
        except Exception:
            pass
        client.close()

    _store_snapshot(conn, run_id, entries)
    summary = _compute_changes(conn, run_id, entries, previous_map, prev_by_hash, inv)
    summary['total_files'] = len(entries)
    summary['total_bytes'] = total_bytes
    _finish_run(conn, run_id, summary)
    conn.commit()
    conn.close()

    duration = time.time() - start_time
    return InventoryScanResult(run_id=run_id, total_files=len(entries), total_bytes=total_bytes, duration_sec=duration)


def _store_snapshot(conn: sqlite3.Connection, run_id: int, entries: List[InventoryEntry]) -> None:
    conn.executemany(
        "INSERT OR REPLACE INTO inventory_files(run_id, path, name, size, mtime, sha256) VALUES (?, ?, ?, ?, ?, ?)",
        [(run_id, e.path, e.name, e.size, e.mtime, e.sha256) for e in entries],
    )


def _compute_changes(
    conn: sqlite3.Connection,
    run_id: int,
    entries: List[InventoryEntry],
    previous_map: Dict[str, sqlite3.Row],
    prev_by_hash: Dict[str, List[sqlite3.Row]],
    inv: InventorySettings,
) -> Dict[str, int]:
    summary = {'added': 0, 'modified': 0, 'renamed': 0, 'deleted': 0}
    transform = (lambda p: p.lower()) if inv.case_insensitive else (lambda p: p)

    matched_prev_paths = set()
    used_hash_sources = set()

    def record_change(change_type: str, path: str, old_row: Optional[sqlite3.Row], new_entry: Optional[InventoryEntry]) -> None:
        summary[change_type] += 1
        conn.execute(
            """
            INSERT INTO inventory_changes(run_id, change_type, path, old_path, size_before, size_after, hash_before, hash_after, seen_before, seen_after)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                change_type,
                path,
                old_row['path'] if old_row else None,
                old_row['size'] if old_row else None,
                new_entry.size if new_entry else None,
                old_row['sha256'] if old_row else None,
                new_entry.sha256 if new_entry else None,
                old_row['last_seen_at'] if old_row else None,
                datetime.utcnow().isoformat(timespec='seconds') + 'Z',
            ),
        )

    for entry in entries:
        key = transform(entry.path)
        prev_row = previous_map.get(key)
        if prev_row:
            matched_prev_paths.add(prev_row['path'])
            same_size = prev_row['size'] == entry.size
            same_mtime = prev_row['mtime'] == entry.mtime
            same_hash = prev_row['sha256'] and entry.sha256 and prev_row['sha256'] == entry.sha256
            if same_size and (same_mtime or same_hash):
                continue
            record_change('modified', entry.path, prev_row, entry)
            continue

        rename_source = None
        if entry.sha256:
            for candidate in prev_by_hash.get(entry.sha256, []):
                if candidate['path'] in matched_prev_paths or candidate['path'] in used_hash_sources:
                    continue
                rename_source = candidate
                break
        if rename_source is not None:
            matched_prev_paths.add(rename_source['path'])
            used_hash_sources.add(rename_source['path'])
            record_change('renamed', entry.path, rename_source, entry)
        else:
            record_change('added', entry.path, None, entry)

    for prev_row in previous_map.values():
        if prev_row['path'] in matched_prev_paths:
            continue
        record_change('deleted', prev_row['path'], prev_row, None)

    now_iso = datetime.utcnow().isoformat(timespec='seconds') + 'Z'
    conn.execute('DELETE FROM inventory_latest')
    conn.executemany(
        "INSERT INTO inventory_latest(path, name, size, mtime, sha256, last_run_id, last_seen_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        [(e.path, e.name, e.size, e.mtime, e.sha256, run_id, now_iso) for e in entries],
    )
    return summary


def get_latest_run_id(db_path: Path) -> Optional[int]:
    if not db_path.exists():
        return None
    conn = _connect(db_path)
    try:
        row = conn.execute("SELECT run_id FROM inventory_runs ORDER BY run_id DESC LIMIT 1").fetchone()
        return int(row[0]) if row else None
    finally:
        conn.close()


def _fetch_changes(conn: sqlite3.Connection, run_id: int) -> List[sqlite3.Row]:
    cur = conn.execute(
        """
        SELECT change_id, change_type, path, old_path, size_before, size_after,
               hash_before, hash_after, seen_before, seen_after
          FROM inventory_changes
         WHERE run_id = ?
         ORDER BY change_id
        """,
        (run_id,),
    )
    return cur.fetchall()


def generate_report(cfg: Config, inv: InventorySettings, run_id: int, output_dir: Optional[Path], db_path: Path, logger: logging.Logger) -> InventoryReportResult:
    conn = _connect(db_path)
    try:
        run_row = conn.execute("SELECT * FROM inventory_runs WHERE run_id = ?", (run_id,)).fetchone()
        if not run_row:
            raise ValueError(f"Run id {run_id} not found in inventory database")
        if output_dir is None:
            timestamp = (run_row['started_at'] or datetime.utcnow().isoformat(timespec='seconds') + 'Z').replace(':', '').replace('-', '')
            output_dir = Path('reports') / 'inventory' / f"{timestamp}_run{run_id}"
        output_dir.mkdir(parents=True, exist_ok=True)

        changes = _fetch_changes(conn, run_id)
        summary = {
            'run_id': run_id,
            'profile': run_row['profile'],
            'remote_root': run_row['remote_root'],
            'hash_mode': run_row['hash_mode'],
            'max_hash_bytes': run_row['max_hash_bytes'],
            'total_files': run_row['total_files'],
            'total_bytes': run_row['total_bytes'],
            'added': run_row['added'],
            'modified': run_row['modified'],
            'renamed': run_row['renamed'],
            'deleted': run_row['deleted'],
        }

        _write_change_csvs(output_dir, changes)
        (output_dir / 'summary.json').write_text(json.dumps(summary, indent=2), encoding='utf-8')
        logger.info("[INVENTORY] wrote report for run %s to %s", run_id, output_dir)
        return InventoryReportResult(run_id=run_id, output_dir=output_dir, summary=summary)
    finally:
        conn.close()


def _write_change_csvs(output_dir: Path, changes: Iterable[sqlite3.Row]) -> None:
    buckets: Dict[str, List[Dict[str, Any]]] = {
        'added': [],
        'modified': [],
        'renamed': [],
        'deleted': [],
    }
    for row in changes:
        record = {
            'path': row['path'],
            'old_path': row['old_path'],
            'size_before': row['size_before'],
            'size_after': row['size_after'],
            'hash_before': row['hash_before'],
            'hash_after': row['hash_after'],
            'seen_before': row['seen_before'],
            'seen_after': row['seen_after'],
        }
        buckets.setdefault(row['change_type'], []).append(record)

    for name, rows in buckets.items():
        path = output_dir / f"{name}.csv"
        if not rows:
            path.write_text('', encoding='utf-8')
            continue
        fieldnames = sorted(rows[0].keys())
        with path.open('w', encoding='utf-8', newline='') as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)


def enrich_with_postgres(
    cfg: Config,
    inv: InventorySettings,
    pg_cfg: PostgresConfig,
    db_path: Path,
    run_id: int,
    output_dir: Path,
    logger: logging.Logger,
) -> Path:
    conn = _connect(db_path)
    try:
        changes = _fetch_changes(conn, run_id)
    finally:
        conn.close()

    enriched_path = output_dir / 'enriched.csv'
    if not changes:
        enriched_path.write_text('', encoding='utf-8')
        logger.info("[INVENTORY] no changes to enrich for run %s", run_id)
        return enriched_path

    names_for_query: List[str] = []
    seen_query: set[str] = set()
    for row in changes:
        candidate = row['path'] or row['old_path']
        if not candidate:
            continue
        name = posixpath.basename(candidate)
        if not name or name in seen_query:
            continue
        seen_query.add(name)
        names_for_query.append(name)

    metadata_map = fetch_metadata(pg_cfg, names_for_query, logger) if names_for_query else {}

    column_order = list(pg_cfg.excel_column_order or [col.alias for col in (pg_cfg.columns or [])])

    def _ensure_column(name: str) -> None:
        if name and name not in column_order:
            column_order.append(name)

    _ensure_column(FILE_PATH_FIELD)
    _ensure_column(EXCLUSION_FIELD)

    def _meta_get(meta: Dict[str, Any], key: str) -> Any:
        if key in meta:
            return meta[key]
        if isinstance(key, str):
            lowered = key.lower()
            for meta_key, value in meta.items():
                if isinstance(meta_key, str) and meta_key.lower() == lowered:
                    return value
            if ' ' in key:
                primary = key.split()[0]
                lowered_primary = primary.lower()
                for meta_key, value in meta.items():
                    if isinstance(meta_key, str) and (meta_key == primary or meta_key.lower() == lowered_primary):
                        return value
        return None

    def _clean(value: Any) -> Any:
        return '' if value is None else value

    def _compute_exclusion(meta: Dict[str, Any], candidate_path: str) -> str:
        data_type_name = str(_meta_get(meta, DATA_TYPE_FIELD) or '').strip()
        file_name_text = str(_meta_get(meta, FILE_NAME_FIELD) or '').strip()
        physical_name = str(_meta_get(meta, PHYSICAL_NAME_FIELD) or '').strip()
        flagged = False
        if data_type_name in EXCLUDE_TYPE_NAMES:
            flagged = True
        name_for_check = file_name_text or physical_name or posixpath.basename(candidate_path)
        normalized_check = name_for_check or ''
        if PLATFORM_KEYWORD and PLATFORM_KEYWORD in normalized_check:
            flagged = True
        if Q_A_KEYWORD and Q_A_KEYWORD in normalized_check.upper():
            flagged = True
        if not flagged:
            return ''
        ext_source = physical_name or name_for_check or posixpath.basename(candidate_path)
        extension = posixpath.splitext(ext_source)[1].lower()
        if extension != '.xlsx':
            return ''
        return 'Y'

    rows: List[Dict[str, Any]] = []
    for change in changes:
        candidate = change['path'] or change['old_path'] or ''
        key = posixpath.basename(candidate)
        lookup_key = key.lower() if pg_cfg.case_insensitive else key
        meta = metadata_map.get(lookup_key, {})
        row_data: Dict[str, Any] = {}
        for column_name in column_order:
            if column_name == FILE_PATH_FIELD:
                value = candidate
            elif column_name == EXCLUSION_FIELD:
                value = _compute_exclusion(meta, candidate)
            elif column_name == PHYSICAL_NAME_FIELD:
                value = _meta_get(meta, column_name) or posixpath.basename(candidate)
            else:
                value = _meta_get(meta, column_name)
            row_data[column_name] = _clean(value)
        rows.append(row_data)

    if not rows:
        enriched_path.write_text('', encoding='utf-8')
        return enriched_path

    with enriched_path.open('w', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=column_order)
        writer.writeheader()
        writer.writerows(rows)
    logger.info("[INVENTORY] wrote enriched report to %s", enriched_path)
    return enriched_path


__all__ = [
    'InventoryEntry',
    'InventoryScanResult',
    'InventoryReportResult',
    'scan_and_record',
    'generate_report',
    'get_latest_run_id',
    'enrich_with_postgres',
]
