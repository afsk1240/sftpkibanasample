"""Operational helpers for remote listing, SSH commands, and Elasticsearch queries."""

from __future__ import annotations

import base64
import json
import ssl
import logging
import stat
from datetime import datetime
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import urllib.error
import urllib.request

import paramiko

from .config import Config, ElasticsearchConfig
from .uploader import connect_sftp

def list_remote_directory(cfg: Config, target: Optional[str], logger: logging.Logger) -> None:
    remote_path = target if target else cfg.remote_root
    if not remote_path:
        raise ValueError('Remote path is empty.')
    logger.info(f'[LIST] remote dir: {remote_path}')
    client, sftp = connect_sftp(cfg)
    try:
        entries = sftp.listdir_attr(remote_path)
    finally:
        try:
            sftp.close()
        except Exception:
            pass
        client.close()
    rows = []
    for entry in sorted(entries, key=lambda x: x.filename.lower()):
        flag = 'd' if stat.S_ISDIR(entry.st_mode) else '-'
        ts = datetime.fromtimestamp(entry.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
        rows.append((flag, entry.filename, entry.st_size, ts))
    print(f'=== {remote_path} ===')
    for flag, name, size, ts in rows:
        print(f"{flag} {size:>12} {ts} {name}")


def connect_ssh_client(host: str, port: int, username: Optional[str], password: Optional[str], key_path: Optional[str], timeout: int, strict: bool) -> paramiko.SSHClient:
    client = paramiko.SSHClient()
    if strict:
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.RejectPolicy())
    else:
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    kwargs: Dict[str, Any] = dict(hostname=host, port=port, username=username, timeout=timeout)
    if password:
        kwargs['password'] = password
    if key_path:
        kwargs['key_filename'] = os.path.expanduser(key_path)
    client.connect(**kwargs)
    return client


def run_ssh_command(cfg: Config, command: str, targets: Optional[List[str]], logger: logging.Logger) -> None:
    es_cfg = cfg.elasticsearch
    if es_cfg is None or not es_cfg.nodes:
        raise RuntimeError('ELASTICSEARCH.nodes 설정이 없습니다. YAML에 노드 정보를 추가하세요.')
    limit = {t for t in targets if t} if targets else None
    for node in es_cfg.nodes:
        if limit and node.name not in limit:
            continue
        logger.info(f'[SSH] {node.name}@{node.host}:{node.ssh_port} -> {command}')
        try:
            with connect_ssh_client(
                host=node.host,
                port=node.ssh_port,
                username=node.ssh_user or cfg.username,
                password=node.ssh_password or cfg.password,
                key_path=node.ssh_key_path or cfg.key_path,
                timeout=cfg.timeout,
                strict=cfg.strict_host_key_checking,
            ) as ssh:
                stdin, stdout, stderr = ssh.exec_command(command, timeout=cfg.timeout)
                out = stdout.read().decode('utf-8', errors='replace').strip()
                err = stderr.read().decode('utf-8', errors='replace').strip()
        except Exception as exc:
            logger.error(f'[SSH] {node.name}: {exc}')
            continue
        if out:
            print(f"\n--- {node.name} ({node.host}) STDOUT ---\n{out}\n")
        if err:
            print(f"--- {node.name} ({node.host}) STDERR ---\n{err}\n")


def execute_elasticsearch_query(cfg: Config, endpoint: str, body: Optional[str], index_override: Optional[str], logger: logging.Logger) -> None:
    es_cfg = cfg.elasticsearch
    if es_cfg is None or not es_cfg.base_url:
        raise RuntimeError('ELASTICSEARCH.base_url 설정이 필요합니다.')
    method = 'GET'
    path = endpoint.strip()
    if ':' in path:
        maybe_method, maybe_path = path.split(':', 1)
        maybe_method_up = maybe_method.upper()
        if maybe_method_up in {'GET', 'POST', 'PUT', 'DELETE', 'HEAD', 'PATCH'}:
            method = maybe_method_up
            path = maybe_path
    if not path.startswith('/'):
        path = '/' + path
    if index_override:
        path = path.replace('{index}', index_override)
    url = es_cfg.base_url.rstrip('/') + path
    payload_bytes = None
    if body:
        payload = body
        if body.startswith('@') and len(body) > 1:
            payload = Path(body[1:]).read_text(encoding='utf-8')
        payload_bytes = payload.encode('utf-8')
    req = urllib.request.Request(url, data=payload_bytes, method=method)
    if payload_bytes is not None:
        req.add_header('Content-Type', 'application/json; charset=utf-8')
    if es_cfg.api_user is not None and es_cfg.api_pass is not None:
        token = base64.b64encode(f"{es_cfg.api_user}:{es_cfg.api_pass}".encode('utf-8')).decode('ascii')
        req.add_header('Authorization', f'Basic {token}')
    context = None
    if not es_cfg.verify_ssl and url.lower().startswith('https'):
        context = ssl._create_unverified_context()
    logger.info(f'[ES] {method} {url}')
    try:
        with urllib.request.urlopen(req, timeout=es_cfg.timeout, context=context) as resp:
            raw = resp.read().decode('utf-8', errors='replace')
            print(f'HTTP {resp.status} {resp.reason}')
            content_type = resp.headers.get('Content-Type', '')
            if 'application/json' in content_type:
                try:
                    parsed = json.loads(raw)
                    print(json.dumps(parsed, ensure_ascii=False, indent=2))
                except Exception:
                    print(raw)
            else:
                print(raw)
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode('utf-8', errors='replace')
        logger.error(f'[ES] HTTP {exc.code}: {exc.reason}')
        if raw:
            try:
                parsed = json.loads(raw)
                print(json.dumps(parsed, ensure_ascii=False, indent=2))
            except Exception:
                print(raw)
    except urllib.error.URLError as exc:
        logger.error(f'[ES] 연결 실패: {exc}')



__all__ = [
    "list_remote_directory",
    "run_ssh_command",
    "execute_elasticsearch_query",
]
