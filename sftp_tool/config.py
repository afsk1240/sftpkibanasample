"""Configuration loading for the refactored SFTP tool."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import getpass


class Env:
    """Helper that reads key/value data from .env or YAML defaults/profiles."""

    def __init__(self, path: Path, profile: Optional[str] = None):
        self.path = path
        self.profile = profile
        self.available_profiles: List[str] = []
        self.data: Dict[str, Any] = {}
        if not path.exists():
            return
        suffix = path.suffix.lower()
        if suffix in {".yaml", ".yml"}:
            self.data = self._load_yaml(path, profile)
        else:
            self.data = self._load_env(path)

    @staticmethod
    def _normalize_keys(mapping: Dict[str, Any]) -> Dict[str, Any]:
        normalised: Dict[str, Any] = {}
        for key, value in (mapping or {}).items():
            if not isinstance(key, str):
                raise ValueError("Configuration keys must be strings.")
            normalised[key.upper()] = value
        return normalised

    def _load_env(self, path: Path) -> Dict[str, Any]:
        data: Dict[str, Any] = {}
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            data[key.strip()] = value.strip().strip('"').strip("'")
        if self.profile is None:
            self.profile = "default"
        return data

    def _load_yaml(self, path: Path, profile: Optional[str]) -> Dict[str, Any]:
        try:
            import yaml  # type: ignore
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("PyYAML is required for YAML configuration support.") from exc

        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if not isinstance(raw, dict):
            raise ValueError(f"{path} root must be a mapping")

        defaults_section = raw.get("defaults") or {}
        if defaults_section and not isinstance(defaults_section, dict):
            raise ValueError(f"{path} defaults section must be a mapping")

        scalar_defaults = {
            k: v for k, v in raw.items()
            if not isinstance(v, dict) and k not in {"default_profile"}
        }
        defaults = self._normalize_keys(defaults_section)
        if scalar_defaults:
            defaults.update(self._normalize_keys(scalar_defaults))

        profiles_section = None
        for key in ("profiles", "environments"):
            section = raw.get(key)
            if section is not None:
                if not isinstance(section, dict):
                    raise ValueError(f"{key} section must be a mapping")
                profiles_section = section
                break
        if profiles_section is None:
            profiles_section = {
                k: v for k, v in raw.items()
                if isinstance(v, dict) and k not in {"defaults", "default_profile"}
            }
            if not profiles_section:
                raise ValueError(f"{path} is missing a profiles/environments section")

        profiles_map = {str(k): (v or {}) for k, v in profiles_section.items()}
        self.available_profiles = sorted(profiles_map.keys())

        chosen = profile or raw.get("default_profile")
        if chosen is None:
            if len(profiles_map) == 1:
                chosen = next(iter(profiles_map))
            else:
                raise ValueError(
                    "Profile must be specified (--profile). Available: "
                    + ", ".join(self.available_profiles)
                )
        chosen = str(chosen)
        if chosen not in profiles_map:
            raise KeyError(
                f"Profile '{chosen}' not found. Available: " + ", ".join(self.available_profiles)
            )
        profile_data = profiles_map[chosen]
        if not isinstance(profile_data, dict):
            raise ValueError(f"Profile '{chosen}' must be a mapping")

        merged: Dict[str, Any] = {}
        merged.update(defaults)
        merged.update(self._normalize_keys(profile_data))

        self.profile = chosen
        return merged

    def get(self, key: str, default: Optional[str] = None) -> Optional[str]:
        return os.getenv(key, self.data.get(key, default))

    def get_int(self, key: str, default: int) -> int:
        val = self.get(key)
        if val is None or val == "":
            return default
        if isinstance(val, bool):
            return int(val)
        if isinstance(val, int):
            return val
        if isinstance(val, float):
            return int(val)
        try:
            return int(str(val))
        except Exception:
            return default

    def get_float(self, key: str, default: float) -> float:
        val = self.get(key)
        if val is None or val == "":
            return default
        if isinstance(val, (int, float)):
            return float(val)
        try:
            return float(str(val))
        except Exception:
            return default

    def get_bool(self, key: str, default: bool) -> bool:
        val = self.get(key)
        if val is None or val == "":
            return default
        if isinstance(val, bool):
            return val
        if isinstance(val, (int, float)):
            return val != 0
        return str(val).lower() not in {"0", "false", "no", "off"}

    def get_list(self, key: str) -> List[str]:
        val = self.get(key, "")
        if not val:
            return []
        if isinstance(val, (list, tuple)):
            result: List[str] = []
            for item in val:
                if item is None:
                    continue
                text_item = str(item).strip()
                if text_item:
                    result.append(text_item)
            return result
        if isinstance(val, str):
            parts = [p.strip() for chunk in val.split(";") for p in chunk.split(",")]
            return [p for p in parts if p]
        text_item = str(val).strip()
        return [text_item] if text_item else []

    def get_json(self, key: str, default: Any = None) -> Any:
        val = self.get(key)
        if val is None or val == "":
            return default
        if isinstance(val, (dict, list)):
            return val
        try:
            return json.loads(val)
        except Exception:
            return default


@dataclass
class ElasticsearchNode:
    name: str
    host: str
    ssh_port: int = 22
    ssh_user: Optional[str] = None
    ssh_key_path: Optional[str] = None
    ssh_password: Optional[str] = None


@dataclass
class ElasticsearchConfig:
    base_url: Optional[str]
    kibana_url: Optional[str]
    api_user: Optional[str]
    api_pass: Optional[str]
    verify_ssl: bool = True
    timeout: float = 10.0
    default_index: Optional[str] = None
    nodes: List[ElasticsearchNode] = field(default_factory=list)


@dataclass
class PostgresColumn:
    expression: str
    alias: str


@dataclass
class PostgresConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: Optional[str]
    sslmode: str = "require"
    connect_timeout: int = 10
    table: Optional[str] = None
    physical_column: Optional[str] = None
    columns: List[PostgresColumn] = field(default_factory=list)
    match_column: Optional[str] = None
    additional_where: Optional[str] = None
    case_insensitive: bool = False


@dataclass
class InventorySettings:
    db_path: Path
    remote_root: str
    hash_mode: str = "changed"
    max_hash_bytes: int = 0
    case_insensitive: bool = False


@dataclass
class Config:
    host: str
    port: int
    username: str
    password: Optional[str]
    key_path: Optional[str]
    remote_root: str
    local_root: Path
    order: List[str]
    local_dirs: Optional[List[Path]]

    exclude_exts: set
    include_globs: List[str]
    exclude_globs: List[str]
    min_size: int
    max_size: int
    newest_scope: str
    newest_count: int

    require_meta: bool
    confirm_put: bool
    include_root_name: bool
    skip_if_exists: str
    resume_enabled: bool
    upload_chunk_size: int

    rate_limit_bps: int
    rate_limit_per_file_bps: int

    max_workers: int
    retries: int
    backoff_base: float
    backoff_max: float
    backoff_jitter: float

    verify_hash: bool
    hash_algo: str
    hash_max_bytes: int
    progress: bool
    progress_print_interval: float
    log_file: str
    timeout: int
    strict_host_key_checking: bool

    create_end_touch: bool
    touch_only: bool
    end_touch_name: str
    quarantine_excluded: bool
    quarantine_failed: bool

    slack_webhook_url: Optional[str]
    smtp_server: Optional[str]
    smtp_port: int
    smtp_user: Optional[str]
    smtp_pass: Optional[str]
    smtp_use_tls: bool
    email_from: Optional[str]
    email_to: List[str]

    server_policies: Dict[str, Any]
    sftp_window_size: Optional[int] = None
    sftp_max_packet_size: Optional[int] = None
    profile_name: str = ""
    config_path: str = ""
    elasticsearch: Optional[ElasticsearchConfig] = None
    inventory: InventorySettings = field(default_factory=lambda: InventorySettings(Path("inventory.sqlite"), "/"))
    postgres: Optional[PostgresConfig] = None


@dataclass
class Stats:
    scanned: int = 0
    selected: int = 0
    excluded_by_ext: int = 0
    skipped_meta_missing: int = 0
    filtered_by_glob: int = 0
    filtered_by_size: int = 0
    filtered_by_newest: int = 0
    uploaded_ok: int = 0
    uploaded_fail: int = 0
    bytes_total: int = 0
    bytes_uploaded: int = 0
    duration_total_sec: float = 0.0


@dataclass
class FilePair:
    base: Path
    local: Path
    remote: str
    size: int
    mtime: float


@dataclass
class FileResult:
    local: str
    remote: str
    size: int
    duration_sec: float
    speed_mb_s: float
    attempts: int
    ok: bool
    verified: Optional[bool]
    error: Optional[str]


def apply_server_policy(cfg: Config) -> Config:
    policies = cfg.server_policies.get(cfg.host) if isinstance(cfg.server_policies, dict) else None
    if not policies:
        return cfg

    def override(key: str, caster):
        if key in policies and policies[key] is not None:
            try:
                return caster(policies[key])
            except Exception:
                return None
        return None

    for key, caster in (
        ("max_workers", int),
        ("rate_limit_bps", int),
        ("rate_limit_per_file_bps", int),
        ("retries", int),
        ("verify_hash", bool),
        ("confirm_put", bool),
        ("upload_chunk_size", int),
        ("skip_if_exists", str),
    ):
        val = override(key, caster)
        if val is not None:
            setattr(cfg, key, val)

    win = override("sftp_window_size", lambda v: int(v) if v is not None else None)
    if win is not None:
        cfg.sftp_window_size = win
    pkt = override("sftp_max_packet_size", lambda v: int(v) if v is not None else None)
    if pkt is not None:
        cfg.sftp_max_packet_size = pkt
    return cfg


def _ci_get(mapping: Dict[str, Any], key: str, default: Any = None) -> Any:
    if not isinstance(mapping, dict):
        return default
    target = key.lower()
    for k, v in mapping.items():
        if isinstance(k, str) and k.lower() == target:
            return v
    return default


def parse_elasticsearch_config(env: Env, cfg: Config) -> Optional[ElasticsearchConfig]:
    raw = env.get_json("ELASTICSEARCH", None)
    if not isinstance(raw, dict):
        return None

    base_url = _ci_get(raw, "base_url")
    kibana_url = _ci_get(raw, "kibana_url")
    api_user = _ci_get(raw, "api_user") or _ci_get(raw, "username")
    api_pass = _ci_get(raw, "api_pass") or _ci_get(raw, "password")
    verify_ssl = bool(_ci_get(raw, "verify_ssl", True))
    try:
        timeout = float(_ci_get(raw, "timeout", 10.0))
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
            try:
                ssh_port = int(_ci_get(item, "ssh_port", 22))
            except Exception:
                ssh_port = 22
            ssh_user = _ci_get(item, "ssh_user") or cfg.username
            ssh_key_path = _ci_get(item, "ssh_key_path") or cfg.key_path
            ssh_password = _ci_get(item, "ssh_password") or cfg.password
            nodes_cfg.append(
                ElasticsearchNode(
                    name=name,
                    host=str(host),
                    ssh_port=ssh_port,
                    ssh_user=str(ssh_user) if ssh_user else None,
                    ssh_key_path=str(ssh_key_path) if ssh_key_path else None,
                    ssh_password=str(ssh_password) if ssh_password else None,
                )
            )

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


def _parse_postgres_columns(raw_columns: Any) -> List[PostgresColumn]:
    columns: List[PostgresColumn] = []
    if not raw_columns:
        return columns
    for item in raw_columns:
        expr: Optional[str] = None
        alias: Optional[str] = None
        if isinstance(item, dict):
            expr = item.get("column") or item.get("expr")
            alias = item.get("alias")
        else:
            text = str(item)
            if "|" in text:
                expr, alias = text.split("|", 1)
            elif " " in text:
                parts = text.split()
                expr, alias = parts[0], " ".join(parts[1:])
            else:
                expr = alias = text
        if not expr:
            continue
        expr = expr.strip()
        alias = alias.strip() if alias else expr
        if not alias:
            alias = expr
        columns.append(PostgresColumn(expression=expr, alias=alias))
    return columns


def parse_postgres_config(env: Env) -> Optional[PostgresConfig]:
    raw = env.get_json("POSTGRES", None)
    if not isinstance(raw, dict):
        return None

    host = _ci_get(raw, "host")
    dbname = _ci_get(raw, "dbname") or _ci_get(raw, "database")
    user = _ci_get(raw, "user") or _ci_get(raw, "username")
    password = _ci_get(raw, "password")
    if not host or not dbname or not user:
        return None

    try:
        port = int(_ci_get(raw, "port", 5432))
    except Exception:
        port = 5432
    sslmode = str(_ci_get(raw, "sslmode", "require"))
    try:
        connect_timeout = int(_ci_get(raw, "connect_timeout", 10))
    except Exception:
        connect_timeout = 10

    table = _ci_get(raw, "table")
    physical_column = _ci_get(raw, "physical_column")
    columns = _parse_postgres_columns(_ci_get(raw, "columns", []))
    match_column = _ci_get(raw, "match_column") or physical_column
    additional_where = _ci_get(raw, "additional_where")
    case_insensitive = bool(_ci_get(raw, "case_insensitive", False))

    if not table or not physical_column:
        return None

    if not columns:
        columns = [PostgresColumn(expression=physical_column, alias=physical_column)]

    return PostgresConfig(
        host=str(host),
        port=port,
        dbname=str(dbname),
        user=str(user),
        password=str(password) if password else None,
        sslmode=sslmode,
        connect_timeout=connect_timeout,
        table=str(table),
        physical_column=str(physical_column),
        columns=columns,
        match_column=str(match_column) if match_column else None,
        additional_where=str(additional_where) if additional_where else None,
        case_insensitive=case_insensitive,
    )


def build_inventory_settings(env: Env, cfg: Config) -> InventorySettings:
    db_value = env.get("INVENTORY_DB", "inventory.sqlite")
    db_path = Path(str(db_value)).expanduser()
    if not db_path.is_absolute():
        cfg_dir = Path(cfg.config_path).parent if cfg.config_path else Path.cwd()
        db_path = (cfg_dir / db_path).resolve()

    remote_root = str(env.get("INVENTORY_REMOTE_ROOT", cfg.remote_root)).rstrip("/") or cfg.remote_root.rstrip("/")
    if remote_root != "/":
        remote_root = remote_root + "/"

    hash_mode = str(env.get("INVENTORY_HASH_MODE", "changed")).lower()
    if hash_mode not in {"changed", "full", "off"}:
        hash_mode = "changed"
    max_hash_bytes = env.get_int("INVENTORY_MAX_HASH_BYTES", cfg.hash_max_bytes)
    case_insensitive = env.get_bool("INVENTORY_CASE_INSENSITIVE", False)

    return InventorySettings(
        db_path=db_path,
        remote_root=remote_root,
        hash_mode=hash_mode,
        max_hash_bytes=max_hash_bytes,
        case_insensitive=case_insensitive,
    )


def load_config(env_path: Path, args: Any) -> Config:
    env = Env(env_path, profile=getattr(args, "profile", None))

    email_to_raw = env.get("EMAIL_TO", "")
    email_to_env = [x.strip() for x in email_to_raw.replace(";", ",").split(",") if x.strip()] if email_to_raw else []

    cfg = Config(
        host=str(env.get("SFTP_HOST", "")),
        port=env.get_int("SFTP_PORT", 22),
        username=str(env.get("SFTP_USERNAME", "")),
        password=env.get("SFTP_PASSWORD", None),
        key_path=env.get("SFTP_KEY_PATH", None),
        remote_root=str(env.get("REMOTE_ROOT", "/")).rstrip("/") + "/",
        local_root=Path(str(env.get("LOCAL_ROOT", "."))).resolve(),
        order=env.get_list("ORDER"),
        local_dirs=None,
        exclude_exts=set([x.strip().lower() for x in env.get_list("EXCLUDE_EXTS")]),
        include_globs=env.get_list("FILTER_INCLUDE_GLOBS"),
        exclude_globs=env.get_list("FILTER_EXCLUDE_GLOBS"),
        min_size=env.get_int("FILTER_MIN_SIZE", 0),
        max_size=env.get_int("FILTER_MAX_SIZE", 0),
        newest_scope=str(env.get("FILTER_NEWEST_SCOPE", "off")).lower(),
        newest_count=env.get_int("FILTER_NEWEST_COUNT", 1),
        require_meta=env.get_bool("REQUIRE_META", True),
        confirm_put=env.get_bool("CONFIRM_PUT", True),
        include_root_name=env.get_bool("INCLUDE_ROOT_NAME", True),
        skip_if_exists=str(env.get("SKIP_IF_EXISTS", "size_equal")).lower(),
        resume_enabled=env.get_bool("RESUME_ENABLED", True),
        upload_chunk_size=env.get_int("UPLOAD_CHUNK_SIZE", 32768),
        rate_limit_bps=env.get_int("RATE_LIMIT_BPS", 0),
        rate_limit_per_file_bps=env.get_int("RATE_LIMIT_PER_FILE_BPS", 0),
        max_workers=env.get_int("MAX_WORKERS", 5),
        retries=env.get_int("RETRIES", 3),
        backoff_base=env.get_float("BACKOFF_BASE", 1.0),
        backoff_max=env.get_float("BACKOFF_MAX", 10.0),
        backoff_jitter=env.get_float("BACKOFF_JITTER", 0.3),
        verify_hash=env.get_bool("VERIFY_HASH", True),
        hash_algo=str(env.get("HASH_ALGO", "sha256")).lower(),
        hash_max_bytes=env.get_int("HASH_MAX_BYTES", 209_715_200),
        progress=env.get_bool("PROGRESS", True),
        progress_print_interval=env.get_float("PROGRESS_PRINT_INTERVAL", 0.5),
        log_file=str(env.get("LOG_FILE", "sftp_upload.log")),
        timeout=env.get_int("TIMEOUT", 30),
        strict_host_key_checking=env.get_bool("STRICT_HOST_KEY_CHECKING", False),
        create_end_touch=env.get_bool("CREATE_END_TOUCH", False),
        touch_only=env.get_bool("TOUCH_ONLY", False),
        end_touch_name=str(env.get("END_TOUCH_NAME", "end.touch")),
        quarantine_excluded=env.get_bool("QUARANTINE_EXCLUDED", True),
        quarantine_failed=env.get_bool("QUARANTINE_FAILED", True),
        slack_webhook_url=env.get("SLACK_WEBHOOK_URL", None),
        smtp_server=env.get("SMTP_SERVER", None),
        smtp_port=env.get_int("SMTP_PORT", 587),
        smtp_user=env.get("SMTP_USER", None),
        smtp_pass=env.get("SMTP_PASS", None),
        smtp_use_tls=env.get_bool("SMTP_USE_TLS", True),
        email_from=env.get("EMAIL_FROM", None),
        email_to=email_to_env,
        server_policies=env.get_json("SERVER_POLICIES", {}) or {},
        profile_name=env.profile or (args.profile or "default"),
        config_path=str(env_path),
    )

    cfg.elasticsearch = parse_elasticsearch_config(env, cfg)
    cfg.inventory = build_inventory_settings(env, cfg)
    cfg.postgres = parse_postgres_config(env)

    if getattr(args, "password", None) == "__PROMPT__":
        cfg.password = getpass.getpass("SFTP password: ")
    elif getattr(args, "password", None) not in (None, "__PROMPT__"):
        cfg.password = args.password

    if getattr(args, "smtp_pass", None) == "__PROMPT__":
        cfg.smtp_pass = getpass.getpass("SMTP password: ")
    elif getattr(args, "smtp_pass", None) not in (None, "__PROMPT__"):
        cfg.smtp_pass = args.smtp_pass

    if args.local_dirs:
        resolved: List[Path] = []
        for directory in args.local_dirs:
            p = Path(directory)
            if not p.is_absolute():
                p = (cfg.local_root / p).resolve()
            else:
                p = p.resolve()
            resolved.append(p)
        cfg.local_dirs = resolved

    if args.email_to:
        tails = [x.strip() for x in args.email_to.replace(";", ",").split(",") if x.strip()]
        cfg.email_to = cfg.email_to + tails

    if getattr(args, "postgres_password", None) == "__PROMPT__":
        if cfg.postgres is None:
            raise ValueError("POSTGRES configuration is not defined in YAML/ENV")
        cfg.postgres.password = getpass.getpass("PostgreSQL password: ")
    elif getattr(args, "postgres_password", None):
        if cfg.postgres is None:
            raise ValueError("POSTGRES configuration is not defined in YAML/ENV")
        cfg.postgres.password = args.postgres_password

    cfg = apply_server_policy(cfg)
    return cfg


__all__ = [
    "Env",
    "Config",
    "Stats",
    "FilePair",
    "FileResult",
    "ElasticsearchNode",
    "ElasticsearchConfig",
    "PostgresColumn",
    "PostgresConfig",
    "InventorySettings",
    "apply_server_policy",
    "parse_elasticsearch_config",
    "parse_postgres_config",
    "load_config",
]
