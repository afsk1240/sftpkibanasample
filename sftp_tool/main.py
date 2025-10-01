"""High-level entrypoint for the SFTP tool."""

from __future__ import annotations

import json
from dataclasses import asdict, replace
from pathlib import Path

from .cli import parse_args
from .config import InventorySettings, load_config
from .inventory import generate_report, get_latest_run_id, scan_and_record, enrich_with_postgres
from .special_ops import execute_elasticsearch_query, list_remote_directory, run_ssh_command
from .uploader import run_upload, setup_logger


def _handle_special_operations(cfg, args, logger) -> bool:
    action_taken = False
    if args.list_remote is not None:
        list_remote_directory(cfg, args.list_remote, logger)
        action_taken = True
    if args.ssh_command:
        targets = []
        if args.ssh_nodes:
            targets = [t.strip() for chunk in args.ssh_nodes.split(";") for t in chunk.split(",") if t.strip()]
        run_ssh_command(cfg, args.ssh_command, targets, logger)
        action_taken = True
    if args.es_query:
        execute_elasticsearch_query(cfg, args.es_query, args.es_body, args.es_index, logger)
        action_taken = True
    return action_taken


def _handle_inventory(cfg, args, logger) -> bool:
    wants_inventory = any(
        [
            args.inventory_scan,
            args.inventory_report,
            args.inventory_enrich_db,
        ]
    )
    if not wants_inventory:
        return False

    inv_settings: InventorySettings = cfg.inventory
    if args.inventory_db:
        inv_settings = replace(inv_settings, db_path=Path(args.inventory_db).expanduser())
    if args.inventory_remote_root:
        root = args.inventory_remote_root.rstrip('/') or '/'
        inv_settings = replace(inv_settings, remote_root=root + '/')
    if args.inventory_hash_mode:
        mode = args.inventory_hash_mode.lower()
        if mode not in {"full", "changed", "off"}:
            raise ValueError("--inventory-hash-mode must be one of full|changed|off")
        inv_settings = replace(inv_settings, hash_mode=mode)
    if args.inventory_max_hash_bytes is not None:
        inv_settings = replace(inv_settings, max_hash_bytes=int(args.inventory_max_hash_bytes))

    db_path = inv_settings.db_path
    run_id_from_scan = None
    report_result = None

    if args.inventory_scan:
        scan_result = scan_and_record(cfg, inv_settings, logger)
        run_id_from_scan = scan_result.run_id
        logger.info(
            "[INVENTORY] scan completed: run=%s files=%s bytes=%s duration=%.2fs",
            scan_result.run_id,
            scan_result.total_files,
            scan_result.total_bytes,
            scan_result.duration_sec,
        )

    if args.inventory_report or args.inventory_enrich_db:
        target_run = run_id_from_scan
        if target_run is None:
            if args.inventory_run_id is not None:
                target_run = args.inventory_run_id
            else:
                target_run = get_latest_run_id(db_path)
        if target_run is None:
            raise RuntimeError("No inventory runs available; perform --inventory-scan first.")

        output_dir = Path(args.inventory_output).expanduser() if args.inventory_output else None
        report_result = generate_report(cfg, inv_settings, target_run, output_dir, db_path, logger)

    if args.inventory_enrich_db:
        if cfg.postgres is None:
            raise RuntimeError("POSTGRES configuration is required for --inventory-enrich-db")
        if report_result is None:
            raise RuntimeError("Inventory report must be generated before enrichment.")
        enrich_with_postgres(cfg, inv_settings, cfg.postgres, db_path, report_result.run_id, report_result.output_dir, logger)

    return True


def main() -> int:
    args = parse_args()
    cfg = load_config(Path(args.env).resolve(), args)
    logger = setup_logger(cfg.log_file)

    if args.show_config:
        printable = asdict(cfg).copy()
        for key in ("password", "smtp_pass"):
            if printable.get(key):
                printable[key] = "***"
        es_mask = printable.get("elasticsearch")
        if isinstance(es_mask, dict) and es_mask.get("api_pass"):
            es_mask["api_pass"] = "***"
        pg_mask = printable.get("postgres")
        if isinstance(pg_mask, dict) and pg_mask.get("password"):
            pg_mask["password"] = "***"
        logger.info("[CONFIG]\n" + json.dumps(printable, ensure_ascii=False, indent=2))

    acted = False
    if _handle_special_operations(cfg, args, logger):
        acted = True
    if _handle_inventory(cfg, args, logger):
        acted = True
    if acted:
        return 0

    return run_upload(cfg, args, logger)


__all__ = ["main"]
