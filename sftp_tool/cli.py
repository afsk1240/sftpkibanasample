
"""Command-line argument parsing for the SFTP tool."""

from __future__ import annotations

import argparse


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="SFTP uploader with inventory and metadata helpers")
    parser.add_argument("--env", type=str, default="sftp.yaml", help="Path to configuration file")
    parser.add_argument("--profile", type=str, help="Configuration profile name")
    parser.add_argument("--local-dirs", type=str, nargs="*", help="Override directories to process (absolute or relative to LOCAL_ROOT)")
    parser.add_argument("--show-config", action="store_true", help="Print effective configuration and exit")
    parser.add_argument("--password", nargs="?", const="__PROMPT__", help="Override SFTP password (omit value to prompt)")
    parser.add_argument("--smtp-pass", nargs="?", const="__PROMPT__", help="Override SMTP password (omit value to prompt)")
    parser.add_argument("--email-to", type=str, help="Append notification recipients (comma/semicolon separated)")
    parser.add_argument("--dry-run", action="store_true", help="Do not upload files; only compute selections")

    # operational helpers
    parser.add_argument("--list-remote", nargs="?", const="", help="List a remote SFTP directory (default: REMOTE_ROOT)")
    parser.add_argument("--ssh-command", type=str, help="Run a command on Elasticsearch nodes via SSH")
    parser.add_argument("--ssh-nodes", type=str, help="Comma/semicolon separated node names to target with --ssh-command")
    parser.add_argument("--es-query", type=str, help="Elasticsearch API request in METHOD:/path form (e.g. POST:/_analyze)")
    parser.add_argument("--es-body", type=str, help="Elasticsearch request body (JSON string or @path/to/file.json)")
    parser.add_argument("--es-index", type=str, help="Value to replace {index} tokens inside --es-query")

    # inventory actions
    parser.add_argument("--inventory-scan", action="store_true", help="Scan remote files and update the inventory database")
    parser.add_argument("--inventory-report", action="store_true", help="Generate change reports from the inventory database")
    parser.add_argument("--inventory-enrich-db", action="store_true", help="Enrich the latest inventory report with PostgreSQL metadata")
    parser.add_argument("--inventory-db", type=str, help="Path to the inventory SQLite database")
    parser.add_argument("--inventory-remote-root", type=str, help="Remote root to scan instead of INVENTORY_REMOTE_ROOT")
    parser.add_argument("--inventory-hash-mode", type=str, help="Override inventory hash mode (full|changed|off)")
    parser.add_argument("--inventory-max-hash-bytes", type=int, help="Override max bytes to hash per file during inventory")
    parser.add_argument("--inventory-run-id", type=int, help="Target run id for reporting/enrichment (default: latest)")
    parser.add_argument("--inventory-output", type=str, help="Directory for inventory reports (default: reports/inventory/<run>)")

    # postgres overrides
    parser.add_argument("--postgres-password", nargs="?", const="__PROMPT__", help="Override PostgreSQL password (omit value to prompt)")

    return parser.parse_args()


__all__ = ["parse_args"]
