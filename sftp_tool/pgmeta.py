"""PostgreSQL helpers for metadata enrichment."""

from __future__ import annotations

import logging
from typing import Dict, Iterable, List, Optional

from .config import PostgresColumn, PostgresConfig

try:  # pragma: no cover - optional dependency
    import psycopg
    from psycopg import sql
    from psycopg.rows import dict_row
except ImportError:  # pragma: no cover - fallback when psycopg missing
    psycopg = None
    sql = None
    dict_row = None


def _require_psycopg() -> None:
    if psycopg is None:
        raise RuntimeError(
            "psycopg (>=3) is required for PostgreSQL integration. Install the 'postgres' extra."
        )


def _safe_identifier(value: Optional[str]) -> List[str]:
    if not value:
        raise ValueError("PostgreSQL table/column names must be configured")
    for chunk in value.split('.'):
        if not chunk:
            raise ValueError(f"Invalid identifier: {value}")
    return value.split('.')


def _sql_expression(expression: str) -> sql.Composed:
    assert sql is not None
    parts = expression.split('.')
    if all(part.replace('_', '').isalnum() for part in parts):
        return sql.Identifier(*parts)
    return sql.SQL(expression)


def fetch_metadata(pg_cfg: PostgresConfig, physical_names: Iterable[str], logger: logging.Logger) -> Dict[str, Dict[str, object]]:
    _require_psycopg()
    assert psycopg is not None and sql is not None and dict_row is not None

    names = [name for name in physical_names if name]
    if not names:
        return {}

    table_ident = _safe_identifier(pg_cfg.table)
    match_column = pg_cfg.match_column or pg_cfg.physical_column
    if not match_column:
        raise ValueError("POSTGRES.match_column or physical_column must be configured")

    columns = pg_cfg.columns or [PostgresColumn(expression=match_column, alias=match_column)]

    select_items = []
    for col in columns:
        expr = _sql_expression(col.expression)
        alias = sql.Identifier(col.alias)
        select_items.append(sql.SQL('{} AS {}').format(expr, alias))

    select_sql = sql.SQL(', ').join(select_items)
    table_sql = sql.Identifier(*table_ident)
    match_ident = _safe_identifier(match_column)
    where_clause = sql.SQL('{col} = ANY(%s)').format(col=sql.Identifier(*match_ident))
    if pg_cfg.additional_where:
        where_clause = sql.SQL('({}) AND ({})').format(where_clause, sql.SQL(pg_cfg.additional_where))

    conn = psycopg.connect(
        host=pg_cfg.host,
        port=pg_cfg.port,
        dbname=pg_cfg.dbname,
        user=pg_cfg.user,
        password=pg_cfg.password,
        sslmode=pg_cfg.sslmode,
        connect_timeout=pg_cfg.connect_timeout,
        row_factory=dict_row,
    )
    try:
        with conn.cursor() as cur:
            query = sql.SQL('SELECT {cols} FROM {table} WHERE {where}').format(
                cols=select_sql,
                table=table_sql,
                where=where_clause,
            )
            cur.execute(query, (names,))
            rows = cur.fetchall()
    finally:
        conn.close()

    match_alias = None
    for col in columns:
        if col.expression == match_column:
            match_alias = col.alias
            break
    if match_alias is None:
        match_alias = match_column

    result: Dict[str, Dict[str, object]] = {}
    for row in rows:
        key_candidate = (
            row.get(match_alias)
            or row.get(match_alias.lower())
            or row.get(match_column)
            or row.get(match_column.lower())
        )
        if key_candidate is None:
            continue
        key = str(key_candidate)
        lookup_key = key.lower() if pg_cfg.case_insensitive else key
        result[lookup_key] = dict(row)

    logger.info("[PGMETA] fetched %s rows from %s", len(result), pg_cfg.table)
    return result


__all__ = ["fetch_metadata"]
