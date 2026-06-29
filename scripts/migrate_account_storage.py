import argparse
import asyncio
import json
import os
from typing import Any
from urllib.parse import parse_qsl, quote, urlencode, urlparse, urlunparse

import sqlalchemy as sa

POSTGRES_DSN_ENV = "ACCOUNT_POSTGRESQL_URL"
MYSQL_DSN_ENV = "ACCOUNT_MYSQL_URL"

JSON_ACCOUNT_COLUMNS = (
    "tags",
    "quota_auto",
    "quota_fast",
    "quota_expert",
    "quota_heavy",
    "quota_grok_4_3",
    "quota_console",
    "ext",
)

_SSL_MODE_QUERY_KEYS = ("sslmode", "ssl-mode", "ssl")
_PG_TO_TIDB_SSL_MODE = {
    "disable": "DISABLED",
    "disabled": "DISABLED",
    "false": "DISABLED",
    "0": "DISABLED",
    "off": "DISABLED",
    "no": "DISABLED",
    "require": "REQUIRED",
    "required": "REQUIRED",
    "true": "REQUIRED",
    "1": "REQUIRED",
    "on": "REQUIRED",
    "yes": "REQUIRED",
    "verify-ca": "VERIFY_CA",
    "verify_ca": "VERIFY_CA",
    "verify-full": "VERIFY_IDENTITY",
    "verify_full": "VERIFY_IDENTITY",
    "verify-identity": "VERIFY_IDENTITY",
    "verify_identity": "VERIFY_IDENTITY",
}


def _normalize_json_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    if value is None:
        return "{}"
    return json.dumps(value)


def _infer_tidb_ssl_mode(source_postgres_url: str) -> str:
    parsed = urlparse(source_postgres_url)
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        if key.lower() not in _SSL_MODE_QUERY_KEYS:
            continue
        mapped = _PG_TO_TIDB_SSL_MODE.get(value.strip().lower())
        if mapped:
            return mapped
    return "REQUIRED"


def build_tidb_mysql_url(
    source_postgres_url: str,
    *,
    host: str,
    port: int = 4000,
    database: str = "grok2api",
    username: str = "root",
    password: str,
    ssl_mode: str | None = None,
) -> str:
    query_items = [("charset", "utf8mb4")]
    effective_ssl_mode = ssl_mode or _infer_tidb_ssl_mode(source_postgres_url)
    if effective_ssl_mode:
        query_items.append(("ssl-mode", effective_ssl_mode))
    netloc = f"{quote(username, safe='')}:{quote(password, safe='')}@{host}:{port}"
    return urlunparse(
        (
            "mysql+aiomysql",
            netloc,
            f"/{database}",
            "",
            urlencode(query_items),
            "",
        )
    )


def convert_rows_to_mysql_payload(
    account_rows: list[dict[str, Any]],
    meta_rows: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    converted_accounts: list[dict[str, Any]] = []
    for row in account_rows:
        normalized = dict(row)
        for column in JSON_ACCOUNT_COLUMNS:
            if column not in normalized:
                continue
            if column == "tags" and normalized[column] is None:
                normalized[column] = "[]"
            elif normalized[column] is None:
                normalized[column] = "{}"
            else:
                normalized[column] = _normalize_json_text(normalized[column])
        converted_accounts.append(normalized)

    converted_meta = [{"key": row["key"], "value": str(row["value"])} for row in meta_rows]
    return {
        "accounts": converted_accounts,
        "account_meta": converted_meta,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Migrate grok2api account storage from PostgreSQL to TiDB/MySQL.",
    )
    parser.add_argument("--source-env", default=POSTGRES_DSN_ENV)
    parser.add_argument("--target-env", default=MYSQL_DSN_ENV)
    parser.add_argument("--source-postgres", default="")
    parser.add_argument("--target-dsn", default="")
    parser.add_argument("--target-host", default="")
    parser.add_argument("--target-port", type=int, default=4000)
    parser.add_argument("--target-database", default="grok2api")
    parser.add_argument("--target-username", default="root")
    parser.add_argument("--target-password", default="")
    parser.add_argument("--target-ssl-mode", default="")
    parser.add_argument("--dump-json", action="store_true")
    return parser.parse_args(argv)


def _resolve_required_value(explicit_value: str, env_name: str) -> str:
    value = explicit_value.strip() or os.getenv(env_name, "").strip()
    if not value:
        raise ValueError(f"Missing required value from --{env_name.lower().replace('_', '-')} or {env_name}.")
    return value


def _resolve_target_dsn(args: argparse.Namespace, source_dsn: str) -> str:
    if args.target_dsn.strip():
        return args.target_dsn.strip()

    env_dsn = os.getenv(args.target_env, "").strip()
    if env_dsn:
        return env_dsn

    if not args.target_host.strip():
        raise ValueError("Missing target host. Provide --target-host or set ACCOUNT_MYSQL_URL.")
    if not args.target_password.strip():
        raise ValueError("Missing target password. Provide --target-password or set ACCOUNT_MYSQL_URL.")

    return build_tidb_mysql_url(
        source_dsn,
        host=args.target_host.strip(),
        port=args.target_port,
        database=args.target_database,
        username=args.target_username,
        password=args.target_password,
        ssl_mode=args.target_ssl_mode.strip() or None,
    )


async def _fetch_source_rows(source_dsn: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    from app.control.account.backends.sql import (
        accounts_table,
        create_pgsql_engine,
        meta_table,
    )

    engine = create_pgsql_engine(source_dsn)
    try:
        async with engine.connect() as conn:
            account_rows = [
                dict(row._mapping)
                for row in (await conn.execute(sa.select(accounts_table))).fetchall()
            ]
            meta_rows = [
                dict(row._mapping)
                for row in (await conn.execute(sa.select(meta_table))).fetchall()
            ]
            return account_rows, meta_rows
    finally:
        await engine.dispose()


async def _write_target_rows(target_dsn: str, payload: dict[str, list[dict[str, Any]]]) -> None:
    from sqlalchemy.dialects.mysql import insert as mysql_insert

    from app.control.account.backends.sql import (
        SqlAccountRepository,
        accounts_table,
        create_mysql_engine,
        meta_table,
    )

    engine = create_mysql_engine(target_dsn)
    repository = SqlAccountRepository(engine, dialect="mysql", dispose_engine=False)
    try:
        await repository.initialize()
        async with engine.begin() as conn:
            for row in payload["accounts"]:
                stmt = mysql_insert(accounts_table).values(**row)
                update_cols = {
                    key: stmt.inserted[key]
                    for key in row
                    if key not in ("token", "created_at")
                }
                await conn.execute(stmt.on_duplicate_key_update(**update_cols))

            for row in payload["account_meta"]:
                stmt = mysql_insert(meta_table).values(**row)
                await conn.execute(
                    stmt.on_duplicate_key_update(value=stmt.inserted.value)
                )
    finally:
        await engine.dispose()


async def _run(args: argparse.Namespace) -> None:
    source_dsn = _resolve_required_value(args.source_postgres, args.source_env)
    account_rows, meta_rows = await _fetch_source_rows(source_dsn)
    payload = convert_rows_to_mysql_payload(account_rows, meta_rows)

    if args.dump_json:
        print(json.dumps(payload, indent=2))
        return

    target_dsn = _resolve_target_dsn(args, source_dsn)
    await _write_target_rows(target_dsn, payload)
    print(
        f"Migrated {len(payload['accounts'])} account rows and "
        f"{len(payload['account_meta'])} meta rows to TiDB."
    )


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
