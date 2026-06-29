import json
import unittest

from scripts.migrate_account_storage import (
    MYSQL_DSN_ENV,
    POSTGRES_DSN_ENV,
    build_tidb_mysql_url,
    convert_rows_to_mysql_payload,
    parse_args,
)


class MigrateAccountStorageTests(unittest.TestCase):
    def test_build_tidb_mysql_url_normalizes_postgres_sslmode_require(self):
        pg_url = (
            "postgresql://avnadmin:secret@pg.example.com:26257/defaultdb"
            "?sslmode=require"
        )

        got = build_tidb_mysql_url(
            pg_url,
            host="gateway01.us-west-2.prod.aws.tidbcloud.com",
            port=4000,
            database="grok2api",
            username="root",
            password="tidb-secret",
        )

        self.assertEqual(
            got,
            "mysql+aiomysql://root:tidb-secret@"
            "gateway01.us-west-2.prod.aws.tidbcloud.com:4000/grok2api"
            "?charset=utf8mb4&ssl-mode=REQUIRED",
        )

    def test_convert_rows_preserves_json_text_and_revision(self):
        account_rows = [
            {
                "token": "tok-1",
                "pool": "basic",
                "status": "active",
                "created_at": 1,
                "updated_at": 2,
                "tags": ["alpha"],
                "quota_auto": {"remaining": 3},
                "quota_fast": {"remaining": 4},
                "quota_expert": {},
                "quota_heavy": {},
                "quota_grok_4_3": {},
                "quota_console": {"remaining": 5},
                "usage_use_count": 6,
                "usage_fail_count": 7,
                "usage_sync_count": 8,
                "last_use_at": 9,
                "last_fail_at": 10,
                "last_fail_reason": "bad",
                "last_sync_at": 11,
                "last_clear_at": 12,
                "state_reason": None,
                "deleted_at": None,
                "ext": {"expired_at": 13},
                "revision": 14,
            }
        ]
        meta_rows = [{"key": "revision", "value": "14"}]

        payload = convert_rows_to_mysql_payload(account_rows, meta_rows)

        self.assertEqual(payload["accounts"][0]["token"], "tok-1")
        self.assertEqual(json.loads(payload["accounts"][0]["tags"]), ["alpha"])
        self.assertEqual(
            json.loads(payload["accounts"][0]["quota_console"])["remaining"], 5
        )
        self.assertEqual(
            json.loads(payload["accounts"][0]["ext"])["expired_at"], 13
        )
        self.assertEqual(payload["accounts"][0]["revision"], 14)
        self.assertEqual(payload["account_meta"], [{"key": "revision", "value": "14"}])

    def test_parse_args_requires_target_tidb_information(self):
        args = parse_args(
            [
                "--source-postgres",
                "postgresql://user:pw@host:5432/db?sslmode=require",
                "--target-host",
                "tidb.example.com",
                "--target-password",
                "pw",
            ]
        )

        self.assertEqual(args.source_postgres, "postgresql://user:pw@host:5432/db?sslmode=require")
        self.assertEqual(args.target_host, "tidb.example.com")
        self.assertEqual(args.target_port, 4000)
        self.assertEqual(args.target_database, "grok2api")
        self.assertEqual(args.target_username, "root")
        self.assertEqual(args.target_password, "pw")
        self.assertEqual(args.source_env, POSTGRES_DSN_ENV)
        self.assertEqual(args.target_env, MYSQL_DSN_ENV)


if __name__ == "__main__":
    unittest.main()
