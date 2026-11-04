"""Run the real core_document model against a disposable local PostgreSQL."""

import os
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from uuid import uuid4

import psycopg2
import yaml

PROJECT = Path(__file__).resolve().parents[2]
DOCUMENTS = "select * from core_document order by document_id"


def dbt(directory, command="run", expected_error=None):
    run = subprocess.run(
        [
            "dbt",
            command,
            "--select",
            "core_document",
            "--project-dir",
            str(PROJECT),
            "--profiles-dir",
            str(directory),
            "--target-path",
            str(directory / "target"),
            "--log-path",
            str(directory / "logs"),
        ],
        env={
            **os.environ,
            "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
            "PGOPTIONS": "-c lock_timeout=10s -c statement_timeout=30s",
        },
        capture_output=True,
        text=True,
        timeout=90,
    )
    output = run.stdout + run.stderr
    if expected_error:
        assert run.returncode != 0 and expected_error in output, output
    else:
        assert run.returncode == 0, output
    if command == "test":
        print(output, end="")


def rows(cur, sql, params=None):
    cur.execute(sql, params)
    return cur.fetchall()


def relation_ids(cur):
    return rows(
        cur,
        """select indrelid::oid, indexrelid::oid from pg_index
        where indrelid = 'core_document'::regclass and indisvalid and indisready
        order by indexrelid""",
    )


def public_can_read(cur):
    return rows(
        cur,
        """select exists(select from pg_class c, aclexplode(c.relacl) a
        where c.oid = 'core_document'::regclass and a.grantee = 0 and a.privilege_type = 'SELECT')""",
    )[0][0]


def build_with_readers(directory, credentials, cur, schema, before):
    with (
        closing(psycopg2.connect(**credentials)) as snapshot,
        closing(psycopg2.connect(**credentials)) as locker,
    ):
        snapshot.set_session(isolation_level="REPEATABLE READ")
        with snapshot.cursor() as reader, locker.cursor() as writer:
            reader.execute(f"set search_path to {schema}")
            reader.execute(
                "select txid_current_snapshot()"
            )  # Snapshot before dbt, first table read after commit.
            writer.execute(
                f"select document_id from {schema}.core_document where document_id = 2 for update"
            )
            with ThreadPoolExecutor(max_workers=1) as pool:
                build = pool.submit(dbt, directory)
                try:
                    deadline = time.monotonic() + 60
                    while not rows(
                        cur,
                        """select pid from pg_stat_activity
                            where %s = any(pg_blocking_pids(pid))""",
                        (locker.get_backend_pid(),),
                    ):
                        if build.done():
                            build.result()
                            raise AssertionError(
                                "dbt finished without updating the locked document"
                            )
                        if time.monotonic() > deadline:
                            raise AssertionError(
                                "dbt did not reach the locked document"
                            )
                        time.sleep(0.05)

                    # An independent reader can start while dbt is waiting on the row.
                    assert rows(cur, DOCUMENTS) == before
                    cur.execute("revoke select on core_document from public")
                finally:
                    locker.rollback()
                build.result()
            assert rows(reader, DOCUMENTS) == before
    assert not public_can_read(cur)


def exercise(directory, credentials, cur, schema):
    cur.execute(
        f"alter default privileges in schema {schema} grant select on tables to public"
    )
    cur.execute("""
        create table staging_document_status (
            document_id int, tenant_id int, guarantor_id int, document_category varchar,
            document_sub_category varchar, document_status varchar, added_at timestamp,
            deleted_at timestamp, total_add_document_events bigint, total_delete_document_events bigint);
        create table staging_document (id int, document_category varchar, document_sub_category varchar,
            document_category_step varchar, document_status varchar, monthly_net_income int,
            created_at timestamp, modified_at timestamp);
        insert into staging_document_status (document_id, tenant_id, document_status, added_at, total_add_document_events)
        values (1, 10, 'DELETED', '2020-01-01', 3), (2, 20, 'ADDED', '2026-01-01', 1)
    """)
    dbt(directory)
    before = rows(cur, DOCUMENTS)
    original_ids = relation_ids(cur)
    assert len(original_ids) == 5 and public_can_read(cur)

    cur.execute("""
        delete from staging_document_status where document_id = 1;
        update staging_document_status set document_status = null where document_id = 2;
        insert into staging_document_status (document_id, document_status) values (3, 'ADDED')
    """)
    build_with_readers(directory, credentials, cur, schema, before)
    after = rows(cur, DOCUMENTS)
    assert after[0] == before[0]  # All columns of the historical document.
    assert rows(
        cur, "select document_id, document_status from core_document order by 1"
    ) == [
        (1, "DELETED"),
        (2, None),
        (3, "ADDED"),
    ]
    assert relation_ids(cur) == original_ids

    cur.execute("""
        grant select on core_document to public;
        create function document_number(d core_document) returns int language sql as 'select d.document_id';
        create view document_reader as select document_number(d) as id from core_document d;
        create function check_incoming_stats() returns trigger language plpgsql as $$
        begin
            if not exists (
                select from pg_stats where schemaname = pg_my_temp_schema()::regnamespace::text
                and attname = 'document_id'
            ) then
                raise exception 'incoming document IDs lack statistics before DELETE';
            end if;
            return null;
        end $$;
        create trigger check_incoming_stats before delete on core_document
            for each statement execute function check_incoming_stats()
    """)
    dbt(directory)
    assert rows(cur, DOCUMENTS) == after
    assert relation_ids(cur) == original_ids and public_can_read(cur)
    assert rows(cur, "select id from document_reader order by id") == [(1,), (2,), (3,)]
    cur.execute("drop trigger check_incoming_stats on core_document")

    cur.execute(
        "insert into staging_document_status select * from staging_document_status where document_id = 3"
    )
    dbt(directory, expected_error="duplicate key value violates unique constraint")
    assert (
        rows(cur, DOCUMENTS) == after
    )  # The failed INSERT must also roll back the DELETE.
    cur.execute("truncate staging_document_status")
    dbt(directory)
    assert rows(cur, DOCUMENTS) == after and relation_ids(cur) == original_ids
    dbt(directory, command="test")


def main():
    schema = "document_test_" + uuid4().hex
    credentials = dict(
        host="127.0.0.1",
        port=int(os.environ.get("PGPORT", 5432)),
        dbname="postgres",
        user="postgres",
        password=os.environ["POSTGRES_PASSWORD"],
    )
    with (
        tempfile.TemporaryDirectory() as tmp,
        closing(psycopg2.connect(**credentials)) as conn,
    ):
        directory = Path(tmp)
        profile = {**credentials, "schema": schema, "type": "postgres", "threads": 1}
        path = directory / "profiles.yml"
        path.touch(mode=0o600)
        path.write_text(
            yaml.safe_dump(
                {"dossierfacile": {"target": "test", "outputs": {"test": profile}}}
            )
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f"create schema {schema}")
            try:
                cur.execute(f"set search_path to {schema}")
                cur.execute("set lock_timeout = '2s'")
                exercise(directory, credentials, cur, schema)
            finally:
                cur.execute(f"drop schema {schema} cascade")


if __name__ == "__main__":
    main()
