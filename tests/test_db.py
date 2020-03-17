import os
import psycopg2
import pytest
import trio
from snekomatic.db import (
    _get_session,
    already_check_and_set,
    retry_txn,
    Already,
)


@pytest.mark.skipif(
    "DESTRUCTIVE_TESTING_RESET_DB" in os.environ,
    reason="destructive db resets enabled",
)
def test_schema_consistency_valiation(heroku_style_pg):
    with psycopg2.connect(os.environ["DATABASE_URL"]) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE unexpected_table_asdofhsdf (hi integer);"
            )

    # Now any attempt to access the database should raise an exception
    with pytest.raises(RuntimeError):
        _get_session()
    with pytest.raises(RuntimeError):
        _get_session()
    with pytest.raises(RuntimeError):
        with retry_txn() as attempts:
            for session in attempts:
                pass


async def test_retry_txn_serializability(heroku_style_pg):
    total_attempts = 0
    found_already_there = 0

    async def task_fn():
        nonlocal total_attempts, found_already_there
        with retry_txn() as attempts:
            for session in attempts:
                total_attempts += 1
                obj = (
                    session.query(Already)
                    .filter_by(domain="d", item="i")
                    .one_or_none()
                )
                await trio.sleep(1)
                if obj is None:
                    session.add(Already(domain="d", item="i"))
                else:
                    found_already_there += 1

    async with trio.open_nursery() as nursery:
        nursery.start_soon(task_fn)
        nursery.start_soon(task_fn)

    assert total_attempts == 3
    assert found_already_there == 1


def test_retry_txn_error_on_early_exit(heroku_style_pg):
    with pytest.raises(AssertionError):
        with retry_txn() as attempts:
            for session in attempts:
                # Can't return directly here, b/c we don't know yet whether
                # the txn will successfully commit.
                return "ok"


def test_already_check_and_set(heroku_style_pg):
    assert not already_check_and_set("d1", "i1")
    assert already_check_and_set("d1", "i1")
    assert already_check_and_set("d1", "i1")

    assert not already_check_and_set("d2", "i1")
    assert already_check_and_set("d2", "i1")

    assert not already_check_and_set("d1", "i2")
    assert already_check_and_set("d1", "i2")
