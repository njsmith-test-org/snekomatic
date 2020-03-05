import os
import psycopg2
import pytest
import pendulum
from snekomatic.db import open_session, already_check_and_set
from .util import mock_time


@pytest.mark.skipif(
    "DESTRUCTIVE_TESTING_RESET_DB" in os.environ,
    reason="destructive db resets enabled",
)
def test_consistency_check(heroku_style_pg):
    with psycopg2.connect(os.environ["DATABASE_URL"]) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE unexpected_table_asdofhsdf (hi integer);"
            )

    # Now any attempt to access the database should raise an exception
    with pytest.raises(RuntimeError):
        with open_session():
            pass
    with pytest.raises(RuntimeError):
        with open_session():
            pass


def test_already_check_and_set(heroku_style_pg):
    with mock_time(pendulum.datetime(2020, 1, 1)):
        assert not already_check_and_set("d1", "i1", pendulum.duration(days=1))
        assert already_check_and_set("d1", "i1", pendulum.duration(days=1))
        assert already_check_and_set("d1", "i1", pendulum.duration(days=1))

        assert not already_check_and_set("d2", "i1", pendulum.duration(days=1))
        assert already_check_and_set("d2", "i1", pendulum.duration(days=1))

        assert not already_check_and_set("d1", "i2", pendulum.duration(days=1))
        assert already_check_and_set("d1", "i2", pendulum.duration(days=1))

    with mock_time(pendulum.datetime(2020, 1, 3)):
        assert not already_check_and_set("d1", "i1", pendulum.duration(days=1))
        assert already_check_and_set("d1", "i1", pendulum.duration(days=1))
