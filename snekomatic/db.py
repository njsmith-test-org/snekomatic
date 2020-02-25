import os
from pathlib import Path
from sqlalchemy import create_engine, MetaData, Table, Column, ForeignKey
from sqlalchemy.types import (
    String,
    Integer,
    Boolean,
    DateTime,
    JSON,
)
from sqlalchemy.sql.expression import select, exists, text
import alembic.config
import alembic.command
import alembic.migration
import alembic.autogenerate
import pprint
import attr

# Required to make sure that constraints like ForeignKey get a stable name so
# migration can be supported.
naming_convention = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

metadata = MetaData(naming_convention=naming_convention)

sent_invitation = Table(
    "persistent_set_sent_invitation",
    metadata,
    Column("entry", String, primary_key=True),
)

worker_tasks = Table(
    "worker_tasks",
    metadata,
    Column("task_id", String, primary_key=True),
    Column("args", JSON, nullable=False),
    Column("started", DateTime, nullable=False),
    Column("finished", Boolean, nullable=False),
)

worker_task_events = Table(
    "worker_task_messages",
    metadata,
    Column("message_id", Integer, primary_key=True),
    Column(
        "task_id", String, ForeignKey("worker_tasks.task_id"), nullable=False
    ),
    Column("message", JSON, nullable=False),
)


@attr.s
class CachedEngine:
    engine = attr.ib()
    database_url = attr.ib()


CACHED_ENGINE = CachedEngine(None, None)


def get_conn():
    global CACHED_ENGINE
    if CACHED_ENGINE.database_url != os.environ["DATABASE_URL"]:
        engine = create_engine(os.environ["DATABASE_URL"])

        # Set this *temporarily* in a *test* environment to reset the database
        # at startup. Useful if you're iterating on db schema changes and
        # aren't ready to mess with alembic migrations yet.
        if "DESTRUCTIVE_TESTING_RESET_DB" in os.environ:
            assert "test" in os.environ["HEROKU_APP_NAME"]
            print("-- DESTRUCTIVE TESTING ENABLED; WIPING DB --")
            with engine.connect() as conn:
                # https://stackoverflow.com/questions/3327312/how-can-i-drop-all-the-tables-in-a-postgresql-database
                conn.execute(text("""
                  DROP SCHEMA public CASCADE;
                  CREATE SCHEMA public;
                  GRANT ALL ON SCHEMA public TO postgres;
                  GRANT ALL ON SCHEMA public TO public;
                  COMMIT;
                """))
            metadata.create_all(engine)
        else:
            # Run any necessary migrations
            with engine.connect() as conn:
                alembic_cfg = alembic.config.Config(
                    Path(__file__).parent / "alembic.ini"
                )
                alembic_cfg.attributes["connection"] = conn
                alembic.command.upgrade(alembic_cfg, "head")

        # Verify that the actual final schema matches what we expect
        with engine.connect() as conn:
            mc = alembic.migration.MigrationContext.configure(conn)
            diff = alembic.autogenerate.compare_metadata(mc, metadata)
            if diff:
                print("!!! mismatch between db schema and code")
                pprint.pprint(diff)
                raise RuntimeError("consistency check failed")

        # Iff that all worked out, then save the engine so we can skip those
        # checks next time
        CACHED_ENGINE = CachedEngine(engine, os.environ["DATABASE_URL"])
    return CACHED_ENGINE.engine.connect()


class SentInvitation:
    @staticmethod
    def contains(name):
        with get_conn() as conn:
            # This is:
            #   SELECT EXISTS (SELECT 1 FROM sent_invitation WHERE entry = ?)
            return conn.execute(
                select(
                    [
                        exists(
                            select([1]).where(sent_invitation.c.entry == name)
                        )
                    ]
                )
            ).scalar()

    @staticmethod
    def add(name):
        with get_conn() as conn:
            conn.execute(sent_invitation.insert(), entry=name)
