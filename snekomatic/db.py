import os
from pathlib import Path
from contextlib import contextmanager
import pprint
import attr
from sqlalchemy import create_engine, MetaData, Column, String
from sqlalchemy.orm import Session
from sqlalchemy.ext.declarative import declarative_base
import alembic.config
import alembic.command
import alembic.migration
import alembic.autogenerate

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

Base = declarative_base(metadata=metadata)


class SentInvitation(Base):
    __tablename__ = "persistent_set_sent_invitation"

    name = Column("entry", String, primary_key=True)


# worker_tasks = Table(
#     "worker_tasks",
#     metadata,
#     Column("task_id", String, primary_key=True),
#     Column("args", JSON, nullable=False),
#     Column("started", DateTime, nullable=False),
#     Column("finished", Boolean, nullable=False),
# )

# worker_task_messages = Table(
#     "worker_task_messages",
#     metadata,
#     Column("message_id", Integer, primary_key=True),
#     Column(
#         "task_id", String, ForeignKey("worker_tasks.task_id"), nullable=False
#     ),
#     Column("message", JSON, nullable=False),
# )


@attr.s(frozen=True)
class CachedEngine:
    engine = attr.ib()
    database_url = attr.ib()


CACHED_ENGINE = CachedEngine(None, None)


@contextmanager
def open_session():
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
                conn.execute(
                    text(
                        """
                        DROP SCHEMA public CASCADE;
                        CREATE SCHEMA public;
                        GRANT ALL ON SCHEMA public TO postgres;
                        GRANT ALL ON SCHEMA public TO public;
                        COMMIT;
                        """
                    )
                )
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
    session = Session(bind=CACHED_ENGINE.engine)
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()
