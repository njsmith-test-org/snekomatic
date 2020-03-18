import os
from pathlib import Path
from contextlib import contextmanager
import pprint
import attr
from sqlalchemy import (
    create_engine,
    MetaData,
    Column,
    String,
    Integer,
    ForeignKey,
    Boolean,
    DateTime,
    text,
    Sequence,
)
from sqlalchemy.exc import OperationalError
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Session
from sqlalchemy.ext.declarative import declarative_base
import alembic.config
import alembic.command
import alembic.migration
import alembic.autogenerate
from psycopg2.errors import SerializationFailure

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


# can we only store pending ones, and delete them after completion?
# or should we store all of them?
# if we store all of them, we can have a page that lists them, and provides
# links to info... but then we also need to have a way to retrieve the info
# from old ones, not just the ability to kick off a new (idempotent) job from
# scratch.
#
# ...or maybe we just need a complete-or-not boolean, + the check_run URL, and
# we can make a web page that links people to the check_run directly?
#
# if we need to automatically retry worker tasks in case of github/our bot
# being flaky, then that might also require some extra state tracking, so we
# can tell which retry we're on and create a unique worker task?
#
# XX TODO: track when housekeeping last ran on each repo+branch, so we can
# schedule future ones (and do something intelligent to spread out the load)
# XX TODO: enough info to let us report back results (comment? check run? I
# guess we create a check run... ...and actually we'll want to create another
# check run at the very end, on the final good revision, with a name like
# "okay-to-merge" or "trio-bot-approved", so we can apply branch protection?
# or can we just restrict access to the bot as a user?)
#
# XX maybe some of the event triggers include the PR's head sha? but not all
# of them (e.g. a regular comment in as a pure issue comment event, no
# PR-specific metadata at all). So I think we'll have to walk to the whole PR
# timeline: https://developer.github.com/v3/issues/timeline/
# in particular, we'll look for "committed" events to find the last head sha
# before the "commented" event whose id matches our triggering event
# kind of annoying, might chew through our API quota, but I guess there's
# nothing to be done...
# (the events are returned in chronological order, so if we could iterate
# backwards it would use fewer API calls)
# ...also uh... are reviews even included in the output?
# ...answer: yes, there's an undocumented "reviewed" event type
# it has keys: id, node_id, user object, body text, commit_id (= the head sha!),
# submitted_at, state, html_url, pull_request_url, author_association, _links,
# and event (= "reviewed")
# but when we get a review or review-comment notification, it already has the
# head sha, so we don't even need to look at the event timeline.
# (and likewise for initial pull-request-created events, though I dunno if we
# really care about supporting /merge inside a PR-created event? well, we do
# want to support /try there.)
# class PendingBuild(Base):
#     __tablename__ = "pending_build"

#     id = Column(
#         "id", Integer, Sequence("pending_build_id_seq"), primary_key=True
#     )
#     repo = Column("repo", String, nullable=False)
#     target_branch = Column("target_branch", String, nullable=False)
#     # type: try, merge, housekeeping
#     type = Column("type", String, nullable=False)
#     # For try/merge only:
#     pr = Column("pr", Integer, nullable=True)
#     # The approved revision within the PR
#     head_sha = Column("head_sha", String, nullable=True)
#     trigger_event_type = Column("trigger_event_type", String, nullable=True)
#     trigger_payload = Column("trigger_payload", JSONB, nullable=True)


class PDictDBEntry(Base):
    __tablename__ = "pdict"

    # Something like "task-result", basically the type of the value
    domain = Column(String, primary_key=True)
    # Identifier for this specific value within the domain (e.g. "task-123")
    item = Column(String, primary_key=True)
    # The current value.
    value = Column(JSONB, nullable=False)


class Already(Base):
    __tablename__ = "already"

    domain = Column(String, primary_key=True)
    item = Column(String, primary_key=True)


# Returns True if we already did this.
# Returns False if we haven't done it, and as a side-effect sets the flag to
# say we've done it. The flag auto-expires after the given time. (Mostly
# intended to allow GC later.)
def already_check_and_set(domain: str, item: str) -> bool:
    with retry_txn() as attempts:
        for session in attempts:
            existing = (
                session.query(Already)
                .filter_by(domain=domain, item=item)
                .one_or_none()
            )
            if existing is not None:
                result = True
            else:
                session.add(Already(domain=domain, item=item))
                result = False
    return result


@attr.s(frozen=True)
class CachedEngine:
    engine = attr.ib()
    database_url = attr.ib()


CACHED_ENGINE = CachedEngine(None, None)


def _get_session():
    global CACHED_ENGINE
    if CACHED_ENGINE.database_url != os.environ["DATABASE_URL"]:
        engine = create_engine(
            os.environ["DATABASE_URL"], isolation_level="SERIALIZABLE"
        )

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
    return Session(bind=CACHED_ENGINE.engine)


@contextmanager
def retry_txn():
    """Helper for retrying database transactions.

    We use Postgres's SERIALIZABLE isolation level, which has very
    convenient semantics: every transaction happens "as if" it was in some
    strict serial order, so race conditions are impossible, at least with
    regards to database reads/writes. The trade-off, though, is that it's
    possible that when we go to commit a transaction, Postgres will report
    that it's impossible to do it in a SERIALIZABLE-safe fashion, in which
    case it rolls it back, and then we need to retry it from the start. (For
    example, maybe we read some data that another transaction later mutated,
    and then we mutated some data that the other transaction read, so there's
    no way to put them in order properly.)

    To make this convenient, we always follow this idiom for database access:

      with retry_txn() as attempts:
          for session in attempts:
              # use the sqlalchemy Session object
              ...

    If the code raises an exception, the transaction is automatically rolled
    back and the session released. Otherwise, this will automatically attempt
    to commit the transaction at the end of the 'for' block, and keep looping
    until this succeeds.

    Note that if you don't raise an exception, you MUST fall off the end of
    the 'for' block. In particular, you CAN'T write code like this:

      # BAD
      with retry_txn() as attempts:
          for session in attempts:
              return session.query(...).one().some_attr

    The problem is the 'return' – it causes Python to forcibly terminate the
    'for' loop. So if Postgres then says we need to retry the transaction...
    there's no way to do that. In this case retry_txn will detect the problem
    and raise an AssertionError, so at least you'll notice. (Note: that's why
    we have this somewhat awkward-to-implement structure with a 'with' around
    a 'for'. An earlier prototype had a 'for' around a 'with', but it made it
    impossible to automatically detect these kinds of errors, so had to be
    scrapped.)

    The correct way to write that:

      # OK
      with retry_txn() as attempts:
          for session in attempts:
              result = session.query(...).one().some_attr
      return result

    """
    committed = False
    pending_session = None

    def session_gen():
        nonlocal committed, pending_session
        while True:
            if pending_session is not None:
                try:
                    pending_session.commit()
                except OperationalError as exc:
                    if (
                        isinstance(exc.orig, SerializationFailure)
                        and exc.orig.pgcode == "40001"
                    ):
                        # The commit() failed because of SERIALIZABLE
                        # isolation level, and should be retried.
                        pass
                    else:
                        raise
                else:
                    committed = True
                    break
                pending_session.close()
                pending_session = None
            pending_session = _get_session()
            yield pending_session

    try:
        yield session_gen()
        if not committed:
            raise AssertionError("retry_txn loop exited early, data lost")
    except:
        if pending_session is not None:
            pending_session.rollback()
        raise
    finally:
        if pending_session is not None:
            pending_session.close()
