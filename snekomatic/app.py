import sys
import os
import trio
from glom import glom
import hypercorn
import hypercorn.trio
import quart
from quart import request
from quart_trio import QuartTrio
from gidgethub.sansio import accept_format

from .db import SentInvitation, open_session
from .gh import GithubApp, reply_url, reaction_url

# we should stash the delivery id in a contextvar and include it in logging
# also maybe structlog? eh print is so handy for now

quart_app = QuartTrio(__name__)
github_app = GithubApp()

if "SENTRY_DSN" in os.environ:
    import sentry_sdk

    sentry_sdk.init(os.environ["SENTRY_DSN"])

    @quart.got_request_exception.connect
    async def error_handler(_, *, exception):
        if isinstance(exception, Exception):
            print(f"Logging error to sentry: {exception!r}")
            sentry_sdk.capture_exception(exception)
        else:
            print(f"NOT logging error to sentry: {exception!r}")


@quart_app.route("/")
async def index():
    return "Hi! 🐍🐍🐍"


@quart_app.route("/webhook/github", methods=["POST"])
async def webhook_github():
    body = await request.get_data()
    await github_app.dispatch_webhook(request.headers, body)
    return ""


@github_app.route_command("ping")
async def handle_ping(command, event_type, payload, gh_client):
    assert command == ["ping"]
    await gh_client.post(
        reply_url(event_type, payload), data={"body": "pong!"}
    )
    await gh_client.post(
        reaction_url(event_type, payload),
        data={"content": "heart"},
        accept=accept_format(version="squirrel-girl-preview"),
    )


@github_app.route_command("/test-task")
async def handle_test_task(command, event_type, payload, gh_client):
    # Could figure out which gh client based on the worker repo instead:
    # https://developer.github.com/v3/apps/#get-a-repository-installation
    # probably want a gh_app.install_for_repo(...) method
    await gh_client.post(
        f"/repos/{os.environ['SNEKOMATIC_WORKER_REPO']}/dispatches",
        data={
            "event_type": "worker-task",
            "client_payload": {
                "taskid": "test",
                # Use the worker code snapshot that matches the currently
                # deployed app. Requires this labs feature be enabled:
                #   https://devcenter.heroku.com/articles/dyno-metadata
                "worker_revision": os.environ["HEROKU_SLUG_COMMIT"],
                "for": glom(payload, "repository.full_name"),
            },
        },
    )


from .autoinvite import autoinvite_routes

github_app.add_routes(autoinvite_routes)


async def main(*, task_status=trio.TASK_STATUS_IGNORED):
    print("~~~ Starting up! ~~~")
    # Make sure database connection works, schema is up to date, run any
    # required migrations, etc.
    with open_session() as conn:
        pass
    # On Heroku, have to bind to whatever $PORT says:
    # https://devcenter.heroku.com/articles/dynos#local-environment-variables
    port = os.environ.get("PORT", 8000)
    async with trio.open_nursery() as nursery:
        config = hypercorn.Config.from_mapping(
            bind=[f"0.0.0.0:{port}"],
            # Log to stdout
            accesslog="-",
            errorlog="-",
            # Setting this just silences a warning:
            worker_class="trio",
        )
        urls = await nursery.start(hypercorn.trio.serve, quart_app, config)
        print("Accepting HTTP requests at:", urls)
        task_status.started(urls)


async def worker(mode):
    import os
    import json
    from glom import glom
    from pathlib import Path
    import pprint
    import subprocess

    print("Mode is:", mode)
    print("working dir:", os.getcwd())
    print("Kicked off by:", os.environ["GITHUB_ACTOR"])

    payload = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
    print("Payload:")
    pprint.pprint(payload)

    task_info = glom(payload, "client_payload")
    print("Task info")
    pprint.pprint(task_info)

    subprocess.run(["ls", "-R"])

    if mode == "sandboxed":
        print("making artifact")
        Path("worker-artifacts-dir").mkdir()
        Path("worker-artifacts-dir/test").write_text("hello")

        subprocess.run(["ls", "-R"])
    else:
        print("reading artifact")
        print("artifact says:", Path("worker-artifacts-dir/test").read_text())
