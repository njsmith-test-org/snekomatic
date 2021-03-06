# Github has two separate APIs that expose info about actions: the "Actions
# API" and the "Checks API". The terminology is confusing, so here's a
# cheatsheet:
#
# Actions yml       Actions API         Checks API
# -------------------------------------------------
#              =    Workflow run   =    Check suite
# Job          =    Workflow job   =    Check run
#
# Also, the "workflow job id" and "check run id" are the same, so you can
# freely take one from the Actions API and use it with the Checks API, and
# vice-versa. But "workflow run id" and "check suite id" are different. And
# the API lets you map from a workflow run id -> check suite id, but not
# vice-versa.
#
# The Actions API exposes richer information, e.g. links to the logs.
# The Checks API is the only one that generates webhook events.

import trio
from glom import glom
import os
import json
from base64 import b64encode
from nacl import encoding, public
from .gh import GithubRoutes
from .persistent import PDict
from .app import github_app
from .util import hash_json
from .db import already_check_and_set

__all__ = ["worker_routes", "run_worker_task_idem"]

worker_routes = GithubRoutes()

# for waiting to find out the check_suite_id+workflow_run_id for a worker task:
# - should get a check_run event
# - could poll... I guess? there isn't really a great way to do this

# for waiting to find out the result of a check_suite:
# - should get an event
# - also want to poll occasionally, for robustness

# for waiting to find out the result of a github actions CI run:
# - should get an event with the appropriate branch on it
# - probably also want to poll for suites on that ref, for robustness
#
# maybe the simplest is to have something like get_check_suite_conclusion,
# where it polls the ref every 5 minutes, and also whenever a check_suite
# is created on that ref.

DID_SETUP_WORKER_TASKS_THIS_RUN = False

# Pretty much copied directly from here:
# https://developer.github.com/v3/actions/secrets/#example-encrypting-a-secret-using-python
def encrypt_gh_secret(public_key: str, secret_value: str) -> str:
    """Encrypt a Unicode string using the public key."""
    public_key = public.PublicKey(
        public_key.encode("utf-8"), encoding.Base64Encoder()
    )
    sealed_box = public.SealedBox(public_key)
    encrypted = sealed_box.encrypt(secret_value.encode("utf-8"))
    return b64encode(encrypted).decode("utf-8")


# This syncs up our secrets with the worker repo. It needs to run once each
# time the secrets change. A convenient way to do this is to run it once per
# run of the bot.
async def setup_worker_tasks():
    global DID_SETUP_WORKER_TASKS_THIS_RUN
    if DID_SETUP_WORKER_TASKS_THIS_RUN:
        return
    repo = os.environ["SNEKOMATIC_WORKER_REPO"]
    gh_client = await github_app.client_for_repo(repo)

    secrets = {
        "GITHUB_USER_AGENT": github_app.user_agent,
        "GITHUB_APP_ID": github_app.app_id,
        "GITHUB_PRIVATE_KEY": github_app.private_key,
    }
    secret_value = json.dumps(secrets)

    key_info = await gh_client.getitem(
        f"/repos/{repo}/actions/secrets/public-key"
    )
    encrypted_value = encrypt_gh_secret(glom(key_info, "key"), secret_value)

    await gh_client.put(
        f"/repos/{repo}/actions/secrets/SNEKOMATIC_WORKER_SECRETS",
        data={
            "encrypted_value": encrypted_value,
            "key_id": glom(key_info, "key_id"),
        },
    )
    DID_SETUP_WORKER_TASKS_THIS_RUN = True


# This kicks off a worker task, at-most-one time. Unfortunately, there isn't
# any way to make this 100% reliable; if the repository_dispatch event is
# somehow lost, then it will never re-try, and run_worker_task will just hang
# indefinitely. If this is a problem, it needs to be handled at a higher
# level, by putting a timeout on run_worker_task and then retrying with a
# *different* 'args' dict.
async def start_worker_task_idem(args):
    task_id = hash_json(args)
    if already_check_and_set("worker-task-started", task_id):
        return task_id

    worker_repo = os.environ["SNEKOMATIC_WORKER_REPO"]
    client = await github_app.client_for_repo(worker_repo)

    await client.post(
        f"/repos/{worker_repo}/dispatches",
        data={
            "event_type": "worker-task",
            "client_payload": {
                "task_id": task_id,
                # Use the worker code snapshot that matches the
                # currently deployed app. Requires this labs feature
                # be enabled:
                #   https://devcenter.heroku.com/articles/dyno-metadata
                "worker_revision": os.environ["HEROKU_SLUG_COMMIT"],
                "args": args,
            },
        },
    )

    return task_id


async def run_worker_task_idem(args, *, task_status):
    await setup_worker_tasks()
    task_id = await start_worker_task_idem(args)
    pdict = PDict("worker-task", task_id)
    task_status.started(pdict)
    check_suite_id = await pdict.glom("check-suite-id")
    conclusion = await get_check_suite_conclusion(
        os.environ["SNEKOMATIC_WORKER_REPO"], check_suite_id
    )
    pdict.update({"conclusion": conclusion})


@worker_routes.route_webhook("check_run")
async def worker_task_check_run_event(event_type, payload, gh_client):
    repo = glom(payload, "repository.full_name")
    app_slug = glom(payload, "check_run.app.slug")
    name = glom(payload, "check_run.name")
    suite_id = glom(payload, "check_run.check_suite.id")

    if repo != os.environ["SNEKOMATIC_WORKER_REPO"]:
        return

    if app_slug != "github-actions":
        return

    if not name.startswith("sandboxed-"):
        return

    (_, task_id) = name.split("-", 1)

    PDict("worker-task", task_id).update(
        {
            "repo": repo,
            "check-suite-id": glom(payload, "check_run.check_suite.id"),
            "check-run-id": glom(payload, "check_run.id"),
            "html-url": glom(payload, "check_run.html_url"),
        }
    )


# XX the stuff below should probably move into a separate file, b/c it's a
# more generally useful utility
async def get_check_suite_conclusion(repo, check_suite_id):
    async with trio.open_nursery() as nursery:
        nursery.start_soon(
            check_suite_result_background_poller, repo, check_suite_id, 5 * 60
        )
        pdict = PDict("check-suite.completed", str(check_suite_id))
        conclusion = await pdict.glom("conclusion")
        nursery.cancel_scope.cancel()
        return conclusion


# Helper for get_check_suite_conclusion
async def check_suite_result_background_poller(
    repo, check_suite_id, interval
):
    gh_client = await github_app.client_for_repo(repo)
    while True:
        response = await gh_client.getitem(
            "/repos/{+repo}/check-suites/{check_suite_id}",
            url_vars={"repo": repo, "check_suite_id": check_suite_id},
            accept="application/vnd.github.antiope-preview+json",
        )
        if glom(response, "status") == "completed":
            PDict("check-suite.completed", str(check_suite_id)).update(
                {"conclusion": glom(response, "conclusion")}
            )
            return
        await trio.sleep(interval)


# Helper for get_check_suite_conclusion
@worker_routes.route_webhook("check_suite", action="completed")
async def check_suite_result_monitor(event_type, payload, gh_client):
    check_suite_id = glom(payload, "check_suite.id")
    conclusion = glom(payload, "check_suite.conclusion")
    PDict("check-suite.completed", str(check_suite_id)).update(
        {"conclusion": conclusion}
    )
