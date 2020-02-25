import pytest
import os
import asks
import pendulum
import attr
import urllib.parse
import json

from snekomatic.autoinvite import (
    already_sent_invitation,
    record_sent_invitation,
)
from snekomatic.gh import GithubApp, BaseGithubClient
from .util import fake_webhook
from .credentials import *


def test_db_helpers(heroku_style_pg):
    assert not already_sent_invitation("foo")
    assert not already_sent_invitation("bar")
    record_sent_invitation("foo")
    assert already_sent_invitation("foo")
    assert not already_sent_invitation("bar")


@attr.s(frozen=True)
class InviteScenario:
    pr_merged = attr.ib()
    in_db = attr.ib()
    member_state = attr.ib()
    expect_in_db_after = attr.ib()
    expect_invite = attr.ib()


INVITE_SCENARIOS = [
    InviteScenario(
        pr_merged=False,
        in_db=False,
        member_state=None,
        expect_in_db_after=False,
        expect_invite=False,
    ),
    InviteScenario(
        pr_merged=True,
        in_db=True,
        member_state=None,
        expect_in_db_after=True,
        expect_invite=False,
    ),
    InviteScenario(
        pr_merged=True,
        in_db=False,
        member_state="active",
        expect_in_db_after=True,
        expect_invite=False,
    ),
    InviteScenario(
        pr_merged=True,
        in_db=False,
        member_state="pending",
        expect_in_db_after=True,
        expect_invite=False,
    ),
    InviteScenario(
        pr_merged=True,
        in_db=False,
        member_state=None,
        expect_in_db_after=True,
        expect_invite=True,
    ),
]


def _succeed(body={}):
    return (
        200,
        {"content-type": "application/json"},
        json.dumps(body).encode("ascii"),
    )


@pytest.mark.parametrize("s", INVITE_SCENARIOS)
async def test_invite_scenarios(s, our_app_url, monkeypatch):
    PR_CREATOR = "julia"
    ORG = "acme"

    ##### Set up the scenario #####

    # What we'll send
    headers, body = fake_webhook(
        "pull_request",
        {
            "action": "closed",
            "pull_request": {
                "merged": s.pr_merged,
                "user": {"login": PR_CREATOR},
                "comments_url": "fake-comments-url",
            },
            "organization": {"login": ORG},
            "installation": {"id": "000000"},
        },
        TEST_WEBHOOK_SECRET,
    )

    # Set up database
    if s.in_db:
        record_sent_invitation(PR_CREATOR)

    # Faking the Github API
    async def fake_token_for(self, installation_id):
        return "xyzzy"

    monkeypatch.setattr(GithubApp, "token_for", fake_token_for)

    did_invite = False
    did_comment = False

    async def fake_request(self, method, url, headers, body):
        print(method, url)
        to_members = url.endswith(f"/orgs/{ORG}/memberships/{PR_CREATOR}")
        if method == "GET" and to_members:
            if s.member_state is not None:
                return _succeed({"state": s.member_state})
            else:
                return (404, {}, "")

        if method == "PUT" and to_members:
            nonlocal did_invite
            did_invite = True
            return _succeed()

        if method == "POST" and url.endswith("/fake-comments-url"):
            nonlocal did_comment
            did_comment = True
            return _succeed()

        assert False  # pragma: no cover

    monkeypatch.setattr(BaseGithubClient, "_request", fake_request)

    ##### Post to the site #####
    response = await asks.post(
        urllib.parse.urljoin(our_app_url, "webhook/github"),
        headers=headers,
        data=body,
    )
    assert response.status_code == 200

    # Checks
    assert did_invite == did_comment
    assert did_invite == s.expect_invite
    assert (already_sent_invitation(PR_CREATOR)) == s.expect_in_db_after
