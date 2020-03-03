"""A simple abstraction for writing Github Apps.

Basic setup:

  gh_app = GithubApp(...secrets and stuff...)

  @gh_app.route_webhook("issue", action="created")
  @gh_app.route_webhook("pull_request", action="created")
  async def handler(event_type, payload, gh_client):
      assert event_type in ["issue", "pull_request"]
      assert payload["action"] == "created"
      # 'gh_client' is a gidgethub-style github API client that automatically
      # uses the right credentials for this webhook event.

  @gh_app.route_command("/ping")
  async def handler(command, event_type, payload, gh_client):
      assert command[0] == "/ping"
      print("arguments:", command[1:])

Integrate into webapp:

  @quart_app.route("/webhook/github", methods=["POST"])
  async def handler():
      headers = request.headers
      body = await request.get_body()
      await gh_app.dispatch_webhook(headers, body)
      return ""

If you want to collect up some routing rules as a bundle and then add them to
an app, like a Flask "blueprint":

  routes = GithubRoutes()

  @routes.route_webhook("issue", action="created")
  async def handler(...):
      ...

  gh_app.add_routes(routes)

If you want to make API requests spontaneously, not in response to a
webhook, then use one of these:

  client = gh_app.app_client
  client = gh_app.client_for_installation_id(installation_id)
  client = await gh_app.client_for_repo(repo)

You can also get an installation token with 'await gh_app.token_for(...)',
which is useful in case you want to run git commands directly using those
credentials.

This should probably be split off into its own library eventually...

Some notes on how this compares to octomachinery, which has overlapping goals:

* We don't have any support for Github Actions
* This code should work with any of trio/asyncio/curio
* Doesn't try to "own" the event loop or web server -> can be integrated
  into a larger web app
* More robust auth token handling (better clock handling; checks for
  expiration before each request rather than just when a new webhook
  arrives)
* We insist that you configure a webhook secret, since anything else is
  totally insecure
* No utility functions that call yaml.load and thus execute arbitrary code
* About 10x fewer lines-of-code

Many thanks to Sviat for octomachinery though, because I would never have
figured out how to do any of this stuff without studying his code.

Also thanks to Brett for gidgethub, which is doing most of the heavy lifting
here.

"""

from collections import defaultdict
import os
from typing import Mapping, Tuple

import anyio
import asks
import attr
import cachetools
from gidgethub.sansio import Event, accept_format
import gidgethub.abc
from glom import glom
import jwt
import pendulum
import marko
from marko.ext.gfm import gfm

__all__ = ["GithubApp", "GithubRoutes"]

# XX TODO: should we catch exceptions in webhook handlers, the same way flask
# etc. catch exceptions in request handlers? right now the first exception
# leaks out of dispatch_webhook and cancels the running of other handlers

# XX octomachinery's preview_version= argument is pretty handy, should we
# adopt it? maybe push upstream to gidgethub?

# Also, we're fighting against gidgethub's structure some here... we should
# talk to Brett about how to make this easier.

# Assume that there might be this much offset between our clock at the time of
# submitting a request and Github's clock at the time of processing the
# request, or vice-versa.
MAX_CLOCK_SKEW = pendulum.Duration(minutes=1)


def _too_close_for_comfort(expires_at):
    return pendulum.now() + MAX_CLOCK_SKEW > expires_at


def _lazy_env_fallback(name):
    def getter(self):
        attr_name = f"_{name}"
        value = getattr(self, attr_name, None)
        if value is not None:
            return value
        envvar_name = f"GITHUB_{name.upper()}"
        if envvar_name in os.environ:
            return os.environ[envvar_name]
        raise RuntimeError(
            f"you must either pass {name} or set {envvar_name}"
        )

    getter.__name__ = name
    return property(getter)


def _all_match(data, restrictions):
    for key, value in restrictions.items():
        if key not in data or data[key] != value:
            return False
    return True


# This should maybe move into gidgethub
class BaseGithubClient(gidgethub.abc.GitHubAPI):
    def __init__(self, session, *args, **kwargs):
        self._session = session
        super().__init__(*args, **kwargs)

    async def _request(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str],
        body: bytes = b"",
    ) -> Tuple[int, Mapping[str, str], bytes]:
        response = await self._session.request(
            method, url, headers=headers, data=body
        )
        # asks stores headers in a regular dict. They're probably lowercase
        # already, but let's be 100% certain.
        lower_headers = {
            key.lower(): value for (key, value) in response.headers.items()
        }
        return response.status_code, lower_headers, response.content

    # Why does gidgethub make this mandatory? it's not used for anything
    async def sleep(self, seconds):
        await anyio.sleep(seconds)


@attr.s
class SegmentedCacheOverlay:
    _underlying = attr.ib()
    _segment = attr.ib()

    def __getitem__(self, key):
        return self._underlying[self._segment, key]

    def __setitem__(self, key, value):
        self._underlying[self._segment, key] = value


class AppGithubClient(BaseGithubClient):
    def __init__(self, app):
        self.app = app
        # Temporarily disabled because of:
        #   https://github.com/theelous3/asks/issues/133
        # cache = SegmentedCacheOverlay(app._cache, None)
        cache = None
        super().__init__(app._session, requester=app.user_agent, cache=cache)

    async def _make_request(self, *args, **kwargs):
        now = pendulum.now()
        jwt_app_token = jwt.encode(
            {
                "iat": (now - MAX_CLOCK_SKEW).int_timestamp,
                "exp": (now + MAX_CLOCK_SKEW).int_timestamp,
                "iss": self.app.app_id,
            },
            key=self.app.private_key,
            algorithm="RS256",
        )
        jwt_app_token = jwt_app_token.decode("ascii")
        kwargs["oauth_token"] = None
        kwargs["jwt"] = jwt_app_token
        return await super()._make_request(*args, **kwargs)


class InstallationGithubClient(BaseGithubClient):
    def __init__(self, app, installation_id):
        self.app = app
        self.installation_id = installation_id
        # Temporarily disabled because of:
        #   https://github.com/theelous3/asks/issues/133
        # cache = SegmentedCacheOverlay(app._cache, installation_id)
        cache = None
        super().__init__(app._session, requester=app.user_agent, cache=cache)

    async def _make_request(self, *args, **kwargs):
        token = await self.app.token_for(self.installation_id)
        kwargs["oauth_token"] = token
        kwargs["jwt"] = None
        return await super()._make_request(*args, **kwargs)


@attr.s
class CachedInstallationToken:
    token = attr.ib(default="")
    # pendulum.DateTime
    expires_at = attr.ib(default=pendulum.datetime(1900, 1, 1))
    # if a refresh is already in progress, an anyio.Event
    # otherwise, None
    refresh_event = attr.ib(default=None)


@attr.s(frozen=True)
class WebhookRoute:
    restrictions = attr.ib()
    async_fn = attr.ib()


# List of webhooks that can carry /-commands. Not included currently:
# - edits/deletions
# - commit comments
# - "team discussion" comments
_COMMENT_EVENTS = {
    ("issues", "opened"),
    ("pull_request", "opened"),
    ("issue_comment", "created"),
    ("pull_request_review", "submitted"),
    ("pull_request_review_comment", "created"),
}


@attr.s
class GithubRoutes:
    _webhook_routes = attr.ib(factory=lambda: defaultdict(list))
    _command_routes = attr.ib(factory=dict)

    def add_webhook(self, async_fn, event_type, **restrictions):
        if len(restrictions) > 1:
            raise TypeError("At most one restriction is allowed (for now)")
        self._webhook_routes[event_type].append(
            WebhookRoute(restrictions, async_fn)
        )

    def route_webhook(self, event_type, **restrictions):
        def decorator(async_fn):
            self.add_webhook(async_fn, event_type, **restrictions)
            return async_fn

        return decorator

    def add_command(self, async_fn, command_name):
        if not command_name.startswith("/"):
            command_name = "/" + command_name
        assert command_name not in self._command_routes
        self._command_routes[command_name] = async_fn

    def route_command(self, command_name):
        def decorator(async_fn):
            self.add_command(async_fn, command_name)
            return async_fn

        return decorator

    def update(self, other_table):
        for event_type, handlers in other_table._webhook_routes.items():
            self._webhook_routes[event_type] += handlers
        for command, handler in other_table._command_routes.items():
            self.add_command(handler, command)


class GithubApp:
    def __init__(
        self,
        *,
        session=None,
        user_agent=None,
        app_id=None,
        private_key=None,
        webhook_secret=None,
        # XX Completely untuned; maybe this is too big, or too small.
        cache_size=500,
    ):
        if session is None:
            # We don't really need to limit simultaneous connections... we're
            # not going to overwhelm github's frontend servers.
            session = asks.Session(connections=100)
        self._session = session
        self._user_agent = user_agent
        self._app_id = app_id
        self._private_key = private_key
        self._webhook_secret = webhook_secret
        self._installation_tokens = defaultdict(CachedInstallationToken)
        self._cache = cachetools.LRUCache(cache_size)
        self._routes = GithubRoutes()

    user_agent = _lazy_env_fallback("user_agent")
    app_id = _lazy_env_fallback("app_id")
    private_key = _lazy_env_fallback("private_key")
    webhook_secret = _lazy_env_fallback("webhook_secret")

    @property
    def app_client(self):
        return AppGithubClient(self)

    async def installation_id_for_repo(self, repo):
        result = await self.app_client.getitem(
            "/repos/{+repo}/installation",
            url_vars={"repo": repo},
            accept=accept_format(version="machine-man-preview"),
        )
        return glom(result, "id")

    def client_for_installation_id(self, installation_id):
        return InstallationGithubClient(self, installation_id)

    async def client_for_repo(self, repo):
        installation_id = await self.installation_id_for_repo(repo)
        return self.client_for_installation_id(installation_id)

    async def token_for(self, installation_id):
        installation_id = int(installation_id)
        cit = self._installation_tokens[installation_id]

        while _too_close_for_comfort(cit.expires_at):
            print(
                f"{installation_id}: Token is uncached or expired or will expire soon"
            )
            if cit.refresh_event is not None:
                print(
                    f"{installation_id}: Renewal already in progress; waiting"
                )
                await cit.refresh_event.wait()
            else:
                print(f"{installation_id}: Renewing now")
                cit.refresh_event = anyio.create_event()
                try:
                    response = await self.app_client.post(
                        "/app/installations/{installation_id}/access_tokens",
                        url_vars={"installation_id": installation_id},
                        accept=accept_format(version="machine-man-preview"),
                        data={},
                    )
                    cit.token = response["token"]
                    cit.expires_at = pendulum.parse(response["expires_at"])
                    assert not _too_close_for_comfort(cit.expires_at)
                    print(f"{installation_id}: Renewed successfully")
                finally:
                    # Make sure that even if we get cancelled, any other tasks
                    # will still wake up (and can retry the operation)
                    await cit.refresh_event.set()
                    cit.refresh_event = None

        return cit.token

    def add_webhook(self, *args, **kwargs):
        return self._routes.add_webhook(*args, **kwargs)

    def route_webhook(self, *args, **kwargs):
        return self._routes.route_webhook(*args, **kwargs)

    def add_command(self, *args, **kwargs):
        return self._routes.add_command(*args, **kwargs)

    def route_command(self, *args, **kwargs):
        return self._routes.route_command(*args, **kwargs)

    def add_routes(self, routing_table):
        self._routes.update(routing_table)

    async def dispatch_webhook(self, headers, body):
        event = Event.from_http(headers, body, secret=self.webhook_secret)
        print(
            f"GH webhook received: type={event.event}, delivery id={event.delivery_id}"
        )
        # Wait a bit to give Github's eventual consistency time to catch up
        await anyio.sleep(1)
        installation_id = glom(event.data, "installation.id", default=None)
        if installation_id is None:
            print("No associated installation; not dispatching")
            return
        client = self.client_for_installation_id(installation_id)
        # XX FIXME: do something cleverer about errors in handlers (e.g. don't
        # let one of them crashing cancel the others)
        async with anyio.create_task_group() as tg:
            for route in self._routes._webhook_routes[event.event]:
                if _all_match(event.data, route.restrictions):
                    print(f"Routing to {route.async_fn!r}")
                    await tg.spawn(
                        route.async_fn, event.event, event.data, client
                    )
            if (event.event, event.data.get("action")) in _COMMENT_EVENTS:
                body = get_comment_body(event.event, event.data)
                for command in parse_commands(body):
                    if command[0] in self._routes._command_routes:
                        await tg.spawn(
                            self._routes._command_routes[command[0]],
                            command,
                            event.event,
                            event.data,
                            client,
                        )
                    else:
                        # We silently ignore unrecognized commands, because lines
                        # starting with / can happen randomly, e.g. because of
                        # absolute paths in warnings/traceback output.
                        pass
        try:
            limit = client.rate_limit.remaining
        except AttributeError:
            pass
        else:
            print(f"Rate limit for install {installation_id}: {limit}")


_COMMENT_BODY_FIELDS_BY_EVENT_TYPE = {
    "issues": "issue.body",
    "pull_request": "pull_request.body",
    "issue_comment": "comment.body",
    "pull_request_review": "review.body",
    "pull_request_review_comment": "comment.body",
}


def get_comment_body(event_type, payload):
    field = _COMMENT_BODY_FIELDS_BY_EVENT_TYPE.get(event_type)
    if field is None:
        raise ValueError(f"unknown event_type: {event_type!r}")
    # Some github objects can have empty bodies (like top-level issues/PRs, or
    # a pull_request_review with no top-level comment). In that case github
    # likes to set the body field to None. We normalize to the empty string.
    body = glom(payload, field)
    if body is None:
        body = ""
    return body


def reply_url(event_type, payload):
    if event_type in ["issues", "issue_comment"]:
        return glom(payload, "issue.comments_url")
    elif event_type in ["pull_request", "pull_request_review"]:
        return glom(payload, "pull_request.comments_url")
    elif event_type in ["pull_request_review_comment"]:
        base_url = glom(payload, "pull_request.review_comments_url")
        try:
            in_reply_to_id = glom(payload, "comment.in_reply_to_id")
        except LookupError:
            in_reply_to_id = glom(payload, "comment.id")
        return f"{base_url}/{in_reply_to_id}/replies"
    else:
        raise ValueError(f"unknown event_type: {event_type!r}")


def reaction_url(event_type, payload):
    if event_type == "issues":
        return glom(payload, "issue.url") + "/reactions"
    elif event_type == "pull_request":
        # XX TODO: check if this is correct (github doesn't document it)
        return glom(payload, "pull_request.issue_url") + "/reactions"
    elif event_type in ["issue_comment", "pull_request_review_comment"]:
        return glom(payload, "comment.url") + "/reactions"
    elif event_type == "pull_request_review":
        # XX TODO: completely made this URL up, there are no docs, no idea if
        # it's right (or even whether there is a right answer)
        # UPDATE: yeah this is wrong
        pr_url = glom(payload, "pull_request.url")
        review_id = glom(payload, "review.id")
        return f"{pr_url}/reviews/{review_id}/reactions"
    else:
        raise ValueError(f"unknown event_type: {event_type!r}")


# We use marko to parse the body as markdown, and then when scanning for
# commands we only look at top-level paragraphs, plain text, rendered as
# standalone lines.
#
# For a quick overview of how marko's AST represents some markdown, run:
#   marko.ast_renderer.ASTRenderer().render(gfm.parse("..."))
def parse_commands(body_text):
    ast = gfm.parse(body_text)
    for para in ast.children:
        # This makes us ignore commands inside blockquotes, lists, code
        # blocks, etc.
        if not isinstance(para, marko.block.Paragraph):
            continue

        # Within a paragraph, we want to find RawText chunks that cover an
        # entire line. So they should start/end with the edge of the paragraph
        # *or* a LineBreak. In commonmark, there's a distinction between
        # "soft" and "hard" line breaks, but in practice github seems to
        # render both of them as hard line breaks, so we don't bother
        # distinguishing.
        def is_line_boundary(i):
            if i < 0:
                return True
            if i >= len(para.children):
                return True
            if isinstance(para.children[i], marko.inline.LineBreak):
                return True
            return False

        for i, child in enumerate(para.children):
            if (
                is_line_boundary(i - 1)
                and is_line_boundary(i + 1)
                and isinstance(child, marko.inline.RawText)
            ):
                line = child.children.strip()
                if line.startswith("/"):
                    yield line.split()
