import re
from urllib.parse import urlsplit


CRON_SESSION = re.compile(r"^cron_([0-9a-f]{12})_[0-9]{8}_[0-9]{6}$")


def add_session(request, *, base_url=None, session_id=None, platform=None, **_context):
    if not base_url or urlsplit(base_url).port != 8081:
        return None
    if not session_id:
        raise ValueError("Hermes session ID missing")
    cron = CRON_SESSION.fullmatch(session_id)
    if cron:
        session_id = f"cron:{cron.group(1)}"
    client_type = "cronjob" if cron or platform == "cron" else (platform or "unknown")
    return {
        "request": {
            **request,
            "extra_body": {
                **(request.get("extra_body") or {}),
                "agent_name": "hermes",
                "agent_session_id": session_id,
                "client_type": client_type,
            },
        }
    }


def register(ctx):
    ctx.register_middleware("llm_request", add_session)
