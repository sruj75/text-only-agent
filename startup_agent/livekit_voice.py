import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

from livekit import api


logger = logging.getLogger("intentive.agent.voice")


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


@dataclass(frozen=True)
class LiveKitDispatchResult:
    mode: str
    fallback_used: bool
    diagnostic_code: str | None = None
    diagnostic: Dict[str, Any] | None = None


def _livekit_env_status() -> Dict[str, Any]:
    required = {
        "LIVEKIT_URL": str(os.getenv("LIVEKIT_URL") or "").strip(),
        "LIVEKIT_API_KEY": str(os.getenv("LIVEKIT_API_KEY") or "").strip(),
        "LIVEKIT_API_SECRET": str(os.getenv("LIVEKIT_API_SECRET") or "").strip(),
    }
    missing = [name for name, value in required.items() if not value]
    return {
        "configured": not missing,
        "missing_env_vars": missing,
        "agent_name": str(os.getenv("LIVEKIT_AGENT_NAME") or "intentive-voice-agent").strip(),
    }


def _dispatch_diagnostic_from_error(error: Exception) -> Dict[str, Any]:
    twirp_code = str(getattr(error, "code", "") or "").strip().lower()
    status = getattr(error, "status", None)
    diagnostic_code = "LIVEKIT_DISPATCH_FALLBACK_USED"
    remediation = "Check LiveKit Cloud status, worker deployment, and dispatch logs."

    if twirp_code in {api.TwirpErrorCode.UNAUTHENTICATED, api.TwirpErrorCode.PERMISSION_DENIED}:
        diagnostic_code = "LIVEKIT_AUTH_MISCONFIG"
        remediation = "Verify LIVEKIT_API_KEY and LIVEKIT_API_SECRET are valid for this project."
    elif twirp_code in {api.TwirpErrorCode.INVALID_ARGUMENT, api.TwirpErrorCode.FAILED_PRECONDITION}:
        diagnostic_code = "LIVEKIT_DISPATCH_INVALID_REQUEST"
        remediation = "Verify room/agent metadata and LIVEKIT_AGENT_NAME configuration."
    elif twirp_code in {api.TwirpErrorCode.NOT_FOUND, api.TwirpErrorCode.BAD_ROUTE}:
        diagnostic_code = "LIVEKIT_AGENT_OR_ROUTE_NOT_FOUND"
        remediation = "Verify LIVEKIT_AGENT_NAME and LiveKit server URL routing."
    elif twirp_code in {api.TwirpErrorCode.UNAVAILABLE, api.TwirpErrorCode.DEADLINE_EXCEEDED}:
        diagnostic_code = "LIVEKIT_DISPATCH_TRANSIENT"
        remediation = "Retry and inspect worker availability and project region health."

    return {
        "diagnostic_code": diagnostic_code,
        "twirp_code": twirp_code or None,
        "status": status,
        "error_type": type(error).__name__,
        "error_message": str(error),
        "remediation": remediation,
    }


class LiveKitVoiceCoordinator:
    """Owns the LiveKit transport boundary for room, token, and dispatch flows.

    The rest of the app should keep thinking in Intentive session terms. This
    coordinator is the only place that translates those sessions into LiveKit
    rooms and participant credentials.
    """

    def __init__(
        self,
        *,
        url: str,
        api_key: str,
        api_secret: str,
        agent_name: str,
        empty_timeout: int,
        departure_timeout: int,
        token_ttl_minutes: int,
    ) -> None:
        self.url = url
        self.api_key = api_key
        self.api_secret = api_secret
        self.agent_name = agent_name
        self.empty_timeout = empty_timeout
        self.departure_timeout = departure_timeout
        self.token_ttl_minutes = token_ttl_minutes

    @classmethod
    def from_env(cls) -> "LiveKitVoiceCoordinator | None":
        env_status = _livekit_env_status()
        if not env_status["configured"]:
            return None
        return cls(
            url=str(os.getenv("LIVEKIT_URL") or "").strip(),
            api_key=str(os.getenv("LIVEKIT_API_KEY") or "").strip(),
            api_secret=str(os.getenv("LIVEKIT_API_SECRET") or "").strip(),
            agent_name=str(env_status["agent_name"]),
            empty_timeout=_env_int("LIVEKIT_ROOM_EMPTY_TIMEOUT", 10 * 60),
            departure_timeout=_env_int("LIVEKIT_ROOM_DEPARTURE_TIMEOUT", 20),
            token_ttl_minutes=_env_int("LIVEKIT_TOKEN_TTL_MINUTES", 30),
        )

    @staticmethod
    def env_status() -> Dict[str, Any]:
        return _livekit_env_status()

    @property
    def token_ttl(self) -> timedelta:
        return timedelta(minutes=self.token_ttl_minutes)

    def participant_identity(self, *, device_id: str, session_id: str) -> str:
        compact_session = session_id.replace("session_", "")[:48]
        compact_device = device_id[:32]
        return f"user-{compact_device}-{compact_session}"

    async def ensure_room(
        self,
        *,
        room_name: str,
        metadata: Dict[str, Any],
    ) -> None:
        async with api.LiveKitAPI(self.url, self.api_key, self.api_secret) as livekit_api:
            listed = await livekit_api.room.list_rooms(api.ListRoomsRequest(names=[room_name]))
            if listed.rooms:
                await livekit_api.room.update_room_metadata(
                    api.UpdateRoomMetadataRequest(
                        room=room_name,
                        metadata=json.dumps(metadata, separators=(",", ":"), ensure_ascii=True),
                    )
                )
                return

            await livekit_api.room.create_room(
                api.CreateRoomRequest(
                    name=room_name,
                    empty_timeout=self.empty_timeout,
                    departure_timeout=self.departure_timeout,
                    metadata=json.dumps(metadata, separators=(",", ":"), ensure_ascii=True),
                )
            )

    async def dispatch_agent(
        self,
        *,
        room_name: str,
        metadata: Dict[str, Any],
    ) -> LiveKitDispatchResult:
        try:
            async with api.LiveKitAPI(self.url, self.api_key, self.api_secret) as livekit_api:
                await livekit_api.agent_dispatch.create_dispatch(
                    api.CreateAgentDispatchRequest(
                        room=room_name,
                        agent_name=self.agent_name,
                        metadata=json.dumps(metadata, separators=(",", ":"), ensure_ascii=True),
                    )
                )
            return LiveKitDispatchResult(mode="explicit", fallback_used=False)
        except Exception as error:  # pragma: no cover - exercised through route behavior
            diagnostic = _dispatch_diagnostic_from_error(error)
            logger.warning(
                "LiveKit explicit dispatch failed; continuing with token fallback",
                extra={
                    "room_name": room_name,
                    **diagnostic,
                },
            )
            return LiveKitDispatchResult(
                mode="token_fallback",
                fallback_used=True,
                diagnostic_code=str(diagnostic["diagnostic_code"]),
                diagnostic=diagnostic,
            )
        return LiveKitDispatchResult(mode="explicit", fallback_used=False)

    def build_participant_token(
        self,
        *,
        room_name: str,
        participant_identity: str,
        participant_name: str,
        participant_metadata: Dict[str, Any],
        dispatch_metadata: Dict[str, Any],
        use_token_dispatch_fallback: bool,
    ) -> tuple[str, str]:
        access_token = (
            api.AccessToken(self.api_key, self.api_secret)
            .with_identity(participant_identity)
            .with_name(participant_name)
            .with_kind("standard")
            .with_metadata(json.dumps(participant_metadata, separators=(",", ":"), ensure_ascii=True))
            .with_grants(
                api.VideoGrants(
                    room_join=True,
                    room=room_name,
                    can_publish=True,
                    can_subscribe=True,
                    can_publish_data=True,
                )
            )
            .with_ttl(self.token_ttl)
        )

        if use_token_dispatch_fallback:
            access_token = access_token.with_room_config(
                api.RoomConfiguration(
                    agents=[
                        api.RoomAgentDispatch(
                            agent_name=self.agent_name,
                            metadata=json.dumps(
                                dispatch_metadata,
                                separators=(",", ":"),
                                ensure_ascii=True,
                            ),
                        )
                    ]
                )
            )

        expires_at = (datetime.now(timezone.utc) + self.token_ttl).isoformat()
        return access_token.to_jwt(), expires_at
