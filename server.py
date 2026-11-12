"""Vyapaar presence, invitations and match message relay."""
import asyncio
from dataclasses import dataclass, field
import itertools
import json
import os
import time

from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

MAX_USERS = 64
INVITE_SECONDS = 40
ids = itertools.count(1)


@dataclass
class Player:
    connection: object
    outbox: asyncio.Queue = field(default_factory=lambda: asyncio.Queue(128))
    name: str = ""
    invitation: int = 0
    match: int = 0


@dataclass
class Invitation:
    members: list[int]
    accepted: set[int]
    expiry: asyncio.TimerHandle | None = None


players: dict[int, Player] = {}
invitations: dict[int, Invitation] = {}
matches: dict[int, list[int]] = {}
MATCH_TO_HOST = {"action_request", "action_done", "payment_request"}
MATCH_TO_ALL = {"action", "unlock", "payment_ready", "payment_approved"}
EVENT_FORMATS = {
    "action_request": (int, str, int),
    "action_done": (int,),
    "payment_request": (int, int),
    "action": (int, str, int, int, int),
    "unlock": (int,),
    "payment_ready": (int, int),
    "payment_approved": (int, int),
}
ACTIONS = {"roll", "skip", "buy", "build"}


def valid_event(event, data):
    if not isinstance(event, str) or not isinstance(data, list):
        return False
    format_ = EVENT_FORMATS.get(event)
    if not format_ or len(data) != len(format_):
        return False
    if any(type(value) is not expected for value, expected in zip(data, format_)):
        return False
    if any(type(value) is int and not -1 <= value <= 2_000_000 for value in data):
        return False
    if event in {"action", "action_request"} and data[1] not in ACTIONS:
        return False
    return event != "action" or (1 <= data[3] <= 6 and 0 <= data[4] <= 3)


def send(player_id, message):
    player = players.get(player_id)
    if player:
        try:
            player.outbox.put_nowait(message)
        except asyncio.QueueFull:
            asyncio.create_task(player.connection.close(1013, "Connection too slow"))


def notice(player_id, message):
    send(player_id, {"type": "notice", "message": message})


def presence():
    roster = [{"id": pid, "name": p.name, "state": "Playing" if p.match else
               ("Invitation" if p.invitation else "Available")}
              for pid, p in players.items() if p.name]
    for pid, p in players.items():
        if p.name and not p.match:
            send(pid, {"type": "presence", "players": roster})


def end_invitation(invite_id, reason):
    invite = invitations.pop(invite_id, None)
    if invite is None:
        return
    if invite.expiry:
        invite.expiry.cancel()
    for pid in invite.members:
        p = players.get(pid)
        if p and p.invitation == invite_id:
            p.invitation = 0
            send(pid, {"type": "invite_closed", "id": invite_id, "message": reason})
    presence()


def invite_players(pid, targets):
    if not isinstance(targets, list) or not 1 <= len(targets) <= 3:
        notice(pid, "Select 1 to 3 available players.")
        return
    if any(type(target) is not int for target in targets):
        return
    if len(set(targets)) != len(targets) or pid in targets:
        return
    members = [pid, *targets]
    for member in members:
        p = players.get(member)
        if not p or not p.name or p.match or p.invitation:
            notice(pid, "One of the selected players is no longer available.")
            return
    invite_id = next(ids)
    invitations[invite_id] = Invitation(members, {pid})
    names = [players[member].name for member in members]
    for member in members:
        players[member].invitation = invite_id
        send(member, {"type": "invitation", "id": invite_id, "host": pid, "names": names})
    presence()
    invitations[invite_id].expiry = asyncio.get_running_loop().call_later(
        INVITE_SECONDS, end_invitation, invite_id, "Invitation expired."
    )


def respond(pid, accept):
    invite_id = players[pid].invitation
    invite = invitations.get(invite_id)
    if invite is None:
        return
    if not accept:
        end_invitation(invite_id, f"{players[pid].name} declined or cancelled.")
        return
    invite.accepted.add(pid)
    if len(invite.accepted) < len(invite.members):
        for member in invite.members:
            send(member, {"type": "invite_progress", "id": invite_id,
                          "accepted": len(invite.accepted), "total": len(invite.members)})
        return
    members = invite.members
    invitations.pop(invite_id)
    if invite.expiry:
        invite.expiry.cancel()
    match_id = next(ids)
    matches[match_id] = members
    names = [players[member].name for member in members]
    for member in members:
        players[member].invitation = 0
        players[member].match = match_id
        send(member, {"type": "start", "seats": members, "names": names})
    presence()


def end_match(pid):
    match_id = players[pid].match
    members = matches.pop(match_id, [])
    for member in members:
        p = players.get(member)
        if p:
            p.match = 0
            if member != pid:
                send(member, {"type": "match_ended", "message": "A player left the match."})
    presence()


def dispatch(pid, message):
    p = players[pid]
    kind = message.get("type")
    if kind == "hello" and not p.name:
        name = message.get("name")
        if isinstance(name, str):
            p.name = ("".join(c for c in name if c.isprintable()).strip()[:18] or "Player") + f" #{pid}"
            send(pid, {"type": "welcome", "id": pid, "name": p.name})
            presence()
        return
    if not p.name:
        return
    if kind == "invite":
        invite_players(pid, message.get("targets"))
    elif kind == "respond" and type(message.get("accept")) is bool and message.get("id") == p.invitation:
        respond(pid, message["accept"])
    elif kind == "leave_match" and p.match:
        end_match(pid)
    elif kind == "match" and p.match:
        event = message.get("event")
        data = message.get("data")
        if not valid_event(event, data):
            return
        roster = matches[p.match]
        host = roster[0]
        payload = {"type": "match", "event": event, "sender": pid, "data": data}
        if event in MATCH_TO_HOST and pid != host:
            send(host, payload)
        elif event in MATCH_TO_ALL and pid == host:
            for member in roster[1:]:
                send(member, payload)


async def writer(player):
    while True:
        await player.connection.send(json.dumps(await player.outbox.get(), ensure_ascii=False))


async def connection_handler(connection):
    if len(players) >= MAX_USERS:
        await connection.close(1013, "Lobby is full")
        return
    pid = next(ids)
    player = players[pid] = Player(connection)
    send_task = asyncio.create_task(writer(player))
    rate_window = time.monotonic()
    messages = 0
    try:
        async for raw in connection:
            now = time.monotonic()
            if now - rate_window > 1:
                rate_window, messages = now, 0
            messages += 1
            if messages > 35:
                await connection.close(1008, "Too many messages")
                break
            try:
                message = json.loads(raw)
            except (ValueError, UnicodeError):
                continue
            if isinstance(message, dict):
                dispatch(pid, message)
    except ConnectionClosed:
        pass
    finally:
        send_task.cancel()
        if player.invitation:
            end_invitation(player.invitation, "A player went offline.")
        if player.match:
            end_match(pid)
        players.pop(pid, None)
        if player.name:
            presence()
        await asyncio.gather(send_task, return_exceptions=True)


async def main():
    port = int(os.environ.get("PORT", "8765"))
    async with serve(connection_handler, "0.0.0.0", port, max_size=4096,
                     ping_interval=20, ping_timeout=20):
        print(f"Vyapaar lobby listening on port {port}", flush=True)
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
