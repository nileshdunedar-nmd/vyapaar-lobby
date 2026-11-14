"""BUSINESS presence, invitations and match message relay."""
import asyncio
from contextlib import closing
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import re
import sqlite3
from pathlib import Path
from dataclasses import dataclass, field
import itertools
import json
import os
import secrets
from http import HTTPStatus
import time

from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

MAX_USERS = 64
ACCOUNT_DB = Path(os.environ.get("ACCOUNT_DB", "business_accounts.sqlite3"))
DAILY_REWARD = 150
STARTING_WALLET = 500
INDIA_TIME = timezone(timedelta(hours=5, minutes=30))
INVITE_SECONDS = 40
RECONNECT_SECONDS = 45
ids = itertools.count(1)


@dataclass
class Player:
    connection: object
    outbox: asyncio.Queue = field(default_factory=lambda: asyncio.Queue(128))
    name: str = ""
    account: str = ""
    invitation: int = 0
    match: int = 0
    token: str = field(default_factory=lambda: secrets.token_urlsafe(32))
    timeout: asyncio.TimerHandle | None = None
    incoming_seq: int = 0
    outgoing_seq: int = 0
    replay: dict = field(default_factory=dict)


@dataclass
class Invitation:
    members: list[int]
    accepted: set[int]
    expiry: asyncio.TimerHandle | None = None


players: dict[int, Player] = {}
sessions: dict[str, int] = {}
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
ACTIONS = {"roll", "skip", "buy", "build", "rent", "pay", "cancel_skip", "accept_sale", "decline_sale", "counter_sale", "accept_counter", "take_loan", "loan_full", "loan_half", "loan_interest", "loan_default"}


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
    if not player:
        return
    if message["type"] in {"start", "match", "match_ended"}:
        player.outgoing_seq += 1
        message = {**message, "seq": player.outgoing_seq}
        player.replay[player.outgoing_seq] = message
        if len(player.replay) > 128:
            # A stalled session cannot safely replay an unbounded match history.
            asyncio.get_running_loop().call_soon(expire_player, player_id)
            return
    deliver(player, message)


def deliver(player, message):
    if player.connection is not None:
        try:
            player.outbox.put_nowait(message)
        except asyncio.QueueFull:
            asyncio.create_task(player.connection.close(1013, "Connection too slow"))


def acknowledge(player, sequence):
    if type(sequence) is int and 0 <= sequence <= player.outgoing_seq:
        for previous in list(player.replay):
            if previous <= sequence:
                del player.replay[previous]


def match_presence(match_id):
    roster = matches.get(match_id, [])
    disconnected = [pid for pid in roster if players[pid].connection is None]
    for pid in roster:
        send(pid, {"type": "match_presence", "disconnected": disconnected})


def expire_player(pid):
    player = players.get(pid)
    if player is None:
        return
    if player.timeout:
        player.timeout.cancel()
    if player.invitation:
        end_invitation(player.invitation, "A player went offline.")
    if player.match:
        end_match(pid)
    sessions.pop(player.token, None)
    players.pop(pid, None)
    if player.connection is not None:
        asyncio.create_task(player.connection.close(1013, "Session closed"))
    presence()


def notice(player_id, message):
    send(player_id, {"type": "notice", "message": message})


def presence():
    roster = [{"id": pid, "name": p.name, "state": "Reconnecting" if p.connection is None else ("Playing" if p.match else
               ("Invitation" if p.invitation else "Available"))}
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
        if not p or not p.name or p.match or p.invitation or p.connection is None:
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


def initialize_accounts():
    ACCOUNT_DB.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(ACCOUNT_DB)) as db, db:
        db.execute("""
            CREATE TABLE IF NOT EXISTS accounts (
                username TEXT PRIMARY KEY,
                display_name TEXT NOT NULL,
                salt BLOB NOT NULL,
                password_hash BLOB NOT NULL,
                dollars INTEGER NOT NULL,
                last_claim TEXT NOT NULL DEFAULT '',
                streak INTEGER NOT NULL DEFAULT 0
            )
        """)


        db.execute("""
            CREATE TABLE IF NOT EXISTS saved_logins (
                digest BLOB PRIMARY KEY,
                username TEXT NOT NULL,
                expires INTEGER NOT NULL
            )
        """)


def saved_account(token):
    if not isinstance(token, str) or not 40 <= len(token) <= 60:
        return None
    digest = hashlib.sha256(token.encode()).digest()
    with closing(sqlite3.connect(ACCOUNT_DB)) as db:
        row = db.execute(
            """SELECT s.username FROM saved_logins s JOIN accounts a ON a.username = s.username
               WHERE s.digest = ? AND s.expires > ?""", (digest, int(time.time())),
        ).fetchone()
    return row[0] if row else None


def issue_login(username):
    token = secrets.token_urlsafe(32)
    now = int(time.time())
    with closing(sqlite3.connect(ACCOUNT_DB)) as db, db:
        db.execute("DELETE FROM saved_logins WHERE expires <= ? OR username = ?", (now, username))
        db.execute("INSERT INTO saved_logins VALUES (?, ?, ?)",
                   (hashlib.sha256(token.encode()).digest(), username, now + 30 * 86400))
    return token


def authenticate(message):
    username = message.get("username")
    password = message.get("password")
    if not isinstance(username, str) or not isinstance(password, str):
        return None
    username = username.strip().lower()
    if not re.fullmatch(r"[a-z0-9_]{3,18}", username) or not 8 <= len(password) <= 72:
        return None
    create = message.get("create") is True
    with closing(sqlite3.connect(ACCOUNT_DB)) as db, db:
        row = db.execute("SELECT salt, password_hash FROM accounts WHERE username = ?", (username,)).fetchone()
        if create:
            name = message.get("name")
            if row or not isinstance(name, str):
                return None
            name = "".join(c for c in name.strip() if c.isprintable() and c != "#")[:18].strip()
            if not name:
                return None
            salt = secrets.token_bytes(16)
            digest = hashlib.scrypt(password.encode(), salt=salt, n=16384, r=8, p=1)
            try:
                db.execute(
                    "INSERT INTO accounts (username, display_name, salt, password_hash, dollars) VALUES (?, ?, ?, ?, ?)",
                    (username, name, salt, digest, STARTING_WALLET),
                )
            except sqlite3.IntegrityError:
                return None
        else:
            salt = row[0] if row else bytes(16)
            digest = hashlib.scrypt(password.encode(), salt=salt, n=16384, r=8, p=1)
            if not row or not hmac.compare_digest(row[1], digest):
                return None
    return username


def account_snapshot(username, claim=False):
    day = datetime.now(INDIA_TIME).date()
    today = day.isoformat()
    yesterday = (day - timedelta(days=1)).isoformat()
    with closing(sqlite3.connect(ACCOUNT_DB)) as db, db:
        if claim:
            db.execute("""
                UPDATE accounts
                SET dollars = dollars + ?, last_claim = ?,
                    streak = CASE WHEN last_claim = ? THEN streak + 1 ELSE 1 END
                WHERE username = ? AND last_claim != ?
            """, (DAILY_REWARD, today, yesterday, username, today))
        name, dollars, last_claim, streak = db.execute(
            "SELECT display_name, dollars, last_claim, streak FROM accounts WHERE username = ?",
            (username,),
        ).fetchone()
    return {"display_name": name, "dollars": dollars, "streak": streak,
            "claimed": last_claim == today, "daily_amount": DAILY_REWARD}


login_slots = asyncio.Semaphore(4)


def dispatch(pid, message):
    p = players[pid]
    kind = message.get("type")
    if not p.name:
        return
    if kind == "heartbeat":
        send(pid, {"type": "heartbeat"})
    elif kind == "daily_bonus" and not p.match:
        send(pid, {"type": "account", **account_snapshot(p.account, claim=True)})
    elif kind == "account_status" and not p.match:
        send(pid, {"type": "account", **account_snapshot(p.account)})
    elif kind == "ack":
        acknowledge(p, message.get("seq"))
    elif kind == "invite":
        invite_players(pid, message.get("targets"))
    elif kind == "respond" and type(message.get("accept")) is bool and message.get("id") == p.invitation:
        respond(pid, message["accept"])
    elif kind == "leave_match" and p.match:
        end_match(pid)
    elif kind == "match" and p.match:
        sequence = message.get("client_seq")
        if type(sequence) is not int or not 1 <= sequence <= p.incoming_seq + 1:
            return
        send(pid, {"type": "client_ack", "seq": sequence})
        if sequence <= p.incoming_seq:
            return
        p.incoming_seq = sequence
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


async def writer(connection, outbox):
    while True:
        await connection.send(json.dumps(await outbox.get(), ensure_ascii=False))


def decode(raw):
    try:
        message = json.loads(raw)
        return message if isinstance(message, dict) else {}
    except (ValueError, UnicodeError):
        return {}


async def connection_handler(connection):
    pid = None
    send_task = None
    player = None
    try:
        first = decode(await asyncio.wait_for(connection.recv(), 12))
        if first.get("type") == "resume":
            token = first.get("token")
            pid = sessions.get(token) if isinstance(token, str) else None
            player = players.get(pid)
            if not player or player.connection is not None:
                await connection.send(json.dumps({
                    "type": "resume_failed", "retry": bool(player),
                }))
                return
            player.timeout.cancel()
            player.timeout = None
            player.connection = connection
            player.outbox = asyncio.Queue(192)
            acknowledge(player, first.get("last_seq"))
            deliver(player, {"type": "resumed", "ack": player.incoming_seq})
            for message in player.replay.values():
                deliver(player, message)
            match_presence(player.match)
            presence()
        elif first.get("type") == "hello" and len(players) < MAX_USERS:
            if first.get("game_version") != 9:
                await connection.send(json.dumps({"type": "update_required"}))
                return
            if login_slots.locked():
                await connection.send(json.dumps({"type": "auth_error", "message": "Server is busy. Try again."}))
                return
            saved = "login_token" in first
            async with login_slots:
                account = saved_account(first.get("login_token")) if saved else await asyncio.to_thread(authenticate, first)
            if saved and not account:
                await connection.send(json.dumps({"type": "auth_error", "clear_saved_login": True,
                                                  "message": "Saved login expired. Sign in again."}))
                return
            if not account or any(p.account == account for p in players.values()):
                await connection.send(json.dumps({
                    "type": "auth_error",
                    "message": "Account unavailable. Check the username and password, or close the other session.",
                }))
                return
            profile = account_snapshot(account)
            pid = next(ids)
            player = players[pid] = Player(connection, account=account,
                                            name=f"{profile['display_name']} #{pid}")
            sessions[player.token] = pid
            login_token = first["login_token"] if saved else issue_login(account)
            send(pid, {"type": "welcome", "id": pid, "name": player.name, "token": player.token,
                       "username": account, "login_token": login_token, **profile})
            presence()
        else:
            await connection.close(1013, "Lobby unavailable")
            return

        send_task = asyncio.create_task(writer(connection, player.outbox))
        rate_window = time.monotonic()
        messages = 0
        async for raw in connection:
            now = time.monotonic()
            if now - rate_window > 1:
                rate_window, messages = now, 0
            messages += 1
            if messages > 60:
                await connection.close(1008, "Too many messages")
                break
            if pid not in players:
                break
            dispatch(pid, decode(raw))
    except (ConnectionClosed, TimeoutError):
        pass
    finally:
        if send_task:
            send_task.cancel()
            await asyncio.gather(send_task, return_exceptions=True)
            if pid in players and player.connection is connection:
                player.connection = None
                if player.match:
                    player.timeout = asyncio.get_running_loop().call_later(
                        RECONNECT_SECONDS, expire_player, pid
                    )
                    match_presence(player.match)
                    presence()
                else:
                    expire_player(pid)


def health_check(connection, request):
    if request.path == "/healthz":
        return connection.respond(HTTPStatus.OK, "BUSINESS lobby is ready.\n")
    return None


async def main():
    initialize_accounts()
    port = int(os.environ.get("PORT", "8765"))
    async with serve(connection_handler, "0.0.0.0", port, max_size=4096,
                     ping_interval=20, ping_timeout=20, process_request=health_check):
        print(f"BUSINESS lobby listening on port {port}", flush=True)
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
