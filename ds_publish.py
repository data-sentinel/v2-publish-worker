# ds_publish.py - Data Sentinel publish worker, Vantage (V2)
#
# Holds one run's publish queue at a time and calls sentinel.sys_post_publish_batch until nothing waits and nothing
# more is coming. All of the publishing is done inside the database by that one call: the worker sends and receives
# ids and counts, and nothing else. It connects to no data source and reads and writes no table.
#
# Built to WORKER-STANDARD 2026-46-10 and to architect 2026-47-46 (01-publish-worker.md), as architect 2026-47-47
# amends it. Where publish differs from the standard, 01 section 2 says so.
#
# Layout of this file
#   1  settings            one Settings class, read once; a value that can't be used exits 2
#   2  logs                one format; ids, codes and counts only
#   3  reasons and health  code + params + one English sentence
#   4  the database        three links: the listen link, the pool of 4, and the batch connection
#   5  the contract        the eight functions this worker calls, checked at start and after a break
#   6  the claim           one queue: batches, the lease, the five ways of giving it up
#   7  the service         check-in, heartbeat, contract checks, reconnects, resume, the idle loop
#   8  endpoints           /health /ready /status
#   9  main                uvicorn, SIGTERM
#
#__version__ = '2026.47.1'   # first build: architect package 2026-47-46, with note 2026-47-47
__version__ = '2026.47.1'

import asyncio
import json
import logging
import os
import random
import sys
import time
from datetime import datetime, timezone

import asyncpg
from quart import Quart, jsonify

WORKER_TYPE = "publish"
STAGE = "publish"

RENEW_SEC = 30               # the lease: renew every 30 seconds, whatever the batch connection is doing
STOP_WITHIN_SEC = 15         # a batch in hand gets 15 seconds once the worker is told to stop, then it is cancelled
WAIT_MORE_SEC = 5            # waiting for more: a NOTIFY run_work for this queue, or 5 seconds
CONTRACT_SEC = 60            # the standard: the contract is checked every 60 seconds
CONTRACT_AFTER_BREAK = (5, 15, 30)
RECONNECT_MAX_SEC = 30
PING_SEC = 5                 # the listen link's cheap statement
POOL_CALL_SEC = 10.0         # the pool's statement timeout
POLL_JITTER = 0.2            # each wait between asks varies by up to a fifth either way
SHUTDOWN_CLAIM_SEC = 25      # SIGTERM: what the claim gets (15 for the batch, then the cancel and one renewal)
SHUTDOWN_SEC = 32            # SIGTERM: everything, inside the service's 40-second stop grace
RETRYABLE_MAX = 5            # 40P01 / 40001 from the batch: called again at once, five in a row is a failure
FINAL_TRIES = 3              # a complete or a release: three timeouts, or the same refusal three times
SLOW_BATCH_MS = 10000

log = logging.getLogger("ds_publish")


# =====================================================================================
# 1. Settings
# =====================================================================================
class SettingsError(Exception):
    def __init__(self, variable, why):
        super().__init__(variable + ": " + why)
        self.variable = variable
        self.why = why


SECRETS_DIR = "/run/secrets"     # the V1 secret files for the database's address (no DS_SECRETS_DIR: 01 section 3.1)


def _secret_text(path, name=None):
    """A secret file's value, as the conductor reads it: the platform's files hold {"<name>": "<value>"};
    a file that isn't that is taken as it is, with the line end removed."""
    with open(path, "r", encoding="utf-8") as f:
        raw = f.read()
    key = name or os.path.basename(path)
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict) and key in obj:
            return str(obj[key])
    except ValueError:
        pass
    return raw.strip("\r\n")


def read_database_password(path):
    """The database password: a Docker secret mounted as a file and read as it is. Read at every connect,
    so a rotated secret is picked up by the next one."""
    return _secret_text(path)


class Settings(object):
    """Every setting, read once at start from the environment."""

    def __init__(self, env=None):
        e = os.environ if env is None else env

        def text(name, default=None):
            v = e.get(name)
            v = v.strip() if isinstance(v, str) else v
            return v if v else default

        def integer(name, default, lo, hi):
            raw = text(name)
            if raw is None:
                return default
            try:
                v = int(raw)
            except ValueError:
                raise SettingsError(name, "must be a whole number")
            if v < lo or v > hi:
                raise SettingsError(name, "must be from " + str(lo) + " to " + str(hi))
            return v

        self.service = text("DS_SERVICE", "ds_publish")
        self.address = text("DS_SERVICE_EXTERNAL_HOST", self.service)
        self.port = integer("DS_SERVICE_EXTERNAL_PORT", 5700, 1, 65535)

        def repository(env_name, secret_name, default=None):
            v = text(env_name)
            if v is not None:
                return v
            path = os.path.join(SECRETS_DIR, secret_name)
            if os.path.isfile(path):
                try:
                    v = _secret_text(path, secret_name).strip()
                except OSError as exc:
                    raise SettingsError(env_name, "the secret file " + path + " can't be read ("
                                        + type(exc).__name__ + ")")
                if v:
                    return v
            if default is None:
                raise SettingsError(env_name, "not set, and there is no secret file " + path)
            return default

        self.db_host = repository("DS_DB_HOST", "repository_host")
        port = repository("DS_DB_PORT", "repository_port", "5432")
        if not port.isdigit() or not 1 <= int(port) <= 65535:
            raise SettingsError("DS_DB_PORT", "'" + port + "' (or the repository_port secret) is not a port number")
        self.db_port = int(port)
        self.db_name = repository("DS_DB_NAME", "repository_name", "data_sentinel")
        self.db_user = repository("DS_DB_USER", "repository_username", "sentinel_worker")
        self.db_password_file = text("DS_DB_PASSWORD_FILE", os.path.join(SECRETS_DIR, "sentinel_worker_password"))
        if not os.path.isfile(self.db_password_file):
            raise SettingsError("DS_DB_PASSWORD_FILE", "there is no file " + self.db_password_file)
        try:
            read_database_password(self.db_password_file)
        except OSError as exc:
            raise SettingsError("DS_DB_PASSWORD_FILE", self.db_password_file + " can't be read ("
                                + type(exc).__name__ + ")")

        self.poll_sec = integer("DS_POLL_SEC", 5, 1, 20)
        self.heartbeat_sec = integer("DS_HEARTBEAT_SEC", 30, 1, 110)    # online means a heartbeat under 2 minutes old
        self.log_dir = text("DS_LOG_DIR", "/mnt/logs")
        self.log_level = (text("DS_LOG_LEVEL", "info") or "info").lower()
        if self.log_level not in ("debug", "info", "warning", "error"):
            raise SettingsError("DS_LOG_LEVEL", "must be debug, info, warning or error")

        # its own (01 section 3.1)
        self.budget = integer("DS_PUBLISH_BUDGET", 200000, 1, 100000000)
        self.statement_min = integer("DS_PUBLISH_STATEMENT_MIN", 30, 1, 120)

    def describe(self):
        """The settings for the start line: no secret, the password file by its path only."""
        return kv(service=self.service, address=self.address, port=self.port,
                  db=str(self.db_host) + ":" + str(self.db_port) + "/" + str(self.db_name), user=self.db_user,
                  poll_sec=self.poll_sec, heartbeat_sec=self.heartbeat_sec,
                  budget=self.budget, statement_min=self.statement_min, log_dir=self.log_dir,
                  log_level=self.log_level)


# =====================================================================================
# 2. Logs
# =====================================================================================
class _NoTraceback(logging.Formatter):
    """The standard's format. A record carrying an exception gets the exception's class instead of its traceback
    and its text."""

    def format(self, record):
        exc = record.exc_info
        record.exc_info = None
        record.exc_text = None
        record.stack_info = None
        line = super().format(record)
        if exc and exc[0] is not None:
            line += " (" + exc[0].__name__ + ")"
        return line.splitlines()[0] if line else line


def setup_logging(settings):
    fmt = _NoTraceback("%(asctime)s|%(levelname)s|%(message)s")
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
    out = logging.StreamHandler(sys.stdout)
    out.setFormatter(fmt)
    root.addHandler(out)
    if settings.log_dir and os.path.isdir(settings.log_dir):
        try:
            fh = logging.FileHandler(os.path.join(settings.log_dir, settings.service + ".log"), encoding="utf-8")
            fh.setFormatter(fmt)
            root.addHandler(fh)
        except OSError:
            pass
    root.setLevel(logging.WARNING)
    log.setLevel(getattr(logging, settings.log_level.upper()))
    for name in ("quart", "uvicorn", "uvicorn.error", "uvicorn.access", "asyncpg", "hypercorn", "asyncio"):
        logging.getLogger(name).setLevel(logging.WARNING)


def kv(**items):
    """key=value pairs: values are ids, codes and counts, never a name or a path."""
    return " ".join(k + "=" + str(v) for k, v in items.items())


def where(exc):
    """An exception as its class and where it happened, never its text or a traceback."""
    tb = exc.__traceback__
    last = None
    while tb is not None:
        last = tb
        tb = tb.tb_next
    place = (last.tb_frame.f_code.co_name + ":" + str(last.tb_lineno)) if last else "?"
    return type(exc).__name__ + "@" + place


def now_iso():
    return datetime.now(timezone.utc).isoformat()


# =====================================================================================
# 3. Reasons and the health summary
# =====================================================================================
# One code for one sentence: no full stop, 200 characters at most. The app shows them from the code and params.
REASONS = {
    "publish_timeout": ("Publishing one object took longer than {minutes} minutes", ("seconds", "minutes")),
    "unexpected": ("The worker failed unexpectedly: {reason}", ("reason",)),
}
REASONS_WHEN = {
    "publish_timeout": "a release as retry: the batch connection's statement timeout (DS_PUBLISH_STATEMENT_MIN) was "
                       "reached, which is one object taking longer than the limit. params are the setting's, in "
                       "seconds and in minutes",
    "unexpected": "a release as retry for the worker's own fault: an exception in its own loop, a refusal it doesn't "
                  "know, 40P01 or 40001 five times in a row from the batch, or a call that fails the same way again "
                  "after the contract check passed. reason is the exception's class, never its text",
}

HEALTH = {
    "idle": ("work", "Idle", ()),
    "working": ("work", "Working on the {stage} stage of run {run_id}", ("stage", "run_id")),
    "waiting_for_work": ("work", "Waiting for objects on run {run_id}", ("run_id",)),
    "winding_down": ("work", "Finishing up on run {run_id}", ("run_id",)),
    "contract_missing": ("problem", "The database is missing functions this worker needs", ("missing",)),
    "starting": ("problem", "The worker is still starting", ()),
    "reconnecting": ("problem", "The worker is reconnecting to the database", ()),
    "stopping": ("problem", "The worker is stopping", ()),
}
HEALTH_WHEN = {
    "idle": "holds no queue (also while a queue is set aside for a broken contract: the summary names none)",
    "working": "holds a queue, and neither waiting_for_work nor winding_down applies",
    "waiting_for_work": "holds a queue, and the last batch took nothing with more coming",
    "winding_down": "told to stop, cancel or pause, or SIGTERM, and the queue not yet left",
    "contract_missing": "the contract check found functions missing, not executable, or without a parameter the "
                        "worker sends. The function names are in params.missing, not in the text. Not ready",
    "starting": "checked in, and not yet ready: before the first contract check and the first read of what it holds",
    "reconnecting": "the link to the database broke after check-in, or a complete or a release timed out three "
                    "times (the database out of reach: the queue is kept). Not ready",
    "stopping": "SIGTERM: the status is stopping",
}


def codes_documents():
    """(health codes, reasons): this worker's own, as the JSON it sends with each hand-off."""
    health = {"version": "publish " + __version__, "worker_type": WORKER_TYPE, "codes": [
        {"key": k, "code": code, "text": t, "params": list(p), "when": HEALTH_WHEN[code]}
        for code, (k, t, p) in HEALTH.items()]}
    reasons = {"version": "publish " + __version__,
               "note": "The reasons this worker sends, all with v_outcome retry. stopped, cancelled and paused send "
                       "no reason: the database writes it. A failed item's reason, publish_failed, is the "
                       "database's. One code for one sentence, no full stop, 200 characters at most.",
               "reasons": [{"code": code, "params": list(p), "text": t, "where": REASONS_WHEN[code],
                            "sent_by": "publish"} for code, (t, p) in REASONS.items()]}
    return health, reasons


def _sentence(template, params):
    text = template.format(**{k: ("?" if v is None else v) for k, v in params.items()})
    text = " ".join(text.split()).rstrip(" .")
    return text if len(text) <= 200 else text[:199].rstrip() + "…"


def reason(code, params=None):
    params = dict(params or {})
    template, keys = REASONS[code]
    params = {k: params.get(k) for k in keys}
    return {"code": code, "params": params, "text": _sentence(template, params)}


def unexpected(exc_or_class):
    name = exc_or_class if isinstance(exc_or_class, str) else type(exc_or_class).__name__
    return reason("unexpected", {"reason": name})


def health_item(code, params=None):
    _key, template, keys = HEALTH[code]
    params = dict(params or {})
    if "missing" in keys:
        return {"code": code, "params": {"missing": list(params.get("missing") or [])}, "text": _sentence(template, {})}
    shown = {k: params.get(k) for k in keys}
    return {"code": code, "params": shown, "text": _sentence(template, shown)}


# =====================================================================================
# 4. The database
# =====================================================================================
class DbGone(Exception):
    """The call didn't get an answer: no link, a dropped link, or a time limit. Its outcome is unknown."""


class ContractBroken(Exception):
    """The function or a parameter is gone or changed (42883, 3F000, 42501, 42P01): back to the contract check."""

    def __init__(self, fn, sqlstate, cls):
        super().__init__(str(fn) + " " + str(sqlstate))
        self.fn = fn
        self.sqlstate = sqlstate
        self.cls = cls


class DbRefusal(Exception):
    """The database answered with an error. Matched on its HINT, never on the message."""

    def __init__(self, sqlstate, hint, cls="PostgresError"):
        super().__init__(str(sqlstate) + " " + str(hint))
        self.sqlstate = sqlstate
        self.hint = hint
        self.cls = cls


class NotService(Exception):
    """The login isn't the workers' login: log it and exit 1."""


class BatchCanceled(Exception):
    """57014 from the batch: the worker's own cancel (it set its mark first) or the statement timeout."""


class BatchRetryable(Exception):
    """40P01 or 40001 from the batch: nothing was kept; call it again at once."""

    def __init__(self, sqlstate, cls):
        super().__init__(sqlstate)
        self.sqlstate = sqlstate
        self.cls = cls


CONTRACT_STATES = {"42883", "3F000", "42501", "42P01"}
BATCH_SQL = ("select sentinel.sys_post_publish_batch(v_run_id => $1, v_partition_id => $2, v_worker_id => $3, "
             "v_attempt => $4, v_budget => $5)")


def _is_link_error(exc):
    if isinstance(exc, (OSError, ConnectionError, asyncpg.exceptions.InterfaceError)):
        return True
    if isinstance(exc, asyncpg.exceptions.PostgresConnectionError):
        return True
    st = getattr(exc, "sqlstate", None) or ""
    return st.startswith("08") or st in ("57P01", "57P02", "57P03")


def _set_keepalive(con):
    """TCP keepalives on the long-lived link: idle 30 s, interval 10 s, 3 probes."""
    try:
        import socket
        tr = getattr(con, "_transport", None)
        sock = tr.get_extra_info("socket") if tr is not None else None
        if sock is None:
            return
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        for opt, val in (("TCP_KEEPIDLE", 30), ("TCP_KEEPINTVL", 10), ("TCP_KEEPCNT", 3)):
            if hasattr(socket, opt):
                sock.setsockopt(socket.IPPROTO_TCP, getattr(socket, opt), val)
    except Exception:
        pass


def _json(value):
    if isinstance(value, str):
        try:
            return json.loads(value)
        except ValueError:
            return value
    return value


class Database(object):
    """Three links (01 section 3.2): the listen link, a pool of 4 for every short call, and one batch connection
    outside the pool that carries sys_post_publish_batch and nothing else."""

    def __init__(self, settings):
        self.s = settings
        self.link = None
        self.pool = None
        self.batch_con = None
        self.batch_pid = None
        self.connected = False
        self.listening = False
        self.generation = 0             # goes up by one at every successful connect of the link and the pool
        self.on_notify = None           # (channel, payload)
        self.on_contract_broken = None

    def _kwargs(self, statement_timeout=None):
        ss = {"application_name": self.s.service}
        if statement_timeout:
            ss["statement_timeout"] = statement_timeout
        return dict(host=self.s.db_host, port=self.s.db_port, database=self.s.db_name, user=self.s.db_user,
                    password=lambda: read_database_password(self.s.db_password_file),
                    statement_cache_size=0, server_settings=ss, timeout=10)

    # ---- the listen link and the pool
    async def connect(self):
        await self.close()
        try:
            link = await asyncpg.connect(**self._kwargs())
            self.link = link
            _set_keepalive(link)
            link.add_termination_listener(self._link_ended)
            self.pool = await asyncpg.create_pool(min_size=1, max_size=4,
                                                  **self._kwargs(str(int(POOL_CALL_SEC)) + "s"))
            await link.add_listener("run_request", self._notified)
            await link.add_listener("run_work", self._notified)
        except asyncpg.PostgresError as e:
            await self.close()
            if getattr(e, "hint", None) == "not_service":
                raise NotService()
            raise DbGone(type(e).__name__)
        except Exception as e:
            await self.close()
            raise DbGone(type(e).__name__)
        self.listening = True
        self.connected = True
        self.generation += 1

    def _link_ended(self, con):
        if con is self.link:
            self.mark_gone()

    def _notified(self, con, pid, channel, payload):
        if self.on_notify is not None and con is self.link:
            try:
                self.on_notify(channel, payload)
            except Exception as e:
                log.error("notify " + kv(where=where(e)))

    async def close(self, batch_too=False):
        self.connected = False
        self.listening = False
        link, pool = self.link, self.pool
        self.link = self.pool = None
        if link is not None:
            try:
                await asyncio.wait_for(link.close(), 2)
            except Exception:
                link.terminate()
        if pool is not None:
            try:
                await asyncio.wait_for(pool.close(), 2)
            except Exception:
                pool.terminate()
        if batch_too:
            await self.close_batch()

    def mark_gone(self):
        if self.connected:
            log.warning("db_lost")
        self.connected = False
        self.listening = False

    async def ping(self):
        """The listen link's cheap statement."""
        if not self.connected or self.link is None:
            raise DbGone("not_connected")
        try:
            await self.link.fetchval("select 1", timeout=PING_SEC)
        except Exception as e:
            self.mark_gone()
            raise DbGone(type(e).__name__)

    async def _pool_fetch(self, sql, args, timeout, many=False):
        if not self.connected or self.pool is None:
            raise DbGone("not_connected")
        try:
            async with self.pool.acquire(timeout=timeout) as con:
                if many:
                    return await con.fetch(sql, *args, timeout=timeout + 1)
                return await con.fetchval(sql, *args, timeout=timeout + 1)
        except asyncpg.exceptions.QueryCanceledError:
            raise DbGone("timeout")                         # the pool's statement timeout
        except asyncpg.PostgresError as e:
            hint = getattr(e, "hint", None)
            if hint == "not_service":
                raise NotService()
            if _is_link_error(e):
                self.mark_gone()
                raise DbGone(type(e).__name__)
            raise
        except (asyncio.TimeoutError, TimeoutError):
            raise DbGone("timeout")
        except Exception as e:
            if _is_link_error(e):
                self.mark_gone()
            raise DbGone(type(e).__name__)

    async def fetch_catalog(self, sql, *args):
        """A read of pg_catalog for the contract check (not a table of the product)."""
        try:
            return await self._pool_fetch(sql, args, POOL_CALL_SEC, many=True)
        except asyncpg.PostgresError as e:
            raise DbRefusal(e.sqlstate, getattr(e, "hint", None), type(e).__name__)

    async def call(self, fn, timeout=POOL_CALL_SEC, **params):
        """select sentinel.<fn>(name => value, ...) on the pool: the answer as Python. jsonb values go as JSON text."""
        names = list(params)
        sql = "select sentinel." + fn + "(" + ", ".join(n + " => $" + str(i + 1) for i, n in enumerate(names)) + ")"
        args = [json.dumps(v) if isinstance(v, (dict, list)) else v for v in params.values()]
        try:
            value = await self._pool_fetch(sql, args, timeout)
        except asyncpg.PostgresError as e:
            if e.sqlstate in CONTRACT_STATES:
                if self.on_contract_broken:
                    self.on_contract_broken()
                raise ContractBroken(fn, e.sqlstate, type(e).__name__)
            raise DbRefusal(e.sqlstate, getattr(e, "hint", None), type(e).__name__)
        return _json(value)

    async def cancel_batch(self):
        """pg_cancel_backend on the worker's own batch connection, through the pool. True when it was sent."""
        pid = self.batch_pid
        if pid is None:
            return False
        try:
            return bool(await self._pool_fetch("select pg_cancel_backend($1)", [pid], POOL_CALL_SEC))
        except asyncpg.PostgresError as e:
            raise DbRefusal(e.sqlstate, getattr(e, "hint", None), type(e).__name__)

    # ---- the batch connection
    async def ensure_batch(self):
        if self.batch_con is not None and not self.batch_con.is_closed():
            return
        await self.close_batch()
        try:
            con = await asyncpg.connect(**self._kwargs(str(self.s.statement_min) + "min"))
        except asyncpg.PostgresError as e:
            if getattr(e, "hint", None) == "not_service":
                raise NotService()
            raise DbGone(type(e).__name__)
        except Exception as e:
            raise DbGone(type(e).__name__)
        self.batch_con = con
        self.batch_pid = con.get_server_pid()

    async def close_batch(self):
        con = self.batch_con
        self.batch_con = None
        self.batch_pid = None
        if con is not None:
            try:
                await asyncio.wait_for(con.close(), 2)
            except Exception:
                con.terminate()

    async def batch(self, run_id, partition_id, worker_id, attempt, budget):
        """One call of sys_post_publish_batch on the batch connection. Autocommits; holds nothing open."""
        con = self.batch_con
        if con is None or con.is_closed():
            raise DbGone("not_connected")
        try:
            value = await con.fetchval(BATCH_SQL, run_id, partition_id, worker_id, attempt, budget)
        except asyncpg.exceptions.QueryCanceledError:
            raise BatchCanceled()
        except (asyncpg.exceptions.DeadlockDetectedError, asyncpg.exceptions.SerializationError) as e:
            raise BatchRetryable(e.sqlstate, type(e).__name__)
        except asyncpg.PostgresError as e:
            hint = getattr(e, "hint", None)
            if hint == "not_service":
                raise NotService()
            if _is_link_error(e):
                await self.close_batch()
                raise DbGone(type(e).__name__)
            if e.sqlstate in CONTRACT_STATES:
                raise ContractBroken("sys_post_publish_batch", e.sqlstate, type(e).__name__)
            raise DbRefusal(e.sqlstate, hint, type(e).__name__)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            if _is_link_error(e):
                await self.close_batch()
                raise DbGone(type(e).__name__)
            raise
        return _json(value)


# =====================================================================================
# 5. The contract (01 section 3.3): eight functions, with the parameters the worker sends. All required.
# =====================================================================================
CONTRACT_BASE = {
    "sys_post_worker_checkin": ("v_worker_type", "v_worker_name", "v_worker_version", "v_address", "v_port",
                                "v_capabilities"),
    "sys_post_worker_heartbeat": ("v_worker_id", "v_status"),
}
CONTRACT_JOB = {
    "sys_post_worker_heartbeat": ("v_worker_id", "v_status", "v_health"),
    "sys_get_partitions_held": ("v_worker_id",),
    "sys_post_partition_claim_next": ("v_worker_id",),
    "sys_post_publish_batch": ("v_run_id", "v_partition_id", "v_worker_id", "v_attempt", "v_budget"),
    "sys_post_partition_renew": ("v_run_id", "v_stage", "v_partition_id", "v_worker_id", "v_attempt",
                                 "v_progress", "v_checkpoint"),
    "sys_post_partition_complete": ("v_run_id", "v_stage", "v_partition_id", "v_worker_id", "v_attempt",
                                    "v_summary"),
    "sys_post_partition_release": ("v_run_id", "v_stage", "v_partition_id", "v_worker_id", "v_attempt",
                                   "v_outcome", "v_code", "v_params", "v_text"),
}

_CONTRACT_SQL = """
select p.proname, p.proargnames, p.proargmodes, has_function_privilege(p.oid, 'EXECUTE') as can_run
  from pg_catalog.pg_proc p join pg_catalog.pg_namespace n on n.oid = p.pronamespace
 where n.nspname = 'sentinel' and p.proname = any($1::text[])
"""


async def check_contract(db):
    """(base_ok, job_ok, missing function names, details). A detail is 'fn', 'fn(v_param)' or 'fn (no execute)'."""
    wanted = sorted(set(CONTRACT_BASE) | set(CONTRACT_JOB))
    rows = await db.fetch_catalog(_CONTRACT_SQL, wanted)
    found = {}
    for r in rows:
        names = list(r["proargnames"] or [])
        modes = list(r["proargmodes"] or [])
        if modes:
            names = [n for n, m in zip(names, modes) if m in ("i", "b", "v")]
        found.setdefault(r["proname"], []).append((set(names), bool(r["can_run"])))

    def problems(fn, params):
        if fn not in found:
            return [fn]
        best = None
        for names, can_run in found[fn]:
            miss = [fn + "(" + p + ")" for p in params if p not in names]
            if not can_run:
                miss.append(fn + " (no execute)")
            if best is None or len(miss) < len(best):
                best = miss
        return best

    base = []
    for fn, params in CONTRACT_BASE.items():
        base += problems(fn, params)
    job = []
    for fn, params in CONTRACT_JOB.items():
        job += problems(fn, params)
    details = sorted(set(base + job))
    names = sorted(set(d.split("(")[0].split(" ")[0] for d in details))
    return not base, not base and not job, names, details


# =====================================================================================
# 6. The claim: one publish queue
# =====================================================================================
_STOP_RANK = {"paused": 1, "stopped": 2, "cancelled": 3, "sigterm": 4}


class Abandon(Exception):
    """A complete or a release refused the same way three times: the queue is given up by no longer renewing it."""

    def __init__(self, sqlstate):
        super().__init__(str(sqlstate))
        self.sqlstate = sqlstate


class Claim(object):

    def __init__(self, svc, run_id, partition_id, attempt, resumed):
        self.svc = svc
        self.db = svc.db
        self.worker_id = svc.worker_id
        self.run_id = int(run_id)
        self.partition_id = int(partition_id)
        self.attempt = int(attempt)
        self.resumed = bool(resumed)
        self.key = str(self.run_id) + ":" + STAGE + ":" + str(self.partition_id)
        self.totals = {"objects": 0, "tags": 0, "failed": 0, "removed": 0}
        self.batches = 0
        self.batch_ms = []
        self.last_batch_ms = None
        self.longest_batch_ms = 0
        self.waiting = {"objects": None, "tags": None}
        self.state = "working"          # working | waiting_for_work | winding_down
        self.stop_kind = None           # paused | stopped | cancelled | sigterm
        self.stop_at = None
        self.sigterm_at = None
        self.lost = False
        self.lost_why = None
        self.contract_broken = None     # (fn, sqlstate, cls), set by a call that met 42883, 3F000, 42501, 42P01
        self.contract_retry = None      # (fn, sqlstate) of the break the worker came back from
        self.set_aside = False          # the queue is off the summary while the contract is broken
        self.db_trouble = False         # a complete or release timed out three times: the database out of reach
        self.finished = False
        self.in_batch = False
        self.cancel_mark = False
        self.abandon_sqlstate = None
        self.poke = asyncio.Event()     # a flag changed: the loop looks at it
        self.work_event = asyncio.Event()
        self.renew_wake = asyncio.Event()
        self.call_lock = asyncio.Lock()
        self.renew_task = None
        self.task = None
        self.started = time.monotonic()
        self.last_progress = self.started
        self.renewals = 0

    # ---- identity and figures
    def ids(self):
        return dict(v_run_id=self.run_id, v_stage=STAGE, v_partition_id=self.partition_id,
                    v_worker_id=self.worker_id, v_attempt=self.attempt)

    def queue_block(self):
        return {"run_id": self.run_id, "stage": STAGE, "partition_id": self.partition_id, "attempt": self.attempt,
                "state": "winding_down" if (self.stop_kind or self.svc.stopping) else self.state,
                "seconds_since_progress": int(time.monotonic() - self.last_progress)}

    def publish_block(self):
        return {"batches": self.batches, "objects": self.totals["objects"], "tags": self.totals["tags"],
                "removed": self.totals["removed"], "failed": self.totals["failed"],
                "last_batch_ms": self.last_batch_ms, "longest_batch_ms": self.longest_batch_ms,
                "waiting": dict(self.waiting)}

    def pct(self, p):
        if not self.batch_ms:
            return 0
        s = sorted(self.batch_ms)
        return s[min(len(s) - 1, int(len(s) * p))]

    # ---- flags
    def request_stop(self, kind):
        if self.stop_kind is None or _STOP_RANK[kind] > _STOP_RANK[self.stop_kind]:
            if self.stop_kind is None:
                self.stop_at = time.monotonic()
            log.info("publish_told " + kv(run=self.run_id, partition=self.partition_id, why=kind,
                                          in_batch=str(self.in_batch).lower()))
            self.stop_kind = kind
        if kind == "sigterm" and self.sigterm_at is None:
            self.sigterm_at = time.monotonic()
        self.poke.set()

    def mark_lost(self, why):
        if not self.lost:
            self.lost = True
            self.lost_why = why
            log.warning("publish_not_held " + kv(run=self.run_id, partition=self.partition_id, attempt=self.attempt,
                                                 by=why))
        self.poke.set()

    def stop_pending(self):
        return self.lost or self.stop_kind is not None or self.contract_broken is not None

    # ---- the lease (01 section 7)
    def _apply_renew(self, ans):
        if self.finished:
            return
        if not isinstance(ans, dict) or not ans.get("held"):
            self.mark_lost("renew")
            return
        w = ans.get("waiting")
        if isinstance(w, dict):
            self.waiting = {"objects": w.get("objects"), "tags": w.get("tags")}
        if ans.get("cancel"):
            self.request_stop("cancelled")
        elif ans.get("stop"):
            self.request_stop("stopped")
        elif ans.get("pause"):
            self.request_stop("paused")

    async def renew_once(self):
        """One renewal with the totals. Raises DbGone or ContractBroken; a refusal is logged."""
        async with self.call_lock:
            if self.finished or self.set_aside or self.lost:
                return None
            try:
                ans = await self.db.call("sys_post_partition_renew", v_progress=dict(self.totals),
                                         v_checkpoint=None, **self.ids())
            except DbRefusal as r:
                if r.hint == "worker_not_found":
                    self.mark_lost("worker_not_found")
                    self.svc.worker_gone()
                    return None
                log.error("renew_refused " + kv(run=self.run_id, sqlstate=r.sqlstate, hint=r.hint or "-"))
                return None
            self.renewals += 1
        self._apply_renew(ans)
        return ans

    async def _renew_loop(self):
        due = time.monotonic() + RENEW_SEC
        while not self.finished and not self.set_aside and not self.lost:
            try:
                await asyncio.wait_for(self.renew_wake.wait(), max(0.0, due - time.monotonic()))
            except (asyncio.TimeoutError, TimeoutError):
                pass
            self.renew_wake.clear()
            if self.finished or self.set_aside or self.lost:
                return
            due = time.monotonic() + RENEW_SEC
            try:
                await self.renew_once()
            except DbGone as e:
                log.warning("renew_failed " + kv(run=self.run_id, why=e))   # tried again at the next tick, or at once
            except ContractBroken as e:                                          # after a reconnect
                self.contract_broken = (e.fn, e.sqlstate, e.cls)
                self.poke.set()
                return
            except NotService:
                self.svc.fatal("not_service")
                return
            except Exception as e:
                log.error("renew " + kv(run=self.run_id, where=where(e)))

    def _start_renewals(self):
        if self.renew_task is None or self.renew_task.done():
            self.renew_task = asyncio.create_task(self._renew_loop())

    async def _stop_renewals(self):
        t = self.renew_task
        self.renew_task = None
        if t is not None and not t.done() and t is not asyncio.current_task():
            t.cancel()
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass

    async def _renew_now(self):
        """One renewal at once, its answer acted on before the next batch (note 2026-47-47, question 2)."""
        for i in range(3):
            try:
                await self.renew_once()
                return
            except DbGone:
                if self.svc.stopping:
                    return
                await asyncio.sleep(2)
            except ContractBroken as e:
                self.contract_broken = (e.fn, e.sqlstate, e.cls)
                return

    # ---- life
    async def run(self):
        log.info("publish_claim " + kv(run=self.run_id, partition=self.partition_id, attempt=self.attempt,
                                       resumed=str(self.resumed).lower()))
        outcome = "kept"
        try:
            if self.resumed:
                await self._renew_now()
            self._start_renewals()
            try:
                outcome = await self._work()
            except Abandon as a:
                self.abandon_sqlstate = a.sqlstate
                outcome = "abandoned"
            except (NotService, asyncio.CancelledError):
                raise
            except Exception as e:
                log.error("publish_fault " + kv(run=self.run_id, where=where(e)))
                try:
                    outcome = await self._leave_retry(unexpected(e))
                except Abandon as a:
                    self.abandon_sqlstate = a.sqlstate
                    outcome = "abandoned"
        except NotService:
            self.svc.fatal("not_service")
        except asyncio.CancelledError:
            outcome = "kept"
        except Exception as e:
            log.error("publish_fault " + kv(run=self.run_id, where=where(e)))
            outcome = "abandoned"
        finally:
            self.finished = True
            await self._stop_renewals()
            self._end_log(outcome)
            self.svc.claim_ended(self, outcome)

    def _end_log(self, outcome):
        line = "publish_end " + kv(run=self.run_id, partition=self.partition_id, attempt=self.attempt,
                                   outcome=outcome, objects=self.totals["objects"], tags=self.totals["tags"],
                                   removed=self.totals["removed"], failed=self.totals["failed"],
                                   batches=self.batches, seconds=int(time.monotonic() - self.started),
                                   batch_p50_ms=self.pct(0.5), batch_p95_ms=self.pct(0.95),
                                   batch_max_ms=self.longest_batch_ms)
        if outcome == "abandoned":
            log.error(line + " " + kv(sqlstate=self.abandon_sqlstate or "-"))
        else:
            log.info(line)

    async def _work(self):
        retryable = 0
        link_wait = 1.0
        while True:
            # the stop point: between two batches, and the only one
            if self.lost:
                return "not_held"
            if self.contract_broken is not None:
                out = await self._contract_hold()
                if out is not None:
                    return out
                continue
            if self.stop_kind == "sigterm":
                await self._keep()
                return "kept"
            if self.stop_kind in ("cancelled", "stopped", "paused"):
                kind = self.stop_kind
                r = await self._release(kind)
                if r == "released":
                    return kind
                if r == "not_requested":                       # withdrawn: the queue is still the worker's
                    log.info("publish_carry_on " + kv(run=self.run_id, was=kind))
                    self.stop_kind = None
                    self.stop_at = None
                    self.state = "working"
                continue

            try:
                await self.db.ensure_batch()
            except DbGone:
                await self._nap(link_wait)                     # a growing wait, up to 30 seconds, never exit
                link_wait = min(link_wait * 2, RECONNECT_MAX_SEC)
                continue
            link_wait = 1.0

            try:
                j = await self._batch()
            except BatchCanceled:
                if self.cancel_mark:
                    continue                                   # its own cancel: nothing of it kept; the top decides
                return await self._leave_retry(reason("publish_timeout", {"seconds": self.svc.s.statement_min * 60,
                                                                          "minutes": self.svc.s.statement_min}))
            except BatchRetryable as e:
                retryable += 1
                if retryable >= RETRYABLE_MAX:
                    return await self._leave_retry(unexpected(e.cls))
                continue
            except DbGone:
                log.warning("batch_link_lost " + kv(run=self.run_id))
                continue                                       # nothing of it kept: called again at the same attempt
            except ContractBroken as e:
                self.contract_broken = (e.fn, e.sqlstate, e.cls)
                continue
            except DbRefusal as r:
                if r.hint == "not_held":
                    self.mark_lost("batch")
                    return "not_held"
                if r.hint == "worker_not_found":
                    self.mark_lost("worker_not_found")
                    self.svc.worker_gone()
                    return "not_held"
                return await self._leave_retry(unexpected(r.cls))
            retryable = 0
            self.contract_retry = None

            if not isinstance(j, dict) or not j.get("held"):
                self.mark_lost("batch")
                return "not_held"
            items = int(j.get("items") or 0)
            self.totals["objects"] += items
            self.totals["tags"] += int(j.get("tags") or 0)
            self.totals["failed"] += int(j.get("failed") or 0)
            self.totals["removed"] += int(j.get("removed") or 0)
            w = j.get("waiting")
            if isinstance(w, dict):
                self.waiting = {"objects": w.get("objects"), "tags": w.get("tags")}
            if items > 0:
                self.state = "working"
                self.last_progress = time.monotonic()
                continue
            if j.get("more_coming"):
                self.state = "waiting_for_work"                # until a batch takes something again
                await self._wait_more()
                continue
            self.state = "working"
            r = await self._final("sys_post_partition_complete", v_summary=dict(self.totals))
            if r is None:
                continue                                       # lost, or SIGTERM's time is up: the top decides
            if r.get("done"):
                self.finished = True
                return "completed"
            if r.get("why") == "not_held":
                self.mark_lost("complete")
                return "not_held"
            # objects_waiting, more_coming: something was dealt after the last batch: back to the batches

    async def _nap(self, sec):
        """A wait that a changed flag ends."""
        if self.stop_pending():
            return
        self.poke.clear()
        try:
            await asyncio.wait_for(self.poke.wait(), sec)
        except (asyncio.TimeoutError, TimeoutError):
            pass

    async def _wait_more(self):
        """Nothing waits, more is coming: a NOTIFY run_work for this queue, 5 seconds, a flag, or SIGTERM."""
        if self.stop_pending():
            return
        self.poke.clear()
        a = asyncio.ensure_future(self.work_event.wait())
        b = asyncio.ensure_future(self.poke.wait())
        try:
            await asyncio.wait({a, b}, timeout=WAIT_MORE_SEC, return_when=asyncio.FIRST_COMPLETED)
        finally:
            a.cancel()
            b.cancel()

    def _cancel_due(self):
        """When the batch in hand is to be cancelled: at once when not held, 15 seconds after a stop, never else."""
        if self.lost:
            return time.monotonic()
        if self.stop_kind is not None and self.stop_at is not None:
            return self.stop_at + STOP_WITHIN_SEC
        return None

    async def _batch(self):
        self.work_event.clear()
        self.cancel_mark = False
        self.in_batch = True
        t0 = time.monotonic()
        task = asyncio.ensure_future(self.db.batch(self.run_id, self.partition_id, self.worker_id, self.attempt,
                                                   self.svc.s.budget))
        ok = False
        try:
            while not task.done():
                self.poke.clear()
                due = self._cancel_due()
                wait = 1.0 if due is None else max(0.0, min(1.0, due - time.monotonic()))
                p = asyncio.ensure_future(self.poke.wait())
                try:
                    await asyncio.wait({task, p}, timeout=wait, return_when=asyncio.FIRST_COMPLETED)
                finally:
                    p.cancel()
                if task.done():
                    break
                due = self._cancel_due()
                if due is not None and time.monotonic() >= due and not self.cancel_mark:
                    self.cancel_mark = True                    # the mark goes first: a 57014 with it is its own
                    told_ms = int((time.monotonic() - (self.stop_at or time.monotonic())) * 1000)
                    why = "not_held" if self.lost else self.stop_kind
                    try:
                        sent = await self.db.cancel_batch()
                    except Exception as e:
                        sent = False
                        log.warning("batch_cancel_failed " + kv(run=self.run_id, why=type(e).__name__))
                    if sent:
                        log.info("batch_cancelled " + kv(run=self.run_id, after_ms=int((time.monotonic() - t0) * 1000),
                                                         told_ms=0 if self.lost else told_ms, why=why))
                    else:
                        self.cancel_mark = False               # tried again in a second
            result = task.result()
            ok = True
            return result
        finally:
            self.in_batch = False
            if not task.done():
                task.cancel()
            ms = int((time.monotonic() - t0) * 1000)
            if ok:
                self.batches += 1
                self.batch_ms.append(ms)
                self.last_batch_ms = ms
                self.longest_batch_ms = max(self.longest_batch_ms, ms)
                if ms >= SLOW_BATCH_MS:
                    r = task.result() if task.done() and not task.cancelled() else {}
                    r = r if isinstance(r, dict) else {}
                    log.info("batch_slow " + kv(run=self.run_id, ms=ms, items=r.get("items", "?"),
                                                tags=r.get("tags", "?")))

    # ---- complete and release (01 section 8; note 2026-47-47 section 2, point 2)
    async def _wait_reconnected(self, sec):
        gen = self.db.generation
        end = time.monotonic() + sec
        while time.monotonic() < end and not self.lost:
            if self.db.connected and self.db.generation != gen:
                return
            if self.stop_kind == "sigterm" and self._sigterm_late():
                return
            await asyncio.sleep(0.2)

    def _sigterm_late(self):
        return self.sigterm_at is not None and time.monotonic() > self.sigterm_at + SHUTDOWN_CLAIM_SEC - 3

    async def _final(self, fn, **params):
        """A complete or a release. Its answer; None when the queue was lost meanwhile or SIGTERM's time ran out.
        A timeout or a lost link: tried at once, up to three times, then the database is out of reach: reconnect
        with a growing wait and try again after each reconnect, the queue kept. The same refusal three times: Abandon."""
        timeouts = 0
        last = None
        same = 0
        backoff = 1.0
        while True:
            if self.lost:
                self.db_trouble = False
                return None
            if self.stop_kind == "sigterm" and self._sigterm_late():
                return None
            try:
                async with self.call_lock:
                    ans = await self.db.call(fn, **self.ids(), **params)
                if self.db_trouble:
                    log.info("db_trouble_over " + kv(run=self.run_id))
                self.db_trouble = False
                if not isinstance(ans, dict):
                    raise DbRefusal("answer", type(ans).__name__)
                return ans
            except DbGone:
                timeouts += 1
                if timeouts < FINAL_TRIES and self.db.connected:
                    continue
                timeouts = 0
                if not self.db_trouble:
                    log.warning("db_trouble " + kv(run=self.run_id, call=fn))
                self.db_trouble = True
                if self.db.connected:
                    self.db.mark_gone()                        # the standard's "database out of reach"
                    self.svc.wake()
                await self._wait_reconnected(backoff)
                backoff = min(backoff * 2, RECONNECT_MAX_SEC)
            except DbRefusal as r:
                if r.hint == "worker_not_found":
                    self.mark_lost("worker_not_found")
                    self.svc.worker_gone()
                    return None
                key = (r.sqlstate, r.hint)
                same = same + 1 if key == last else 1
                last = key
                log.warning("final_refused " + kv(run=self.run_id, call=fn, sqlstate=r.sqlstate, hint=r.hint or "-",
                                                  n=same))
                if same >= FINAL_TRIES:
                    raise Abandon(r.sqlstate)
                await asyncio.sleep(1)

    async def _release(self, outcome, why=None):
        """'released', 'not_requested', 'not_held', or None (lost, or SIGTERM's time ran out)."""
        if why is None:
            code = params = text = None                        # stopped, cancelled, paused: the database writes it
        else:
            code, params, text = why["code"], why["params"], why["text"]
        self.state = "winding_down"
        ans = await self._final("sys_post_partition_release", v_outcome=outcome, v_code=code, v_params=params,
                                v_text=text)
        if ans is None:
            return None
        if ans.get("released"):
            self.finished = True
            return "released"
        w = ans.get("why")
        if w == "not_held":
            self.mark_lost("release")
            return "not_held"
        if w == "not_requested":
            return "not_requested"
        raise Abandon("released_" + str(w))

    async def _leave_retry(self, why):
        """Release as retry with a reason: the worker can't go on with this queue."""
        r = await self._release("retry", why)
        if r == "released":
            log.info("publish_retry " + kv(run=self.run_id, code=why["code"]))
            return "retry"
        if self.lost or r == "not_held":
            return "not_held"
        return "kept"

    async def _keep(self):
        """SIGTERM: one more renewal, so the lease is fresh, and no release."""
        try:
            await asyncio.wait_for(self._renew_now(), 6)
        except Exception:
            pass
        self.finished = True

    # ---- a broken contract with a queue in hand (note 2026-47-47 section 2, point 3)
    async def _contract_hold(self):
        fn, sqlstate, cls = self.contract_broken
        if self.contract_retry == (fn, sqlstate):
            # the check passed, the queue was still the worker's, and the same call failed the same way again
            self.contract_broken = None
            log.error("contract_not_the_cause " + kv(run=self.run_id, call=fn, sqlstate=sqlstate))
            return await self._leave_retry(unexpected(cls))
        log.error("publish_set_aside " + kv(run=self.run_id, partition=self.partition_id, attempt=self.attempt,
                                            call=fn, sqlstate=sqlstate))
        self.set_aside = True                                  # off the summary; no more renewals
        await self._stop_renewals()
        seq = self.svc.contract_seq
        self.svc.contract_broke()
        while True:
            if self.svc.stopping or self.svc.fatal_reason:
                return "kept"
            c = self.svc.contract
            if self.db.connected and self.svc.contract_seq > seq and c["job"]:
                try:
                    held = await self.db.call("sys_get_partitions_held", v_worker_id=self.worker_id)
                    break
                except (DbGone, ContractBroken, DbRefusal):
                    seq = self.svc.contract_seq
            await asyncio.sleep(0.5)
        held = held if isinstance(held, list) else []
        mine = any(isinstance(h, dict) and h.get("stage") == STAGE and int(h.get("run_id", -1)) == self.run_id
                   and int(h.get("partition_id", -1)) == self.partition_id and int(h.get("attempt", -1)) == self.attempt
                   for h in held)
        if not mine:
            self.mark_lost("taken_back")
            return "not_held"
        log.info("publish_carry_on " + kv(run=self.run_id, was="contract", attempt=self.attempt))
        self.set_aside = False
        self.contract_broken = None
        self.contract_retry = (fn, sqlstate)
        self.state = "working"
        await self._renew_now()
        self._start_renewals()
        return None


# =====================================================================================
# 7. The service
# =====================================================================================
class Service(object):

    def __init__(self, settings, db=None):
        self.s = settings
        self.db = db if db is not None else Database(settings)
        self.db.on_contract_broken = self.contract_broke
        self.db.on_notify = self._on_notify
        self.worker_id = None
        self.status_word = "starting"
        self.contract = {"base": False, "job": False, "missing": [], "details": [], "checked_at": None}
        self.contract_seq = 0
        self.claim = None
        self.claim_lock = asyncio.Lock()
        self.stopping = False
        self.fatal_reason = None
        self.started_at = datetime.now(timezone.utc)
        self.last_heartbeat = None
        self.health_sent = None
        self.read_work = False
        self.abandoned = set()          # (run, partition, attempt) not to be taken up again while still held
        self.counts = {"completed": 0, "released": {"stopped": 0, "cancelled": 0, "paused": 0, "retry": 0},
                       "lost": 0, "abandoned": 0, "kept": 0}
        self.asks = {"count": 0, "last_ask": None, "last_answer": None}
        self._wake = asyncio.Event()
        self._ask_now = asyncio.Event()
        self._task = None
        self._ask_task = None
        self._contract_due = 0.0
        self._extra_checks = []
        self._heartbeat_due = 0.0
        self._ping_due = 0.0
        self._reconcile_due = 0.0       # what it holds is read at start, and again when it may have changed
        self._asking = False            # the contract check has passed since the link last came up
        self._exit_hook = None

    # ---- state
    @property
    def ready(self):
        c = self.claim
        return (self.db.connected and self.worker_id is not None and self.contract["job"] and self.read_work
                and not self.stopping and self.fatal_reason is None and not (c is not None and c.db_trouble))

    def ready_step(self):
        c = self.claim
        if self.stopping:
            return "stopping"
        if not self.db.connected or (c is not None and c.db_trouble):
            return "reconnecting" if self.worker_id is not None else "connecting"
        if not self.contract["base"] or not self.contract["job"]:
            return "contract_missing" if self.contract["checked_at"] else "checking_contract"
        if self.worker_id is None:
            return "checking_in"
        if not self.read_work:
            return "reading_held"
        return "ready"

    def _shown_claim(self):
        c = self.claim
        return c if (c is not None and not c.set_aside) else None

    def health(self):
        c = self.claim
        if self.ready:
            problem = None
        elif self.stopping:
            problem = health_item("stopping")
        elif self.contract["checked_at"] and (not self.contract["base"] or not self.contract["job"]):
            problem = health_item("contract_missing", {"missing": self.contract["missing"]})
        elif self.worker_id is not None and (not self.db.connected or (c is not None and c.db_trouble)):
            problem = health_item("reconnecting")
        else:
            problem = health_item("starting")
        shown = self._shown_claim()
        if shown is None:
            work = health_item("idle")
        elif shown.stop_kind or self.stopping:
            work = health_item("winding_down", {"run_id": shown.run_id})
        elif shown.state == "waiting_for_work":
            work = health_item("waiting_for_work", {"run_id": shown.run_id})
        else:
            work = health_item("working", {"stage": STAGE, "run_id": shown.run_id})
        return {"ready": self.ready, "problem": problem, "work": work,
                "detail": {"queue": shown.queue_block() if shown else None, "last_ask": self.asks["last_ask"],
                           "publish": shown.publish_block() if shown else None}}

    def status(self):
        shown = self._shown_claim()
        c = self.claim
        return {
            "worker": {"name": self.s.service, "id": self.worker_id, "version": __version__,
                       "status": self.status_word, "started_at": self.started_at.isoformat(),
                       "step": self.ready_step()},
            "database": {"connected": self.db.connected, "listening": self.db.listening, "lock": None,
                         "contract": {"ok": self.contract["job"], "missing": self.contract["missing"],
                                      "details": self.contract["details"], "checked_at": self.contract["checked_at"]},
                         "last_heartbeat": self.last_heartbeat, "health_sent": self.health_sent},
            "queue": shown.queue_block() if shown else None,
            "asks": {"count": self.asks["count"], "last_ask": self.asks["last_ask"],
                     "last_answer": self.asks["last_answer"], "poll_sec": self.s.poll_sec},
            "publish": {"current": shown.publish_block() if shown else None,
                        "set_aside": ({"run_id": c.run_id, "partition_id": c.partition_id, "attempt": c.attempt}
                                      if c is not None and c.set_aside else None),
                        "budget": self.s.budget, "statement_min": self.s.statement_min,
                        "since_start": {"completed": self.counts["completed"],
                                        "released": dict(self.counts["released"]),
                                        "lost": self.counts["lost"], "abandoned": self.counts["abandoned"],
                                        "kept": self.counts["kept"]}},
            "health": self.health(),
        }

    def capabilities(self):
        return {"publish": {"types": []}}      # able for every type of data source: publishing is the same work

    def version(self):
        return __version__

    def wake(self):
        self._wake.set()

    def fatal(self, why):
        if self.fatal_reason is None:
            self.fatal_reason = why
            log.error("fatal " + kv(why=why))
            if self._exit_hook:
                self._exit_hook(1)

    def contract_broke(self):
        """A call met 42883, 3F000, 42501 or 42P01: the contract check now, then at 5, 15 and 30 seconds."""
        now = time.monotonic()
        self._contract_due = 0.0
        self._extra_checks = [now + d for d in CONTRACT_AFTER_BREAK]
        self.wake()

    def worker_gone(self):
        """worker_not_found: the database was rebuilt. Check in again; what it holds is read again after."""
        if self.worker_id is not None:
            log.warning("worker_not_found")
        self.worker_id = None
        self._reconcile_due = 0.0
        self.wake()

    def _on_notify(self, channel, payload):
        if channel == "run_request":
            if self.claim is None:
                self._ask_now.set()
        elif channel == "run_work":
            c = self.claim
            if c is not None and payload == c.key:
                c.work_event.set()

    # ---- life
    async def start(self):
        log.info("publish_start " + kv(version=__version__, python=sys.version.split()[0]) + " " + self.s.describe())
        self._task = asyncio.create_task(self._supervise())
        self._ask_task = asyncio.create_task(self._ask_loop())

    async def _supervise(self):
        backoff = 1.0
        first = True
        while self.fatal_reason is None and not self.stopping:
            try:
                if not self.db.connected:
                    self._asking = False
                    try:
                        await self.db.connect()
                    except DbGone:
                        await self._sleep(backoff)
                        backoff = min(backoff * 2, RECONNECT_MAX_SEC)
                        continue
                    backoff = 1.0
                    log.info("db_connected " + kv(first=str(first).lower()))
                    if not first:
                        now = time.monotonic()
                        self._extra_checks = [now + d for d in CONTRACT_AFTER_BREAK]
                    first = False
                    await self._after_connect()
                now = time.monotonic()
                if now >= self._contract_due or (self._extra_checks and now >= self._extra_checks[0]):
                    while self._extra_checks and now >= self._extra_checks[0]:
                        self._extra_checks.pop(0)
                    await self._check_contract()
                if self.worker_id is None and self.contract["base"]:
                    await self._checkin()
                if now >= self._heartbeat_due and self.worker_id is not None:
                    await self._heartbeat()
                if now >= self._ping_due:
                    self._ping_due = now + PING_SEC
                    await self.db.ping()
                if (self._reconcile_due is not None and now >= self._reconcile_due and self.contract["job"]
                        and self.worker_id is not None and self.claim is None and not self.stopping):
                    self._reconcile_due = None
                    try:
                        await self._reconcile()
                    except Exception:
                        self._reconcile_due = time.monotonic() + 1.0     # read again in a second
                        raise
            except NotService:
                self.fatal("not_service")
                return
            except DbGone:
                pass
            except ContractBroken:
                pass
            except Exception as e:
                log.error("supervise " + kv(where=where(e)))
            await self._sleep(1.0)

    async def _sleep(self, sec):
        try:
            await asyncio.wait_for(self._wake.wait(), sec)
        except (asyncio.TimeoutError, TimeoutError):
            pass
        self._wake.clear()

    async def _after_connect(self):
        await self._check_contract()
        if self.contract["base"]:
            if self.worker_id is None:
                await self._checkin()
            await self._heartbeat()
        c = self.claim
        if c is not None:
            c.renew_wake.set()               # renew at once: notifications sent meanwhile are lost
            c.work_event.set()               # and look for work at once
        elif self.contract["job"] and self.worker_id is not None:
            self._reconcile_due = 0.0

    async def _check_contract(self):
        self._contract_due = time.monotonic() + CONTRACT_SEC
        base, job, missing, details = await check_contract(self.db)
        changed = details != self.contract["details"] or self.contract["checked_at"] is None
        self.contract.update(base=base, job=job, missing=missing, details=details, checked_at=now_iso())
        self.contract_seq += 1
        self._asking = bool(job)
        if changed:
            if details:
                log.error("contract_missing " + kv(n=len(details)) + " missing=" + ",".join(details))
                now = time.monotonic()
                self._extra_checks = [now + d for d in CONTRACT_AFTER_BREAK]    # a break: also at 5, 15, 30 s
            else:
                log.info("contract_ok")
            self._heartbeat_due = 0.0        # the summary says so at once

    async def _checkin(self):
        ans = await self.db.call("sys_post_worker_checkin", v_worker_type=WORKER_TYPE, v_worker_name=self.s.service,
                                 v_worker_version=self.version(), v_address=self.s.address, v_port=self.s.port,
                                 v_capabilities=self.capabilities())
        self.worker_id = int(ans["worker_id"])
        log.info("checked_in " + kv(worker_id=self.worker_id, version=self.version()))
        self._heartbeat_due = 0.0
        self._reconcile_due = 0.0

    async def _heartbeat(self, again=True):
        self._heartbeat_due = time.monotonic() + self.s.heartbeat_sec
        if self.worker_id is None:
            return
        use_health = "sys_post_worker_heartbeat(v_health)" not in self.contract["details"]
        h = self.health() if use_health else None
        try:
            if use_health:
                await self.db.call("sys_post_worker_heartbeat", v_worker_id=self.worker_id, v_status=self.status_word,
                                   v_health=h)
            else:
                await self.db.call("sys_post_worker_heartbeat", v_worker_id=self.worker_id, v_status=self.status_word)
        except DbRefusal as r:
            if r.hint == "worker_not_found" and again:
                self.worker_gone()
                c = self.claim
                if c is not None:
                    c.renew_wake.set()       # a rebuilt database: the renewal says not held
                await self._checkin()
                return await self._heartbeat(again=False)
            if r.hint == "health_invalid":
                log.error("health_invalid")
                await self.db.call("sys_post_worker_heartbeat", v_worker_id=self.worker_id, v_status=self.status_word)
                h = None
            else:
                raise
        self.last_heartbeat = now_iso()
        self.health_sent = h

    async def _reconcile(self):
        """What the database says this worker holds (01 section 4). A publish queue it holds is taken up again at the
        same attempt, with no new claim. The first time, the status turns active and a heartbeat goes at once,
        and only then does the queue's work (or the idle loop) start."""
        held = await self.db.call("sys_get_partitions_held", v_worker_id=self.worker_id)
        held = [h for h in (held if isinstance(held, list) else []) if isinstance(h, dict)]
        keys = set()
        for h in held:
            try:
                keys.add((int(h["run_id"]), int(h.get("partition_id", 1)), int(h["attempt"])))
            except (KeyError, TypeError, ValueError):
                pass
        self.abandoned &= keys                   # gone from the list: the conductor took it back
        if not self.read_work:
            self.read_work = True
            self.status_word = "active"
            await self._heartbeat()              # the database offers a queue only to a worker it sees active
        for h in held:
            if h.get("stage") != STAGE:
                log.warning("held_other_stage " + kv(run=h.get("run_id"), stage=h.get("stage")))
                continue
            k = (int(h["run_id"]), int(h.get("partition_id", 1)), int(h["attempt"]))
            if k in self.abandoned:
                continue
            async with self.claim_lock:
                if self.claim is not None or self.stopping:
                    continue
                self._start_claim(k[0], k[1], k[2], resumed=True)

    def _start_claim(self, run_id, partition_id, attempt, resumed):
        c = Claim(self, run_id, partition_id, attempt, resumed)
        self.claim = c
        c.task = asyncio.create_task(c.run())
        return c

    def claim_ended(self, claim, outcome):
        if outcome == "completed":
            self.counts["completed"] += 1
        elif outcome in self.counts["released"]:
            self.counts["released"][outcome] += 1
        elif outcome == "not_held":
            self.counts["lost"] += 1
        elif outcome == "abandoned":
            self.counts["abandoned"] += 1
            self.abandoned.add((claim.run_id, claim.partition_id, claim.attempt))
        else:
            self.counts["kept"] += 1
        if self.claim is claim:
            self.claim = None
        self._heartbeat_due = 0.0                # the summary moves on at once
        self.wake()
        self._ask_now.set()                      # ask for the next queue at once

    # ---- the idle loop: a worker takes its work (01 section 5)
    def can_ask(self):
        return (self.ready and self._asking and self.claim is None and not self.claim_lock.locked()
                and self._reconcile_due is None)

    async def _ask_loop(self):
        while self.fatal_reason is None and not self.stopping:
            wait = self.s.poll_sec * random.uniform(1.0 - POLL_JITTER, 1.0 + POLL_JITTER)
            try:
                await asyncio.wait_for(self._ask_now.wait(), wait)
            except (asyncio.TimeoutError, TimeoutError):
                pass
            self._ask_now.clear()
            if not self.can_ask():
                continue
            try:
                await self.ask()
            except NotService:
                self.fatal("not_service")
                return
            except Exception as e:
                log.error("ask " + kv(where=where(e)))

    async def ask(self):
        """One ask for a publish queue. True when one was taken."""
        async with self.claim_lock:
            if self.claim is not None or self.stopping:
                return False
            try:
                ans = await self.db.call("sys_post_partition_claim_next", v_worker_id=self.worker_id)
            except DbGone:
                self._reconcile_due = 0.0        # the claim may have gone through with its answer lost
                self.wake()
                return False
            except ContractBroken:
                return False
            except DbRefusal as r:
                self.asks["count"] += 1
                self.asks["last_ask"] = now_iso()
                self.asks["last_answer"] = r.hint or r.sqlstate
                if r.hint == "worker_not_found":
                    self.worker_gone()
                else:
                    log.warning("ask_refused " + kv(sqlstate=r.sqlstate, hint=r.hint or "-"))
                return False
            self.asks["count"] += 1
            self.asks["last_ask"] = now_iso()
            if not isinstance(ans, dict):
                self.asks["last_answer"] = "?"
                return False
            if ans.get("claimed"):
                self.asks["last_answer"] = "claimed"
                try:
                    run_id, stage = int(ans["run_id"]), str(ans.get("stage", STAGE))
                    partition_id, attempt = int(ans.get("partition_id", 1)), int(ans["attempt"])
                except (KeyError, TypeError, ValueError):
                    log.error("ask_answer " + kv(kind="claimed_without_ids"))
                    self._reconcile_due = 0.0
                    return False
                if stage != STAGE:
                    log.warning("held_other_stage " + kv(run=run_id, stage=stage))
                    return False
                self._start_claim(run_id, partition_id, attempt, resumed=False)
                return True
            why = ans.get("why")
            self.asks["last_answer"] = why
            if why == "worker_busy":
                self._reconcile_due = 0.0        # it holds a queue it doesn't know about
                self.wake()
            return False

    # ---- SIGTERM (01 section 8.5)
    async def shutdown(self):
        if self.stopping:
            return
        self.stopping = True
        self.status_word = "stopping"
        log.info("stopping")
        c = self.claim
        if c is not None:
            c.request_stop("sigterm")            # the batch in hand gets its 15 seconds, then the cancel
        try:
            await asyncio.wait_for(self._heartbeat(), 5)
        except Exception:
            pass
        if c is not None and c.task is not None:
            try:
                await asyncio.wait_for(asyncio.shield(c.task), SHUTDOWN_CLAIM_SEC)
            except Exception:
                pass
        for t in (self._ask_task, self._task):
            if t is not None:
                t.cancel()
        await self.db.close(batch_too=True)
        log.info("stopped")


# =====================================================================================
# 8. Endpoints
# =====================================================================================
def create_app(settings):
    app = Quart(__name__)
    svc = Service(settings)
    app.config["service"] = svc

    @app.before_serving
    async def _start():
        await svc.start()

    @app.after_serving
    async def _stop():
        await svc.shutdown()

    @app.errorhandler(Exception)
    async def failed(e):
        code = getattr(e, "code", None)
        if isinstance(code, int) and 400 <= code < 500:
            return jsonify({"error": type(e).__name__}), code
        log.error("endpoint " + kv(where=where(e)))
        return jsonify({"error": "internal"}), 500

    @app.get("/health")
    async def health():
        return jsonify({"up": True}), 200

    @app.get("/ready")
    async def ready():
        if svc.ready:
            return jsonify({"ready": True, "step": "ready"}), 200
        return jsonify({"ready": False, "step": svc.ready_step()}), 503

    @app.get("/status")
    async def status():
        return jsonify(svc.status()), 200

    # Nothing calls a worker: no /run, no /stop. It asks for its queue, and a stop reaches it at its next renewal.
    return app


# =====================================================================================
# 9. main
# =====================================================================================
def main():
    try:
        settings = Settings()
    except SettingsError as e:
        print(time.strftime("%Y-%m-%d %H:%M:%S") + "|ERROR|publish settings " + e.variable + ": " + e.why, flush=True)
        os._exit(2)
    setup_logging(settings)

    import uvicorn

    app = create_app(settings)
    svc = app.config["service"]
    exit_code = {"code": 0}

    class Server(uvicorn.Server):
        """SIGTERM winds the work down first, then lets uvicorn stop."""

        def handle_exit(self, sig, frame):
            if self.should_exit:
                self.force_exit = True
                return
            if getattr(self, "_winding", False):
                return
            self._winding = True
            loop = self._ds_loop

            def begin():
                async def wind():
                    try:
                        await asyncio.wait_for(svc.shutdown(), SHUTDOWN_SEC)
                    except Exception:
                        pass
                    finally:
                        self.should_exit = True
                asyncio.ensure_future(wind())
            loop.call_soon_threadsafe(begin)

    config = uvicorn.Config(app, host="0.0.0.0", port=settings.port, log_level="warning",
                            access_log=False, lifespan="on")
    server = Server(config)

    def exit_hook(code):
        exit_code["code"] = code
        server.should_exit = True

    svc._exit_hook = exit_hook

    async def serve():
        server._ds_loop = asyncio.get_running_loop()
        await server.serve()

    try:
        asyncio.run(serve())
    except BaseException as e:                                # never a traceback in the log
        if not isinstance(e, (KeyboardInterrupt, SystemExit)):
            log.error("main " + kv(where=where(e)))
            exit_code["code"] = exit_code["code"] or 1
    logging.shutdown()
    os._exit(exit_code["code"])


if __name__ == "__main__":
    main()
