"""保存目录并提供接收、修复和回收所用的事务边界。"""

from contextlib import contextmanager
from pathlib import Path
import sqlite3
import time
from collections.abc import Callable, Iterator

from strata.runtime.errors import unavailable

SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS collections(
        name TEXT PRIMARY KEY,
        retention_seconds INTEGER NOT NULL CHECK(retention_seconds > 0),
        head INTEGER NOT NULL DEFAULT 0,
        created_at INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS deposits(
        id TEXT PRIMARY KEY,
        collection TEXT NOT NULL REFERENCES collections(name),
        token TEXT NOT NULL,
        parent INTEGER NOT NULL,
        manifest TEXT NOT NULL,
        manifest_hash TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        expires_at INTEGER NOT NULL,
        status TEXT NOT NULL CHECK(status IN ('open','published','canceled','expired')),
        revision INTEGER,
        UNIQUE(collection,token),
        CHECK((status = 'published') = (revision IS NOT NULL))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS expected_parts(
        deposit TEXT NOT NULL REFERENCES deposits(id),
        digest TEXT NOT NULL,
        size INTEGER NOT NULL,
        PRIMARY KEY(deposit,digest)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS objects(
        digest TEXT PRIMARY KEY,
        size INTEGER NOT NULL,
        added_at INTEGER NOT NULL,
        verified_at INTEGER NOT NULL,
        sweep_state TEXT NOT NULL DEFAULT 'live' CHECK(sweep_state IN ('live','marked')),
        marked_at INTEGER,
        CHECK((sweep_state = 'marked') = (marked_at IS NOT NULL))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS received_parts(
        deposit TEXT NOT NULL,
        digest TEXT NOT NULL REFERENCES objects(digest),
        received_at INTEGER NOT NULL,
        PRIMARY KEY(deposit,digest),
        FOREIGN KEY(deposit,digest) REFERENCES expected_parts(deposit,digest)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS revisions(
        collection TEXT NOT NULL REFERENCES collections(name),
        number INTEGER NOT NULL,
        deposit TEXT NOT NULL UNIQUE REFERENCES deposits(id),
        parent INTEGER NOT NULL,
        manifest_hash TEXT NOT NULL,
        manifest TEXT NOT NULL,
        published_at INTEGER NOT NULL,
        retain_until INTEGER NOT NULL,
        retired_at INTEGER,
        PRIMARY KEY(collection,number)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS revision_parts(
        collection TEXT NOT NULL,
        number INTEGER NOT NULL,
        digest TEXT NOT NULL,
        PRIMARY KEY(collection,number,digest),
        FOREIGN KEY(collection,number) REFERENCES revisions(collection,number)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS holds(
        collection TEXT NOT NULL,
        number INTEGER NOT NULL,
        reference TEXT NOT NULL,
        reason TEXT NOT NULL,
        placed_at INTEGER NOT NULL,
        released_at INTEGER,
        PRIMARY KEY(collection,number,reference),
        FOREIGN KEY(collection,number) REFERENCES revisions(collection,number)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS incidents(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        digest TEXT NOT NULL,
        opened_at INTEGER NOT NULL,
        observed_at INTEGER NOT NULL,
        reason TEXT NOT NULL,
        resolved_at INTEGER,
        resolution TEXT
    )
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS active_incident ON incidents(digest) WHERE resolved_at IS NULL",
    "CREATE INDEX IF NOT EXISTS revisions_by_digest ON revision_parts(digest,collection,number)",
    "CREATE INDEX IF NOT EXISTS received_by_digest ON received_parts(digest,deposit)",
    "CREATE INDEX IF NOT EXISTS deposit_expiry ON deposits(status,expires_at,id)",
    "CREATE INDEX IF NOT EXISTS sweep_age ON objects(sweep_state,added_at,digest)",
    """
    CREATE TABLE IF NOT EXISTS events(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        collection TEXT REFERENCES collections(name),
        kind TEXT NOT NULL,
        entity TEXT NOT NULL,
        at INTEGER NOT NULL,
        payload TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS events_by_collection ON events(collection,id)",
    """
    CREATE TABLE IF NOT EXISTS shipments(
        id TEXT PRIMARY KEY,
        collection TEXT NOT NULL,
        revision INTEGER NOT NULL,
        credential TEXT NOT NULL,
        max_volume_bytes INTEGER NOT NULL CHECK(max_volume_bytes > 0),
        total_bytes INTEGER NOT NULL CHECK(total_bytes >= 0),
        volume_count INTEGER NOT NULL CHECK(volume_count > 0),
        manifest_hash TEXT NOT NULL,
        status TEXT NOT NULL CHECK(status IN ('open','complete','revoked')),
        hold_reference TEXT NOT NULL UNIQUE,
        note TEXT,
        created_at INTEGER NOT NULL,
        completed_at INTEGER,
        revoked_at INTEGER,
        FOREIGN KEY(collection,revision) REFERENCES revisions(collection,number)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS shipment_volumes(
        shipment TEXT NOT NULL REFERENCES shipments(id),
        number INTEGER NOT NULL,
        ordinal INTEGER NOT NULL,
        start_offset INTEGER NOT NULL,
        end_offset INTEGER NOT NULL,
        size INTEGER NOT NULL CHECK(size >= 0),
        digest TEXT NOT NULL,
        PRIMARY KEY(shipment,number),
        UNIQUE(shipment,ordinal),
        CHECK(number = ordinal + 1),
        CHECK(end_offset > start_offset OR size = 0)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS shipment_dispatches(
        shipment TEXT NOT NULL,
        number INTEGER NOT NULL,
        first_at INTEGER NOT NULL,
        last_at INTEGER NOT NULL,
        send_count INTEGER NOT NULL DEFAULT 1,
        PRIMARY KEY(shipment,number),
        FOREIGN KEY(shipment,number) REFERENCES shipment_volumes(shipment,number)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS shipment_acks(
        shipment TEXT NOT NULL,
        number INTEGER NOT NULL,
        first_at INTEGER NOT NULL,
        last_at INTEGER NOT NULL,
        repeat_count INTEGER NOT NULL DEFAULT 0,
        reported_digest TEXT,
        PRIMARY KEY(shipment,number),
        FOREIGN KEY(shipment,number) REFERENCES shipment_volumes(shipment,number)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS shipment_reports(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        shipment TEXT NOT NULL,
        number INTEGER NOT NULL,
        at INTEGER NOT NULL,
        kind TEXT NOT NULL,
        reported_digest TEXT,
        detail TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS shipments_by_revision ON shipments(collection,revision,status)",
    "CREATE INDEX IF NOT EXISTS shipment_reports_by_set ON shipment_reports(shipment,id)",
)

# Tables added by schema revision 2 (split-volume handovers). Version 1 catalogs
# migrate by creating exactly these objects.
MIGRATION_V2 = tuple(statement for statement in SCHEMA if "shipment" in statement)


class Database:
    def __init__(self, path: Path, clock: Callable[[], int] | None = None):
        self.path = path
        self.clock = clock or (lambda: int(time.time()))
        path.parent.mkdir(parents=True, exist_ok=True)
        with self.transaction() as connection:
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0, 1, 2):
                raise RuntimeError(f"Unsupported vault catalog version {version}")
            if version == 0:
                for statement in SCHEMA:
                    connection.execute(statement)
                connection.execute("PRAGMA user_version=2")
            elif version == 1:
                for statement in MIGRATION_V2:
                    connection.execute(statement)
                connection.execute("PRAGMA user_version=2")

    def now(self) -> int:
        return self.clock()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=15, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=15000")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    @contextmanager
    def transaction(self, *, write: bool = True) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            yield connection
            connection.commit()
        except sqlite3.OperationalError as error:
            connection.rollback()
            if getattr(error, "sqlite_errorcode", 0) & 255 in (
                sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED
            ):
                raise unavailable("Archive is busy, retry the operation") from error
            raise
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()
