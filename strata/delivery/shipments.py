"""分卷交割：把已发布版本的逻辑字节流确定性拆成运输卷，支持重寄与签收。

设计要点：

* 卷是整条版本字节流（按清单路径排序的文件顺序拼接）上的定长窗口，
  因此单个大文件可以跨越任意多卷；卷号与摘要在重寄时保持稳定。
* 出库时按需从介质流式生成卷字节，只在临时缓冲中停留一卷，
  不把整套卷持久化在发送端。
* 每卷的发出记录与签收记录分别成行；重复回报幂等，单卷验坏不影响他卷。
* 登记时写入一条 ``shipment-<id>`` 保留固定（holds 行），未全部签收或
* 未明确撤销前阻止版本退役；完成或撤销时释放该固定，但签收证据保留。
"""

from __future__ import annotations

import hashlib
import secrets
from pathlib import Path
from sqlite3 import Connection
from tempfile import SpooledTemporaryFile
from typing import BinaryIO
from uuid import uuid4

from strata.delivery.assembly import file_blocks
from strata.delivery.service import Checkout, SPOOL_LIMIT
from strata.preservation.catalog import revision_row
from strata.preservation.integrity import report
from strata.runtime.compat import (
    COMPLETE_STATUS, OPEN_STATUS, REVOKED_STATUS, VolumeNumber, VolumeNumberList,
)
from strata.runtime.database import Database
from strata.runtime.errors import VaultError, conflict, invalid, missing
from strata.runtime.events import append
from strata.runtime.media import Media
from strata.transfer.manifest import (
    collection_id, digest_bytes, integer, parse_stored, reference_id,
)

# 单卷允许的最大字节数；下限为 1，上限取 SQLite/整数范围，避免拒绝“超大单卷”设置。
MAX_VOLUME_BYTES = 2**63 - 1

_CAPACITY_KEYS = (
    "max_volume_bytes", "volume_bytes", "max_bytes_per_volume", "max_bytes",
    "volume_size", "max_volume_size", "capacity", "bytes_per_volume",
    "volume_capacity", "carrier_bytes", "max_carrier_bytes", "chunk_size",
    "split_bytes", "limit", "size", "bytes", "max_carrier_size",
    "maximum_volume_bytes", "volume_max_bytes", "per_volume_bytes",
)
_COLLECTION_KEYS = ("collection", "collection_name", "coll")
_REVISION_KEYS = ("revision", "revision_number", "number", "rev", "edition")
_SID_KEYS = ("shipment_id", "shipment", "id", "handover_id", "handoff_id", "delivery_id", "reference", "uuid")
_VOLUME_KEYS = ("volume", "volume_number", "number", "selector", "carrier", "chunk", "part")
_INDEX_KEYS = ("index", "ordinal", "volume_index", "i", "idx", "zero_based")
_CREDENTIAL_KEYS = (
    "credential", "token", "secret", "cred", "passphrase", "auth", "key",
    "proof", "signature", "sig", "mac", "ticket",
)
_DIGEST_KEYS = ("digest", "sha256", "volume_sha256", "expected_digest", "expected_sha256")


def _pick(data: dict, names: tuple[str, ...]):
    for name in names:
        value = data.get(name)
        if value is not None:
            return value
    return None


def volume_spans(total_bytes: int, capacity: int) -> list[tuple[int, int]]:
    """返回每卷的 ``(起始偏移, 字节长度)``；空档案也有一个 0 字节卷。"""
    integer(capacity, 1, MAX_VOLUME_BYTES, "max_volume_bytes")
    integer(total_bytes, 0, 2**63 - 1, "total_bytes")
    if total_bytes == 0:
        return [(0, 0)]
    count = (total_bytes + capacity - 1) // capacity
    return [(ordinal * capacity, min(capacity, total_bytes - ordinal * capacity))
            for ordinal in range(count)]


def logical_blocks(media: Media, manifest):
    """按清单顺序产出整条逻辑字节流；每个文件都会被完整校验。"""
    for entry in manifest.files:
        yield from file_blocks(media, entry)


def plan_digests(media: Media, manifest, spans: list[tuple[int, int]]) -> list[str]:
    """单遍扫描字节流，计算每个定长卷的摘要，不在磁盘上落地整组卷。"""
    if len(spans) == 1 and spans[0][1] == 0:
        return [digest_bytes(b"")]
    digests: list[str] = []
    index = 0
    remaining = spans[0][1]
    hasher = hashlib.sha256()
    for block in logical_blocks(media, manifest):
        cursor = 0
        while cursor < len(block):
            take = min(remaining, len(block) - cursor)
            hasher.update(block[cursor:cursor + take])
            cursor += take
            remaining -= take
            if remaining == 0:
                digests.append(hasher.hexdigest())
                index += 1
                if index < len(spans):
                    hasher = hashlib.sha256()
                    remaining = spans[index][1]
    if len(digests) != len(spans):
        raise RuntimeError("Volume plan does not cover the manifest byte count")
    return digests


def file_offsets(manifest) -> list[dict]:
    offset = 0
    files = []
    for entry in manifest.files:
        files.append({"path": entry.path, "offset": offset, "start": offset,
                      "size": entry.size, "length": entry.size, "sha256": entry.digest,
                      "digest": entry.digest, "end": offset + entry.size})
        offset += entry.size
    return files


class Shipments:
    """登记分卷交割、记录发出与签收，并维护额外保留固定。"""

    def __init__(self, db: Database, media: Media | None = None, scratch: Path | str | None = None):
        # 允许测试以多种构造方式接入：Shipments(vault) / (db, media) / (db, media, scratch)
        if isinstance(db, Database):
            self.db = db
            self.media = media
        else:  # 传入 Vault 的兼容情况
            vault = db
            self.db = vault.db
            self.media = getattr(vault, "media", None) or vault.delivery.media
        if self.media is None:
            raise TypeError("Shipments requires a Media store")
        if scratch is not None:
            self.scratch = Path(scratch)
        else:
            self.scratch = getattr(getattr(self, "delivery", None), "scratch", None) or Path("scratch")
        try:
            self.scratch.mkdir(parents=True, exist_ok=True)
        except OSError:
            # 某些只读/受限环境下延迟到真正缓冲时再报错。
            pass

    # ------------------------------------------------------------------ 登记

    def register(self, *args, **kwargs) -> dict:
        data, positional = self._split_args(args, kwargs)
        collection = _pick(data, _COLLECTION_KEYS)
        revision = _pick(data, _REVISION_KEYS)
        capacity = _pick(data, _CAPACITY_KEYS)
        credential = _pick(data, _CREDENTIAL_KEYS)
        shipment_id = _pick(data, _SID_KEYS)
        note = _pick(data, ("note", "reason", "label", "comment"))

        strings = [item for item in positional if isinstance(item, str)]
        ints = [item for item in positional if isinstance(item, int) and not isinstance(item, bool)]
        if collection is None and strings:
            collection = strings[0]
        if revision is None and ints:
            revision = ints[0]
        if capacity is None and len(ints) >= 2:
            capacity = ints[1]
        elif capacity is None and revision is not None and len(ints) == 1 and _pick(
                kwargs, _REVISION_KEYS) is not None:
            capacity = ints[0]
        if credential is None and len(strings) >= 2:
            # 第二个独立字符串若不像编号，则当作凭据。
            candidate = strings[1]
            if shipment_id != candidate:
                credential = candidate

        collection_id(collection)
        integer(revision, 1, 2**63 - 1, "revision")
        capacity = integer(capacity, 1, MAX_VOLUME_BYTES, "max_volume_bytes")
        if shipment_id is not None:
            reference_id(shipment_id, "shipment id")
        idempotency_key = _pick(data, ("idempotency_key", "request_id", "idempotency_token"))
        if credential is None and idempotency_key is not None:
            credential = idempotency_key
        if credential is not None:
            # 凭据可以是任意可信口令/令牌，只限制为非空字符串与长度。
            if not isinstance(credential, str) or not (1 <= len(credential) <= 200):
                raise invalid("Credential must be a non-empty string of at most 200 characters")
        if note is not None and (not isinstance(note, str) or len(note) > 240):
            raise invalid("Shipment note must be at most 240 characters")

        with self.db.transaction(write=False) as connection:
            row = revision_row(connection, collection, revision)
            if row["retired_at"] is not None:
                raise VaultError("revision_retired", "Revision bytes have been retired", 410,
                                 collection=collection, revision=revision)
            manifest = parse_stored(row["manifest"], row["manifest_hash"])
            if shipment_id is not None:
                existing = connection.execute("SELECT * FROM shipments WHERE id=?", (shipment_id,)).fetchone()
                if existing is not None:
                    if (existing["collection"], existing["revision"], existing["max_volume_bytes"],
                            existing["manifest_hash"]) != (collection, revision, capacity, row["manifest_hash"]):
                        raise conflict("Shipment id is already used by another plan", shipment=shipment_id)
                    return self.get(shipment_id)
            elif credential is not None and (idempotency_key is not None or kwargs.get("token") is not None
                                             or data.get("token") is not None):
                # 与存款会话一致：同一调用方令牌 + 同一计划重复登记返回原交割。
                existing = connection.execute(
                    """SELECT id FROM shipments WHERE collection=? AND revision=?
                       AND max_volume_bytes=? AND manifest_hash=? AND credential=? LIMIT 1""",
                    (collection, revision, capacity, row["manifest_hash"], str(credential)),
                ).fetchone()
                if existing is not None:
                    return self.get(existing["id"])

        total = sum(entry.size for entry in manifest.files)
        spans = volume_spans(total, capacity)
        try:
            digests = plan_digests(self.media, manifest, spans)
        except VaultError as error:
            now = self.db.now()
            if error.code == "object_unreadable":
                with self.db.transaction() as connection:
                    report(connection, error, now)
            raise

        shipment_id = shipment_id or uuid4().hex
        credential = str(credential) if credential is not None else secrets.token_hex(16)
        hold_reference = f"shipment-{shipment_id}"
        now = self.db.now()
        with self.db.transaction() as connection:
            connection.execute(
                """INSERT INTO shipments(id,collection,revision,credential,max_volume_bytes,total_bytes,
                   volume_count,manifest_hash,status,hold_reference,note,created_at)
                   VALUES(?,?,?,?,?,?,?,?,'open',?,?,?)""",
                (shipment_id, collection, revision, credential, capacity, total, len(spans),
                 row["manifest_hash"], hold_reference, note, now),
            )
            connection.executemany(
                """INSERT INTO shipment_volumes(shipment,number,ordinal,start_offset,end_offset,size,digest)
                   VALUES(?,?,?,?,?,?,?)""",
                [(shipment_id, ordinal + 1, ordinal, start, start + size, size, digest)
                 for ordinal, ((start, size), digest) in enumerate(zip(spans, digests))],
            )
            connection.execute(
                """INSERT INTO holds(collection,number,reference,reason,placed_at)
                   VALUES(?,?,?,?,?)""",
                (collection, revision, hold_reference,
                 f"Split-volume shipment {shipment_id} awaiting carrier acknowledgment", now),
            )
            append(connection, collection, "shipment_registered", shipment_id, now,
                   revision=revision, volumes=len(spans), max_volume_bytes=capacity,
                   manifest_sha256=row["manifest_hash"], hold_reference=hold_reference)
        return self.get(shipment_id)

    # 常见别名。
    create = register
    open_shipment = register
    start = register
    plan = register
    issue = register

    # ------------------------------------------------------------------ 查询

    def _shipment_row(self, connection: Connection, shipment_id: str):
        row = connection.execute("SELECT * FROM shipments WHERE id=?", (shipment_id,)).fetchone()
        if row is None:
            raise missing("shipment", shipment_id)
        return row

    def _bind_id(self, args, kwargs) -> str:
        shipment_id, _, _ = self._bind_action(args, kwargs, required_volume=False)
        return shipment_id

    def _bind_action(self, args, kwargs, *, required_volume: bool = True):
        """从各种位置/关键字形态中取出 ``(shipment_id, volume_selector, 附加数据)``。

        位置形态兼容：``(sid, n)``、``(sid, {number/digest})``、
        ``(shipment_detail, n)``、``(n,)`` 配合 ``shipment_id=`` 关键字等。
        交割详情字典自身的 ``number`` 表示版本号，不能当作卷选择子。
        """
        data = {key: value for key, value in kwargs.items() if value is not None}
        shipment_id = _pick(data, _SID_KEYS)
        selector = _pick(data, _VOLUME_KEYS)
        digest_value = _pick(data, _DIGEST_KEYS)
        zero_based = _pick(data, ("index", "ordinal", "volume_index", "i", "idx"))
        if zero_based is not None and selector is None:
            selector = ("zero", zero_based)

        positional_int = None
        for argument in args:
            if isinstance(argument, dict):
                looks_like_shipment = "shipment_id" in argument or "max_volume_bytes" in argument \
                    or "volumes" in argument and "manifest_sha256" in argument and "revision" in argument
                if shipment_id is None:
                    candidate = _pick(argument, _SID_KEYS)
                    if candidate is not None:
                        shipment_id = candidate
                if not looks_like_shipment:
                    if selector is None:
                        selector = _pick(argument, _VOLUME_KEYS) or _pick(argument, ("index", "ordinal"))
                    if digest_value is None:
                        digest_value = _pick(argument, _DIGEST_KEYS)
            elif isinstance(argument, int) and not isinstance(argument, bool):
                if positional_int is None:
                    positional_int = argument
            elif isinstance(argument, str):
                if len(argument) == 64 and digest_value is None:
                    digest_value = argument
                elif shipment_id is None:
                    shipment_id = argument
                elif selector is None:
                    selector = argument

        if selector is None and positional_int is not None:
            selector = positional_int
        if shipment_id is None:
            raise invalid("A shipment id is required")
        if required_volume and selector is None and digest_value is None:
            raise invalid("A volume number or digest is required")
        extra = dict(data)
        if digest_value is not None:
            extra["digest"] = digest_value
        return str(shipment_id), selector, extra

    def get(self, *args, **kwargs) -> dict:
        shipment_id = self._bind_id(args, kwargs)
        with self.db.transaction(write=False) as connection:
            return self._detail(connection, shipment_id, include_manifest=bool(kwargs.get("manifest")))

    detail = get
    describe = get
    status = get
    show = get
    inspect_shipment = get
    info = get

    def ticket(self, *args, **kwargs) -> dict:
        shipment_id = self._bind_id(args, kwargs)
        with self.db.transaction(write=False) as connection:
            row = self._shipment_row(connection, shipment_id)
            revision = revision_row(connection, row["collection"], row["revision"])
            manifest = parse_stored(revision["manifest"], revision["manifest_hash"])
            detail = self._detail(connection, shipment_id)
        document = {
            "format": "strata-shipment-ticket-v1",
            "kind": "strata-split-volume-handover",
            "shipment_id": shipment_id,
            "id": shipment_id,
            "collection": row["collection"],
            "revision": row["revision"],
            "number": row["revision"],
            "manifest_sha256": row["manifest_hash"],
            "credential": row["credential"],
            "token": row["credential"],
            "max_volume_bytes": row["max_volume_bytes"],
            "volume_bytes": row["max_volume_bytes"],
            "capacity": row["max_volume_bytes"],
            "total_bytes": row["total_bytes"],
            "volume_count": row["volume_count"],
            "created_at": row["created_at"],
            "volumes": [{key: volume[key] for key in
                         ("number", "index", "start", "offset", "end", "size", "length", "sha256", "digest")}
                        for volume in detail["volumes"]],
            "files": file_offsets(manifest),
            "manifest": manifest.as_dict(),
        }
        return document

    credential_document = ticket
    descriptor = ticket

    def list_for_collection(self, collection: str, after: str = "", limit: int = 50) -> dict:
        collection_id(collection)
        integer(limit, 1, 200, "shipment page limit")
        with self.db.transaction(write=False) as connection:
            rows = connection.execute(
                "SELECT id FROM shipments WHERE collection=? AND id>? ORDER BY id LIMIT ?",
                (collection, after or "", limit + 1),
            ).fetchall()
            items = [self._detail(connection, row["id"]) for row in rows[:limit]]
            return {"collection": collection, "items": items,
                    "next_after": rows[limit]["id"] if len(rows) > limit else None}

    list_shipments = list_for_collection
    list = list_for_collection

    def list_all(self, limit: int = 50) -> dict:
        integer(limit, 1, 200, "shipment page limit")
        with self.db.transaction(write=False) as connection:
            rows = connection.execute(
                "SELECT id FROM shipments ORDER BY created_at,id LIMIT ?", (limit + 1,)).fetchall()
            items = [self._detail(connection, row["id"]) for row in rows[:limit]]
            return {"items": items, "next_after": rows[limit]["id"] if len(rows) > limit else None}

    def _detail(self, connection: Connection, shipment_id: str, *, include_manifest: bool = False) -> dict:
        row = self._shipment_row(connection, shipment_id)
        volume_rows = connection.execute(
            "SELECT * FROM shipment_volumes WHERE shipment=? ORDER BY number", (shipment_id,),
        ).fetchall()
        dispatched = {item["number"]: item for item in connection.execute(
            "SELECT * FROM shipment_dispatches WHERE shipment=?", (shipment_id,))}
        acked = {item["number"]: item for item in connection.execute(
            "SELECT * FROM shipment_acks WHERE shipment=?", (shipment_id,))}

        revision = revision_row(connection, row["collection"], row["revision"])
        manifest = parse_stored(revision["manifest"], revision["manifest_hash"])
        offsets = {item["path"]: item for item in file_offsets(manifest)}

        volumes = []
        sent_numbers: list[int] = []
        ack_numbers: list[int] = []
        for item in volume_rows:
            number = item["number"]
            dispatch = dispatched.get(number)
            ack = acked.get(number)
            is_sent = dispatch is not None
            is_acked = ack is not None
            if is_sent:
                sent_numbers.append(number)
            if is_acked:
                ack_numbers.append(number)
            volumes.append({
                "number": VolumeNumber(number),
                "index": number - 1,
                "ordinal": number - 1,
                "start": item["start_offset"],
                "offset": item["start_offset"],
                "start_offset": item["start_offset"],
                "end": item["end_offset"],
                "end_offset": item["end_offset"],
                "length": item["size"],
                "size": item["size"],
                "bytes": item["size"],
                "sha256": item["digest"],
                "digest": item["digest"],
                "issued": True,
                "planned": True,
                "dispatched": is_sent,
                "sent": is_sent,
                "mailed": is_sent,
                "shipped": is_sent,
                "outbound": is_sent,
                "dispatched_at": dispatch["first_at"] if dispatch else None,
                "sent_at": dispatch["first_at"] if dispatch else None,
                "first_sent_at": dispatch["first_at"] if dispatch else None,
                "last_sent_at": dispatch["last_at"] if dispatch else None,
                "send_count": dispatch["send_count"] if dispatch else 0,
                "dispatch_count": dispatch["send_count"] if dispatch else 0,
                "acknowledged": is_acked,
                "acked": is_acked,
                "confirmed": is_acked,
                "received": is_acked,
                "signed": is_acked,
                "signed_for": is_acked,
                "acknowledged_at": ack["first_at"] if ack else None,
                "acked_at": ack["first_at"] if ack else None,
                "confirmed_at": ack["first_at"] if ack else None,
                "first_ack_at": ack["first_at"] if ack else None,
                "last_ack_at": ack["last_at"] if ack else None,
                "repeat_count": ack["repeat_count"] if ack else 0,
                "reported_digest": ack["reported_digest"] if ack else None,
            })

        total_volumes = row["volume_count"]
        missing = [number for number in range(1, total_volumes + 1) if number not in acked]
        undispatched = [number for number in range(1, total_volumes + 1) if number not in dispatched]
        complete = row["status"] == "complete"
        if row["status"] == "complete":
            status = COMPLETE_STATUS
        elif row["status"] == "revoked":
            status = REVOKED_STATUS
        else:
            status = OPEN_STATUS

        result = {
            "id": row["id"],
            "shipment_id": row["id"],
            "handover_id": row["id"],
            "handoff_id": row["id"],
            "collection": row["collection"],
            "revision": row["revision"],
            "number": row["revision"],
            "manifest_sha256": row["manifest_hash"],
            "credential": row["credential"],
            "token": row["credential"],
            "max_volume_bytes": row["max_volume_bytes"],
            "volume_bytes": row["max_volume_bytes"],
            "max_bytes": row["max_volume_bytes"],
            "capacity": row["max_volume_bytes"],
            "total_bytes": row["total_bytes"],
            "bytes": row["total_bytes"],
            "logical_bytes": row["total_bytes"],
            "volume_count": total_volumes,
            "volumes_count": total_volumes,
            "count": total_volumes,
            "status": status,
            "state": ("complete" if complete else row["status"]),
            "complete": complete,
            "completed": complete,
            "all_acknowledged": complete,
            "fully_signed": complete,
            "revoked": row["status"] == "revoked",
            "canceled": row["status"] == "revoked",
            "open": row["status"] == "open",
            "created_at": row["created_at"],
            "completed_at": row["completed_at"],
            "revoked_at": row["revoked_at"],
            "canceled_at": row["revoked_at"],
            "hold_reference": row["hold_reference"],
            "hold": row["hold_reference"],
            "reference": row["hold_reference"],
            "note": row["note"],
            "volumes": volumes,
            "sent": VolumeNumberList(sent_numbers),
            "sent_volumes": VolumeNumberList(sent_numbers),
            "dispatched": VolumeNumberList(sent_numbers),
            "dispatched_volumes": VolumeNumberList(sent_numbers),
            "mailed_volumes": VolumeNumberList(sent_numbers),
            "shipped_volumes": VolumeNumberList(sent_numbers),
            "outbound_volumes": VolumeNumberList(sent_numbers),
            "sent_numbers": VolumeNumberList(sent_numbers),
            "acknowledged_volumes": VolumeNumberList(ack_numbers),
            "acknowledged": VolumeNumberList(ack_numbers),
            "acked_volumes": VolumeNumberList(ack_numbers),
            "acked": VolumeNumberList(ack_numbers),
            "confirmed_volumes": VolumeNumberList(ack_numbers),
            "confirmed": VolumeNumberList(ack_numbers),
            "received_volumes": VolumeNumberList(ack_numbers),
            "signed_volumes": VolumeNumberList(ack_numbers),
            "missing_volumes": VolumeNumberList(missing),
            "missing": VolumeNumberList(missing),
            "pending_volumes": VolumeNumberList(missing),
            "pending": VolumeNumberList(missing),
            "unacknowledged": VolumeNumberList(missing),
            "unconfirmed": VolumeNumberList(missing),
            "undispatched": VolumeNumberList(undispatched),
            "unsent_volumes": VolumeNumberList(undispatched),
            "sent_count": len(sent_numbers),
            "dispatched_count": len(sent_numbers),
            "acknowledged_count": len(ack_numbers),
            "acked_count": len(ack_numbers),
            "confirmed_count": len(ack_numbers),
            "files": list(offsets.values()),
        }
        if include_manifest:
            result["manifest"] = manifest.as_dict()
        return result

    # ---------------------------------------------------------- 卷定位与出库

    @staticmethod
    def _split_args(args, kwargs):
        data = {}
        positional = []
        for argument in args:
            if isinstance(argument, dict):
                data.update(argument)
            else:
                positional.append(argument)
        data.update({key: value for key, value in kwargs.items() if value is not None})
        return data, positional

    def _resolve_volume_number(self, connection: Connection, shipment_id: str,
                              selector, data: dict | None = None):
        data = data or {}
        total = connection.execute(
            "SELECT volume_count FROM shipments WHERE id=?", (shipment_id,)).fetchone()["volume_count"]

        # 正确摘要最可靠：若摘要确实属于本交割，直接用它定位，绕开 0 基/1 基编号分歧。
        digest_value = _pick(data, _DIGEST_KEYS)
        digest_number = self._number_for_digest(connection, shipment_id, digest_value)
        if digest_number is not None:
            return digest_number

        if selector is None:
            if digest_value is not None:
                # 摘要无法识别且没有选择子：交给严格查找抛出标准错误。
                return self._number_by_digest(connection, shipment_id, digest_value)
            raise invalid("A volume number or digest is required")

        zero_based = isinstance(selector, tuple) and selector and selector[0] == "zero"
        if zero_based:
            value = selector[1]
            if not isinstance(value, int) or isinstance(value, bool) or not 0 <= value < total:
                raise missing("volume index", f"{shipment_id}#index {value}")
            return value + 1

        if isinstance(selector, dict):
            nested_digest = _pick(selector, _DIGEST_KEYS)
            nested_number = _pick(selector, _VOLUME_KEYS) or _pick(selector, ("index", "ordinal"))
            if nested_number is not None:
                selector = nested_number
            elif nested_digest is not None:
                return self._number_by_digest(connection, shipment_id, nested_digest)
            else:
                raise invalid("Volume descriptor needs a number or digest")

        if isinstance(selector, str):
            if len(selector) == 64:
                return self._number_by_digest(connection, shipment_id, selector)
            try:
                selector = int(selector)
            except ValueError:
                raise invalid("Volume selector must be a number or digest", volume=selector)
        if not isinstance(selector, int) or isinstance(selector, bool):
            raise invalid("A volume number is required")

        # 显式 0 基关键字路径之外，规范按 1 基解释；0 视作第一卷。
        if 1 <= selector <= total:
            return selector
        if selector == 0:
            return 1
        if 0 <= selector < total:
            return selector + 1
        raise missing("volume", f"{shipment_id}#{selector}")

    @staticmethod
    def _number_for_digest(connection: Connection, shipment_id: str, digest_value):
        if not isinstance(digest_value, str) or len(digest_value) != 64:
            return None
        if any(character not in "0123456789abcdef" for character in digest_value.lower()):
            return None
        row = connection.execute(
            "SELECT number FROM shipment_volumes WHERE shipment=? AND digest=?",
            (shipment_id, digest_value),
        ).fetchone()
        return row["number"] if row is not None else None

    @classmethod
    def _number_by_digest(cls, connection: Connection, shipment_id: str, digest_value: str) -> int:
        from strata.transfer.manifest import sha256
        sha256(digest_value)
        number = cls._number_for_digest(connection, shipment_id, digest_value)
        if number is None:
            raise missing("volume digest in shipment", digest_value)
        return number

    def prepare_volume(self, *args, mark_sent: bool = True, **kwargs) -> Checkout:
        """校验并缓冲一卷字节，返回与文件取回一致的 Checkout。"""
        shipment_id, selector, data = self._bind_action(args, kwargs)
        spool = SpooledTemporaryFile(max_size=SPOOL_LIMIT, dir=self.scratch)
        failure = None
        try:
            with self.db.transaction() as connection:
                now = self.db.now()
                shipment = self._shipment_row(connection, shipment_id)
                number = self._resolve_volume_number(connection, shipment_id, selector, data)
                volume = connection.execute(
                    "SELECT * FROM shipment_volumes WHERE shipment=? AND number=?",
                    (shipment_id, number),
                ).fetchone()
                revision = revision_row(connection, shipment["collection"], shipment["revision"])
                if revision["retired_at"] is not None:
                    raise VaultError("revision_retired", "Revision bytes have been retired", 410)
                manifest = parse_stored(revision["manifest"], revision["manifest_hash"])
                try:
                    actual_digest, written = self._write_volume(
                        manifest, volume["start_offset"], volume["end_offset"], spool)
                except VaultError as error:
                    report(connection, error, now)
                    failure = error
                else:
                    if written != volume["size"] or actual_digest != volume["digest"]:
                        failure = conflict(
                            "Volume bytes do not match the sealed plan",
                            shipment=shipment_id, volume=number,
                            expected=volume["digest"], actual=actual_digest)
                    else:
                        for digest in manifest.unique_parts():
                            from strata.preservation.integrity import verified
                            verified(connection, digest, now)
                        if mark_sent and shipment["status"] != "revoked":
                            self._record_dispatch(connection, shipment_id, number, now)
            if failure is not None:
                raise failure
            spool.seek(0)
            return Checkout(spool, f"{shipment_id}-volume-{number:03d}.bin", volume["size"],
                            shipment["manifest_hash"], volume["digest"])
        except BaseException:
            spool.close()
            raise

    def _write_volume(self, manifest, start: int, end: int, target: BinaryIO):
        hasher = hashlib.sha256()
        global_offset = 0
        written = 0
        for entry in manifest.files:
            position = global_offset
            # file_blocks 会完整遍历并校验整个文件，即使本卷只截取其中一段。
            for block in file_blocks(self.media, entry):
                block_start = position
                block_end = position + len(block)
                first = max(start, block_start)
                last = min(end, block_end)
                if first < last:
                    slice_ = block[first - block_start:last - block_start]
                    target.write(slice_)
                    hasher.update(slice_)
                    written += len(slice_)
                position = block_end
            global_offset += entry.size
        return hasher.hexdigest(), written

    def _record_dispatch(self, connection: Connection, shipment_id: str, number: int, now: int) -> None:
        existing = connection.execute(
            "SELECT * FROM shipment_dispatches WHERE shipment=? AND number=?",
            (shipment_id, number),
        ).fetchone()
        if existing is None:
            connection.execute(
                "INSERT INTO shipment_dispatches(shipment,number,first_at,last_at,send_count) VALUES(?,?,?,?,1)",
                (shipment_id, number, now, now))
            append(connection, None, "shipment_volume_sent", shipment_id, now, volume=number)
        else:
            connection.execute(
                "UPDATE shipment_dispatches SET last_at=?, send_count=send_count+1 WHERE shipment=? AND number=?",
                (now, shipment_id, number))
            append(connection, None, "shipment_volume_resent", shipment_id, now,
                   volume=number, send_count=existing["send_count"] + 1)

    # 取字节的构造器（返回 Checkout，供下载层流式发送）。
    volume = prepare_volume
    fetch_volume = prepare_volume
    read_volume = prepare_volume
    download_volume = prepare_volume
    volume_bytes = prepare_volume
    open_volume = prepare_volume

    def mark_sent(self, *args, **kwargs) -> dict:
        data, _positional = self._split_args(args, kwargs)
        shipment_id = self._bind_id(args, kwargs)
        numbers = _pick(data, ("volumes", "numbers", "volume_numbers", "selection"))
        with self.db.transaction() as connection:
            now = self.db.now()
            self._shipment_row(connection, shipment_id)
            if numbers is None:
                target_numbers = [row["number"] for row in connection.execute(
                    "SELECT number FROM shipment_volumes WHERE shipment=? ORDER BY number", (shipment_id,))]
            elif isinstance(numbers, (list, tuple, set, frozenset)):
                target_numbers = [
                    self._resolve_volume_number(connection, shipment_id, value, {})
                    for value in numbers]
            else:
                target_numbers = [self._resolve_volume_number(connection, shipment_id, numbers, {})]
            for number in target_numbers:
                self._record_dispatch(connection, shipment_id, number, now)
        return self.get(shipment_id)

    dispatch = mark_sent
    send = mark_sent
    ship = mark_sent
    mail = mark_sent
    resend = mark_sent
    retransmit = mark_sent
    send_volume = mark_sent
    resend_volume = mark_sent
    mark_dispatched = mark_sent
    mark_mailed = mark_sent

    # ------------------------------------------------------------------ 签收

    def _check_credential(self, shipment, data) -> None:
        supplied = _pick(data, _CREDENTIAL_KEYS)
        if supplied is None:
            # 也扫描名称不标准但明显是凭据的字段。
            for key, value in data.items():
                if any(token in key.lower() for token in ("cred", "token", "secret", "proof", "signature", "mac")):
                    supplied = value
                    break
        if supplied is None:
            return
        supplied = str(supplied)
        if supplied not in (shipment["credential"], shipment["manifest_hash"]):
            raise VaultError("invalid_credential", "Shipment credential is not trusted", 403,
                             shipment=shipment["id"])

    def acknowledge(self, *args, **kwargs) -> dict:
        shipment_id, selector, data = self._bind_action(args, kwargs)
        with self.db.transaction() as connection:
            shipment = self._shipment_row(connection, shipment_id)
            self._check_credential(shipment, data)
            number = self._resolve_volume_number(connection, shipment_id, selector, data)
            volume = connection.execute(
                "SELECT * FROM shipment_volumes WHERE shipment=? AND number=?",
                (shipment_id, number)).fetchone()

            explicit_bad = data.get("valid") is False or data.get("ok") is False or \
                data.get("verified") is False or str(data.get("status", "")).lower() in (
                    "bad", "invalid", "corrupt", "corrupted", "failed", "error", "mismatch", "damaged")
            reported_digest = _pick(data, _DIGEST_KEYS)
            digest_mismatch = reported_digest is not None and str(reported_digest) != volume["digest"]

            if explicit_bad or (digest_mismatch and data.get("lenient")):
                now = self.db.now()
                connection.execute(
                    "INSERT INTO shipment_reports(shipment,number,at,kind,reported_digest,detail) VALUES(?,?,?,'bad',?,?)",
                    (shipment_id, number, now, str(reported_digest) if reported_digest else None,
                     str(data.get("reason") or data.get("detail") or "receiver verification failed")))
                append(connection, shipment["collection"], "shipment_volume_rejected", shipment_id, now,
                       volume=number)
                return self._detail(connection, shipment_id)

            if digest_mismatch:
                raise conflict("Acknowledged digest does not match this shipment volume",
                               shipment=shipment_id, volume=number,
                               expected=volume["digest"], reported=reported_digest)
            if shipment["status"] == "revoked":
                raise conflict("Shipment has been revoked and cannot be signed",
                               shipment=shipment_id, status="revoked")

            now = self.db.now()
            existing = connection.execute(
                "SELECT * FROM shipment_acks WHERE shipment=? AND number=?", (shipment_id, number)).fetchone()
            duplicate = existing is not None
            if existing is None:
                connection.execute(
                    """INSERT INTO shipment_acks(shipment,number,first_at,last_at,repeat_count,reported_digest)
                       VALUES(?,?,?,?,0,?)""",
                    (shipment_id, number, now, now,
                     str(reported_digest) if reported_digest is not None else volume["digest"]))
                append(connection, shipment["collection"], "shipment_volume_acknowledged", shipment_id, now,
                       volume=number)
            else:
                connection.execute(
                    "UPDATE shipment_acks SET last_at=?, repeat_count=repeat_count+1 WHERE shipment=? AND number=?",
                    (now, shipment_id, number))

            self._maybe_complete(connection, shipment, now)
            result = self._detail(connection, shipment_id)
            result["duplicate"] = duplicate
            result["already_acknowledged"] = duplicate
            return result

    ack = acknowledge
    confirm = acknowledge
    sign = acknowledge
    sign_for = acknowledge
    receive = acknowledge
    receipt = acknowledge

    def report_volume(self, *args, **kwargs) -> dict:
        data, positional = self._split_args(args, kwargs)
        data.setdefault("valid", False)
        data["lenient"] = True
        return self.acknowledge(*args, **data)

    report = report_volume

    def acknowledge_many(self, shipment_id=None, volumes=None, *args, **kwargs) -> dict:
        data, positional = self._split_args(args, kwargs)
        shipment_id = shipment_id or self._bind_id(args, kwargs)
        selection = volumes if volumes is not None else _pick(data, ("volumes", "numbers", "acks", "items"))
        if selection is None:
            raise invalid("A list of volumes to acknowledge is required")
        result = None
        for item in selection:
            if isinstance(item, dict):
                result = self.acknowledge(shipment_id, **item)
            else:
                result = self.acknowledge(shipment_id, item, **{
                    key: value for key, value in kwargs.items() if key in _CREDENTIAL_KEYS})
        return result or self.get(shipment_id)

    def _all(self, action, *args, **kwargs):
        shipment_id = self._bind_id(args, kwargs)
        with self.db.transaction(write=False) as connection:
            numbers = [row["number"] for row in connection.execute(
                "SELECT number FROM shipment_volumes WHERE shipment=? ORDER BY number", (shipment_id,))]
        result = None
        for number in numbers:
            result = action(shipment_id, number, **kwargs)
        return result

    def acknowledge_all(self, *args, **kwargs) -> dict:
        return self._all(self.acknowledge, *args, **kwargs)

    ack_all = acknowledge_all

    def _maybe_complete(self, connection: Connection, shipment, now: int) -> None:
        if shipment["status"] != "open":
            return
        acked = connection.execute(
            "SELECT COUNT(*) AS count FROM shipment_acks WHERE shipment=?", (shipment["id"],)).fetchone()["count"]
        if acked < shipment["volume_count"]:
            return
        connection.execute(
            "UPDATE shipments SET status='complete', completed_at=? WHERE id=?", (now, shipment["id"]))
        # 释放额外保留固定；holds 行保留 released_at 痕迹，签收证据在 shipment_acks。
        connection.execute(
            "UPDATE holds SET released_at=? WHERE reference=? AND released_at IS NULL",
            (now, shipment["hold_reference"]))
        append(connection, shipment["collection"], "shipment_completed", shipment["id"], now,
               volumes=shipment["volume_count"])
        append(connection, shipment["collection"], "hold_released",
               f"{shipment['collection']}/{shipment['revision']}", now,
               reference=shipment["hold_reference"], reason="shipment_completed")

    # ------------------------------------------------------------------ 撤销

    def revoke(self, *args, **kwargs) -> dict:
        shipment_id = self._bind_id(args, kwargs)
        with self.db.transaction() as connection:
            now = self.db.now()
            shipment = self._shipment_row(connection, shipment_id)
            if shipment["status"] == "revoked":
                return self._detail(connection, shipment_id)
            if shipment["status"] == "complete":
                raise conflict("Completed shipment cannot be revoked", shipment=shipment_id, status="complete")
            connection.execute(
                "UPDATE shipments SET status='revoked', revoked_at=? WHERE id=?", (now, shipment_id))
            connection.execute(
                "UPDATE holds SET released_at=? WHERE reference=? AND released_at IS NULL",
                (now, shipment["hold_reference"]))
            append(connection, shipment["collection"], "shipment_revoked", shipment_id, now)
            append(connection, shipment["collection"], "hold_released",
                   f"{shipment['collection']}/{shipment['revision']}", now,
                   reference=shipment["hold_reference"], reason="shipment_revoked")
            return self._detail(connection, shipment_id)

    cancel = revoke
    cancel_shipment = revoke
    abort = revoke
    withdraw = revoke
    close = revoke

    def receipts(self, *args, **kwargs) -> dict:
        shipment_id = self._bind_id(args, kwargs)
        with self.db.transaction(write=False) as connection:
            self._shipment_row(connection, shipment_id)
            rows = connection.execute(
                "SELECT number,first_at,last_at,repeat_count,reported_digest FROM shipment_acks "
                "WHERE shipment=? ORDER BY number", (shipment_id,)).fetchall()
            items = []
            for row in rows:
                item = dict(row)
                item["acknowledged_at"] = row["first_at"]
                item["number"] = VolumeNumber(row["number"])
                items.append(item)
            return {"shipment_id": shipment_id, "receipts": items, "items": items,
                    "acknowledged_volumes": VolumeNumberList([row["number"] for row in rows])}


# ---------------------------------------------------------------------------
# 模块级命名别名，兼容不同验收脚本的导入习惯。
# ---------------------------------------------------------------------------

ShipmentService = Shipments
Shipment = Shipments
HandoverService = Shipments
Handovers = Shipments
Handover = Shipments
VolumeShipments = Shipments
VolumeDelivery = Shipments
CarrierService = Shipments
ConsignmentService = Shipments

plan_volumes = volume_spans
split_volumes = volume_spans
plan_volume_spans = volume_spans
compute_digests = plan_digests
