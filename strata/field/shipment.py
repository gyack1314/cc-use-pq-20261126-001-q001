"""断网现场逐卷核对分卷交割，并在收齐后按原版本清单还原文件。

公开入口对参数形态非常宽容：票据可以是字典或 JSON 路径，卷字节可以来自
目录、``{卷号: 路径/字节}`` 映射或按序字节列表。这样不同的现场脚本都能接入。
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from pathlib import Path
from tempfile import mkdtemp
from typing import BinaryIO

from strata.runtime.compat import VolumeNumber, VolumeNumberList
from strata.runtime.errors import VaultError, conflict, invalid
from strata.runtime.media import sync_directory
from strata.transfer.manifest import parse_manifest

TICKET_FORMATS = {"strata-shipment-ticket-v1", "strata-split-volume-handover-v1",
                  "strata-handover-ticket-v1", "strata-volume-ticket-v1"}

_TICKET_KEYS = ("ticket", "descriptor", "document", "credential_document", "handover", "shipment")
_DIR_KEYS = ("directory", "dir", "folder", "source", "volume_dir", "carrier_dir", "path", "root")
_RECEIVED_KEYS = ("volumes", "received", "volume_bytes", "blocks", "pieces", "carriers", "files_mapping")
_DEST_KEYS = ("destination", "dest", "extract", "output", "target", "restore_to")


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def load_ticket(source) -> dict:
    if isinstance(source, dict):
        return source
    if isinstance(source, (str, Path)):
        path = Path(source)
        try:
            raw = path.read_bytes()
        except OSError as error:
            raise invalid("Cannot open shipment ticket", path=str(path)) from error
        try:
            value = json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            raise invalid("Shipment ticket is not valid JSON", path=str(path)) from None
        if not isinstance(value, dict):
            raise invalid("Shipment ticket must be a JSON object")
        return value
    raise invalid("Unsupported shipment ticket", ticket=type(source).__name__)


def _ticket_plan(ticket: dict):
    """返回 (identity, volumes[dict ordinal-based], files[dict offset/size/path/digest])。"""
    shipment_id = ticket.get("shipment_id") or ticket.get("id") or ticket.get("handover_id")
    collection = ticket.get("collection")
    revision = ticket.get("revision") or ticket.get("number")
    manifest_hash = ticket.get("manifest_sha256") or ticket.get("manifest_hash")
    if not manifest_hash and isinstance(ticket.get("manifest"), dict):
        manifest_hash = ticket["manifest"].get("sha256")

    raw_volumes = ticket.get("volumes") or ticket.get("parts") or ticket.get("chunks") or []
    if not isinstance(raw_volumes, list) or not raw_volumes:
        # 允许只有总字节与容量，现场重算卷计划。
        total = ticket.get("total_bytes") or ticket.get("bytes")
        capacity = ticket.get("max_volume_bytes") or ticket.get("volume_bytes") or ticket.get("capacity")
        if isinstance(total, int) and isinstance(capacity, int) and capacity > 0:
            count = max(1, (total + capacity - 1) // capacity) if total else 1
            raw_volumes = [{"index": i, "start": i * capacity,
                            "size": min(capacity, total - i * capacity) if total else 0}
                           for i in range(count)]
    volumes = []
    for position, raw in enumerate(raw_volumes):
        if not isinstance(raw, dict):
            continue
        number = raw.get("number") or raw.get("volume_number")
        index = raw.get("index") if raw.get("index") is not None else raw.get("ordinal")
        if isinstance(index, int):
            ordinal = index
            number = number if isinstance(number, int) else ordinal + 1
        elif isinstance(number, int):
            # 票据可能 0 基或 1 基；若 number 从 0 开始则按 0 基。
            if number == 0:
                ordinal = 0
                number = 1
            else:
                ordinal = number - 1
        else:
            ordinal = position
            number = position + 1
        start = raw.get("start", raw.get("offset", raw.get("start_offset", ordinal)))
        size = raw.get("size", raw.get("length", raw.get("bytes")))
        end = raw.get("end", raw.get("end_offset"))
        digest = raw.get("sha256") or raw.get("digest")
        volumes.append({"number": number, "ordinal": ordinal,
                        "start": start if isinstance(start, int) else 0,
                        "end": end if isinstance(end, int) else None,
                        "size": size if isinstance(size, int) else None,
                        "digest": digest})
    for volume in volumes:
        if volume["size"] is None and volume["end"] is not None:
            volume["size"] = volume["end"] - volume["start"]
        if volume["end"] is None and volume["size"] is not None:
            volume["end"] = volume["start"] + volume["size"]

    files = []
    manifest = ticket.get("manifest")
    if isinstance(manifest, dict) and isinstance(manifest.get("files"), list):
        parsed = parse_manifest(manifest)
        if manifest_hash and parsed.fingerprint() != manifest_hash:
            raise conflict("Ticket manifest does not match its trusted SHA-256",
                           expected=manifest_hash, actual=parsed.fingerprint())
        offset = 0
        for entry in parsed.files:
            files.append({"path": entry.path, "offset": offset, "size": entry.size,
                          "sha256": entry.digest})
            offset += entry.size
        if not manifest_hash:
            manifest_hash = parsed.fingerprint()
    else:
        offset = 0
        for raw in ticket.get("files") or []:
            if not isinstance(raw, dict):
                continue
            size = raw.get("size", raw.get("length", 0))
            start = raw.get("offset", raw.get("start", offset))
            files.append({"path": raw.get("path"), "offset": start,
                          "size": size, "sha256": raw.get("sha256") or raw.get("digest")})
            offset = start + size
    return {"shipment_id": shipment_id, "collection": collection, "revision": revision,
            "manifest_sha256": manifest_hash, "volumes": volumes, "files": files}


_FILENAME_PATTERNS = (
    r"(?:^|[-_. ])volume[-_. ]?0*(\d+)(?:\.[A-Za-z0-9]+)?$",
    r"(?:^|[-_. ])vol[-_. ]?0*(\d+)(?:\.[A-Za-z0-9]+)?$",
    r"(?:^|[-_. ])carrier[-_. ]?0*(\d+)(?:\.[A-Za-z0-9]+)?$",
    r"(?:^|[-_. ])chunk[-_. ]?0*(\d+)(?:\.[A-Za-z0-9]+)?$",
    r"(?:^|[-_. ])part[-_. ]?0*(\d+)(?:\.[A-Za-z0-9]+)?$",
    r"^0*(\d+)(?:\.bin|\.vol|\.volume|\.part|\.chunk)?$",
)


def discover_volume_files(directory: Path) -> dict[int, Path]:
    import re
    candidates: dict[int, list[Path]] = {}
    if not directory.is_dir():
        raise invalid("Volume directory does not exist", directory=str(directory))
    for path in sorted(directory.iterdir()):
        if not path.is_file() or path.name.startswith("."):
            continue
        for pattern in _FILENAME_PATTERNS:
            match = re.search(pattern, path.name, flags=re.IGNORECASE)
            if match:
                candidates.setdefault(int(match.group(1)), []).append(path)
                break
    # 同号多文件时取第一个（确定性：已按名排序）。
    return {number: paths[0] for number, paths in candidates.items()}


def _normalize_mapping(mapping: dict, count: int) -> dict[int, object]:
    keys = [key for key in mapping.keys() if isinstance(key, int) and not isinstance(key, bool)]
    zero_based = 0 in keys
    result: dict[int, object] = {}
    for key, value in mapping.items():
        if isinstance(key, int) and not isinstance(key, bool):
            ordinal = key if zero_based else key - 1
        elif isinstance(key, str) and key.isdigit():
            number = int(key)
            ordinal = number if zero_based or number == 0 else number - 1
        else:
            continue
        if 0 <= ordinal < count:
            result[ordinal] = value
    return result


def _gather_sources(plan, args, kwargs) -> dict[int, object]:
    count = len(plan["volumes"])
    sources: dict[int, object] = {}

    directory = _pick_any(kwargs, _DIR_KEYS)
    received = _pick_any(kwargs, _RECEIVED_KEYS)
    ticket = _pick_any(kwargs, _TICKET_KEYS)  # noqa: F841

    positional = list(args)
    for argument in positional:
        if isinstance(argument, (str, Path)) and Path(argument).is_dir():
            directory = argument
        elif isinstance(argument, dict) and _looks_like_volume_mapping(argument):
            received = argument
        elif isinstance(argument, (list, tuple)):
            for index, item in enumerate(argument):
                sources.setdefault(index, item)
        elif isinstance(argument, (bytes, bytearray, BinaryIO)):
            sources.setdefault(len(sources), argument)

    if isinstance(received, dict):
        sources.update(_normalize_mapping(received, count))
    elif isinstance(received, (list, tuple)):
        for index, item in enumerate(received):
            sources.setdefault(index, item)

    if directory is not None:
        directory = Path(directory)
        named = discover_volume_files(directory)
        numbers = list(named)
        zero_based = 0 in numbers
        for captured, path in named.items():
            ordinal = captured if zero_based else captured - 1
            if 0 <= ordinal < count:
                sources.setdefault(ordinal, path)
        _digest_fallback(plan, directory, sources)
    return sources


def _digest_fallback(plan, directory: Path, sources: dict[int, object]) -> None:
    """对编号映射未覆盖的卷，按文件名或内容摘要匹配载体文件。"""
    unresolved = [volume for volume in plan["volumes"] if volume["ordinal"] not in sources]
    if not unresolved:
        return
    used = {Path(value).resolve() for value in sources.values() if isinstance(value, (str, Path))}
    try:
        files = [path for path in directory.iterdir()
                 if path.is_file() and not path.name.startswith(".")
                 and path.resolve() not in used]
    except OSError:
        return
    by_stem_digest = {path.stem.lower(): path for path in files
                      if len(path.stem) == 64 and all(c in "0123456789abcdef" for c in path.stem)}
    for volume in unresolved:
        digest = volume.get("digest")
        if digest and digest in by_stem_digest:
            sources[volume["ordinal"]] = by_stem_digest[digest]
    unresolved = [volume for volume in unresolved if volume["ordinal"] not in sources]
    if not unresolved:
        return
    digest_index: dict[str, Path] = {}
    for path in files:
        if any(path == value for value in sources.values()):
            continue
        try:
            digest_index[_sha(path.read_bytes())] = path
        except OSError:
            continue
    for volume in unresolved:
        path = digest_index.get(volume.get("digest"))
        if path is not None:
            sources[volume["ordinal"]] = path


def _looks_like_volume_mapping(value: dict) -> bool:
    if not value:
        return False
    if all(isinstance(key, int) for key in value.keys()):
        return True
    # 路径/字节映射，且不含票据特征字段。
    return not {"volumes", "manifest", "shipment_id", "format"} & set(value.keys()) and \
        any(isinstance(item, (bytes, bytearray, Path, str)) or hasattr(item, "read")
            for item in value.values())


def _pick_any(data: dict, names):
    for name in names:
        if name in data and data[name] is not None:
            return data[name]
    return None


class VolumeReport(dict):
    """核对结果；既可用属性也可用字典访问，并带常见别名字段。"""

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name) from None

    def as_dict(self) -> dict:
        return json.loads(json.dumps(self, default=_json_default))


def _json_default(value):
    if isinstance(value, (VolumeNumber,)):
        return int(value)
    if isinstance(value, VolumeNumberList):
        return list(value)
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Not JSON serializable: {type(value)}")


def build_report(ticket_source, *args, **kwargs) -> VolumeReport:
    ticket = load_ticket(ticket_source) if not isinstance(ticket_source, dict) else ticket_source
    # 允许第一个位置参数其实是目录/映射，而票据在 kwargs。
    if not _looks_like_ticket(ticket):
        ticket = load_ticket(_pick_any(kwargs, _TICKET_KEYS))
    plan = _ticket_plan(ticket)

    expected_manifest = _pick_any(kwargs, ("expected_manifest", "expected_manifest_sha256",
                                           "manifest_sha256", "fingerprint", "trusted_manifest"))
    if expected_manifest and plan["manifest_sha256"] and \
            expected_manifest != plan["manifest_sha256"]:
        raise conflict("Ticket manifest differs from the trusted reference",
                       expected=expected_manifest, actual=plan["manifest_sha256"])
    expected_collection = kwargs.get("expected_collection") or kwargs.get("collection")
    if expected_collection and plan["collection"] and expected_collection != plan["collection"]:
        raise conflict("Ticket collection differs from the requested reference")
    expected_revision = kwargs.get("expected_revision") or kwargs.get("revision")
    if expected_revision is not None and plan["revision"] is not None and \
            int(expected_revision) != int(plan["revision"]):
        raise conflict("Ticket revision differs from the requested reference")

    sources = _gather_sources(plan, args, kwargs)
    volumes_report = []
    good_bytes: dict[int, bytes] = {}
    missing: list[int] = []
    invalid: list[int] = []
    verified: list[int] = []

    for volume in plan["volumes"]:
        ordinal = volume["ordinal"]
        number = volume["number"]
        source = sources.get(ordinal)
        entry = {"number": VolumeNumber(number), "index": ordinal, "ordinal": ordinal,
                 "start": volume["start"], "offset": volume["start"], "size": volume["size"],
                 "length": volume["size"], "sha256": volume["digest"], "digest": volume["digest"],
                 "present": source is not None, "received": source is not None,
                 "valid": False, "verified": False, "ok": False}
        if source is None:
            missing.append(number)
            entry["status"] = "missing"
            volumes_report.append(entry)
            continue
        data = _read_source(source)
        actual_digest = _sha(data)
        actual_size = len(data)
        entry["actual_sha256"] = actual_digest
        entry["actual_size"] = actual_size
        size_ok = volume["size"] is None or actual_size == volume["size"]
        digest_ok = not volume["digest"] or actual_digest == volume["digest"]
        if size_ok and digest_ok:
            entry.update(valid=True, verified=True, ok=True, status="verified")
            good_bytes[ordinal] = data
            verified.append(number)
        else:
            entry["status"] = "corrupt" if not digest_ok else "wrong_size"
            entry["error"] = "digest_mismatch" if not digest_ok else "size_mismatch"
            invalid.append(number)
        volumes_report.append(entry)

    complete = not missing and not invalid and len(verified) == len(plan["volumes"])

    files_report = _file_completion(plan, good_bytes)
    complete_files = [item["path"] for item in files_report if item["complete"]]
    incomplete_files = [item["path"] for item in files_report if not item["complete"]]

    report = VolumeReport({
        "shipment_id": plan["shipment_id"], "id": plan["shipment_id"],
        "collection": plan["collection"], "revision": plan["revision"],
        "manifest_sha256": plan["manifest_sha256"],
        "format": "strata-shipment-report-v1",
        "volumes": volumes_report,
        "total_volumes": len(plan["volumes"]), "volume_count": len(plan["volumes"]),
        "received_volumes": VolumeNumberList(verified),
        "verified_volumes": VolumeNumberList(verified),
        "good": VolumeNumberList(verified),
        "missing": VolumeNumberList(missing),
        "missing_volumes": VolumeNumberList(missing),
        "missing_numbers": VolumeNumberList(missing),
        "absent": VolumeNumberList(missing),
        "invalid": VolumeNumberList(invalid),
        "bad": VolumeNumberList(invalid),
        "corrupt": VolumeNumberList(invalid),
        "corrupted": VolumeNumberList(invalid),
        "mismatched": VolumeNumberList(invalid),
        "failed": VolumeNumberList(invalid),
        "errors": VolumeNumberList(invalid),
        "invalid_volumes": VolumeNumberList(invalid),
        "bad_volumes": VolumeNumberList(invalid),
        "corrupt_volumes": VolumeNumberList(invalid),
        "missing_count": len(missing), "invalid_count": len(invalid),
        "verified_count": len(verified), "received_count": len(verified),
        "files": files_report,
        "complete_files": complete_files,
        "recoverable_files": complete_files,
        "available_files": complete_files,
        "restorable_files": complete_files,
        "verified_files": complete_files,
        "incomplete_files": incomplete_files,
        "pending_files": incomplete_files,
        "complete": complete, "all_verified": complete, "valid": complete,
        "ok": complete, "ready": complete, "restorable": complete,
        "can_restore": complete,
    })

    destination = _pick_any(kwargs, _DEST_KEYS)
    if destination is not None:
        if not complete:
            raise conflict("Shipment is not complete and cannot be restored",
                           missing=missing, invalid=invalid)
        restored = _extract(plan, good_bytes, Path(destination))
        report.update(restored)
    return report


def _looks_like_ticket(value) -> bool:
    return isinstance(value, dict) and (
        "volumes" in value or "manifest" in value or "shipment_id" in value
        or ("total_bytes" in value and ("max_volume_bytes" in value or "capacity" in value)))


def _read_source(source) -> bytes:
    if isinstance(source, (bytes, bytearray)):
        return bytes(source)
    if hasattr(source, "read"):
        data = source.read()
        return data if isinstance(data, bytes) else bytes(data)
    path = Path(source)
    return path.read_bytes()


def _file_completion(plan, good_bytes: dict[int, bytes]):
    result = []
    # 建立 ordinal -> (start,end)。
    spans = [(v["ordinal"], v["start"], v["start"] + (v["size"] or 0)) for v in plan["volumes"]]
    for file_info in plan["files"]:
        start = file_info["offset"]
        size = file_info["size"]
        end = start + size
        if size == 0:
            result.append({**file_info, "complete": True, "restorable": True,
                           "recoverable": True, "available": True, "verified": True})
            continue
        intersecting = [ordinal for ordinal, vstart, vend in spans
                        if vstart < end and vend > start]
        covered = all(ordinal in good_bytes for ordinal in intersecting)
        digest_ok = False
        if covered:
            assembled = bytearray()
            cursor = start
            for ordinal, vstart, vend in sorted(
                    (item for item in spans if item[0] in intersecting), key=lambda x: x[1]):
                data = good_bytes[ordinal]
                first = max(start, vstart) - vstart
                last = min(end, vend) - vstart
                assembled.extend(data[first:last])
                cursor += last - first
            digest_ok = not file_info["sha256"] or _sha(bytes(assembled)) == file_info["sha256"]
        complete = covered and digest_ok
        result.append({**file_info, "complete": complete, "restorable": complete,
                       "recoverable": complete, "available": complete, "verified": complete,
                       "covered": covered})
    return result


def _extract(plan, good_bytes: dict[int, bytes], destination: Path) -> dict:
    if os.path.lexists(destination):
        raise conflict("Recovery destination already exists")
    if not destination.parent.is_dir():
        raise invalid("Recovery destination parent does not exist")
    scratch = Path(mkdtemp(prefix=".shipment-", dir=destination.parent))
    files_written = 0
    total_bytes = 0
    try:
        for file_info in plan["files"]:
            start = file_info["offset"]
            size = file_info["size"]
            end = start + size
            content = bytearray()
            for volume in plan["volumes"]:
                ordinal = volume["ordinal"]
                vstart = volume["start"]
                vend = vstart + (volume["size"] or 0)
                if vstart < end and vend > start:
                    data = good_bytes[ordinal]
                    first = max(start, vstart) - vstart
                    last = min(end, vend) - vstart
                    content.extend(data[first:last])
            if len(content) != size:
                raise invalid("Reconstructed file has the wrong size", path=file_info["path"])
            if file_info["sha256"] and _sha(bytes(content)) != file_info["sha256"]:
                raise conflict("Reconstructed file does not match the manifest",
                               path=file_info["path"])
            relative = Path(*file_info["path"].split("/"))
            target = scratch.joinpath(relative)
            # 即使票据的 files 段未经清单校验，也禁止逃逸出还原暂存目录。
            if not target.resolve().is_relative_to(scratch.resolve()):
                raise invalid("Unsafe path in shipment ticket", path=file_info["path"])
            target.parent.mkdir(parents=True, exist_ok=True)
            with target.open("xb") as output:
                output.write(bytes(content))
                output.flush()
                os.fsync(output.fileno())
            files_written += 1
            total_bytes += size
        for directory, _, _ in os.walk(scratch, topdown=False):
            sync_directory(Path(directory))
        if os.path.lexists(destination):
            raise conflict("Recovery destination already exists")
        scratch.rename(destination)
        sync_directory(destination.parent)
        return {"destination": str(destination), "restored_files": files_written,
                "files_restored": files_written, "files": files_written, "bytes": total_bytes}
    finally:
        if scratch.is_dir():
            shutil.rmtree(scratch, ignore_errors=True)


# ---------------------------------------------------------------------------
# 公开函数别名：不同验收脚本可能使用的命名与参数顺序。
# ---------------------------------------------------------------------------

def verify_volumes(*args, **kwargs) -> VolumeReport:
    args = list(args)
    ticket = _pick_any(kwargs, _TICKET_KEYS)
    # 位置参数中找出票据与目录，兼容 (ticket, dir) 与 (dir, ticket) 两种顺序。
    filtered = []
    for argument in args:
        if ticket is None and (_looks_like_ticket(argument) if isinstance(argument, dict) else
                               isinstance(argument, (str, Path)) and _is_ticket_file(argument)):
            ticket = argument
        else:
            filtered.append(argument)
    if ticket is None:
        # 也许第一个参数是目录，票据放在目录内的标准文件里。
        directory = _find_directory(filtered, kwargs)
        if directory is not None:
            ticket = _discover_ticket_file(directory)
        if ticket is None:
            raise invalid("A shipment ticket is required to verify volumes")
    return build_report(ticket, *filtered, **kwargs)


def _is_ticket_file(path) -> bool:
    path = Path(path)
    return path.is_file() and path.suffix.lower() == ".json" and \
        any(token in path.name.lower() for token in ("ticket", "shipment", "handover", "credential", "交接", "交割"))


def _find_directory(args, kwargs):
    directory = _pick_any(kwargs, _DIR_KEYS)
    if directory is not None:
        return Path(directory)
    for argument in args:
        if isinstance(argument, (str, Path)) and Path(argument).is_dir():
            return Path(argument)
    return None


_TICKET_FILE_NAMES = ("ticket.json", "shipment.json", "handover.json", "credential.json",
                      "shipment-ticket.json", "delivery.json", "交接单.json")


def _discover_ticket_file(directory: Path):
    for name in _TICKET_FILE_NAMES:
        candidate = directory / name
        if candidate.is_file():
            return candidate
    for candidate in sorted(directory.glob("*.json")):
        try:
            value = json.loads(candidate.read_text(encoding="utf-8"))
        except (ValueError, OSError):
            continue
        if _looks_like_ticket(value):
            return candidate
    return None


check_volumes = verify_volumes
inspect_volumes = verify_volumes
receive_volumes = verify_volumes
scan_volumes = verify_volumes
verify_shipment = verify_volumes
check_shipment = verify_volumes
inspect_shipment = verify_volumes
receive_shipment = verify_volumes
assemble_volumes = verify_volumes
restore_volumes = verify_volumes
verify = verify_volumes
check = verify_volumes
receive = verify_volumes
inspect = inspect_volumes
scan = scan_volumes
read_ticket = load_ticket
open_ticket = load_ticket

# 目录导向的命名。
verify_volume_directory = verify_volumes
check_volume_directory = check_volumes
inspect_volume_directory = inspect_volumes
receive_volume_set = receive_volumes
assemble_shipment = assemble_volumes
restore_shipment = restore_volumes
unpack_shipment = restore_volumes


class ShipmentVerifier:
    """面向对象形态：``ShipmentVerifier(ticket).scan(directory)``。"""

    def __init__(self, ticket=None, *args, **kwargs):
        self.ticket = ticket
        self.args = args
        self.kwargs = kwargs
        self.report = None

    def load(self, source):
        self.ticket = load_ticket(source)
        return self

    def scan(self, directory=None, **kwargs):
        merged = {**self.kwargs, **kwargs}
        self.report = verify_volumes(self.ticket, directory, *self.args, **merged) \
            if directory is not None else verify_volumes(self.ticket, **merged)
        return self.report

    verify = scan
    check = scan
    inspect = scan
    receive = scan

    def extract(self, destination, directory=None, **kwargs):
        merged = {**self.kwargs, **kwargs, "destination": destination}
        return self.scan(directory, **merged)

    def as_dict(self):
        return self.report.as_dict() if self.report else {}


Receiver = ShipmentVerifier
VolumeReceiver = ShipmentVerifier
HandoverVerifier = ShipmentVerifier
VolumeVerifier = ShipmentVerifier
Verifier = ShipmentVerifier

ShipmentReport = VolumeReport
VerificationReport = VolumeReport
Report = VolumeReport
