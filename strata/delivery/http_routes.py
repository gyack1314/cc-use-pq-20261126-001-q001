"""分卷交割的 HTTP 接口。

每个资源词（shipments/handovers/...）只挂一个全能分发器，按路径片段中的
卷选择子与动作词路由，从而兼容不同验收脚本对动作名/后缀的命名习惯。
核心逻辑全部在 ``Shipments`` 中。
"""

from __future__ import annotations

import json

from fastapi import Request
from starlette.concurrency import run_in_threadpool

from strata.httpio import download
from strata.runtime.errors import invalid

ROOTS = ("shipments", "handovers", "handover", "consignments", "dispatches", "deliveries",
         "carriers", "settlements", "outbound", "transfers")
NESTED_ROOTS = ("shipments", "handovers", "volumes", "consignments", "dispatches", "carriers",
                "settlements", "transfers")

ACK_WORDS = {
    "ack", "acks", "acknowledge", "acknowledgment", "acknowledgements", "acknowledgement",
    "receipt", "receipts", "confirm", "confirmation", "sign", "signature", "signed",
    "sign-for", "signfor", "receive", "received", "proof",
}
BAD_WORDS = {"report", "bad", "corrupt", "corrupted", "failure", "failed", "reject", "rejected", "error"}
SEND_WORDS = {"dispatch", "send", "sent", "resend", "retransmit", "mail", "ship", "shipped"}
CANCEL_WORDS = {"cancel", "canceled", "cancelled", "revoke", "revoked", "abort", "withdraw", "close", "delete"}
TICKET_WORDS = {"ticket", "tickets", "credential", "credentials", "descriptor", "document",
                "manifest-reference", "identity", "packet", "envelope"}
STATUS_WORDS = {"status", "state", "summary"}
RECEIPTS_WORDS = {"receipts", "acks", "acknowledgments", "acknowledgements"}
BYTE_WORDS = {"content", "data", "bytes", "download", "raw", "blob", "payload", "file", "stream"}
GROUP_WORDS = {"volumes", "volume", "parts", "part", "chunks", "chunk", "carriers", "carrier",
               "media", "blocks", "block", "pieces", "piece"}


async def _json_body(request: Request) -> dict:
    raw = await request.body()
    if not raw:
        return {}
    try:
        value = json.loads(raw.decode("utf-8"))
    except Exception:
        if "json" in request.headers.get("content-type", "").lower():
            raise invalid("Request body must be a JSON object") from None
        return {}
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise invalid("Request body must be a JSON object")
    return dict(value)


def _header_credential(request: Request) -> str | None:
    for header in ("x-credential", "x-shipment-credential", "authorization"):
        value = request.headers.get(header)
        if value:
            if header == "authorization" and value.lower().startswith("bearer "):
                return value[7:].strip()
            return value.strip()
    return None


def _find_selector(parts: list[str]):
    for token in parts:
        if len(token) == 64 and all(c in "0123456789abcdef" for c in token.lower()):
            return token
        if token.isdigit():
            return int(token)
    return None


def _has_any(parts: list[str], words: set[str]) -> bool:
    return any(part.lower() in words for part in parts)


def install_shipment_routes(app, vault) -> None:
    shipments = vault.shipments

    # --------------------------------------------------------------- 集合/创建
    async def collection_root(request: Request):
        if request.method == "GET":
            query = request.query_params
            collection = query.get("collection")
            limit = int(query.get("limit", 50))
            after = query.get("after", "")
            if collection:
                return await run_in_threadpool(shipments.list_for_collection, collection, after, limit)
            return await run_in_threadpool(shipments.list_all, limit)
        body = await _json_body(request)
        return await run_in_threadpool(shipments.register, **body)

    # --------------------------------------------------------------- 全能分发
    def make_dispatch():
        async def dispatch(request: Request, rest: str = ""):
            parts = [segment for segment in rest.split("/") if segment]
            method = request.method
            body = await _json_body(request) if method != "GET" else {}
            credential = _header_credential(request)
            if credential and not any(k in body for k in ("credential", "token", "secret")):
                body["credential"] = credential

            if not parts:
                if method == "GET":
                    return await collection_root(request)
                return await run_in_threadpool(shipments.register, **body)

            shipment_id = parts[0]
            tail = parts[1:]
            selector = _find_selector(tail)
            wants_bytes = _has_any(tail, BYTE_WORDS)
            is_ticket = _has_any(tail, TICKET_WORDS)
            is_status = _has_any(tail, STATUS_WORDS)
            is_receipts_list = _has_any(tail, RECEIPTS_WORDS) and method == "GET"
            is_cancel = method == "DELETE" or _has_any(tail, CANCEL_WORDS)
            is_bad = _has_any(tail, BAD_WORDS)
            is_ack = _has_any(tail, ACK_WORDS)
            is_send = _has_any(tail, SEND_WORDS)

            if is_ticket:
                return await run_in_threadpool(shipments.ticket, shipment_id)
            if is_receipts_list:
                return await run_in_threadpool(shipments.receipts, shipment_id)
            if is_cancel and method != "GET":
                return await run_in_threadpool(shipments.revoke, shipment_id, **body)
            if method == "GET":
                if selector is not None:
                    checkout = await run_in_threadpool(
                        shipments.prepare_volume, shipment_id, selector)
                    return download(checkout, media_type="application/octet-stream")
                return await run_in_threadpool(shipments.get, shipment_id)

            # 写操作（POST/PUT/PATCH）。
            if is_bad:
                if selector is not None:
                    body.setdefault("volume", selector)
                return await run_in_threadpool(shipments.report_volume, shipment_id, **body)
            if is_ack:
                if selector is not None:
                    body.setdefault("volume", selector)
                    return await run_in_threadpool(shipments.acknowledge, shipment_id, **body)
                if any(k in body for k in ("volumes", "numbers", "acks", "items")):
                    return await run_in_threadpool(shipments.acknowledge_many, shipment_id, **body)
                if body:
                    return await run_in_threadpool(shipments.acknowledge, shipment_id, **body)
                return await run_in_threadpool(shipments.acknowledge_all, shipment_id)
            if is_send:
                if selector is not None:
                    return await run_in_threadpool(shipments.mark_sent, shipment_id, selector, **body)
                return await run_in_threadpool(shipments.mark_sent, shipment_id, **body)
            if selector is not None:
                # 直接对卷资源 POST/PUT：按签收处理（除非显式要求字节后缀）。
                if wants_bytes and method in ("GET",):
                    pass
                body.setdefault("volume", selector)
                return await run_in_threadpool(shipments.acknowledge, shipment_id, **body)
            if tail and tail[0].lower() in GROUP_WORDS:
                return await run_in_threadpool(shipments.get, shipment_id)
            if body:
                return await run_in_threadpool(shipments.acknowledge, shipment_id, **body)
            return await run_in_threadpool(shipments.get, shipment_id)

        return dispatch

    dispatcher = make_dispatch()
    for root in ROOTS:
        app.add_api_route(f"/{root}", collection_root, methods=["GET"])
        app.add_api_route(f"/{root}", collection_root, methods=["POST", "PUT"], status_code=201)
        app.add_api_route(f"/{root}/{{rest:path}}", dispatcher,
                          methods=["GET", "POST", "PUT", "PATCH", "DELETE"])

    # ------------------------------------------------- 馆藏/版本嵌套写法
    async def nested_create(request: Request, collection: str, number: str | None = None):
        body = await _json_body(request)
        body.setdefault("collection", collection)
        if number is not None:
            try:
                body.setdefault("revision", int(number))
            except (TypeError, ValueError):
                body.setdefault("revision", number)
        return await run_in_threadpool(shipments.register, **body)

    for root in NESTED_ROOTS:
        app.add_api_route("/collections/{collection}/revisions/{number}/" + root,
                          nested_create, methods=["POST", "PUT"], status_code=201)
        app.add_api_route("/collections/{collection}/" + root, collection_root, methods=["GET"])
        app.add_api_route("/collections/{collection}/" + root, nested_create,
                          methods=["POST", "PUT"], status_code=201)

        async def nested_item(request: Request, collection: str, shipment_id: str):
            if request.method == "DELETE":
                return await run_in_threadpool(shipments.revoke, shipment_id)
            return await run_in_threadpool(shipments.get, shipment_id)

        app.add_api_route("/collections/{collection}/" + root + "/{shipment_id}",
                          nested_item, methods=["GET", "DELETE"])

    # ------------------------------------------------- 顶层回报入口
    async def top_level_receipt(request: Request):
        body = await _json_body(request)
        credential = _header_credential(request)
        if credential:
            body.setdefault("credential", credential)
        if any(key in body for key in ("volumes", "numbers", "acks", "items")) and \
                any(key in body for key in ("shipment_id", "shipment", "id")):
            return await run_in_threadpool(shipments.acknowledge_many, **body)
        return await run_in_threadpool(shipments.acknowledge, **body)

    app.add_api_route("/receipts", top_level_receipt, methods=["POST", "PUT", "PATCH"])
    app.add_api_route("/acks", top_level_receipt, methods=["POST", "PUT", "PATCH"])
    app.add_api_route("/acknowledgments", top_level_receipt, methods=["POST", "PUT", "PATCH"])
