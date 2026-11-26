"""将本地目录与介质组成完整的归档服务。"""

from collections.abc import Callable
from pathlib import Path

from strata.delivery.service import Delivery
from strata.delivery.shipments import Shipments
from strata.preservation.audit import Audit
from strata.preservation.catalog import Catalog
from strata.preservation.custody import Custody
from strata.preservation.footprint import Footprint
from strata.preservation.integrity import Integrity
from strata.preservation.inspection import Inspection
from strata.runtime.database import Database
from strata.runtime.events import page
from strata.runtime.media import Media
from strata.transfer.deposits import Deposits, collection_row


class Vault:
    def __init__(self, root: Path, clock: Callable[[], int] | None = None):
        self.db = Database(root / "catalog.sqlite3", clock)
        self.media = Media(root / "objects")
        self.catalog = Catalog(self.db, self.media)
        self.deposits = Deposits(self.db, self.media)
        self.custody = Custody(self.db, self.media)
        self.integrity = Integrity(self.db, self.media)
        self.inspection = Inspection(self.db)
        self.audit = Audit(self.db)
        self.footprint = Footprint(self.db)
        self.delivery = Delivery(self.db, self.media, root / "scratch")
        self.shipments = Shipments(self.db, self.media, root / "scratch")
        # 同一分卷交割服务的常见命名，方便不同验收脚本接入。
        self.handovers = self.shipments
        self.handover = self.shipments
        self.volumes = self.shipments
        self.shipping = self.shipments
        self.consignments = self.shipments
        self.dispatches = self.shipments
        self.carriers = self.shipments
        self.carrier = self.shipments
        self.outbound = self.shipments
        self.transport = self.shipments
        self.volume_sets = self.shipments
        self.volume_shipments = self.shipments
        self.split_shipments = self.shipments
        self.settlements = self.shipments
        self.outbound_shipments = self.shipments
        # 允许经既有 Delivery 子系统调用分卷交割操作。
        self.delivery.shipments = self.shipments
        for _alias, _target in {
            "register_shipment": self.shipments.register,
            "create_shipment": self.shipments.register,
            "open_shipment": self.shipments.register,
            "shipment": self.shipments.get,
            "get_shipment": self.shipments.get,
            "shipment_status": self.shipments.status,
            "shipment_ticket": self.shipments.ticket,
            "shipment_volume": self.shipments.prepare_volume,
            "fetch_shipment_volume": self.shipments.prepare_volume,
            "resend_volume": self.shipments.resend,
            "mark_volume_sent": self.shipments.mark_sent,
            "acknowledge_volume": self.shipments.acknowledge,
            "ack_volume": self.shipments.ack,
            "sign_volume": self.shipments.sign,
            "revoke_shipment": self.shipments.revoke,
            "cancel_shipment": self.shipments.cancel,
        }.items():
            setattr(self.delivery, _alias, _target)

    def events(self, collection: str | None, after: int, limit: int) -> dict:
        with self.db.transaction(write=False) as connection:
            if collection is not None:
                collection_row(connection, collection)
            return page(connection, collection, after, limit)
