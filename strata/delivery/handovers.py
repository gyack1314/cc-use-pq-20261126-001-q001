"""“handovers”命名下的分卷交割服务入口。"""

from strata.delivery.shipments import (  # noqa: F401
    Handover,
    HandoverService,
    Handovers,
    Shipment,
    ShipmentService,
    Shipments,
    plan_volumes,
    split_volumes,
    volume_spans,
)
