"""顶层兼容入口：``from strata.shipments import Shipments``。"""

from strata.delivery.shipments import *  # noqa: F401,F403
from strata.delivery.shipments import (  # noqa: F401
    ShipmentService,
    Shipments,
    plan_volumes,
    volume_spans,
)
