"""“载体（carrier）”命名下的现场分卷核对入口。"""

from strata.field.shipment import *  # noqa: F401,F403
from strata.field.shipment import ShipmentVerifier, verify_volumes  # noqa: F401

verify_carriers = verify_volumes
CarrierReceiver = ShipmentVerifier
