"""“交割/交接”命名下的离线核对入口。"""

from strata.field.shipment import *  # noqa: F401,F403
from strata.field.shipment import (  # noqa: F401
    HandoverVerifier,
    ShipmentVerifier,
    verify_shipment,
    verify_volumes,
)

verify_handover = verify_shipment
check_handover = verify_shipment
inspect_handover = verify_shipment
receive_handover = verify_shipment
