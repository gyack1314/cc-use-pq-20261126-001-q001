"""分卷现场核对模块的常见命名入口。"""

from strata.field.shipment import *  # noqa: F401,F403
from strata.field.shipment import (  # noqa: F401
    ShipmentVerifier,
    verify_volumes,
)

verify_volume_set = verify_volumes
check_volume_set = verify_volumes
inspect_volume_set = verify_volumes
VolumeVerifier = ShipmentVerifier
