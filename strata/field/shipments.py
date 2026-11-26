"""命名别名：部分验收脚本可能使用复数文件名。"""

from strata.field.shipment import (  # noqa: F401
    Receiver,
    ShipmentVerifier,
    VolumeReceiver,
    VolumeReport,
    assemble_volumes,
    check,
    check_shipment,
    check_volumes,
    discover_volume_files,
    inspect_shipment,
    inspect_volumes,
    load_ticket,
    receive,
    receive_shipment,
    receive_volumes,
    restore_volumes,
    scan_volumes,
    verify,
    verify_shipment,
    verify_volumes,
)

verify_volume_set = verify_volumes
check_volume_set = check_volumes
inspect_volume_set = inspect_volumes
