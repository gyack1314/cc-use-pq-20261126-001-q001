"""以“volumes”命名的分卷计划/服务入口。"""

from strata.delivery.shipments import (  # noqa: F401
    Shipments as VolumeService,
    Shipments as Volumes,
    plan_digests,
    plan_volumes,
    volume_spans,
)
