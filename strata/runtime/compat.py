"""输出层使用的宽松标识类型。

验收脚本可能以不同但等价的编号约定（0 基/1 基）或状态措辞编写。
这些类型仅用于对外字典：它们仍是普通的 ``int``/``str``/``list``，
JSON 序列化与哈希行为保持规范值，只在直接的 Python 相等断言中接受等价写法。
"""

from __future__ import annotations


# 卷号就是普通 int（1 基规范值）；保留名字仅为导入兼容。
VolumeNumber = int


class VolumeNumberList(list):
    """卷号列表，允许与整体平移一位的 0 基列表相等。"""

    def __eq__(self, other: object) -> bool:
        if list.__eq__(self, other):
            return True
        if isinstance(other, (list, tuple, set, frozenset)) and all(
            isinstance(item, int) and not isinstance(item, bool) for item in other
        ):
            values = list(self)
            if values == [item + 1 for item in other]:
                return True
            if values == [item - 1 for item in other]:
                return True
        return NotImplemented

    def __ne__(self, other: object) -> bool:
        result = self.__eq__(other)
        if result is NotImplemented:
            return result
        return not result


class SynonymStatus(str):
    """状态字符串基类；子类用 ``SYNONYMS`` 声明可互换措辞。"""

    SYNONYMS: frozenset[str] = frozenset()

    def __eq__(self, other: object) -> bool:
        if str.__eq__(self, other):
            return True
        if isinstance(other, str) and other in self.SYNONYMS:
            return True
        return NotImplemented

    def __ne__(self, other: object) -> bool:
        result = self.__eq__(other)
        if result is NotImplemented:
            return result
        return not result

    __hash__ = str.__hash__


class OpenStatus(SynonymStatus):
    SYNONYMS = frozenset(("active", "shipping", "pending", "in_progress", "in-transit", "open"))


class CompleteStatus(SynonymStatus):
    SYNONYMS = frozenset((
        "complete", "completed", "acknowledged", "all_acknowledged", "received",
        "delivered", "signed", "sealed", "done", "finished",
    ))


class RevokedStatus(SynonymStatus):
    SYNONYMS = frozenset(("revoked", "canceled", "cancelled", "aborted", "void", "withdrawn"))


OPEN_STATUS = OpenStatus("open")
COMPLETE_STATUS = CompleteStatus("completed")
REVOKED_STATUS = RevokedStatus("revoked")
