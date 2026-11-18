from __future__ import annotations

from typing import Optional


class FenceTokenStore:
    """Compare-and-swap fencing token store (etcd/ZooKeeper style).

    Exactly one holder exists at any time. Acquiring the token for a new
    holder atomically revokes the previous holder, which is what "fencing
    the old primary" means: its writes stop being permitted by storage.
    """

    def __init__(self) -> None:
        self._holder: Optional[str] = None
        self._token: int = 0

    def acquire(self, node_id: str) -> Optional[int]:
        """Take ownership for node_id, evicting any previous holder."""
        self._token += 1
        self._holder = node_id
        return self._token

    def try_acquire_if_free(self, node_id: str) -> Optional[int]:
        """Acquire only if nobody holds the token (used to test races)."""
        if self._holder is not None:
            return None
        return self.acquire(node_id)

    def release(self, node_id: str, token: int) -> bool:
        if self._holder == node_id and self._token == token:
            self._holder = None
            return True
        return False

    def is_fenced(self, node_id: str) -> bool:
        """A node is fenced when it is not the current token holder."""
        return self._holder is not None and self._holder != node_id

    def write_permitted(self, node_id: str, token: int) -> bool:
        """Storage-side check: only the live holder may persist writes."""
        return self._holder == node_id and self._token == token

    @property
    def holder(self) -> Optional[str]:
        return self._holder
