from __future__ import annotations

import secrets

from pyvenue.domain import OrderId


def new_order_id() -> OrderId:
    # TODO: Replace in the future, but for quick testing good enough now.
    return OrderId(secrets.token_hex(8))