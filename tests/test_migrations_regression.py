from __future__ import annotations

from pathlib import Path


def test_session_messages_primary_key_is_scoped_to_session():
    migration_path = Path("migrations/20260304193000_session_messages.sql")
    sql = migration_path.read_text(encoding="utf-8").lower()

    assert "primary key (session_id, id)" in sql
