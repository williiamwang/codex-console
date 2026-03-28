import asyncio
from contextlib import contextmanager

from src.database.models import Base, Proxy
from src.database.session import DatabaseSessionManager
from src.web.routes import settings as settings_routes


def _build_manager(tmp_path):
    db_path = tmp_path / "settings_cleanup_disabled.db"
    manager = DatabaseSessionManager(f"sqlite:///{db_path}")
    Base.metadata.create_all(bind=manager.engine)
    return manager


def _create_proxy(session, *, name, enabled):
    proxy = Proxy(
        name=name,
        type="http",
        host=f"{name}.example",
        port=8000,
        enabled=enabled,
    )
    session.add(proxy)
    session.flush()
    return proxy


def test_cleanup_disabled_proxies_deletes_only_disabled(monkeypatch, tmp_path):
    manager = _build_manager(tmp_path)

    with manager.session_scope() as session:
        _create_proxy(session, name="enabled", enabled=True)
        _create_proxy(session, name="disabled-1", enabled=False)
        _create_proxy(session, name="disabled-2", enabled=False)

    @contextmanager
    def fake_get_db():
        session = manager.SessionLocal()
        try:
            yield session
        finally:
            session.close()

    monkeypatch.setattr(settings_routes, "get_db", fake_get_db)

    result = asyncio.run(settings_routes.cleanup_disabled_proxy_items())

    assert result["success"] is True
    assert result["deleted_count"] == 2

    with manager.session_scope() as session:
        remaining = session.query(Proxy).all()
        assert len(remaining) == 1
        assert remaining[0].name == "enabled"
