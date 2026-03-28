from src.database.crud import (
    bulk_update_proxies_by_host_port,
    create_proxy,
    delete_disabled_proxies,
    delete_proxies_by_host_port,
    get_proxies,
    get_proxies_by_host_port,
)
from src.database.models import Base
from src.database.session import DatabaseSessionManager


def test_delete_disabled_proxies_only_removes_disabled_items(tmp_path):
    db_path = tmp_path / "proxy_crud_helpers_disabled.db"
    manager = DatabaseSessionManager(f"sqlite:///{db_path}")
    Base.metadata.create_all(bind=manager.engine)

    session = manager.SessionLocal()
    try:
        create_proxy(session, name="enabled-1", type="http", host="a.example", port=8001, enabled=True)
        create_proxy(session, name="disabled-1", type="http", host="b.example", port=8002, enabled=False)
        create_proxy(session, name="disabled-2", type="http", host="c.example", port=8003, enabled=False)

        deleted_count = delete_disabled_proxies(session)
        assert deleted_count == 2

        remaining = get_proxies(session, limit=20)
        remaining_names = sorted([item.name for item in remaining])
        assert remaining_names == ["enabled-1"]
    finally:
        session.close()


def test_proxy_helpers_query_update_delete_by_normalized_host_port(tmp_path):
    db_path = tmp_path / "proxy_crud_helpers.db"
    manager = DatabaseSessionManager(f"sqlite:///{db_path}")
    Base.metadata.create_all(bind=manager.engine)

    session = manager.SessionLocal()
    try:
        p1 = create_proxy(session, name="p1", type="http", host=" Example.com ", port=8080, enabled=True)
        p2 = create_proxy(session, name="p2", type="http", host="example.COM", port=8080, enabled=True)
        create_proxy(session, name="p3", type="http", host="example.com", port=9090, enabled=True)
        create_proxy(session, name="p4", type="http", host="other.com", port=8080, enabled=True)

        matched = get_proxies_by_host_port(session, " example.com ", 8080)
        matched_ids = {item.id for item in matched}
        assert matched_ids == {p1.id, p2.id}

        updated_count = bulk_update_proxies_by_host_port(
            session,
            "EXAMPLE.COM",
            8080,
            enabled=False,
            fail_count=7,
        )
        assert updated_count == 2

        all_proxies = get_proxies(session, limit=20)
        proxy_map = {item.name: item for item in all_proxies}
        assert proxy_map["p1"].enabled is False
        assert proxy_map["p2"].enabled is False
        assert proxy_map["p1"].fail_count == 7
        assert proxy_map["p2"].fail_count == 7
        assert proxy_map["p3"].enabled is True
        assert proxy_map["p4"].enabled is True

        deleted_count = delete_proxies_by_host_port(session, "  example.com", 8080)
        assert deleted_count == 2

        remaining = get_proxies(session, limit=20)
        remaining_names = sorted([item.name for item in remaining])
        assert remaining_names == ["p3", "p4"]
    finally:
        session.close()
