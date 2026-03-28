from src.database.models import Base, Proxy
from src.database.session import DatabaseSessionManager
from src.web.routes import registration as registration_routes


def _build_manager(tmp_path):
    db_path = tmp_path / "registration_proxy_cleanup.db"
    manager = DatabaseSessionManager(f"sqlite:///{db_path}")
    Base.metadata.create_all(bind=manager.engine)
    return manager


def _create_proxy(session, *, name, host, port, username=None, password=None, fail_count=0, reason=None):
    proxy = Proxy(
        name=name,
        type="http",
        host=host,
        port=port,
        username=username,
        password=password,
        enabled=True,
        fail_count=fail_count,
        last_failure_reason=reason,
    )
    session.add(proxy)
    session.flush()
    return proxy


def test_cleanup_failed_proxy_syncs_fail_count_by_host_port(tmp_path):
    manager = _build_manager(tmp_path)

    with manager.session_scope() as session:
        p1 = _create_proxy(
            session,
            name="p1",
            host="same.example",
            port=9000,
            username="u1",
            password="p1",
            fail_count=1,
            reason="old-1",
        )
        _create_proxy(
            session,
            name="p2",
            host="same.example",
            port=9000,
            username="u2",
            password="p2",
            fail_count=3,
            reason="old-2",
        )
        _create_proxy(
            session,
            name="other",
            host="other.example",
            port=9001,
            fail_count=2,
            reason="other-old",
        )

        registration_routes.cleanup_failed_proxy(
            session,
            proxy_id=p1.id,
            task_uuid="task-sync",
            error_message="network timeout",
        )

    with manager.session_scope() as session:
        same_group = (
            session.query(Proxy)
            .filter(Proxy.host == "same.example", Proxy.port == 9000)
            .order_by(Proxy.id.asc())
            .all()
        )
        assert len(same_group) == 2
        assert {p.fail_count for p in same_group} == {4}
        assert {p.last_failure_reason for p in same_group} == {"network timeout"}

        other = session.query(Proxy).filter(Proxy.host == "other.example", Proxy.port == 9001).one()
        assert other.fail_count == 2
        assert other.last_failure_reason == "other-old"


def test_cleanup_failed_proxy_deletes_all_records_on_5th_failure(tmp_path):
    manager = _build_manager(tmp_path)

    with manager.session_scope() as session:
        _create_proxy(
            session,
            name="p1",
            host="delete.example",
            port=9100,
            username="u1",
            password="p1",
            fail_count=4,
            reason="old-1",
        )
        p2 = _create_proxy(
            session,
            name="p2",
            host="delete.example",
            port=9100,
            username="u2",
            password="p2",
            fail_count=2,
            reason="old-2",
        )
        _create_proxy(
            session,
            name="keep",
            host="keep.example",
            port=9101,
            fail_count=1,
            reason="keep-old",
        )

        registration_routes.cleanup_failed_proxy(
            session,
            proxy_id=p2.id,
            task_uuid="task-delete",
            error_message="verification error",
        )

    with manager.session_scope() as session:
        deleted_group_count = (
            session.query(Proxy)
            .filter(Proxy.host == "delete.example", Proxy.port == 9100)
            .count()
        )
        assert deleted_group_count == 0

        keep = session.query(Proxy).filter(Proxy.host == "keep.example", Proxy.port == 9101).one()
        assert keep.fail_count == 1
        assert keep.last_failure_reason == "keep-old"


def test_update_proxy_usage_resets_all_records_by_host_port_from_url(tmp_path):
    manager = _build_manager(tmp_path)

    with manager.session_scope() as session:
        _create_proxy(
            session,
            name="p1",
            host="reset.example",
            port=9200,
            username="u1",
            password="p1",
            fail_count=4,
            reason="timeout",
        )
        _create_proxy(
            session,
            name="p2",
            host="reset.example",
            port=9200,
            username="u2",
            password="p2",
            fail_count=2,
            reason="proxy fail",
        )
        _create_proxy(
            session,
            name="other",
            host="other.example",
            port=9201,
            fail_count=3,
            reason="other reason",
        )

        registration_routes.update_proxy_usage(
            session,
            proxy_id=None,
            proxy_url="http://reset.example:9200",
        )

    with manager.session_scope() as session:
        reset_group = session.query(Proxy).filter(Proxy.host == "reset.example", Proxy.port == 9200).all()
        assert len(reset_group) == 2
        assert {p.fail_count for p in reset_group} == {0}
        assert {p.last_failure_reason for p in reset_group} == {None}

        other = session.query(Proxy).filter(Proxy.host == "other.example", Proxy.port == 9201).one()
        assert other.fail_count == 3
        assert other.last_failure_reason == "other reason"


def test_cleanup_failed_proxy_no_matching_record_is_noop(tmp_path):
    manager = _build_manager(tmp_path)

    with manager.session_scope() as session:
        _create_proxy(
            session,
            name="existing",
            host="exists.example",
            port=9300,
            fail_count=1,
            reason="exists",
        )

        registration_routes.cleanup_failed_proxy(
            session,
            proxy_id=None,
            proxy_url="http://missing.example:9999",
            task_uuid="task-noop",
            error_message="connect timeout",
        )

    with manager.session_scope() as session:
        items = session.query(Proxy).all()
        assert len(items) == 1
        assert items[0].host == "exists.example"
        assert items[0].port == 9300
        assert items[0].fail_count == 1
        assert items[0].last_failure_reason == "exists"
