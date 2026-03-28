import asyncio
from contextlib import contextmanager
from pathlib import Path

from src.database import crud
from src.database.models import Base, Proxy
from src.database.session import DatabaseSessionManager
from src.web.routes import registration as registration_routes


class _FakeSettings:
    def __init__(self, dynamic_enabled=True, dynamic_api_url="http://dynamic.api", proxy_url=None):
        self.proxy_dynamic_enabled = dynamic_enabled
        self.proxy_dynamic_api_url = dynamic_api_url
        self.proxy_url = proxy_url
        self.proxy_dynamic_api_key = None
        self.proxy_dynamic_api_key_header = "X-API-Key"
        self.proxy_dynamic_result_field = ""


def _fill_enabled_proxies(session, count):
    for i in range(count):
        crud.create_proxy(
            session,
            name=f"pool-{i}",
            type="http",
            host=f"pool-{i}.example",
            port=8000 + i,
            enabled=True,
        )


def _get_proxy_by_name(session, name):
    return session.query(Proxy).filter(Proxy.name == name).first()


def test_get_proxy_for_registration_prefers_dynamic_when_pool_below_10(monkeypatch, tmp_path):
    manager = _build_manager(tmp_path)

    with manager.session_scope() as session:
        _fill_enabled_proxies(session, 9)

        monkeypatch.setattr(registration_routes, "get_settings", lambda: _FakeSettings(dynamic_enabled=True))
        monkeypatch.setattr(registration_routes, "_fetch_dynamic_proxy_url", lambda: "http://dyn.example:9000")

        proxy_url, proxy_id = registration_routes.get_proxy_for_registration(session)

        assert proxy_url == "http://dyn.example:9000"
        assert proxy_id is None


def test_get_proxy_for_registration_prefers_pool_when_pool_reaches_10(monkeypatch, tmp_path):
    manager = _build_manager(tmp_path)

    with manager.session_scope() as session:
        _fill_enabled_proxies(session, 10)

        monkeypatch.setattr(registration_routes, "get_settings", lambda: _FakeSettings(dynamic_enabled=True))
        monkeypatch.setattr(registration_routes, "_fetch_dynamic_proxy_url", lambda: "http://dyn.example:9000")

        proxy_url, proxy_id = registration_routes.get_proxy_for_registration(session)

        assert proxy_id is not None
        assert proxy_url is not None
        assert "dyn.example" not in proxy_url


def test_backfill_dynamic_proxy_on_success_creates_proxy_record(tmp_path):
    manager = _build_manager(tmp_path)

    with manager.session_scope() as session:
        registration_routes.backfill_proxy_if_dynamic_success(
            session,
            proxy_id=None,
            proxy_url="http://newdyn.example:9443",
        )

        created = _get_proxy_by_name(session, "动态回灌-newdyn.example:9443")
        assert created is not None
        assert created.host == "newdyn.example"
        assert created.port == 9443
        assert created.enabled is True


def test_backfill_dynamic_proxy_on_success_skips_when_already_exists(tmp_path):
    manager = _build_manager(tmp_path)

    with manager.session_scope() as session:
        crud.create_proxy(
            session,
            name="existing",
            type="http",
            host="dup.example",
            port=9001,
            enabled=True,
        )

        registration_routes.backfill_proxy_if_dynamic_success(
            session,
            proxy_id=None,
            proxy_url="http://dup.example:9001",
        )

        matched = session.query(Proxy).filter(Proxy.host == "dup.example", Proxy.port == 9001).all()
        assert len(matched) == 1


def test_backfill_dynamic_proxy_on_success_skips_pool_proxy(tmp_path):
    manager = _build_manager(tmp_path)

    with manager.session_scope() as session:
        existing = crud.create_proxy(
            session,
            name="pool-item",
            type="http",
            host="pool.example",
            port=9050,
            enabled=True,
        )

        registration_routes.backfill_proxy_if_dynamic_success(
            session,
            proxy_id=existing.id,
            proxy_url=existing.proxy_url,
        )

        matched = session.query(Proxy).filter(Proxy.host == "pool.example", Proxy.port == 9050).all()
        assert len(matched) == 1


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


def test_cleanup_failed_proxy_deletes_all_records_on_3rd_failure(tmp_path):
    manager = _build_manager(tmp_path)

    with manager.session_scope() as session:
        _create_proxy(
            session,
            name="p1",
            host="delete.example",
            port=9100,
            username="u1",
            password="p1",
            fail_count=2,
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


def test_cleanup_failed_proxy_transient_error_uses_higher_threshold(tmp_path):
    manager = _build_manager(tmp_path)

    with manager.session_scope() as session:
        _create_proxy(
            session,
            name="p1",
            host="transient.example",
            port=9400,
            fail_count=2,
            reason="old",
        )

        registration_routes.cleanup_failed_proxy(
            session,
            proxy_id=None,
            proxy_url="http://transient.example:9400",
            task_uuid="task-transient",
            error_message="connect timeout",
        )

    with manager.session_scope() as session:
        left = (
            session.query(Proxy)
            .filter(Proxy.host == "transient.example", Proxy.port == 9400)
            .count()
        )
        assert left == 1
        proxy = session.query(Proxy).filter(Proxy.host == "transient.example", Proxy.port == 9400).one()
        assert proxy.fail_count == 3


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


def test_batch_parallel_reports_feedback_only_once_per_task(monkeypatch, tmp_path):
    manager = _build_manager(tmp_path)
    feedback_calls = []

    class _Resp:
        status_code = 200

    def fake_post(url, json=None, timeout=None):
        feedback_calls.append({"url": url, "json": json, "timeout": timeout})
        return _Resp()

    async def fake_run_registration_task(*args, **kwargs):
        task_uuid = args[0]
        proxy_url = "http://once.example:9000"
        with manager.session_scope() as session:
            registration_routes.crud.update_registration_task(
                session,
                task_uuid,
                status="completed",
                proxy=proxy_url,
            )
        registration_routes.requests.post(
            "http://127.0.0.1:8001/feedback",
            json={"proxy": proxy_url, "success": True},
            timeout=3,
        )


    @contextmanager
    def fake_get_db():
        session = manager.SessionLocal()
        try:
            yield session
        finally:
            session.close()

    monkeypatch.setattr(registration_routes, "get_db", fake_get_db)
    monkeypatch.setattr(registration_routes.requests, "post", fake_post)
    monkeypatch.setattr(registration_routes, "run_registration_task", fake_run_registration_task)

    task_uuid = "batch-feedback-task"
    with manager.session_scope() as session:
        registration_routes.crud.create_registration_task(session, task_uuid=task_uuid, proxy=None)

    asyncio.run(
        registration_routes.run_batch_parallel(
            batch_id="batch-feedback",
            task_uuids=[task_uuid],
            email_service_type="tempmail",
            proxy=None,
            email_service_config=None,
            email_service_id=None,
            concurrency=1,
        )
    )

    assert len(feedback_calls) == 1
    assert feedback_calls[0]["url"] == "http://127.0.0.1:8001/feedback"
    assert feedback_calls[0]["json"]["proxy"] == "http://once.example:9000"
    assert feedback_calls[0]["json"]["success"] is True


def test_batch_pipeline_reports_feedback_only_once_per_task(monkeypatch, tmp_path):
    manager = _build_manager(tmp_path)
    feedback_calls = []

    class _Resp:
        status_code = 200

    def fake_post(url, json=None, timeout=None):
        feedback_calls.append({"url": url, "json": json, "timeout": timeout})
        return _Resp()

    async def fake_run_registration_task(*args, **kwargs):
        task_uuid = args[0]
        proxy_url = "http://once-pipeline.example:9100"
        with manager.session_scope() as session:
            registration_routes.crud.update_registration_task(
                session,
                task_uuid,
                status="completed",
                proxy=proxy_url,
            )
        registration_routes.requests.post(
            "http://127.0.0.1:8001/feedback",
            json={"proxy": proxy_url, "success": True},
            timeout=3,
        )


    @contextmanager
    def fake_get_db():
        session = manager.SessionLocal()
        try:
            yield session
        finally:
            session.close()

    monkeypatch.setattr(registration_routes, "get_db", fake_get_db)
    monkeypatch.setattr(registration_routes.requests, "post", fake_post)
    monkeypatch.setattr(registration_routes, "run_registration_task", fake_run_registration_task)

    task_uuid = "batch-feedback-pipeline-task"
    with manager.session_scope() as session:
        registration_routes.crud.create_registration_task(session, task_uuid=task_uuid, proxy=None)

    asyncio.run(
        registration_routes.run_batch_pipeline(
            batch_id="batch-feedback-pipeline",
            task_uuids=[task_uuid],
            email_service_type="tempmail",
            proxy=None,
            email_service_config=None,
            email_service_id=None,
            interval_min=0,
            interval_max=0,
            concurrency=1,
        )
    )

    assert len(feedback_calls) == 1
    assert feedback_calls[0]["url"] == "http://127.0.0.1:8001/feedback"
    assert feedback_calls[0]["json"]["proxy"] == "http://once-pipeline.example:9100"
    assert feedback_calls[0]["json"]["success"] is True


def test_registration_route_models_defined_once():
    path = Path(registration_routes.__file__)
    content = path.read_text(encoding="utf-8")

    assert content.count("class RegistrationTaskCreate(BaseModel):") == 1
    assert content.count("class BatchRegistrationRequest(BaseModel):") == 1
    assert content.count("class RegistrationTaskResponse(BaseModel):") == 1
    assert content.count("class BatchRegistrationResponse(BaseModel):") == 1
    assert content.count("class TaskListResponse(BaseModel):") == 1
    assert content.count("class OutlookAccountForRegistration(BaseModel):") == 1
    assert content.count("class OutlookAccountsListResponse(BaseModel):") == 1
    assert content.count("class OutlookBatchRegistrationRequest(BaseModel):") == 1
    assert content.count("class OutlookBatchRegistrationResponse(BaseModel):") == 1
