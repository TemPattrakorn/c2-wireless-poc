"""
Unit tests for Web UI static assets delivery, importmap, and ES module serving.
Tests handler logic and static route resolution directly using aiohttp.test_utils
without requiring external network or live socket ports.
"""

from __future__ import annotations

import pytest
from aiohttp import web
from aiohttp.test_utils import make_mocked_request

from node import C2NodeDaemon




async def test_master_serves_html_with_importmap(master_daemon: C2NodeDaemon) -> None:
    """Verify Master serves index.html configured with ES module importmap."""
    app = master_daemon.http_server.app
    req = make_mocked_request("GET", "/", app=app)
    resp = await app._handle(req)

    assert resp.status == 200
    assert isinstance(resp, web.FileResponse)
    html_content = resp._path.read_text(encoding="utf-8")
    assert "C2 Wireless Command & Control Dashboard" in html_content
    assert '<script type="importmap">' in html_content
    assert '"alpinejs": "/static/vendor/alpine.esm.js"' in html_content
    assert '<script type="module" src="/static/app.js"></script>' in html_content


async def test_master_serves_app_es_module(master_daemon: C2NodeDaemon) -> None:
    """Verify Master serves app.js with ES module syntax."""
    app = master_daemon.http_server.app
    req = make_mocked_request("GET", "/static/app.js", app=app)
    resp = await app._handle(req)

    assert resp.status == 200
    assert isinstance(resp, web.FileResponse)
    js_content = resp._path.read_text(encoding="utf-8")
    assert "import Alpine from 'alpinejs'" in js_content
    assert "export function c2Dashboard()" in js_content
    assert "Alpine.data('c2Dashboard', c2Dashboard)" in js_content
    assert "Alpine.start()" in js_content


async def test_master_serves_vendored_alpine_esm(master_daemon: C2NodeDaemon) -> None:
    """Verify Master serves alpine.esm.js from /static/vendor/."""
    app = master_daemon.http_server.app
    req = make_mocked_request("GET", "/static/vendor/alpine.esm.js", app=app)
    resp = await app._handle(req)

    assert resp.status == 200
    assert isinstance(resp, web.FileResponse)
    esm_content = resp._path.read_text(encoding="utf-8")
    assert "export" in esm_content
    assert "Alpine" in esm_content


async def test_master_serves_styles(master_daemon: C2NodeDaemon) -> None:
    """Verify Master serves styles.css."""
    app = master_daemon.http_server.app
    req = make_mocked_request("GET", "/static/styles.css", app=app)
    resp = await app._handle(req)

    assert resp.status == 200
    assert isinstance(resp, web.FileResponse)
    css_content = resp._path.read_text(encoding="utf-8")
    assert "--primary:" in css_content


async def test_worker_does_not_serve_web_ui(worker_daemon: C2NodeDaemon) -> None:
    """Verify Worker node does not mount or serve dashboard routes."""
    app = worker_daemon.http_server.app
    req = make_mocked_request("GET", "/", app=app)
    with pytest.raises(web.HTTPNotFound):
        await app._handle(req)
