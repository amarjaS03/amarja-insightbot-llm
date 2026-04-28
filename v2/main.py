"""
FastAPI v2 application entry point.
"""
import asyncio
import logging
import os
import sys
import warnings
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# Support both:
# - `uvicorn v2.main:app` from repository root
# - `python v2/main.py` direct execution
v2_dir = Path(__file__).resolve().parent
repo_root = v2_dir.parent
for path in (repo_root, v2_dir):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from v2.core.middlewares.authorization_middleware import AuthorizationMiddleware, EXCLUDED_PATHS
from v2.core.socketio_manager import get_socketio_manager
from execution_layer.utils.llm_core import set_provider
from v2.utils.env import init_env
from v2.common.logger import add_log
from v2.routes import api_router

load_dotenv()


def _configure_runtime_noise() -> None:
    """Reduce noisy warning/log output in local/dev runs."""
    warnings.filterwarnings("ignore")
    warnings.filterwarnings("ignore", category=UserWarning, module="pydantic")
    os.environ["GRPC_VERBOSITY"] = "ERROR"
    os.environ["GLOG_minloglevel"] = "2"
    logging.getLogger("absl").setLevel(logging.ERROR)
    logging.getLogger("uvicorn.error").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)


async def _job_log_emit_loop(app: FastAPI):
    """Emit pending job logs to job_logs_{job_id} rooms for real-time streaming (reconnect-safe)."""
    from v2.common.logger import get_and_clear_pending_job_log_emits

    while True:
        await asyncio.sleep(0.5)
        try:
            mgr = getattr(app.state, "socketio_manager", None)
            if not mgr or not getattr(mgr, "sio", None):
                continue
            pending = get_and_clear_pending_job_log_emits()
            for jid, entry in pending:
                await mgr.sio.emit("job_log", entry, room=f"job_logs_{jid}")
        except asyncio.CancelledError:
            break
        except Exception:
            pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handle startup/shutdown hooks."""
    cleanup_task = None
    log_emit_task = None
    try:
        from v2.modules.simple_qna.services.background_tasks import start_chat_cleanup_task

        cleanup_task = asyncio.create_task(
            start_chat_cleanup_task(inactivity_hours=6, cleanup_interval=3600)
        )
        log_emit_task = asyncio.create_task(_job_log_emit_loop(app))
        add_log("FastAPI startup complete. Chat cleanup and job log emit tasks started.")
    except Exception as exc:
        add_log(f"Error during startup: {str(exc)}")
        raise

    yield # gives result one by one

    try:
        add_log("Shutting down FastAPI application...")
        if log_emit_task:
            log_emit_task.cancel()
            try:
                await log_emit_task
            except asyncio.CancelledError:
                pass
        if cleanup_task:
            cleanup_task.cancel()
            try:
                await cleanup_task
            except asyncio.CancelledError:
                pass
        add_log("FastAPI shutdown complete")
    except Exception as exc:
        add_log(f"Error during shutdown: {str(exc)}")


def _build_openapi(app: FastAPI) -> dict[str, Any]:
    from fastapi.openapi.utils import get_openapi

    schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )
    schema.setdefault("components", {})
    schema["components"]["securitySchemes"] = {
        "BearerAuth": {
            "type": "http",
            "scheme": "bearer",
            "bearerFormat": "JWT",
            "description": "Enter your Firebase Bearer token",
        }
    }

    base_url = (os.getenv("BASE_URL") or "").rstrip("/")
    if base_url:
        schema["servers"] = [{"url": base_url, "description": "Production server"}]

    if "paths" in schema:
        for path, methods in schema["paths"].items():
            if path in EXCLUDED_PATHS:
                continue
            for method in methods.values():
                if isinstance(method, dict) and "security" not in method:
                    method["security"] = [{"BearerAuth": []}]
    return schema


def _configure_openapi(app: FastAPI) -> None:
    def custom_openapi():
        if app.openapi_schema:
            return app.openapi_schema
        app.openapi_schema = _build_openapi(app)
        return app.openapi_schema

    app.openapi = custom_openapi


def _configure_middlewares(app: FastAPI, constants: dict[str, Any]) -> None:
    raw_origins = list(constants.get("cors_allowed_origins") or [])
    # Merge CORS_EXTRA_ORIGINS from env (comma-separated) for tunnel URLs, e.g. https://xxx.trycloudflare.com
    extra = (os.getenv("CORS_EXTRA_ORIGINS") or "").strip()
    if extra:
        raw_origins.extend(o.strip() for o in extra.split(",") if o.strip())
    cors_allowed_origins: list[str] = []
    for origin in raw_origins:
        if not isinstance(origin, str):
            continue
        normalized = origin.strip()
        if normalized != "*":
            normalized = normalized.rstrip("/")
        if normalized and normalized not in cors_allowed_origins:
            cors_allowed_origins.append(normalized)

    # Allow all origins when "*" is present (e.g. for Swagger/tunnel access); credentials must be False
    allow_all = "*" in cors_allowed_origins
    if allow_all:
        cors_allowed_origins = ["*"]

    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_allowed_origins,
        allow_credentials=not allow_all,
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        allow_headers=[
            "Content-Type",
            "Authorization",
            "X-Requested-With",
            "X-API-Key",
            "API-Key",
            "Bearer",
        ],
    )
    app.add_middleware(AuthorizationMiddleware)
    add_log("Authorization middleware enabled")


def _mount_socketio(app: FastAPI): # Wraps the FastAPI app in a SocketIO ASGI app, registers events
    """
    Keep Socket.IO mounted as ASGI so one server handles:
    - regular FastAPI HTTP endpoints
    - Socket.IO websocket/polling transport at /socket.io
    """
    socketio_manager = get_socketio_manager()

    from v2.modules.job_framework.manager.job.job_manager import JobManager
    from v2.modules.session_framework.manager.session.session_manager import SessionManager

    socketio_manager.register_events(JobManager(), SessionManager())
    app.state.socketio_manager = socketio_manager
    add_log("SocketIO server initialized for execution-layer progress streaming")
    return socketio_manager.mount_to_app(app)


def create_app():
    """Create and configure the v2 application."""
    _configure_runtime_noise()
    constants = init_env()
    set_provider()

    fastapi_app = FastAPI(
        title="Insight Bot API v2",
        description="FastAPI version of Insight Bot API. All API endpoints require Firebase Bearer token auth.",
        version="2.0.0",
        swagger_ui_parameters={"displayRequestDuration": True, "persistAuthorization": True},
        lifespan=lifespan,
    )
    fastapi_app.state.constants = constants

    _configure_openapi(fastapi_app)
    _configure_middlewares(fastapi_app, constants)
    fastapi_app.include_router(api_router)

    @fastapi_app.exception_handler(Exception)
    async def global_exception_handler(request, exc):
        if not isinstance(exc, (KeyboardInterrupt, asyncio.CancelledError)):
            add_log(f"Unhandled exception: {str(exc)}")
        return JSONResponse(
            status_code=500,
            content={
                "result": "fail",
                "status_code": 500,
                "message": str(exc),
                "error": "Internal server error",
            },
        )

    # Returned app is ASGI-wrapped to serve both FastAPI + Socket.IO.
    return _mount_socketio(fastapi_app)


app = create_app() # Entry point for uvicorn; also allows direct execution with `python v2/main.py`


def run_app(host: str = "0.0.0.0", port: int = 8000, debug: bool = True) -> None:
    import uvicorn

    if debug:
        uvicorn.run("v2.main:app", host=host, port=port, ws="websockets", log_level="warning", reload=True)
    else:
        uvicorn.run(create_app(), host=host, port=port, ws="websockets", log_level="warning", reload=False)


if __name__ == "__main__":
    run_app()
 