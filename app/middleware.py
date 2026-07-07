from __future__ import annotations

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import RedirectResponse


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
        response.headers.setdefault("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
        if request.url.scheme == "https":
            response.headers.setdefault(
                "Strict-Transport-Security",
                "max-age=31536000; includeSubDomains",
            )
        return response


class BlockLoggedInUserFromAdminMiddleware(BaseHTTPMiddleware):
    """
    If a normal (non-admin) user is logged in and hits protected admin pages
    by mistake, redirect them back to the user dashboard.

    Admins are identified by session key "admin_id" or the 2FA pending flow.
    """

    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        if path.startswith("/admin"):
            allowed_entry_points = {
                "/admin",
                "/admin/login",
                "/admin/logout",
                "/admin/setup",
                "/admin/2fa",
                "/admin/2fa/setup",
                "/admin/2fa/qr",
            }
            # Avoid assertion if SessionMiddleware isn't active / ordered correctly.
            session = request.scope.get("session")
            if session is None:
                return await call_next(request)
            user_id = session.get("user_id")
            is_admin_flow = bool(session.get("admin_id") or session.get("admin_2fa_pending"))
            if user_id and not is_admin_flow and path not in allowed_entry_points:
                return RedirectResponse(url="/dashboard", status_code=302)
        return await call_next(request)
