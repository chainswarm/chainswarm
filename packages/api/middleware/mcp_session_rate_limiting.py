import time
import asyncio
from typing import Dict, Optional, List, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from collections import defaultdict
from loguru import logger
from fastmcp import Context
import os


@dataclass
class MCPSessionData:
    """Data structure to track session rate limiting information"""
    session_id: str
    api_key: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)
    last_activity: datetime = field(default_factory=datetime.now)
    hourly_requests: List[datetime] = field(default_factory=list)
    ten_minute_requests: List[datetime] = field(default_factory=list)
    total_requests: int = 0
    
    def update_activity(self):
        """Update last activity timestamp"""
        self.last_activity = datetime.now()
    
    def is_expired(self, timeout_hours: int = 2) -> bool:
        """Check if session data has expired"""
        expiry_time = self.last_activity + timedelta(hours=timeout_hours)
        return datetime.now() > expiry_time
    
    def is_authenticated(self) -> bool:
        """Check if session has valid API key"""
        return self.api_key is not None and self.api_key.strip() != ""


class MCPRateLimitError(Exception):
    """Custom exception for MCP rate limit violations"""
    
    def __init__(self, session_id: str, limit_info: dict):
        self.session_id = session_id
        self.limit_info = limit_info
        super().__init__(f"Rate limit exceeded for session {session_id}")


class MCPSessionRateLimiter:
    """Session-based rate limiter for MCP server using FastMCP sessions"""
    
    def __init__(self):
        # Session storage: session_id -> MCPSessionData
        self.sessions: Dict[str, MCPSessionData] = {}
        
        # Configuration from environment variables
        self.enabled = os.getenv("MCP_SESSION_RATE_LIMIT_ENABLED", "true").lower() == "true"
        self.public_hourly_limit = int(os.getenv("MCP_PUBLIC_HOURLY_LIMIT", "100"))
        self.public_ten_minute_limit = int(os.getenv("MCP_PUBLIC_TEN_MINUTE_LIMIT", "20"))
        self.authenticated_hourly_limit = int(os.getenv("MCP_AUTHENTICATED_HOURLY_LIMIT", "500"))
        self.authenticated_ten_minute_limit = int(os.getenv("MCP_AUTHENTICATED_TEN_MINUTE_LIMIT", "50"))
        self.session_timeout_hours = int(os.getenv("MCP_SESSION_TIMEOUT_HOURS", "2"))
        self.authenticated_session_timeout_hours = int(os.getenv("MCP_AUTHENTICATED_SESSION_TIMEOUT_HOURS", "4"))
        self.cleanup_interval_minutes = int(os.getenv("MCP_CLEANUP_INTERVAL_MINUTES", "10"))
        
        # Cleanup task will be started when needed
        self._cleanup_task = None
        self._cleanup_started = False
        
        logger.info(f"MCP Session Rate Limiter initialized - Enabled: {self.enabled}")
        logger.info(f"Public limits: {self.public_hourly_limit}/hour, {self.public_ten_minute_limit}/10min")
        logger.info(f"Authenticated limits: {self.authenticated_hourly_limit}/hour, {self.authenticated_ten_minute_limit}/10min")
    
    def _start_cleanup_task(self):
        """Start background cleanup task if not already started"""
        if not self._cleanup_started and self._cleanup_task is None:
            try:
                self._cleanup_task = asyncio.create_task(self._cleanup_loop())
                self._cleanup_started = True
                logger.info("Started MCP session cleanup task")
            except RuntimeError:
                # No event loop running yet, will start later
                logger.debug("No event loop available, cleanup task will start on first use")
    
    async def _cleanup_loop(self):
        """Background task to clean up expired sessions"""
        while True:
            try:
                await asyncio.sleep(self.cleanup_interval_minutes * 60)
                self.cleanup_expired_sessions()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in session cleanup task: {e}")
    
    def cleanup_expired_sessions(self):
        """Remove expired session data"""
        now = datetime.now()
        expired_sessions = []
        
        for session_id, session_data in self.sessions.items():
            timeout_hours = (
                self.authenticated_session_timeout_hours 
                if session_data.is_authenticated() 
                else self.session_timeout_hours
            )
            
            if session_data.is_expired(timeout_hours):
                expired_sessions.append(session_id)
        
        for session_id in expired_sessions:
            del self.sessions[session_id]
            logger.debug(f"Cleaned up expired session: {session_id}")
        
        if expired_sessions:
            logger.info(f"Cleaned up {len(expired_sessions)} expired sessions")
    
    def get_session_limits(self, api_key: Optional[str]) -> dict:
        """Get rate limits based on API key presence"""
        is_authenticated = api_key is not None and api_key.strip() != ""
        
        return {
            "hourly_limit": self.authenticated_hourly_limit if is_authenticated else self.public_hourly_limit,
            "ten_minute_limit": self.authenticated_ten_minute_limit if is_authenticated else self.public_ten_minute_limit,
            "is_authenticated": is_authenticated
        }
    
    def _clean_old_requests(self, requests: List[datetime], window_minutes: int) -> List[datetime]:
        """Remove requests older than the specified window"""
        cutoff_time = datetime.now() - timedelta(minutes=window_minutes)
        return [req_time for req_time in requests if req_time > cutoff_time]
    
    def check_and_increment(self, session_id: str, api_key: Optional[str] = None) -> Tuple[bool, dict]:
        """
        Check rate limit and increment counter if allowed
        
        Returns:
            Tuple[bool, dict]: (is_allowed, limit_info)
        """
        if not self.enabled:
            return True, {"rate_limiting_disabled": True}
        
        # Start cleanup task if not already started
        if not self._cleanup_started:
            self._start_cleanup_task()
        
        now = datetime.now()
        
        # Get or create session data
        if session_id not in self.sessions:
            self.sessions[session_id] = MCPSessionData(
                session_id=session_id,
                api_key=api_key
            )
        
        session_data = self.sessions[session_id]
        
        # Update API key if provided (session upgrade)
        if api_key and not session_data.api_key:
            session_data.api_key = api_key
            logger.info(f"Session {session_id} upgraded with API key")
        
        # Update activity
        session_data.update_activity()
        
        # Get limits for this session
        limits = self.get_session_limits(session_data.api_key)
        
        # Clean old requests
        session_data.hourly_requests = self._clean_old_requests(session_data.hourly_requests, 60)
        session_data.ten_minute_requests = self._clean_old_requests(session_data.ten_minute_requests, 10)
        
        # Check limits
        hourly_count = len(session_data.hourly_requests)
        ten_minute_count = len(session_data.ten_minute_requests)
        
        limit_info = {
            "session_id": session_id,
            "hourly_limit": limits["hourly_limit"],
            "current_hourly_usage": hourly_count,
            "ten_minute_limit": limits["ten_minute_limit"],
            "current_ten_minute_usage": ten_minute_count,
            "is_authenticated": limits["is_authenticated"],
            "total_requests": session_data.total_requests
        }
        
        # Check if limits exceeded
        if hourly_count >= limits["hourly_limit"]:
            # Calculate retry after (time until oldest request expires)
            oldest_request = min(session_data.hourly_requests) if session_data.hourly_requests else now
            retry_after = int((oldest_request + timedelta(hours=1) - now).total_seconds())
            limit_info["retry_after"] = max(retry_after, 1)
            limit_info["limit_type"] = "hourly"
            return False, limit_info
        
        if ten_minute_count >= limits["ten_minute_limit"]:
            # Calculate retry after (time until oldest request expires)
            oldest_request = min(session_data.ten_minute_requests) if session_data.ten_minute_requests else now
            retry_after = int((oldest_request + timedelta(minutes=10) - now).total_seconds())
            limit_info["retry_after"] = max(retry_after, 1)
            limit_info["limit_type"] = "ten_minute"
            return False, limit_info
        
        # Increment counters
        session_data.hourly_requests.append(now)
        session_data.ten_minute_requests.append(now)
        session_data.total_requests += 1
        
        logger.debug(f"Rate limit check passed for session {session_id}: {hourly_count+1}/{limits['hourly_limit']} hourly, {ten_minute_count+1}/{limits['ten_minute_limit']} per 10min")
        
        return True, limit_info
    
    def get_session_stats(self, session_id: str) -> Optional[dict]:
        """Get current statistics for a session"""
        if session_id not in self.sessions:
            return None
        
        session_data = self.sessions[session_id]
        limits = self.get_session_limits(session_data.api_key)
        
        # Clean old requests for accurate counts
        session_data.hourly_requests = self._clean_old_requests(session_data.hourly_requests, 60)
        session_data.ten_minute_requests = self._clean_old_requests(session_data.ten_minute_requests, 10)
        
        return {
            "session_id": session_id,
            "created_at": session_data.created_at.isoformat(),
            "last_activity": session_data.last_activity.isoformat(),
            "is_authenticated": session_data.is_authenticated(),
            "total_requests": session_data.total_requests,
            "hourly_limit": limits["hourly_limit"],
            "current_hourly_usage": len(session_data.hourly_requests),
            "ten_minute_limit": limits["ten_minute_limit"],
            "current_ten_minute_usage": len(session_data.ten_minute_requests)
        }
    
    def get_all_sessions_stats(self) -> dict:
        """Get statistics for all active sessions"""
        return {
            "total_sessions": len(self.sessions),
            "authenticated_sessions": sum(1 for s in self.sessions.values() if s.is_authenticated()),
            "public_sessions": sum(1 for s in self.sessions.values() if not s.is_authenticated()),
            "total_requests": sum(s.total_requests for s in self.sessions.values()),
            "sessions": [self.get_session_stats(sid) for sid in self.sessions.keys()]
        }
    
    def stop_cleanup_task(self):
        """Stop the background cleanup task"""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            self._cleanup_task = None


# Global rate limiter instance
mcp_session_rate_limiter = MCPSessionRateLimiter()


def extract_session_info(ctx: Context) -> dict:
    """Extract session information from FastMCP context"""
    session_id = str(ctx.session)
    
    # Try to get API key from request
    api_key = None
    try:
        request = ctx.get_http_request()
        if request:
            # Check query parameters first
            api_key = request.query_params.get("x-api-key")
            # Check headers as fallback
            if not api_key:
                api_key = request.headers.get("x-api-key")
    except Exception as e:
        logger.debug(f"Could not extract request info from context: {e}")
    
    return {
        "session_id": session_id,
        "api_key": api_key
    }


def session_rate_limit(func):
    """Decorator to apply session-based rate limiting to MCP tools"""
    from functools import wraps
    
    @wraps(func)
    async def wrapper(*args, **kwargs):
        # Find Context in kwargs or args
        ctx = None
        
        # Check kwargs first
        if 'ctx' in kwargs:
            ctx = kwargs['ctx']
        else:
            # Look for Context in args
            for arg in args:
                if isinstance(arg, Context):
                    ctx = arg
                    break
        
        if ctx and mcp_session_rate_limiter.enabled:
            try:
                session_info = extract_session_info(ctx)
                allowed, limit_info = mcp_session_rate_limiter.check_and_increment(
                    session_info["session_id"],
                    session_info["api_key"]
                )
                
                if not allowed:
                    logger.warning(f"Rate limit exceeded for session {session_info['session_id']}")
                    
                    # Return error response instead of raising exception
                    error_response = {
                        "error": "session_rate_limit_exceeded",
                        "message": "Session rate limit exceeded. Try again later or provide an API key for higher limits.",
                        "session_id": session_info["session_id"],
                        "limits": {
                            "hourly_limit": limit_info["hourly_limit"],
                            "current_hourly_usage": limit_info["current_hourly_usage"],
                            "ten_minute_limit": limit_info["ten_minute_limit"],
                            "current_ten_minute_usage": limit_info["current_ten_minute_usage"]
                        },
                        "is_authenticated": limit_info["is_authenticated"],
                        "total_requests": limit_info["total_requests"]
                    }
                    
                    if "retry_after" in limit_info:
                        error_response["retry_after"] = limit_info["retry_after"]
                        error_response["limit_type"] = limit_info["limit_type"]
                    
                    if not limit_info["is_authenticated"]:
                        error_response["upgrade_message"] = "Get an API key for higher limits (500/hour vs 100/hour)"
                    
                    return error_response
                
            except Exception as e:
                logger.error(f"Error in session rate limiting: {e}")
                # Continue without rate limiting if there's an error
        
        return await func(*args, **kwargs)
    
    return wrapper