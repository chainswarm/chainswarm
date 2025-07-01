import time
from typing import Dict, Optional
from fastapi import Request, HTTPException, status
from fastapi.responses import JSONResponse
import os
from loguru import logger
from collections import defaultdict
from datetime import datetime, timedelta
from packages.indexers.base import get_metrics_registry

# Simple in-memory rate limiting storage
class InMemoryRateLimiter:
    def __init__(self):
        self.requests = defaultdict(list)
        self.limit_per_hour = 100
        self.rate_limit_hits_total = None
        self.rate_limit_bypassed_total = None
        self.rate_limit_current_usage = None
        self._init_metrics()
    
    def _init_metrics(self):
        """Initialize rate limiting metrics"""
        # Try to get metrics from both API services
        import os
        network = os.getenv("NETWORK", "torus").lower()
        for service_name in [f"{network}-api", f"{network}-block-stream-api"]:
            registry = get_metrics_registry(service_name)
            if registry:
                self.rate_limit_hits_total = registry.create_counter(
                    'rate_limit_hits_total',
                    'Total rate limit hits',
                    ['client_ip', 'endpoint']
                )
                self.rate_limit_bypassed_total = registry.create_counter(
                    'rate_limit_bypassed_total',
                    'Total rate limit bypasses',
                    ['endpoint', 'reason']
                )
                self.rate_limit_current_usage = registry.create_gauge(
                    'rate_limit_current_usage',
                    'Current rate limit usage per client',
                    ['client_ip']
                )
                break
    
    def is_allowed(self, client_ip: str, endpoint: str = "unknown") -> tuple[bool, Optional[int]]:
        """Check if request is allowed and return (allowed, retry_after_seconds)"""
        now = datetime.now()
        hour_ago = now - timedelta(hours=1)
        
        # Clean old requests
        self.requests[client_ip] = [
            req_time for req_time in self.requests[client_ip]
            if req_time > hour_ago
        ]
        
        # Update current usage metric
        if self.rate_limit_current_usage:
            self.rate_limit_current_usage.labels(client_ip=client_ip).set(len(self.requests[client_ip]))
        
        # Check if limit exceeded
        if len(self.requests[client_ip]) >= self.limit_per_hour:
            # Record rate limit hit
            if self.rate_limit_hits_total:
                self.rate_limit_hits_total.labels(client_ip=client_ip, endpoint=endpoint).inc()
            
            # Calculate retry after (time until oldest request expires)
            oldest_request = min(self.requests[client_ip])
            retry_after = int((oldest_request + timedelta(hours=1) - now).total_seconds())
            return False, max(retry_after, 1)
        
        # Add current request
        self.requests[client_ip].append(now)
        
        # Update current usage metric
        if self.rate_limit_current_usage:
            self.rate_limit_current_usage.labels(client_ip=client_ip).set(len(self.requests[client_ip]))
        
        return True, None
    
    def record_bypass(self, endpoint: str, reason: str):
        """Record a rate limit bypass"""
        if self.rate_limit_bypassed_total:
            self.rate_limit_bypassed_total.labels(endpoint=endpoint, reason=reason).inc()

# Global rate limiter instance
rate_limiter = InMemoryRateLimiter()

def get_client_ip(request: Request) -> str:
    """Extract client IP from request"""
    # Check for forwarded headers first
    forwarded_for = request.headers.get("x-forwarded-for")
    if forwarded_for:
        return forwarded_for.split(",")[0].strip()
    
    real_ip = request.headers.get("x-real-ip")
    if real_ip:
        return real_ip
    
    # Fall back to client host
    if hasattr(request, "client") and request.client:
        return request.client.host
    
    return "unknown"

def has_api_key(request: Request) -> bool:
    """Check if request has x-api-key header"""
    api_key = request.headers.get("x-api-key")
    return api_key is not None and api_key.strip() != ""

def should_apply_rate_limit(request: Request) -> bool:
    """Determine if rate limiting should be applied to this request"""
    # Skip rate limiting if x-api-key header is present
    if has_api_key(request):
        # REMOVED: Debug logging - not needed
        # Record bypass metric
        rate_limiter.record_bypass(request.url.path, "api_key")
        return False
    
    # REMOVED: Debug logging - not needed
    return True

async def rate_limit_middleware(request: Request, call_next):
    """Middleware to apply rate limiting based on API key presence"""
    
    # Check if we should apply rate limiting
    if should_apply_rate_limit(request):
        try:
            client_ip = get_client_ip(request)
            allowed, retry_after = rate_limiter.is_allowed(client_ip, request.url.path)
            
            if not allowed:
                # ENHANCED: Strategic warning with context (rate limit violations are important)
                logger.warning(
                    "Rate limit exceeded - blocking request",
                    extra={
                        "client_ip": client_ip,
                        "endpoint": request.url.path,
                        "retry_after_seconds": retry_after,
                        "limit_per_hour": rate_limiter.limit_per_hour,
                        "user_agent": request.headers.get("user-agent", "unknown"),
                        "method": request.method
                    }
                )
                return JSONResponse(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    content={
                        "error": "Rate limit exceeded",
                        "message": "Too many requests. Please try again later or provide an x-api-key header.",
                        "retry_after": str(retry_after)
                    },
                    headers={"Retry-After": str(retry_after)}
                )
        except Exception as e:
            logger.error(
                "Rate limiting middleware error",
                error=e,
                operation="rate_limit_check",
                client_ip=get_client_ip(request),
                endpoint=request.url.path,
                method=request.method,
            )

    response = await call_next(request)
    return response