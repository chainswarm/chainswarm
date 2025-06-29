"""
Correlation ID middleware for FastAPI applications.

This middleware ensures that all API requests have correlation IDs for tracing
and links them to the enhanced logging system.
"""

import time
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from packages.indexers.base.enhanced_logging import generate_correlation_id, set_correlation_id


class CorrelationMiddleware(BaseHTTPMiddleware):
    """
    Middleware to handle correlation IDs for request tracing.
    
    This middleware:
    1. Extracts correlation ID from request headers or generates a new one
    2. Sets the correlation ID in thread-local storage for logging
    3. Adds the correlation ID to response headers
    4. Ensures all logs within the request context include the correlation ID
    """
    
    async def dispatch(self, request: Request, call_next):
        # Extract or generate correlation ID
        correlation_id = (
            request.headers.get("X-Correlation-ID") or 
            request.headers.get("x-correlation-id") or
            generate_correlation_id()
        )
        
        # Store in request state and thread-local storage
        request.state.correlation_id = correlation_id
        set_correlation_id(correlation_id)
        
        # Add request start time for duration calculation
        request.state.start_time = time.time()
        
        try:
            # Process the request
            response = await call_next(request)
            
            # Add correlation ID to response headers
            response.headers["X-Correlation-ID"] = correlation_id
            
            return response
            
        except Exception as e:
            # Ensure correlation ID is in error response
            response = Response(
                content=f"Internal server error: {str(e)}",
                status_code=500,
                headers={"X-Correlation-ID": correlation_id}
            )
            return response
        
        finally:
            # Clear correlation ID from thread-local storage
            set_correlation_id(None)


def get_request_context(request: Request) -> dict:
    """
    Extract request context for logging.
    
    Args:
        request: FastAPI request object
        
    Returns:
        dict: Request context for logging
    """
    return {
        "method": request.method,
        "url": str(request.url),
        "path": request.url.path,
        "query_params": dict(request.query_params),
        "client_ip": request.client.host if request.client else "unknown",
        "user_agent": request.headers.get("user-agent", "unknown"),
        "correlation_id": getattr(request.state, 'correlation_id', None),
        "processing_time": time.time() - getattr(request.state, 'start_time', time.time())
    }


def sanitize_params(params: dict) -> dict:
    """
    Sanitize request parameters for logging (remove sensitive data).
    
    Args:
        params: Request parameters
        
    Returns:
        dict: Sanitized parameters
    """
    sensitive_keys = {'password', 'token', 'api_key', 'secret', 'auth'}
    sanitized = {}
    
    for key, value in params.items():
        if any(sensitive in key.lower() for sensitive in sensitive_keys):
            sanitized[key] = "[REDACTED]"
        else:
            sanitized[key] = value
    
    return sanitized