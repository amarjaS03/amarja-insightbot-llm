"""
Query Utilities for Simple QnA v2

Helper functions for query formatting and validation.
"""

from typing import Optional


def validate_query(query: str) -> tuple[bool, Optional[str]]:
    """
    Validate a query string.
    
    Args:
        query: User query string
        
    Returns:
        tuple: (is_valid, error_message)
    """
    if not query:
        return False, "Query cannot be empty"
    
    if not isinstance(query, str):
        return False, "Query must be a string"
    
    query_stripped = query.strip()
    if len(query_stripped) < 3:
        return False, "Query must be at least 3 characters long"
    
    if len(query_stripped) > 5000:
        return False, "Query must be less than 5000 characters"
    
    return True, None


def sanitize_query(query: str) -> str:
    """
    Sanitize a query string (remove extra whitespace, etc.).
    
    Args:
        query: Raw query string
        
    Returns:
        str: Sanitized query
    """
    if not query:
        return ""
    
    # Remove leading/trailing whitespace
    sanitized = query.strip()
    
    # Replace multiple spaces with single space
    import re
    sanitized = re.sub(r'\s+', ' ', sanitized)
    
    return sanitized


def format_query_for_display(query: str, max_length: int = 100) -> str:
    """
    Format query for display (truncate if too long).
    
    Args:
        query: Query string
        max_length: Maximum length before truncation
        
    Returns:
        str: Formatted query string
    """
    if not query:
        return ""
    
    if len(query) <= max_length:
        return query
    
    return query[:max_length - 3] + "..."
