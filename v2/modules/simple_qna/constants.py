"""
Simple QnA Constants

Single source of truth for chat constraints and policies.
"""

# Maximum number of completed queries per chat before auto-rollover
MAX_QUERIES_PER_CHAT = 30

# Chat rollover policy
AUTO_CLOSE_ON_LIMIT = True
AUTO_CREATE_NEW_CHAT_ON_ROLLOVER = True
