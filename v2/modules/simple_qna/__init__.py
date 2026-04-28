"""
Simple QnA Module for FastAPI v2

Provides chat-based QnA functionality where:
- 1 Chat = 1 Long-Running Job
- Multiple queries per chat stored in Firestore subcollection
- Each query executes via Cloud Run (ephemeral)
"""
