"""
Session Model for FastAPI v2
Common model representing a session in the system
"""
from pydantic import BaseModel, Field, model_serializer
from typing import Optional, List
from datetime import datetime
from v2.common.model.userModel import UserModel
from v2.common.model.jobInfo import JobInfo


class SessionModel(BaseModel):
    """Model for session with camelCase field names"""
    user: Optional[UserModel] = Field(None, description="User associated with the session")
    sessionId: str = Field(..., description="Session unique identifier")
    dataSource: str = Field(..., description="Data source for the session")
    label: str = Field(..., description="Label for the session")
    status: str = Field(..., description="Session status")
    createdOn: Optional[datetime] = Field(None, description="Session creation timestamp")
    currentStep: Optional[str] = Field(None, description="Current step in session")
    nextStep: Optional[str] = Field(None, description="Next step in session")
    credentialId: Optional[str] = Field(None, description="Credential ID associated with session")
    jobIds: Optional[List[str]] = Field(default_factory=list, description="List of job IDs associated with session (deprecated, use jobs instead)")
    jobs: Optional[List[JobInfo]] = Field(default_factory=list, description="List of job details associated with session")
    pseudonymized: Optional[bool] = Field(default=False, description="If true, report was generated with pseudonymized data; web can show button to call depseudonymize-report")
    suggestedQuestions: Optional[List[str]] = Field(default_factory=list, description="Suggested deep analysis questions for the session")
    suggestedQuestionsSimple: Optional[List[str]] = Field(default_factory=list, description="Suggested simple QnA questions for the session")
    
    @model_serializer(mode='wrap')
    def serialize_model(self, handler, info):
        """
        Custom serializer that excludes:
        - user field when it's None
        - suggestedQuestions if it's empty or None (for insightbot)
        - suggestedQuestionsSimple if it's empty or None (for standard bot)
        """
        result = handler(self)
        if isinstance(result, dict):
            # Remove user field if it's None
            if result.get('user') is None:
                result = {k: v for k, v in result.items() if k != 'user'}
            
            # Remove suggestedQuestions if empty or None (only show if exists and has values)
            suggested_questions = result.get('suggestedQuestions')
            if not suggested_questions or (isinstance(suggested_questions, list) and len(suggested_questions) == 0):
                result = {k: v for k, v in result.items() if k != 'suggestedQuestions'}
            
            # Remove suggestedQuestionsSimple if empty or None (only show if exists and has values)
            suggested_questions_simple = result.get('suggestedQuestionsSimple')
            if not suggested_questions_simple or (isinstance(suggested_questions_simple, list) and len(suggested_questions_simple) == 0):
                result = {k: v for k, v in result.items() if k != 'suggestedQuestionsSimple'}
        return result
    
    class Config:
        from_attributes = True
