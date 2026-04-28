"""
Common helper functions
"""
from typing import Any, List, Union
from pydantic import BaseModel

def to_dict(data: Union[BaseModel, List[BaseModel], dict, List[dict], Any]) -> Union[dict, List[dict], Any]:
    """
    Convert Pydantic models to dictionaries recursively.
    Handles single models, lists of models, or already-dict data.
    """
    if isinstance(data, list):
        return [to_dict(item) for item in data]
    elif isinstance(data, BaseModel):
        # Pydantic v2 uses model_dump(), v1 uses dict()
        if hasattr(data, 'model_dump'):
            return data.model_dump()
        elif hasattr(data, 'dict'):
            return data.dict()
        else:
            return data
    else:
        return data

