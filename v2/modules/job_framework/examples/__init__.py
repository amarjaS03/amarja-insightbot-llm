"""
Job Framework Examples

These are EXAMPLE handler functions showing how to write async functions
that work with the Job Framework. They are NOT part of the core framework.

Copy and adapt these patterns for your own business logic.

Usage in YOUR application code:

    from v2.modules.job_framework.examples.sample_handlers import data_fetch_example
    
    # Create job via API or manager
    job = await job_manager._create_job(job_data)
    
    # Execute with YOUR function (could be one of these examples or your own)
    await job_manager._execute_job(job.job_id, execute_func=data_fetch_example)
"""
from v2.modules.job_framework.examples.sample_handlers import (
    data_fetch_example,
    domain_dictionary_example,
    analysis_example,
    qna_example,
)

__all__ = [
    "data_fetch_example",
    "domain_dictionary_example",
    "analysis_example",
    "qna_example",
]

