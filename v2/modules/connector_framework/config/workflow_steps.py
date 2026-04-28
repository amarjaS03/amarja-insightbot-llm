"""
Workflow step definitions for each connector type.

Each connector has its own workflow steps that guide users through
the process from connection to completion.
"""

CONNECTOR_WORKFLOW_STEPS = {
    "mssql": {
        "1": {"name": "Connect to Microsoft SQL Server", "next": "2"},
        "2": {"name": "Select Tables", "next": "3"},
        "3": {"name": "Create Domain Knowledge", "next": "4"},
        "4": {"name": "Preview & Edit", "next": "5"},
        "5": {"name": "Complete", "next": None}
    },
    "mysql": {
        "1": {"name": "Connect to MySQL", "next": "2"},
        "2": {"name": "Select Tables", "next": "3"},
        "3": {"name": "Create Domain Knowledge", "next": "4"},
        "4": {"name": "Preview & Edit", "next": "5"},
        "5": {"name": "Complete", "next": None}
    },
    "salesforce": {
        "1": {"name": "Connect to Salesforce", "next": "2"},
        "2": {"name": "Select Subject", "next": "3"},
        "3": {"name": "Create Domain Knowledge", "next": "4"},
        "4": {"name": "Preview & Edit", "next": "5"},
        "5": {"name": "Complete", "next": None}
    },
    "acumatica": {
        "1": {"name": "Connect to Acumatica", "next": "2"},
        "2": {"name": "Select Subject", "next": "3"},
        "3": {"name": "Create Domain Knowledge", "next": "4"},
        "4": {"name": "Preview & Edit", "next": "5"},
        "5": {"name": "Complete", "next": None}
    },
    "file_upload": {
        "1": {"name": "Upload & Validate", "next": "2"},
        "2": {"name": "Create Domain Knowledge", "next": "3"},
        "3": {"name": "Preview & Edit", "next": "4"},
        "4": {"name": "Complete", "next": None}
    },
    "sample_data": {
        "1": {"name": "Select Sample Dataset", "next": "2"},
        "2": {"name": "Complete", "next": None}
    },
    "n8n": {
        "1": {"name": "Connect to N8N", "next": "2"},
        "2": {"name": "Create Domain Knowledge", "next": "3"},
        "3": {"name": "Preview & Edit", "next": "4"},
        "4": {"name": "Complete", "next": None}
    }
}   

# Job type to step number mapping
# This maps job types to the workflow step they represent
JOB_TYPE_TO_STEP = {
    "data_fetch": "2",  # After connect, before domain
    "file_upload": "1",  # Upload step
    "copy_sample_data": "1",  # Copy step
    "fetch_n8n_data": "1",  # Fetch n8n data step
    "domain_dictionary": "2",  # Domain generation step (should be step 2, not 3)
    "domain_save": "4",  # Default for most connectors (overridden to "3" for file_upload in _get_step_for_job_type)
}

# Job dependencies: which job types depend on which other job types
# Format: {dependent_job_type: [required_job_types]}
JOB_DEPENDENCIES = {
    "domain_dictionary": ["data_fetch", "file_upload", "copy_sample_data", "fetch_n8n_data"],  # Domain generation needs data first
    "domain_save": ["domain_dictionary"],  # Domain save needs domain generation first
    # Analysis and QnA can run independently - they don't strictly require domain_dictionary
    # Domain dictionary is helpful but not required for analysis execution
    # "analysis": ["domain_dictionary"],  # Analysis can run without domain dictionary
    # "qna": ["domain_dictionary"],  # QnA can run without domain dictionary
}

# Data loading job types (jobs that load data into the session)
DATA_LOADING_JOB_TYPES = {"data_fetch", "file_upload", "copy_sample_data", "fetch_n8n_data"}

