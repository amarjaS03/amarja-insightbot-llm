"""
EXAMPLE Handler Functions for Job Framework

=============================================================================
THESE ARE EXAMPLES - NOT PART OF THE CORE FRAMEWORK
=============================================================================

These sample functions demonstrate how to write async functions that:
1. Receive a JobContext
2. Use ctx.check_cancellation() for cancellation support
3. Use ctx.create_milestone() for progress tracking
4. Return a result dict

COPY AND ADAPT these patterns for your own business logic.

The Job Framework is completely function-agnostic. You can execute
ANY async function that follows this pattern:

    async def your_custom_function(ctx: JobContext) -> Dict[str, Any]:
        # Your business logic here
        ctx.check_cancellation()
        await ctx.create_milestone("Step 1 done")
        return {"result": "success"}

    # Execute it:
    await job_manager._execute_job(job_id, execute_func=your_custom_function)

=============================================================================
"""
from typing import Any, Dict
import asyncio
import json
import os
from pathlib import Path
from datetime import datetime

from v2.common.logger import add_log
from v2.modules.job_framework.manager.job.job import JobContext


def _write_json_file(path, data):
    """Sync helper for JSON file writing"""
    with open(path, 'w') as f:
        json.dump(data, f, indent=2, default=str)


async def data_fetch_example(ctx: JobContext) -> Dict[str, Any]:
    """
    EXAMPLE: Data fetching with progress tracking.
    
    Demonstrates:
    - Using ctx.job_config to get parameters
    - Calling ctx.check_cancellation() between steps
    - Creating milestones for progress tracking
    - Integrating with ConnectorManager (external dependency)
    
    Expected job_config:
        - tables: List of table names to fetch
    """
    from v2.modules.connector_framework.manager.connector.connector_manager import ConnectorManager
    
    tables = ctx.job_config.get("tables", [])
    session_id = str(ctx.session_id)
    
    if not tables:
        return {"status": "error", "message": "No tables specified in job_config"}
    
    add_log(f"[DataFetchExample] Starting fetch for {len(tables)} tables")
    
    connector_manager = ConnectorManager()
    
    # Check connector health
    healthy = await connector_manager.health(session_id)
    if not healthy:
        return {"status": "error", "message": "Connector not active"}
    
    connector = await connector_manager._get_connector(session_id)
    if not connector:
        return {"status": "error", "message": "Connector not found"}
    
    await ctx.create_milestone(
        f"Starting data fetch: {len(tables)} tables",
        data={"tables": tables}
    )
    
    # Setup output directory
    project_root = Path(__file__).resolve().parents[4]
    input_data_dir = project_root / "execution_layer" / "input_data" / session_id
    os.makedirs(input_data_dir, exist_ok=True)
    
    saved_files = []
    failed_tables = []
    
    for i, table_name in enumerate(tables):
        # CHECK CANCELLATION - important for long-running loops
        ctx.check_cancellation()
        
        # CREATE MILESTONE - enables progress tracking
        await ctx.create_milestone(
            f"Fetching: {table_name} ({i+1}/{len(tables)})",
            data={"table": table_name, "progress": i + 1, "total": len(tables)}
        )
        
        try:
            df = await connector.get_data(table_name)
            ctx.check_cancellation()  # Check again after slow operation
            
            pkl_path = input_data_dir / f"{table_name}.pkl"
            df.to_pickle(str(pkl_path))
            saved_files.append(f"{table_name}.pkl")
            
        except Exception as e:
            failed_tables.append({"table": table_name, "error": str(e)})
    
    await ctx.create_milestone(
        f"Completed: {len(saved_files)}/{len(tables)} tables",
        data={"saved_files": saved_files, "failed_tables": failed_tables}
    )
    
    return {
        "status": "success" if saved_files else "error",
        "fetched": saved_files,
        "failed": failed_tables
    }


async def domain_dictionary_example(ctx: JobContext) -> Dict[str, Any]:
    """
    EXAMPLE: Domain dictionary generation with progress tracking.
    
    Demonstrates:
    - Reading files and processing them one by one
    - Progress milestones for each file
    - Handling errors gracefully
    """
    import pandas as pd
    import json
    
    session_id = str(ctx.session_id)
    
    project_root = Path(__file__).resolve().parents[4]
    input_data_dir = project_root / "execution_layer" / "input_data" / session_id
    
    if not input_data_dir.exists():
        return {"status": "error", "message": "No input data found"}
    
    pkl_files = list(input_data_dir.glob("*.pkl"))
    if not pkl_files:
        return {"status": "error", "message": "No .pkl files found"}
    
    await ctx.create_milestone(f"Processing {len(pkl_files)} files")
    
    domain_dict = {"session_id": session_id, "generated_at": datetime.now().isoformat(), "tables": {}}
    
    for i, pkl_file in enumerate(pkl_files):
        ctx.check_cancellation()
        
        await ctx.create_milestone(
            f"Processing: {pkl_file.stem} ({i+1}/{len(pkl_files)})"
        )
        
        try:
            df = pd.read_pickle(str(pkl_file))
            domain_dict["tables"][pkl_file.stem] = {
                "row_count": len(df),
                "columns": [{"name": c, "dtype": str(df[c].dtype)} for c in df.columns]
            }
        except Exception as e:
            domain_dict["tables"][pkl_file.stem] = {"error": str(e)}
    
    output_path = input_data_dir / "domain_directory.json"
    # Use async-compatible file write via asyncio.to_thread
    await asyncio.to_thread(_write_json_file, output_path, domain_dict)
    
    await ctx.create_milestone("Domain dictionary completed")
    
    return {"status": "success", "path": str(output_path)}


async def analysis_example(ctx: JobContext) -> Dict[str, Any]:
    """
    EXAMPLE: Analysis job placeholder.
    
    Replace this with your actual analysis pipeline.
    """
    await ctx.create_milestone("Starting analysis")
    ctx.check_cancellation()
    
    # YOUR ANALYSIS LOGIC HERE
    
    await ctx.create_milestone("Analysis completed")
    return {"status": "success", "message": "Implement your analysis logic"}


async def qna_example(ctx: JobContext) -> Dict[str, Any]:
    """
    EXAMPLE: QnA job placeholder.
    
    Replace this with your actual QnA pipeline.
    """
    question = ctx.job_config.get("question", "")
    
    await ctx.create_milestone(f"Processing: {question[:50]}...")
    ctx.check_cancellation()
    
    # YOUR QNA LOGIC HERE
    
    await ctx.create_milestone("QnA completed")
    return {"status": "success", "question": question, "answer": "Implement your QnA logic"}

