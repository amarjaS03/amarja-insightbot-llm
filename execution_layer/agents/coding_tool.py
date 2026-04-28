from jupyter_client.manager import KernelManager
from typing import Dict, Optional, Any
import os
import queue
import time
from pydantic import BaseModel, Field
from langchain.tools import StructuredTool

# Try to let numeric kernels use all provisioned vCPUs.
# (Cloud Run sets CPU limit; these env vars influence BLAS/threaded ops.)
_DEFAULT_THREADS = os.getenv("ANALYSIS_MAX_THREADS", "4")
for _k in (
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
    "NUMEXPR_NUM_THREADS",
):
    os.environ.setdefault(_k, _DEFAULT_THREADS)

class CodeExecutionInput(BaseModel):
    code: str = Field(..., description="Python code to execute")
    description: Optional[str] = Field(None, description="Description of the code")

class JupyterExecutionTool:
    def __init__(self):
        self.km = KernelManager()
        self.km.start_kernel()
        self.kc = self.km.client()
        self.kc.start_channels()
        
        # Initialize kernel with common imports
        self._setup_kernel()

    def _setup_kernel(self):
        # NOTE: Keep this flush-left to avoid IndentationError in the kernel.
        setup_code = """
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from IPython.display import display
%matplotlib inline
plt.style.use('ggplot')

# --- Safety patch: categorical fillna ---
# LLM-generated code often does `fillna(0)` broadly; this raises:
# "Cannot setitem on a Categorical with a new category".
# We defensively add the fill value to categories and retry.
_old_series_fillna = pd.Series.fillna
def _series_fillna_safe(self, value=None, *args, **kwargs):
    try:
        return _old_series_fillna(self, value=value, *args, **kwargs)
    except TypeError as e:
        if value is not None and isinstance(self.dtype, pd.CategoricalDtype) and "Categorical" in str(e):
            try:
                s2 = self.copy()
                s2 = s2.cat.add_categories([value])
                return _old_series_fillna(s2, value=value, *args, **kwargs)
            except Exception:
                raise
        raise
pd.Series.fillna = _series_fillna_safe
"""
        self.execute_code(setup_code)

    def execute_code(self, code: str) -> Dict[str, Any]:
        outputs = []
        error = None
        
        try:
            msg_id = self.kc.execute(code)
            # Default overall timeout (seconds) can be tuned via env var.
            # The old behavior used a hard 10s iopub timeout and treated "no message"
            # as an execution failure, which breaks long-running plots/IO.
            overall_timeout_s = int(os.getenv("JUPYTER_EXEC_TIMEOUT_SECONDS", "300"))
            start = time.monotonic()

            saw_idle = False
            got_execute_reply = False

            while True:
                if (time.monotonic() - start) > overall_timeout_s:
                    error = f"Execution timeout after {overall_timeout_s} seconds"
                    outputs.append(error)
                    break

                # Non-fatal iopub polling: absence of messages just means the kernel
                # is still working (e.g., heavy computation, file IO, plotting).
                msg = None
                try:
                    msg = self.kc.get_iopub_msg(timeout=1)
                except queue.Empty:
                    msg = None

                if msg is not None:
                    if msg.get("parent_header", {}).get("msg_id") != msg_id:
                        # Skip unrelated messages from other executions.
                        pass
                    else:
                        msg_type = msg.get("header", {}).get("msg_type")
                        content = msg.get("content", {}) or {}

                        if msg_type in ("execute_result", "display_data"):
                            data = content.get("data", {}) or {}
                            if "text/plain" in data:
                                outputs.append(data["text/plain"])
                        elif msg_type == "stream":
                            outputs.append(content.get("text", ""))
                        elif msg_type == "error":
                            # Prefer full traceback when available.
                            tb = content.get("traceback") or []
                            tb_text = "\n".join(tb).strip()
                            error = f"Error: {content.get('ename')}: {content.get('evalue')}"
                            outputs.append(tb_text or error)
                        elif msg_type == "status":
                            if content.get("execution_state") == "idle":
                                saw_idle = True

                # Also wait for execute_reply on shell channel; this is a reliable
                # signal that the execution request finished.
                if not got_execute_reply:
                    try:
                        shell_msg = self.kc.get_shell_msg(timeout=0.1)
                    except queue.Empty:
                        shell_msg = None

                    if shell_msg is not None:
                        if shell_msg.get("parent_header", {}).get("msg_id") == msg_id:
                            if shell_msg.get("header", {}).get("msg_type") == "execute_reply":
                                got_execute_reply = True

                if got_execute_reply and saw_idle:
                    break
                    
        except Exception as e:
            msg = str(e) or repr(e)
            error = f"Execution error: {msg}"
            outputs.append(error)
            
        return {
            "output": "\n".join(outputs),
            "error": error,
            "success": error is None
        }

    def cleanup(self):
        self.kc.stop_channels()
        self.km.shutdown_kernel()

    def get_tool(self) -> StructuredTool:
        def execute_wrapper(code: str, description: Optional[str] = None) -> Dict[str, Any]:
            return self.execute_code(code)
            
        return StructuredTool(
            name="code_executor",
            description="Executes Python code in a Jupyter kernel and returns the output",
            func=execute_wrapper,
            args_schema=CodeExecutionInput
        )


# if __name__ == "__main__":
#     # Instantiate the tool
#     jupyter_tool = JupyterExecutionTool()
#     code_executor = jupyter_tool.get_tool()

#     # Prepare test input according to CodeExecutionInput schema
#     test_code = "print('hello')"
#     tool_input = {"code": test_code}

#     # Execute and capture the result
#     result = code_executor.run(tool_input)

#     # Display results
#     print("Test Results:")
#     print(f"Output: {result['output']}")
#     print(f"Error: {result['error']}")
#     print(f"Success: {result['success']}")

#     # Shutdown kernel
#     jupyter_tool.cleanup()
