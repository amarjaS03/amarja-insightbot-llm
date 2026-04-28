# Create main API router without a global path prefix
from fastapi import APIRouter
from v2.modules.session_framework.controllers.session.sessions_controller import router as sessions_router
from v2.modules.credential_framework.controllers.credential.credentials_controller import router as credentials_router
from v2.modules.connector_framework.controllers.connector.connectors_controller import router as connectors_router
from v2.modules.connector_framework.controllers.sample_data.sample_data_controller import router as sample_data_router
from v2.modules.connector_framework.controllers.domain.domain_controller import router as domain_router
from v2.modules.connector_framework.controllers.data.data_controller import router as data_controller
from v2.modules.job_framework.controllers.job.jobs_controller import router as jobs_router, internal_router as jobs_internal_router
from v2.modules.admin_framework.controllers.admin.admin_controller import router as admin_router
from v2.modules.marketing_framework.controllers.marketing.marketing_controller import router as marketing_router
from v2.common.gcp.controllers.cloud_run_controller import router as gcp_cloud_run_router
from v2.modules.analysis.controllers.analysis_controller import router as analysis_router
from v2.modules.simple_qna.controllers.simple_qna_controller import router as simple_qna_router
from v2.modules.utility.controllers.utility_controller import router as utility_router
from v2.modules.pseudonymization.controllers.pseudonymization_controller import router as pseudonymization_router
from v2.modules.performance.controllers.performance_controller import router as performance_router

api_router = APIRouter()

# Include all controllers under the main API router
api_router.include_router(sessions_router, tags=["Sessions"])
api_router.include_router(credentials_router, tags=["Credentials"])
api_router.include_router(connectors_router, tags=["Connectors"])
api_router.include_router(sample_data_router, tags=["Sample Data"])
api_router.include_router(domain_router, tags=["Domain"])
api_router.include_router(data_controller, tags=["Data"])
api_router.include_router(jobs_router, tags=["Jobs"])
api_router.include_router(jobs_internal_router, tags=["Internal"])
api_router.include_router(admin_router, tags=["Admin"])
api_router.include_router(marketing_router, tags=["Marketing"])
api_router.include_router(gcp_cloud_run_router, tags=["GCP Cloud Run"])
api_router.include_router(analysis_router, tags=["Analysis"])
api_router.include_router(simple_qna_router, tags=["Simple QnA"])
api_router.include_router(utility_router, tags=["Utility"])
api_router.include_router(pseudonymization_router, tags=["Pseudonymization"])
api_router.include_router(performance_router, tags=["Performance"])
