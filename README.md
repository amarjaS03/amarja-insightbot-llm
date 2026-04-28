# Deep Analysis API - InsightBot Backend

This is a comprehensive data analysis backend that implements a multi-layer architecture for intelligent data analysis and reporting. The system provides AI-powered data analysis capabilities with support for Salesforce and Acumatica integrations.

## Architecture Overview

The system consists of three main layers:

1. **API Layer** - Handles authentication, session management, and request routing
2. **Execution Layer** - Executes data analysis workflows in isolated Docker containers
3. **Cloud Functions** - Firebase cloud functions for data processing and integrations

## Directory Structure

```
deep-analysis-api/
├── api_layer/                    # API Layer components
│   ├── api_server.py             # Main Flask API server with WebSocket support
│   ├── session_manager.py        # Docker container session management
│   ├── job_manager.py            # Analysis job orchestration
│   ├── firebase_user_manager.py  # Firebase authentication and user management
│   ├── firebase_data_models.py   # Firebase data models
│   ├── firebase_config.py        # Firebase configuration
│   ├── admin_routes.py           # Admin API endpoints
│   ├── user_routes.py            # User management endpoints
│   ├── email_routes.py           # Email management endpoints
│   ├── refresh_token.py          # Google OAuth token refresh
│   ├── blueprints/               # Modular API blueprints
│   │   ├── sessions.py           # Session management endpoints
│   │   ├── data.py              # Data validation and processing
│   │   ├── salesforce.py        # Salesforce integration
│   │   ├── acumatica.py          # Acumatica integration
│   │   └── socketio_events.py    # WebSocket event handlers
│   └── config/                   # Configuration files
│       └── firebase-adminsdk-*.json
├── execution_layer/              # Execution Layer components
│   ├── execution_api.py          # Main execution API for containers
│   ├── run_execution_api.py      # Container entry point
│   ├── agents/                   # AI analysis agents
│   │   ├── data_analysis_agent.py  # Main analysis coordinator
│   │   ├── eda_agent.py            # Exploratory data analysis
│   │   ├── hypothesis_agent.py     # Hypothesis generation and testing
│   │   ├── narrator_agent.py       # Report generation
│   │   ├── executor.py             # Code execution agent
│   │   └── token_manager.py        # Token usage management
│   ├── input_data/              # Session-specific input data
│   └── output_data/             # Generated analysis outputs
├── acumatica_cloud_function/     # Firebase Cloud Function for Acumatica
│   ├── functions/
│   │   ├── main.py              # Cloud function entry point
│   │   ├── dataProcessing.py    # Data processing logic
│   │   └── requirements.txt     # Cloud function dependencies
│   ├── firebase.json            # Firebase configuration
│   ├── firestore.rules          # Firestore security rules
│   └── firestore.indexes.json  # Firestore indexes
├── salesforce_cloud_function/    # Firebase Cloud Function for Salesforce
│   ├── functions/
│   │   ├── main.py              # Cloud function entry point
│   │   └── requirements.txt     # Cloud function dependencies
│   ├── firebase.json            # Firebase configuration
│   ├── firestore.rules          # Firestore security rules
│   └── firestore.indexes.json  # Firestore indexes
├── Dockerfile                    # Docker image for execution containers
├── build_image.sh               # Docker build script
├── main.py                      # Main API server entry point
├── logger.py                    # Centralized logging module
├── requirements.txt             # Python dependencies
├── jobs.json                    # Job state persistence
└── sessions.json                # Session state persistence
```

## How It Works

### 1. API Layer
- **Authentication**: Google OAuth integration with Firebase user management
- **Session Management**: Docker container lifecycle management for isolated execution
- **Job Orchestration**: Queues and manages analysis jobs with real-time status updates
- **WebSocket Support**: Real-time communication for job progress and status updates
- **Integration Support**: Salesforce and Acumatica data connectors

### 2. Execution Layer
- **Isolated Execution**: Runs analysis in Docker containers for security and isolation
- **AI Agents**: Specialized agents for different analysis tasks:
  - Data Analysis Agent: Main coordinator and query analysis
  - EDA Agent: Exploratory data analysis and visualization
  - Hypothesis Agent: Statistical hypothesis testing
  - Narrator Agent: Report generation and summarization
  - Executor Agent: Code execution and data processing
- **Token Management**: Tracks and limits OpenAI API usage
- **Report Generation**: Creates comprehensive HTML reports with visualizations

### 3. Cloud Functions
- **Data Processing**: Handles data extraction and transformation from external systems
- **Firebase Integration**: Manages data persistence and user authentication
- **Scalable Processing**: Serverless functions for handling large datasets

## Setup and Installation

### Prerequisites
- Python 3.11
- Docker
- Firebase CLI
- Google Cloud SDK (for cloud functions)

### 1. Clone and Setup Virtual Environment

```bash
# Clone the repository
git clone <repository-url>
cd deep-analysis-api

# Create virtual environment
python -m venv .venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# install cloudflare
winget install --id Cloudflare.cloudflared

# run cloudflare tunnel for cloud run
cloudflared tunnel --url http://localhost:8000
```


### 2. Environment Configuration

Create a `.env` file in the root directory:

```bash
# OpenAI Configuration
OPENAI_API_KEY=your_openai_api_key
MODEL_NAME=gpt-5.4

# Google OAuth Configuration
GOOGLE_CLIENT_ID=your_google_client_id
GOOGLE_CLIENT_SECRET=your_google_client_secret
GOOGLE_REDIRECT_URI=your_redirect_uri

# Firebase Configuration
FIREBASE_PROJECT_ID=your_firebase_project_id
FIREBASE_PRIVATE_KEY_ID=your_private_key_id
FIREBASE_PRIVATE_KEY=your_private_key
FIREBASE_CLIENT_EMAIL=your_client_email
FIREBASE_CLIENT_ID=your_client_id
FIREBASE_AUTH_URI=https://accounts.google.com/o/oauth2/auth
FIREBASE_TOKEN_URI=https://oauth2.googleapis.com/token

# API Configuration
CORS_ALLOWED_ORIGINS=http://localhost:3000,http://127.0.0.1:3000
```

### 3. Firebase Setup

```bash
# Install Firebase CLI
npm install -g firebase-tools

# Login to Firebase
firebase login

# Initialize Firebase in project directories
cd acumatica_cloud_function
firebase init functions
cd ../salesforce_cloud_function
firebase init functions
```

### 4. Docker Setup

```bash
# Build the execution environment Docker image
./build_image.sh
# or manually:
docker build -t code-execution-env .
```

### 5. Running the Application

#### Development Mode (Local)

```bash
# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate

# Start the API server
python main.py
```

The API will be available at `http://localhost:5000`

#### Cloud Functions (Local Development)

For local development of cloud functions:

```bash
# Acumatica Cloud Function
cd acumatica_cloud_function
firebase emulators:start --only functions

# Salesforce Cloud Function
cd salesforce_cloud_function
firebase emulators:start --only functions
```

#### Production Deployment

```bash
# Deploy cloud functions
cd acumatica_cloud_function
firebase deploy --only functions

cd ../salesforce_cloud_function
firebase deploy --only functions

# Deploy main API (using your preferred deployment method)
# Example with Docker:
docker run -d -p 5000:5000 --env-file .env code-execution-env
```

## API Endpoints

### Authentication
- `POST /google_auth` - Exchange Google auth code for tokens
- `POST /refresh_token` - Refresh Google OAuth tokens
- `GET /system/status` - Get system status and user permissions

### Session Management
- `GET /session_id` - Create a new analysis session
- `GET /validate_session/<session_id>` - Validate session existence
- `GET /session_status/<session_id>` - Get session status
- `POST /restart_session/<session_id>` - Restart a session
- `POST /cleanup_session/<session_id>` - Clean up session resources

### Data Management
- `POST /validate_data` - Validate uploaded data files
- `POST /generate_domain_dictionary` - Generate domain-specific data dictionary
- `POST /save_domain_dictionary` - Save domain dictionary for analysis

### Analysis
- `POST /create_job` - Create a new analysis job
- `GET /job_status/<job_id>` - Get job status
- `GET /job_report/<job_id>` - Get analysis report
- `GET /analysis_history` - Get user's analysis history
- `GET /analysis_report/<job_id>` - Get specific analysis report

### Integrations
- `POST /salesforce/save_credentials` - Save Salesforce credentials
- `POST /acumatica/save_credentials` - Save Acumatica credentials

### System
- `GET /hello` - Health check endpoint
- `GET /logs` - Stream system logs
- `GET /job_logs/<job_id>` - Stream job-specific logs

### WebSocket Events
- `connect` - Client connection
- `disconnect` - Client disconnection
- `join_session` - Join a session room
- `job_status` - Real-time job status updates
- `job_complete` - Job completion notification
- `job_error` - Job error notifications

## Development Workflow

### 1. Local Development
1. Activate virtual environment
2. Start the main API server: `python main.py`
3. Use Firebase emulators for cloud function development
4. Test with frontend application

### 2. Testing Cloud Functions Locally
```bash
# Start Firebase emulators
firebase emulators:start --only functions,firestore

# Test functions using Firebase CLI or HTTP requests
curl -X POST http://localhost:5001/your-project/us-central1/function-name
```

### 3. Deployment
1. Deploy cloud functions: `firebase deploy --only functions`
2. Deploy main API to your preferred platform
3. Update environment variables in production

## Key Features

- **AI-Powered Analysis**: Uses OpenAI GPT models for intelligent data analysis
- **Real-time Updates**: WebSocket support for live job progress
- **Secure Execution**: Docker container isolation for code execution
- **Multi-tenant**: User-based authentication and data isolation
- **Scalable**: Cloud functions for handling large datasets
- **Integration Ready**: Built-in support for Salesforce and Acumatica
- **Comprehensive Reporting**: Generates detailed HTML reports with visualizations 

 python.exe run_execution_api.py 