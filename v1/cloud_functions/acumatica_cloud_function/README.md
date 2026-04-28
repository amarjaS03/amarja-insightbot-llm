# Acumatica Cloud Function

This cloud function provides a connector to fetch data from Acumatica ERP and store it in Firebase Storage.

## Features

- **Authentication**: Supports Acumatica session-based authentication
- **Data Fetching**: Retrieves data from Acumatica entities using REST API
- **Subject Areas**: Pre-configured subject areas (financial, sales, inventory)
- **Firebase Storage**: Automatically saves fetched data as pickle files
- **Automatic Cleanup**: Deletes old files when new data is fetched

## Prerequisites

1. Firebase project with Cloud Functions enabled
2. Acumatica instance with API access
3. Google Cloud Secret Manager for storing credentials

## Setup

### 1. Install Dependencies

```bash
cd functions
pip install -r requirements.txt
```

### 2. Configure Credentials in Secret Manager

Store Acumatica credentials in Google Cloud Secret Manager with the following format:

**Secret Name**: `{email}_acumatica` (e.g., `user_example_com_acumatica`)

**Secret Value** (JSON):
```json
{
  "instance_url": "https://your-instance.acumatica.com",
  "username": "your_username",
  "password": "your_password",
  "company": "YourCompany",
  "branch": "YourBranch"
}
```

### 3. Deploy Cloud Function

```bash
# From the acumatica_cloud_function directory
firebase deploy --only functions
```

## API Usage

### Endpoint

POST request to: `https://your-region-your-project.cloudfunctions.net/zingworks_acumatica_connector`

### Request Body

```json
{
  "email": "user@example.com",
  "subject_area": "financial",
  "subject_area_name": "Financial",
  "subject_area_description": "Financial data analysis"
}
```

### Parameters

- `email` (required): User's email address (used to retrieve credentials from Secret Manager)
- `subject_area` (optional): Subject area to fetch. Options:
  - `financial` (default): Customer, Vendor, JournalTransaction
  - `sales`: Customer, SalesOrder, SalesInvoice
  - `inventory`: StockItem, ItemWarehouse, InventoryReceipt
- `subject_area_name` (optional): Display name for the subject area
- `subject_area_description` (optional): Description of the subject area

### Response

```json
{
  "status": "success",
  "message": "Subject area 'financial' extraction complete: 3/3 entities processed, 150 total records",
  "subject_area": {
    "id": "financial",
    "name": "Financial",
    "description": "Financial data analysis",
    "entities": ["Customer", "Vendor", "JournalTransaction"]
  },
  "summary": {
    "total_entities_requested": 3,
    "successful_entities": 3,
    "total_records_extracted": 150
  },
  "results": {
    "Customer": {
      "record_count": 50,
      "dataframe_shape": [50, 20],
      "firebase_pickle_file": "gs://bucket/path/Customer_20250930_120000.pkl",
      "firebase_public_url": "https://...",
      "firebase_file_size": 12345,
      "firebase_save_status": "success",
      "deleted_old_files": [],
      "deleted_files_count": 0,
      "status": "success"
    }
  }
}
```

## Acumatica API Details

### Authentication

Acumatica uses session-based authentication:
1. Login to `/entity/auth/login` with credentials
2. Receive session cookies
3. Use cookies for subsequent API calls
4. Logout from `/entity/auth/logout` when done

### Entity Endpoints

Entities are accessed via: `{instance_url}/entity/Default/23.200.001/{EntityName}`

Example entities:
- Customer
- Vendor
- SalesOrder
- SalesInvoice
- StockItem
- JournalTransaction

### Pagination

Data is fetched using OData-style pagination:
- `$top`: Number of records per page (100)
- `$skip`: Number of records to skip

## Data Storage

### Firebase Storage Structure

```
{user_email_sanitized}/
  └── data/
      └── acumatica/
          ├── Customer_20250930_120000.pkl
          ├── Vendor_20250930_120000.pkl
          └── ...
```

### File Format

Data is stored as pandas DataFrame pickle files (.pkl) for efficient storage and retrieval.

## Security Considerations

1. **Credentials**: Always store credentials in Google Cloud Secret Manager, never in code
2. **CORS**: Currently allows all origins (*) - restrict in production
3. **Authentication**: Add API key authentication before production deployment
4. **File Access**: Files are currently made public - consider private files with signed URLs

## Error Handling

The function handles various error scenarios:
- Invalid credentials
- Network errors
- Missing parameters
- Invalid subject areas
- Entity fetch failures

All errors return appropriate HTTP status codes and error messages.

## Testing

### Local Testing

```bash
# Start Firebase emulators
firebase emulators:start
```

### Production Testing

```bash
# Test with curl
curl -X POST https://your-function-url \
  -H "Content-Type: application/json" \
  -d '{
    "email": "user@example.com",
    "subject_area": "financial"
  }'
```

## Differences from Salesforce Connector

1. **Authentication**: Session-based cookies vs OAuth tokens
2. **API Structure**: REST entities vs SOQL queries
3. **Pagination**: OData-style vs Bulk API jobs
4. **Data Format**: Direct JSON vs CSV conversion
5. **Logout**: Explicit logout required

## TODO

- [ ] Add API key authentication
- [ ] Use UserID instead of email for file paths
- [ ] Add support for custom entity endpoints
- [ ] Add support for OData query parameters
- [ ] Implement retry logic for failed requests
- [ ] Add caching for frequently accessed data
- [ ] Support for custom field selection
- [ ] Add webhook support for real-time updates

## Support

For issues or questions, please contact the development team.
