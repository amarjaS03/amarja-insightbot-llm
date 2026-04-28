"""
Data Service for Salesforce, Acumatica, MySQL, and MSSQL
Creates queries and URLs based on subject areas (Salesforce/Acumatica) or table names (MySQL/MSSQL)
"""
from typing import Dict, List, Optional, Any
from v2.modules.connector_framework.config.subject_area_entity import (
    salesforce_subject_area_entities,
    acumatica_subject_area_entities
)


# Subject area to Salesforce objects mapping
# This can be extended to include more subject areas like 'manufacturing', 'service', etc.
SALESFORCE_SUBJECT_AREA_OBJECTS = {
    'sales': ['Account', 'Opportunity'],
    # Add more subject areas as needed:
    # 'manufacturing': ['Product2', 'WorkOrder', 'Asset'],
    # 'service': ['Case', 'ServiceAppointment', 'WorkOrder'],
    # 'marketing': ['Campaign', 'Lead', 'Contact'],
}


def create_salesforce_queries_for_subject_area(
    subject_area: str,
    limit: Optional[int] = None,
    where_clause: Optional[str] = None
) -> Dict[str, str]:
    """
    Create SOQL queries for all objects in a given Salesforce subject area.
    
    This method maps a subject area (e.g., 'sales', 'manufacturing') to Salesforce objects
    and generates SOQL queries for each object using the field configurations from
    salesforce_subject_area_entities.
    
    Args:
        subject_area: The subject area identifier (e.g., 'sales', 'manufacturing')
        limit: Optional limit for number of records to fetch per query
        where_clause: Optional WHERE clause to filter records (e.g., "CreatedDate >= LAST_N_DAYS:30")
    
    Returns:
        Dictionary mapping object names to their SOQL queries
        Example: {
            'Account': 'SELECT AccountNumber, Name, ... FROM Account',
            'Opportunity': 'SELECT AccountId, Amount, ... FROM Opportunity'
        }
    
    Raises:
        ValueError: If subject_area is not supported or if objects are not configured
    
    Example:
        >>> queries = create_salesforce_queries_for_subject_area('sales')
        >>> print(queries['Account'])
        SELECT AccountNumber, AccountSource, AnnualRevenue, ... FROM Account
    """
    # Validate subject area
    if subject_area not in SALESFORCE_SUBJECT_AREA_OBJECTS:
        print(f"subject_area: {subject_area}")
        supported_areas = list(SALESFORCE_SUBJECT_AREA_OBJECTS.keys())
        raise ValueError(
            f"Subject area '{subject_area}' is not supported. "
            f"Supported areas: {supported_areas}"
        )
    
    # Get objects for the subject area
    sobject_names = SALESFORCE_SUBJECT_AREA_OBJECTS[subject_area]
    print(f"sobject_names: {sobject_names}")
    if not sobject_names:
        raise ValueError(
            f"No objects configured for subject area '{subject_area}'"
        )
    
    queries = {}
    
    # Generate SOQL query for each object
    for sobject_name in sobject_names:
        # Get entity configuration
        entity_config = salesforce_subject_area_entities.get(sobject_name)
        
        if not entity_config:
            raise ValueError(
                f"Entity configuration not found for '{sobject_name}'. "
                f"Please add it to salesforce_subject_area_entities in subject_area_entity.py"
            )
        
        # Get select fields from configuration
        select_fields = entity_config.get('select_fields', [])
        print(f"select_fields: {select_fields}")
        if not select_fields:
            raise ValueError(
                f"No select_fields configured for '{sobject_name}'"
            )
        
        # Build SELECT clause
        fields_str = ', '.join(select_fields)
        
        # Build SOQL query
        soql_query = f"SELECT {fields_str} FROM {sobject_name}"
        
        # Add WHERE clause if provided
        if where_clause:
            soql_query += f" WHERE {where_clause}"
        
        # Add LIMIT clause if provided
        if limit:
            soql_query += f" LIMIT {limit}"
        
        queries[sobject_name] = soql_query
    
    return queries


def get_salesforce_objects_for_subject_area(subject_area: str) -> List[str]:
    """
    Get list of Salesforce objects for a given subject area.
    
    Args:
        subject_area: The subject area identifier (e.g., 'sales', 'manufacturing')
    
    Returns:
        List of Salesforce object names for the subject area
    
    Raises:
        ValueError: If subject_area is not supported
    
    Example:
        >>> objects = get_salesforce_objects_for_subject_area('sales')
        >>> print(objects)
        ['Account', 'Opportunity']
    """
    if subject_area not in SALESFORCE_SUBJECT_AREA_OBJECTS:
        supported_areas = list(SALESFORCE_SUBJECT_AREA_OBJECTS.keys())
        raise ValueError(
            f"Subject area '{subject_area}' is not supported. "
            f"Supported areas: {supported_areas}"
        )
    
    return SALESFORCE_SUBJECT_AREA_OBJECTS[subject_area].copy()


def get_salesforce_fields_for_object(object_name: str) -> List[str]:
    """
    Get list of fields configured for a Salesforce object.
    
    Args:
        object_name: Name of the Salesforce object (e.g., 'Account', 'Opportunity')
    
    Returns:
        List of field names configured for the object
    
    Raises:
        ValueError: If object_name is not configured
    
    Example:
        >>> fields = get_salesforce_fields_for_object('Account')
        >>> print(fields[:5])
        ['AccountNumber', 'AccountSource', 'AnnualRevenue', 'BillingCity', 'BillingCountry']
    """
    entity_config = salesforce_subject_area_entities.get(object_name)
    
    if not entity_config:
        raise ValueError(
            f"Entity configuration not found for '{object_name}'. "
            f"Please add it to salesforce_subject_area_entities in subject_area_entity.py"
        )
    
    select_fields = entity_config.get('select_fields', [])
    
    if not select_fields:
        raise ValueError(
            f"No select_fields configured for '{object_name}'"
        )

    print(f"select_fields: {select_fields}")
    
    return select_fields.copy()


def create_salesforce_query_for_object(
    object_name: str,
    limit: Optional[int] = None,
    where_clause: Optional[str] = None,
    custom_fields: Optional[List[str]] = None
) -> str:

    """
    Create a SOQL query for a specific Salesforce object.
    
    Args:
        object_name: Name of the Salesforce object (e.g., 'Account', 'Opportunity')
        limit: Optional limit for number of records to fetch
        where_clause: Optional WHERE clause to filter records
        custom_fields: Optional list of custom fields to use instead of configured fields
    
    Returns:
        SOQL query string
    
    Raises:
        ValueError: If object_name is not configured or if no fields are available
    
    Example:
        >>> query = create_salesforce_query_for_object('Account', limit=100)
        >>> print(query)
        SELECT AccountNumber, AccountSource, ... FROM Account LIMIT 100
    """
    # Use custom fields if provided, otherwise get from configuration
    if custom_fields:
        select_fields = custom_fields
    else:
        select_fields = get_salesforce_fields_for_object(object_name)
    
    if not select_fields:
        raise ValueError(f"No fields available for '{object_name}'")
    
    # Build SELECT clause
    fields_str = ', '.join(select_fields)
    
    # Build SOQL query
    soql_query = f"SELECT {fields_str} FROM {object_name}"
    
    # Add WHERE clause if provided
    if where_clause:
        soql_query += f" WHERE {where_clause}"
    
    # Add LIMIT clause if provided
    if limit:
        soql_query += f" LIMIT {limit}"
    
    print(f"soql_query: {soql_query}")
    return soql_query


# Subject area to Acumatica entities mapping
# This can be extended to include more subject areas
ACUMATICA_SUBJECT_AREA_ENTITIES = {
    'Financial': ['Customer', 'Vendor', 'JournalTransaction'],
    'Sales': ['SalesOrder', 'SalesInvoice', 'Shipment', 'Invoice'],
    'Inventory': ['StockItem'],  # Removed ItemWarehouse - StockItem already provides warehouse.pkl
}


def create_acumatica_url_for_entity(
    tenant_url: str,
    entity_name: str,
    api_version: str = "23.200.001",
    company: str = "Default",
    custom_fields: Optional[List[str]] = None,
    custom_expand: Optional[str] = None,
    return_relative_path: bool = False
) -> str:
    """
    Create Acumatica API URL or relative path for a specific entity with select fields and expand parameters.
    
    This method builds an Acumatica REST API URL using entity configurations from
    acumatica_subject_area_entities, similar to how the cloud function does it.
    
    Args:
        tenant_url: Acumatica tenant URL (e.g., 'https://company.acumatica.com')
        entity_name: Name of the Acumatica entity (e.g., 'StockItem', 'SalesOrder')
        api_version: API version (default: '23.200.001')
        company: Company/branch name (default: 'Default')
        custom_fields: Optional list of custom fields to use instead of configured fields
        custom_expand: Optional expand string to use instead of configured expand
        return_relative_path: If True, returns relative path only (for Connectzify connector).
                             If False, returns full URL (for direct API calls).
    
    Returns:
        If return_relative_path=True: Relative endpoint path with query parameters
        Example: '/entity/Default/23.200.001/StockItem?$select=InventoryID,Description,...&$expand=WarehouseDetails'
        
        If return_relative_path=False: Complete Acumatica API URL with query parameters
        Example: 'https://company.acumatica.com/entity/Default/23.200.001/StockItem?$select=InventoryID,Description,...&$expand=WarehouseDetails'
    
    Raises:
        ValueError: If entity_name is not configured or if no fields are available
    
    Example:
        >>> url = create_acumatica_url_for_entity('https://company.acumatica.com', 'StockItem')
        >>> print(url)
        https://company.acumatica.com/entity/Default/23.200.001/StockItem?$select=InventoryID,Description,...&$expand=WarehouseDetails
        
        >>> path = create_acumatica_url_for_entity('https://company.acumatica.com', 'StockItem', return_relative_path=True)
        >>> print(path)
        /entity/Default/23.200.001/StockItem?$select=InventoryID,Description,...&$expand=WarehouseDetails
    """
    # Get entity configuration
    entity_config = acumatica_subject_area_entities.get(entity_name)
    
    if not entity_config:
        raise ValueError(
            f"Entity configuration not found for '{entity_name}'. "
            f"Please add it to acumatica_subject_area_entities in subject_area_entity.py"
        )
    
    # Get select fields - use custom if provided, otherwise from configuration
    if custom_fields:
        select_fields = custom_fields
    else:
        select_fields = entity_config.get('select_fields', [])
    
    if not select_fields:
        raise ValueError(
            f"No select_fields configured for '{entity_name}'"
        )
    
    # Get expand - use custom if provided, otherwise from configuration
    if custom_expand is not None:
        expand = custom_expand
    else:
        expand = entity_config.get('expand')
    
    # Build relative endpoint path (Connectzify connector will prepend tenant_url)
    endpoint_path = f'/entity/{company}/{api_version}/{entity_name}'
    
    # Build query parameters
    query_params = []
    
    # Add $select parameter
    if select_fields:
        select_string = ",".join(select_fields)
        query_params.append(f"$select={select_string}")
    
    # Add $expand parameter if expand is configured
    if expand:
        query_params.append(f"$expand={expand}")
    
    # Add query string if we have parameters
    if query_params:
        endpoint_path += '?' + '&'.join(query_params)
    
    # Return relative path for Connectzify connector, or full URL for direct API calls
    if return_relative_path:
        return endpoint_path
    else:
        # Build full URL by combining tenant_url with endpoint_path
        # Remove trailing slash from tenant_url if present
        base_url = tenant_url.rstrip('/')
        return f'{base_url}{endpoint_path}'


def create_acumatica_urls_for_subject_area(
    tenant_url: str,
    subject_area: str,
    api_version: str = "23.200.001",
    company: str = "Default",
    return_relative_paths: bool = True
) -> Dict[str, str]:
    """
    Create Acumatica API URLs or relative paths for all entities in a given subject area.
    
    This method maps a subject area (e.g., 'Sales', 'Inventory', 'Financial') to Acumatica entities
    and generates API URLs or relative paths for each entity using the field configurations from
    acumatica_subject_area_entities.
    
    Args:
        tenant_url: Acumatica tenant URL (e.g., 'https://company.acumatica.com')
        subject_area: The subject area identifier (e.g., 'Sales', 'Inventory', 'Financial')
        api_version: API version (default: '23.200.001')
        company: Company/branch name (default: 'Default')
        return_relative_paths: If True, returns relative paths only (for Connectzify connector).
                               If False, returns full URLs (for direct API calls).
    
    Returns:
        Dictionary mapping entity names to their API URLs or relative paths
        If return_relative_paths=True:
            Example: {
                'SalesOrder': '/entity/Default/23.200.001/SalesOrder?$select=...&$expand=...',
                'SalesInvoice': '/entity/Default/23.200.001/SalesInvoice?$select=...&$expand=...'
            }
        If return_relative_paths=False:
            Example: {
                'SalesOrder': 'https://company.acumatica.com/entity/Default/23.200.001/SalesOrder?$select=...&$expand=...',
                'SalesInvoice': 'https://company.acumatica.com/entity/Default/23.200.001/SalesInvoice?$select=...&$expand=...'
            }
    
    Raises:
        ValueError: If subject_area is not supported or if entities are not configured
    
    Example:
        >>> paths = create_acumatica_urls_for_subject_area('https://company.acumatica.com', 'Sales')
        >>> print(paths['SalesOrder'])
        /entity/Default/23.200.001/SalesOrder?$select=...&$expand=...
    """
    # Validate subject area
    if subject_area not in ACUMATICA_SUBJECT_AREA_ENTITIES:
        supported_areas = list(ACUMATICA_SUBJECT_AREA_ENTITIES.keys())
        raise ValueError(
            f"Subject area '{subject_area}' is not supported. "
            f"Supported areas: {supported_areas}"
        )
    
    # Get entities for the subject area
    entity_names = ACUMATICA_SUBJECT_AREA_ENTITIES[subject_area]
    
    if not entity_names:
        raise ValueError(
            f"No entities configured for subject area '{subject_area}'"
        )
    
    urls = {}
    
    # Generate API URL or relative path for each entity
    for entity_name in entity_names:
        url = create_acumatica_url_for_entity(
            tenant_url=tenant_url,
            entity_name=entity_name,
            api_version=api_version,
            company=company,
            return_relative_path=return_relative_paths
        )
        urls[entity_name] = url
    
    return urls


def get_acumatica_entities_for_subject_area(subject_area: str) -> List[str]:
    """
    Get list of Acumatica entities for a given subject area.
    
    Args:
        subject_area: The subject area identifier (e.g., 'Sales', 'Inventory', 'Financial')
    
    Returns:
        List of Acumatica entity names for the subject area
    
    Raises:
        ValueError: If subject_area is not supported
    
    Example:
        >>> entities = get_acumatica_entities_for_subject_area('Sales')
        >>> print(entities)
        ['SalesOrder', 'SalesInvoice', 'Shipment', 'Invoice']
    """
    if subject_area not in ACUMATICA_SUBJECT_AREA_ENTITIES:
        supported_areas = list(ACUMATICA_SUBJECT_AREA_ENTITIES.keys())
        raise ValueError(
            f"Subject area '{subject_area}' is not supported. "
            f"Supported areas: {supported_areas}"
        )
    
    return ACUMATICA_SUBJECT_AREA_ENTITIES[subject_area].copy()


def get_acumatica_fields_for_entity(entity_name: str) -> List[str]:
    """
    Get list of fields configured for an Acumatica entity.
    
    Args:
        entity_name: Name of the Acumatica entity (e.g., 'StockItem', 'SalesOrder')
    
    Returns:
        List of field names configured for the entity
    
    Raises:
        ValueError: If entity_name is not configured
    
    Example:
        >>> fields = get_acumatica_fields_for_entity('StockItem')
        >>> print(fields[:5])
        ['InventoryID', 'Description', 'ItemClass', 'BaseUOM', 'DefaultPrice']
    """
    entity_config = acumatica_subject_area_entities.get(entity_name)
    
    if not entity_config:
        raise ValueError(
            f"Entity configuration not found for '{entity_name}'. "
            f"Please add it to acumatica_subject_area_entities in subject_area_entity.py"
        )
    
    select_fields = entity_config.get('select_fields', [])
    
    if not select_fields:
        raise ValueError(
            f"No select_fields configured for '{entity_name}'"
        )
    
    return select_fields.copy()


def get_acumatica_expand_for_entity(entity_name: str) -> Optional[str]:
    """
    Get expand configuration for an Acumatica entity.
    
    Args:
        entity_name: Name of the Acumatica entity (e.g., 'StockItem', 'SalesOrder')
    
    Returns:
        Expand string if configured, None otherwise
    
    Raises:
        ValueError: If entity_name is not configured
    
    Example:
        >>> expand = get_acumatica_expand_for_entity('StockItem')
        >>> print(expand)
        WarehouseDetails
    """
    entity_config = acumatica_subject_area_entities.get(entity_name)
    
    if not entity_config:
        raise ValueError(
            f"Entity configuration not found for '{entity_name}'. "
            f"Please add it to acumatica_subject_area_entities in subject_area_entity.py"
        )
    
    return entity_config.get('expand')


# ============================================================================
# MySQL and MSSQL Query Generation Functions
# ============================================================================

def create_mysql_query_for_table(
    table_name: str,
    limit: Optional[int] = None,
    where_clause: Optional[str] = None,
    custom_fields: Optional[List[str]] = None
) -> str:
    """
    Create a MySQL SELECT query for a specific table.
    
    Args:
        table_name: Name of the MySQL table
        limit: Optional limit for number of records to fetch
        where_clause: Optional WHERE clause to filter records (e.g., "id > 100")
        custom_fields: Optional list of custom fields to select. If not provided, selects all (*)
    
    Returns:
        MySQL SELECT query string
        Example: 'SELECT * FROM users WHERE id > 100 LIMIT 1000'
    
    Raises:
        ValueError: If table_name is empty
    
    Example:
        >>> query = create_mysql_query_for_table('users', limit=100, where_clause='status = "active"')
        >>> print(query)
        SELECT * FROM users WHERE status = "active" LIMIT 100
    """
    if not table_name or not table_name.strip():
        raise ValueError("table_name is required and cannot be empty")
    
    # Sanitize table name (remove any potential SQL injection characters)
    table_name = table_name.strip().replace('`', '')
    
    # Build SELECT clause
    if custom_fields and len(custom_fields) > 0:
        # Escape field names and join them
        fields = [f"`{field.strip().replace('`', '')}`" for field in custom_fields if field.strip()]
        if not fields:
            fields = ["*"]
        select_clause = ", ".join(fields)
    else:
        select_clause = "*"
    
    # Build base query
    query = f"SELECT {select_clause} FROM `{table_name}`"
    
    # Add WHERE clause if provided
    if where_clause:
        query += f" WHERE {where_clause}"
    
    # Add LIMIT clause if provided
    if limit is not None and limit > 0:
        query += f" LIMIT {limit}"
    
    return query


def create_mssql_query_for_table(
    table_name: str,
    limit: Optional[int] = None,
    where_clause: Optional[str] = None,
    custom_fields: Optional[List[str]] = None
) -> str:
    """
    Create a MSSQL (SQL Server) SELECT query for a specific table.
    
    Args:
        table_name: Name of the MSSQL table
        limit: Optional limit for number of records to fetch (uses TOP clause)
        where_clause: Optional WHERE clause to filter records (e.g., "id > 100")
        custom_fields: Optional list of custom fields to select. If not provided, selects all (*)
    
    Returns:
        MSSQL SELECT query string
        Example: 'SELECT TOP 1000 * FROM users WHERE id > 100'
    
    Raises:
        ValueError: If table_name is empty
    
    Example:
        >>> query = create_mssql_query_for_table('users', limit=100, where_clause='status = ''active''')
        >>> print(query)
        SELECT TOP 100 * FROM users WHERE status = 'active'
    """
    if not table_name or not table_name.strip():
        raise ValueError("table_name is required and cannot be empty")
    
    # Sanitize table name (remove any potential SQL injection characters)
    table_name = table_name.strip().replace('[', '').replace(']', '')
    
    # Build SELECT clause
    if custom_fields and len(custom_fields) > 0:
        # Escape field names and join them
        fields = [f"[{field.strip().replace('[', '').replace(']', '')}]" for field in custom_fields if field.strip()]
        if not fields:
            fields = ["*"]
        select_clause = ", ".join(fields)
    else:
        select_clause = "*"
    
    # Build base query with TOP clause for MSSQL
    if limit is not None and limit > 0:
        query = f"SELECT TOP {limit} {select_clause} FROM [{table_name}]"
    else:
        query = f"SELECT {select_clause} FROM [{table_name}]"
    
    # Add WHERE clause if provided
    if where_clause:
        query += f" WHERE {where_clause}"
    
    return query


def create_mysql_queries_for_tables(
    table_names: List[str],
    limit: Optional[int] = None,
    where_clause: Optional[str] = None,
    custom_fields_map: Optional[Dict[str, List[str]]] = None
) -> Dict[str, str]:
    """
    Create MySQL SELECT queries for multiple tables.
    Returns format: {table_name: query} - same as Salesforce function.
    
    Args:
        table_names: List of table names
        limit: Optional limit for number of records to fetch per table
        where_clause: Optional WHERE clause to apply to all tables
        custom_fields_map: Optional dict mapping table names to their custom field lists
            Example: {'users': ['id', 'name', 'email'], 'orders': ['id', 'user_id', 'total']}
    
    Returns:
        Dictionary mapping table names to their MySQL queries
        Format: {table_name: query} - same as Salesforce
        Example: {
            'users': 'SELECT * FROM `users` LIMIT 1000',
            'orders': 'SELECT `id`, `user_id`, `total` FROM `orders` LIMIT 1000'
        }
    
    Raises:
        ValueError: If table_names is empty
    
    Example:
        >>> queries = create_mysql_queries_for_tables(['users', 'orders'], limit=100)
        >>> print(queries)  # Same format as Salesforce: {table_name: query}
        {'users': 'SELECT * FROM `users` LIMIT 100', 'orders': 'SELECT * FROM `orders` LIMIT 100'}
    """
    if not table_names:
        raise ValueError("table_names list cannot be empty")
    
    queries = {}
    
    for table_name in table_names:
        # Get custom fields for this table if provided
        custom_fields = None
        if custom_fields_map and table_name in custom_fields_map:
            custom_fields = custom_fields_map[table_name]
        
        # Generate query for this table
        query = create_mysql_query_for_table(
            table_name=table_name,
            limit=limit,
            where_clause=where_clause,
            custom_fields=custom_fields
        )
        queries[table_name] = query
    
    return queries


def create_mssql_queries_for_tables(
    table_names: List[str],
    limit: Optional[int] = None,
    where_clause: Optional[str] = None,
    custom_fields_map: Optional[Dict[str, List[str]]] = None
) -> Dict[str, str]:
    """
    Create MSSQL SELECT queries for multiple tables.
    Returns format: {table_name: query} - same as Salesforce function.
    
    Args:
        table_names: List of table names
        limit: Optional limit for number of records to fetch per table (uses TOP clause)
        where_clause: Optional WHERE clause to apply to all tables
        custom_fields_map: Optional dict mapping table names to their custom field lists
            Example: {'users': ['id', 'name', 'email'], 'orders': ['id', 'user_id', 'total']}
    
    Returns:
        Dictionary mapping table names to their MSSQL queries
        Format: {table_name: query} - same as Salesforce
        Example: {
            'users': 'SELECT TOP 1000 * FROM [users]',
            'orders': 'SELECT TOP 1000 [id], [user_id], [total] FROM [orders]'
        }
    
    Raises:
        ValueError: If table_names is empty
    
    Example:
        >>> queries = create_mssql_queries_for_tables(['users', 'orders'], limit=100)
        >>> print(queries)  # Same format as Salesforce: {table_name: query}
        {'users': 'SELECT TOP 100 * FROM [users]', 'orders': 'SELECT TOP 100 * FROM [orders]'}
    """
    if not table_names:
        raise ValueError("table_names list cannot be empty")
    
    queries = {}
    
    for table_name in table_names:
        # Get custom fields for this table if provided
        custom_fields = None
        if custom_fields_map and table_name in custom_fields_map:
            custom_fields = custom_fields_map[table_name]
        
        # Generate query for this table
        query = create_mssql_query_for_table(
            table_name=table_name,
            limit=limit,
            where_clause=where_clause,
            custom_fields=custom_fields
        )
        queries[table_name] = query
    
    return queries

