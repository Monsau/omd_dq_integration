import os
import yaml
from metadata.generated.schema.api.data.createDataContract import CreateDataContractRequest
from metadata.generated.schema.entity.data.dataContract import ContractStatus, SchemaField
from metadata.generated.schema.entity.data.table import Table
from metadata.generated.schema.tests.testCase import TestCase
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.generated.schema.security.client.openMetadataJWTClientConfig import OpenMetadataJWTClientConfig
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import OpenMetadataConnection, AuthProvider
from metadata.generated.schema.entity.data.dataContract import QualityExpectation
import uuid
from dotenv import load_dotenv
import logging
# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
# Load environment variables from .env file
load_dotenv(override=True)

def load_contract_yaml(yaml_path):
    with open(yaml_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def conntect_OMD(OMD_JWT_TOKEN, OMD_API_URL):
    """
    Connect to OpenMetadata using the provided JWT token and API URL.
    """
    security_config = OpenMetadataJWTClientConfig(jwtToken=OMD_JWT_TOKEN)
    server_config = OpenMetadataConnection(
        hostPort=OMD_API_URL,
        securityConfig=security_config,
        authProvider=AuthProvider.openmetadata,
    )
    omd_client = OpenMetadata(server_config)
    omd_client.health_check()
    logger.error(f"[STEP 1] Successfully connected to OpenMetadata at {OMD_API_URL}")
    return omd_client

def create_schema_fields(table_name, contract_dict):
    """
    Create SchemaField objects from the provided schema fields.
    """
    allowed_types = {
            "RECORD", "NULL", "BOOLEAN", "INT", "LONG", "BYTES", "FLOAT", "DOUBLE",
            "TIMESTAMP", "TIMESTAMPZ", "TIME", "DATE", "STRING", "ARRAY", "MAP",
            "ENUM", "UNION", "FIXED", "ERROR", "UNKNOWN"
        }
    type_map = {
            "NUMBER": "DOUBLE",
            "INTEGER": "INT",
            "FLOAT": "FLOAT",
            "DECIMAL": "DOUBLE",
            "STRING": "STRING",
            "BOOLEAN": "BOOLEAN",
            "TIMESTAMP": "TIMESTAMP",
            "DATE": "DATE",
            "ARRAY": "ARRAY",
            "OBJECT": "RECORD",
            "MAP": "MAP"
        }
    schema_fields = []
    for field in contract_dict.get('schema', {}).get('fields', []):
            raw_type = field.get('type', 'STRING').upper()
            dtype = type_map.get(raw_type, raw_type)
            if dtype not in allowed_types:
                dtype = "STRING"
            schema_fields.append(
                SchemaField(
                    name=field.get('name'),
                    dataType=dtype,
                    description=field.get('description'),
                    displayName=field.get('name'),
                    fullyQualifiedName=f"{table_name}.{field.get('name')}",
                    tags=None
                )
            )
    logger.error(f"[STEP 3] Schema Contract defined with {len(schema_fields)} columns.")
    return schema_fields

def create_quality_expectation(contract_dict, quality_expectations):
    """Create QualityExpectation objects from the provided contract dictionary.
    """
    for q in contract_dict.get('quality', []):
        definition = q.get('implementation') or q.get('description') or q.get('name')
        # Only set allowed fields for QualityExpectation
        qe_kwargs = {
            'name': q.get('name'),
                'definition': definition
            }
            # Only add raiseIncident if the model allows it (if not, skip)
        try:
                quality_expectations.append(QualityExpectation(**qe_kwargs))
        except TypeError:
            # If raiseIncident is allowed in your OpenMetadata version, add it here
            pass
    logger.error(f"[STEP 4] Data Quality Contract defined with {len(quality_expectations)} expectations.")

def create_data_contract(table_entity, contract_dict, schema_fields, quality_expectations):
    """
    Create a Data Contract object from the provided table entity and contract dictionary.
    """
    
    logger.error("[STEP 5] Assemble and Submit the Data Contract...")
    try:
            contract_request = CreateDataContractRequest(
                name=f"{table_entity.name.root}_contract",
                displayName=f"{table_entity.displayName} Contract",
                description=contract_dict.get('description', {}).get('purpose', ''),
                entity=EntityReference(id=table_entity.id, type="table"),
                status=ContractStatus.Active,
                schema=schema_fields,
                qualityExpectations=quality_expectations
            )
    except Exception as e:
            logger.error(f"[STEP 5] Error: Failed to assemble Data Contract: {e}")
    if not contract_request or not contract_request.entity:
            logger.error("[STEP 5] Error: Entity is empty. Cannot submit Data Contract.")
            return
    return contract_request

def push_contract_to_openmetadata(contract_request, table_name=None, omd_client=None, OMD_API_URL=None, OMD_JWT_TOKEN=None):
    """
    Push the Data Contract to OpenMetadata using the SDK or REST API.
    """
    logger.error("[STEP 6] Submitting Data Contract to OpenMetadata...")
    logger.error("[STEP 6] Please wait, this may take a few seconds...")
    logger.error(f"[STEP 6] Target Table: {table_name}")

    created_contract = None
    try:
        # Use the REST API as a fallback if the SDK fails
        created_contract = omd_client.create_or_update(contract_request)
    except Exception as e:
            logger.error(f"[STEP 6] Error: Failed to create or update Data Contract via SDK: {e}")
            # Fallback: Try direct REST API upload
            import requests
            import json
            try:
                api_url = f"{OMD_API_URL.rstrip('/')}/v1/dataContracts"
                headers = {
                    "Authorization": f"Bearer {OMD_JWT_TOKEN}",
                    "Content-Type": "application/json"
                }
                # Convert contract_request to dict, then to JSON
                contract_dict_api = contract_request.model_dump() if hasattr(contract_request, 'model_dump') else contract_request.dict()
                # Fix: Convert all enum values and UUIDs to serializable types for JSON serialization

                def enum_to_value(obj):
                    if isinstance(obj, list):
                        return [enum_to_value(i) for i in obj]
                    elif isinstance(obj, dict):
                        return {k: enum_to_value(v) for k, v in obj.items()}
                    elif hasattr(obj, 'value'):
                        return obj.value
                    elif isinstance(obj, uuid.UUID):
                        return str(obj)
                    else:
                        return obj
                contract_dict_api = enum_to_value(contract_dict_api)
                # Remove any keys ending with '_' (e.g., schema_) that are not valid in the API
                def remove_trailing_underscore_keys(obj):
                    if isinstance(obj, list):
                        return [remove_trailing_underscore_keys(i) for i in obj]
                    elif isinstance(obj, dict):
                        # Only keep keys that do not end with '_' and are not private (do not start with '_')
                        return {k.rstrip('_'): remove_trailing_underscore_keys(v) for k, v in obj.items() if not k.endswith('_') and not k.startswith('_')}
                    else:
                        return obj
                contract_dict_api = remove_trailing_underscore_keys(contract_dict_api)
                
                # Try POST first
                response = requests.post(api_url, headers=headers, data=json.dumps(contract_dict_api))
                if response.status_code in (200, 201):
                    logger.error("[STEP 6] --- Success (REST API fallback, POST)! ---")
                    logger.error(f"[STEP 6] Data Contract created via REST API. Status: {response.status_code}")
                    logger.error(f"[STEP 6] View it in OpenMetadata: {OMD_API_URL}/v1/dataContracts")
                else:
                    logger.error(f"[STEP 6] POST failed with status {response.status_code}, trying PUT...")
                    response = requests.put(api_url, headers=headers, data=json.dumps(contract_dict_api))
                    if response.status_code in (200, 201):
                        logger.error("[STEP 6] --- Success (REST API fallback, PUT)! ---")
                        logger.error(f"[STEP 6] Data Contract updated via REST API. Status: {response.status_code}")
                        logger.error(f"[STEP 6] View it in OpenMetadata: {OMD_API_URL}/v1/dataContracts")
                    else:
                        logger.error(f"[STEP 6] --- Error (REST API fallback, PUT) ---")
                        logger.error(f"[STEP 6] Response: {response.status_code} {response.text}")
                return
            except Exception as e2:
                logger.error(f"[STEP 6] Error: REST API fallback also failed: {e2}")
                return
    if created_contract:
        logger.error("[STEP 6] --- Success! ---")
        logger.error(f"[STEP 6] Data Contract '{created_contract.displayName}' is now {created_contract.status.value}.")
        logger.error(f"[STEP 6] Applied to: {created_contract.entity.fullyQualifiedName}")
        logger.error(f"[STEP 6] View it in OpenMetadata: {OMD_API_URL}/v1/dataContracts")
    else:
        logger.error("[STEP 6] --- Error ---")
        logger.error("[STEP 6] Failed to create or update the Data Contract.")

def create_wind_farm_data_contract_from_yaml(yaml_path):
    """
    Load a Data Contract from YAML and apply it to a table in OpenMetadata.
    Prerequisites:
      - The target table must exist in OpenMetadata with a defined schema.
      - Data Quality tests referenced in the contract must already be created.
      - The following environment variables must be set:
        OMD_API_URL, OMD_JWT_TOKEN
    """
    try:
        logger.error("[STEP 1] Configure Connection to OpenMetadata...")
        OMD_JWT_TOKEN = os.environ.get("OMD_JWT_TOKEN")
        OMD_API_URL = os.environ.get("OMD_API_URL")
        omd_client=conntect_OMD(OMD_JWT_TOKEN, OMD_API_URL)
        # Load the contract YAML file
        logger.error(f"[STEP 2] Load contract YAML from {yaml_path} ...")
        contract_dict = load_contract_yaml(yaml_path)
        table_name = os.environ.get("TABLE_FQN")
        if not table_name:
            logger.error("[STEP 2] Error: 'name' field missing in contract YAML.")
            return
        # Check if the table exists in OpenMetadata
        table_entity = omd_client.get_by_name(entity=Table, fqn=table_name, fields=["columns"])
        if not table_entity:
            logger.error(f"[STEP 2] Error: Table '{table_name}' not found. Please create it first.")
            return
        logger.error(f"[STEP 2] Found target data asset: {table_name}")

        # Map YAML schema fields to SchemaField objects
        logger.error("[STEP 3] Create Schema Fields from contract YAML...")
        schema_fields = create_schema_fields(table_name, contract_dict)
        
        # Map YAML quality expectations to OpenMetadata format
        quality_expectations = []
        create_quality_expectation(contract_dict, quality_expectations)
        if not quality_expectations:
            logger.error("[STEP 4] Warning: No valid quality expectations found in the contract YAML.")
            return
        # Assemble the Data Contract request
        contract_request = create_data_contract(table_entity, contract_dict, schema_fields, quality_expectations)
        if not contract_request:
            logger.error("[STEP 5] Error: Failed to create Data Contract request.")
            return
        # Push the Data Contract to OpenMetadata
        push_contract_to_openmetadata(contract_request, table_name, omd_client, OMD_API_URL, OMD_JWT_TOKEN)
        logger.error("[STEP 6] Data Contract successfully created and pushed to OpenMetadata.")
    except Exception as e:
        logger.error(f"\n[STEP 0] Error: An unexpected error occurred: {e}")

if __name__ == "__main__":
    # Path to your contract.yaml file
    create_wind_farm_data_contract_from_yaml("contract.yaml")
