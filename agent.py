# main.py
import os
from datetime import timedelta
from metadata.generated.schema.api.data.createDataContract import CreateDataContractRequest
from metadata.generated.schema.entity.data.dataContract import DataContract, ContractStatus
from metadata.generated.schema.entity.data.table import Column, DataType, Table
from metadata.generated.schema.tests.testCase import TestCase
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.generated.schema.security.client.openMetadataJWTClientConfig import OpenMetadataJWTClientConfig
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import OpenMetadataConnection, AuthProvider
from dotenv import load_dotenv
# Load environment variables from .env file
load_dotenv(override=True)
def create_wind_farm_data_contract():
    """
    This agent script creates and applies the Wind Farm Data Contract to a table
    in OpenMetadata, based on the specifications in the playbook.
    
    This version dynamically fetches the schema from the existing table metadata.
    
    Prerequisites:
    1. The target table must exist in OpenMetadata with a defined schema.
    2. The Data Quality tests referenced in the contract must already be created
       and associated with the target table.
    3. The following environment variables must be set:
       - OPENMETADATA_URL: The URL of your OpenMetadata server (e.g., http://localhost:8585/api)
       - OPENMETADATA_JWT_TOKEN: The JWT token for authentication.
    """
    try:
        # --- 1. Configure Connection to OpenMetadata ---
        OMD_JWT_TOKEN = os.environ.get("OMD_JWT_TOKEN")
        OMD_API_URL = os.environ.get("OMD_API_URL")
        TABLE_FQN = os.environ.get("TABLE_FQN")
        security_config = OpenMetadataJWTClientConfig(jwtToken=OMD_JWT_TOKEN)
        server_config = OpenMetadataConnection(
            hostPort=OMD_API_URL,
            securityConfig=security_config,
            authProvider=AuthProvider.openmetadata,
        )
        omd_client = OpenMetadata(server_config)
        omd_client.health_check()

        # --- 2. Define Target Asset and Required Test Cases ---
        # The FQN (Fully Qualified Name) of the table the contract applies to.
        # Replace with your actual table FQN.
        table_entity = omd_client.get_by_name(entity=Table, fqn=TABLE_FQN, fields=["columns"])

        if not table_entity:
            print(f"Error: Table '{TABLE_FQN}' not found. Please create it first.")
            return

        print(f"Found target data asset: {TABLE_FQN}")
        # Get references to the Data Quality tests defined in the playbook.
        # These tests MUST exist before running this script.
        print("Fetching required Data Quality tests...")
        quality_test_fqns = [
            f"{TABLE_FQN}.id.columnValuesToBeUnique",
            f"{TABLE_FQN}.installed_capacity_pos.columnValueMinToBe",
            f"{TABLE_FQN}.location.geo_coordinate.latitude.columnValueToBeBetween",
            f"{TABLE_FQN}.location.geo_coordinate.longitude.columnValueToBeBetween",
            f"{TABLE_FQN}.grid_properties.tso_code.columnValuesToMatchRegex",
            f"{TABLE_FQN}.wind_farm_properties.hub_height.columnValueToBeBetween"
        ]
        
        test_references = []
        for test_fqn in quality_test_fqns:
            test_case = omd_client.get_by_name(entity=TestCase, fqn=test_fqn)
            if test_case:
                test_references.append(EntityReference(id=test_case.id, type="testCase"))
                print(f"  - Found test: {test_fqn}")
            else:
                print(f"  - Warning: Test case '{test_fqn}' not found. It will be skipped in the contract.")

        # --- 3. Construct the Data Contract from the Playbook ---
        print("\nConstructing Data Contract...")
        
        # Part A: Schema Contract (Dynamically fetched from existing table metadata)
        print("  - Fetching schema dynamically from the database...")
        if not table_entity.columns:
            print("Error: No columns found for the table. Cannot create schema contract.")
            return
        print("  - Schema Contract defined with the following columns:")
        for column in table_entity.columns:
            print(f"    - {column.name} ({column.dataType.value})") 
        # Define the schema contract using the table's columns
        # Note: Ensure that the columns are of type Column and have valid data types.
        # This is a simplified example; you may need to adjust based on your actual column definitions.
           
        try:
            schema_contract = DataContract.Schema(columns=table_entity.columns)
            print(f"  - Schema Contract defined with {len(table_entity.columns)} columns.")
        except Exception as e:
            print(f"Error creating schema contract: {e} {schema_contract}")
            return
        # Note: The schema contract can be more complex, including nested structures, etc.
        # Part B: Data Quality Contract
        quality_contract = DataContract.Quality(tests=test_references)
        print(f"  - Data Quality Contract defined with {len(test_references)} tests.")

        # Part C: Data Freshness Contract (24 hours)
        freshness_contract = DataContract.Freshness(
            updateFrequency=timedelta(days=1)
        )
        print("  - Data Freshness Contract defined for updates every 24 hours.")

        # Assemble the full contract request
        contract_request = CreateDataContractRequest(
            name=f"{table_entity.name.root}_contract",
            displayName=f"{table_entity.displayName} Contract",
            description=f"Data Contract for the {table_entity.displayName} data asset, enforced by an automated agent.",
            dataAsset=EntityReference(id=table_entity.id, type="table"),
            status=ContractStatus.Active, # Set to Draft for review, or Active to enforce immediately
            schema=schema_contract,
            quality=quality_contract,
            freshness=freshness_contract
        )

        # --- 4. Create or Update the Data Contract in OpenMetadata ---
        print("\nSubmitting Data Contract to OpenMetadata...")
        created_contract = omd_client.create_or_update(data=contract_request)

        if created_contract:
            print("\n--- Success! ---")
            print(f"Data Contract '{created_contract.displayName}' is now {created_contract.status.value}.")
            print(f"Applied to: {created_contract.dataAsset.fullyQualifiedName}")
            print(f"View it in OpenMetadata: {OMD_API_URL.replace('/api', '')}/table/{TABLE_FQN}/contracts")
        else:
            print("\n--- Error ---")
            print("Failed to create or update the Data Contract.")

    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")

if __name__ == "__main__":
    create_wind_farm_data_contract()
