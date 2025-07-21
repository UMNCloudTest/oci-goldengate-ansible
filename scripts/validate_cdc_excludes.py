#!/usr/bin/env python3
"""
CDC Field Exclude Validation Script

This script connects to Databricks, queries the cdc_field_exclude_list table,
and validates that GoldenGate extract configurations include required COLEXC
statements for fields that should be excluded from CDC.

Requirements:
- databricks-sql-connector
- Environment variables: DATABRICKS_SERVER_HOSTNAME, DATABRICKS_HTTP_PATH, DATABRICKS_ACCESS_TOKEN

Usage:
    python validate_cdc_excludes.py [--config-path CONFIG_PATH] [--environment ENV]
"""

import json
import os
import sys
import argparse
import re
from typing import Dict, List, Set, Tuple
from databricks import sql


class CDCValidationError(Exception):
    """Custom exception for CDC validation failures"""
    pass


class CDCExcludeValidator:
    def __init__(self, databricks_hostname: str, databricks_http_path: str, databricks_token: str):
        """
        Initialize the CDC Exclude Validator
        
        Args:
            databricks_hostname: Databricks workspace hostname
            databricks_http_path: HTTP path for SQL warehouse
            databricks_token: Databricks access token
        """
        self.databricks_hostname = databricks_hostname
        self.databricks_http_path = databricks_http_path
        self.databricks_token = databricks_token
        self.exclude_fields = {}  # Will store {table_name: [field_names]}
        
    def connect_to_databricks(self):
        """Establish connection to Databricks"""
        try:
            self.connection = sql.connect(
                server_hostname=self.databricks_hostname,
                http_path=self.databricks_http_path,
                access_token=self.databricks_token
            )
            print("✅ Successfully connected to Databricks")
            return True
        except Exception as e:
            print(f"❌ Failed to connect to Databricks: {str(e)}")
            return False
    
    def get_exclude_fields(self) -> Dict[str, List[str]]:
        """
        Query Databricks for fields that should be excluded from CDC
        
        Returns:
            Dictionary mapping table names to list of field names to exclude
        """
        query = """
        SELECT 
            table_name,
            field_name
        FROM cdc_field_exclude_list
        WHERE active = true
        ORDER BY table_name, field_name
        """
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            
            # Group results by table name
            exclude_map = {}
            for row in results:
                table_name = row[0].upper()  # Convert to uppercase for matching
                field_name = row[1].upper()  # Convert to uppercase for matching
                
                if table_name not in exclude_map:
                    exclude_map[table_name] = []
                exclude_map[table_name].append(field_name)
            
            print(f"✅ Retrieved exclude fields for {len(exclude_map)} tables")
            for table, fields in exclude_map.items():
                print(f"   {table}: {', '.join(fields)}")
            
            self.exclude_fields = exclude_map
            return exclude_map
            
        except Exception as e:
            raise CDCValidationError(f"Failed to query cdc_field_exclude_list: {str(e)}")
    
    def parse_extract_config(self, config_path: str) -> Dict:
        """
        Parse the extracts.json configuration file
        
        Args:
            config_path: Path to the extracts.json file
            
        Returns:
            Parsed JSON configuration
        """
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
            print(f"✅ Successfully parsed extract configuration: {config_path}")
            return config
        except FileNotFoundError:
            raise CDCValidationError(f"Configuration file not found: {config_path}")
        except json.JSONDecodeError as e:
            raise CDCValidationError(f"Invalid JSON in configuration file: {str(e)}")
    
    def extract_table_configs(self, extract_config: Dict) -> List[Tuple[str, str, str]]:
        """
        Extract TABLE configurations from extract config
        
        Args:
            extract_config: Parsed extract configuration
            
        Returns:
            List of tuples: (extract_name, table_statement, full_config_text)
        """
        table_configs = []
        
        for extract in extract_config.get('extracts', []):
            extract_name = extract.get('name', 'UNKNOWN')
            
            # Look for TABLE statements in the config
            # This could be in various places - we'll check common locations
            config_sections = []
            
            # Check if there's a raw config section
            if 'raw_config' in extract.get('config', {}):
                config_sections.append(extract['config']['raw_config'])
            
            # Check if there's a parameters section
            if 'parameters' in extract.get('config', {}):
                if isinstance(extract['config']['parameters'], str):
                    config_sections.append(extract['config']['parameters'])
                elif isinstance(extract['config']['parameters'], list):
                    config_sections.extend(extract['config']['parameters'])
            
            # Check for TABLE statements in any string values within config
            def find_table_statements(obj, prefix=""):
                if isinstance(obj, str):
                    # Look for TABLE statements in the string
                    table_pattern = r'TABLE\s+[\w\.\*]+[^\n]*'
                    matches = re.finditer(table_pattern, obj, re.IGNORECASE | re.MULTILINE)
                    for match in matches:
                        table_statement = match.group(0).strip()
                        table_configs.append((extract_name, table_statement, obj))
                elif isinstance(obj, dict):
                    for key, value in obj.items():
                        find_table_statements(value, f"{prefix}.{key}" if prefix else key)
                elif isinstance(obj, list):
                    for i, item in enumerate(obj):
                        find_table_statements(item, f"{prefix}[{i}]" if prefix else f"[{i}]")
            
            # Search through the entire config
            find_table_statements(extract.get('config', {}))
        
        print(f"✅ Found {len(table_configs)} TABLE statements across all extracts")
        for extract_name, table_stmt, _ in table_configs:
            print(f"   {extract_name}: {table_stmt[:60]}{'...' if len(table_stmt) > 60 else ''}")
        
        return table_configs
    
    def extract_table_name(self, table_statement: str) -> str:
        """
        Extract the actual table name from a TABLE statement
        
        Args:
            table_statement: The TABLE statement string
            
        Returns:
            The table name (without schema prefix if present)
        """
        # Pattern to match TABLE statement and extract table name
        # Handles cases like: TABLE SCHEMA.TABLE_NAME, TABLE TABLE_NAME, etc.
        pattern = r'TABLE\s+([\w\.]+)'
        match = re.search(pattern, table_statement, re.IGNORECASE)
        
        if match:
            table_ref = match.group(1)
            # If there's a schema prefix, get just the table name
            if '.' in table_ref:
                table_name = table_ref.split('.')[-1]
            else:
                table_name = table_ref
            return table_name.upper()
        
        return ""
    
    def validate_colexc_statements(self, table_configs: List[Tuple[str, str, str]]) -> List[str]:
        """
        Validate that required COLEXC statements are present
        
        Args:
            table_configs: List of table configurations to validate
            
        Returns:
            List of validation errors
        """
        errors = []
        
        for extract_name, table_statement, full_config in table_configs:
            table_name = self.extract_table_name(table_statement)
            
            if not table_name:
                errors.append(f"❌ {extract_name}: Could not extract table name from: {table_statement}")
                continue
            
            # Check if this table has required exclude fields
            if table_name in self.exclude_fields:
                required_fields = self.exclude_fields[table_name]
                print(f"🔍 Validating {extract_name}: {table_name} (requires COLEXC for: {', '.join(required_fields)})")
                
                # Check if COLEXC statements are present for required fields
                missing_colexc = []
                for field in required_fields:
                    # Look for COLEXC statement with this field
                    colexc_pattern = rf'COLEXC\s+[^,\s]*{re.escape(field)}[^,\s]*'
                    if not re.search(colexc_pattern, full_config, re.IGNORECASE):
                        missing_colexc.append(field)
                
                if missing_colexc:
                    errors.append(
                        f"❌ {extract_name}: Table {table_name} missing COLEXC for fields: {', '.join(missing_colexc)}"
                    )
                else:
                    print(f"✅ {extract_name}: Table {table_name} has all required COLEXC statements")
            else:
                print(f"ℹ️  {extract_name}: Table {table_name} has no exclude field requirements")
        
        return errors
    
    def close_connection(self):
        """Close Databricks connection"""
        if hasattr(self, 'connection') and self.connection:
            self.connection.close()
            print("✅ Closed Databricks connection")


def main():
    parser = argparse.ArgumentParser(description='Validate CDC field excludes in GoldenGate configuration')
    parser.add_argument('--config-path', 
                       default='../config/extracts.json',
                       help='Path to extracts.json configuration file')
    parser.add_argument('--environment',
                       default=os.getenv('TARGET_ENV', 'dev'),
                       help='Target environment for validation')
    
    args = parser.parse_args()
    
    # Get Databricks connection details from environment variables
    databricks_hostname = os.getenv('DATABRICKS_SERVER_HOSTNAME')
    databricks_http_path = os.getenv('DATABRICKS_HTTP_PATH')
    databricks_token = os.getenv('DATABRICKS_ACCESS_TOKEN')
    
    if not all([databricks_hostname, databricks_http_path, databricks_token]):
        print("❌ Missing required Databricks environment variables:")
        print("   - DATABRICKS_SERVER_HOSTNAME")
        print("   - DATABRICKS_HTTP_PATH")
        print("   - DATABRICKS_ACCESS_TOKEN")
        sys.exit(1)
    
    print(f"🚀 Starting CDC field exclude validation for environment: {args.environment}")
    print(f"📁 Configuration file: {args.config_path}")
    print(f"🔗 Databricks hostname: {databricks_hostname}")
    
    validator = CDCExcludeValidator(
        databricks_hostname=databricks_hostname,
        databricks_http_path=databricks_http_path,
        databricks_token=databricks_token
    )
    
    try:
        # Connect to Databricks
        if not validator.connect_to_databricks():
            sys.exit(1)
        
        # Get exclude fields from Databricks
        validator.get_exclude_fields()
        
        # Parse extract configuration
        extract_config = validator.parse_extract_config(args.config_path)
        
        # Extract table configurations
        table_configs = validator.extract_table_configs(extract_config)
        
        if not table_configs:
            print("⚠️  No TABLE statements found in extract configuration")
            return
        
        # Validate COLEXC statements
        errors = validator.validate_colexc_statements(table_configs)
        
        if errors:
            print("\n❌ CDC Field Exclude Validation FAILED:")
            for error in errors:
                print(f"   {error}")
            print(f"\n💡 Fix these issues by adding appropriate COLEXC statements to your extract configurations")
            sys.exit(1)
        else:
            print("\n✅ CDC Field Exclude Validation PASSED: All required COLEXC statements are present")
    
    except CDCValidationError as e:
        print(f"❌ Validation Error: {str(e)}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected Error: {str(e)}")
        sys.exit(1)
    finally:
        validator.close_connection()


if __name__ == "__main__":
    main()