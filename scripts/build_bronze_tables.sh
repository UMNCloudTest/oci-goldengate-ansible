#!/bin/bash
set -euo pipefail

# Build Bronze Tables Script
# This script extracts table names from extracts.json configuration and triggers
# a Databricks job called "build_bronze_tables" with the table list as a parameter.

# Script configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
CONFIG_FILE="${PROJECT_DIR}/config/extracts.json"
JOB_NAME="build_bronze_tables"
DEFAULT_TIMEOUT=1800  # 30 minutes

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Function to display usage
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Build bronze tables in Databricks based on GoldenGate extract configuration.

OPTIONS:
    -c, --config FILE       Path to extracts.json config file (default: ../../config/extracts.json)
    -j, --job-name NAME     Databricks job name (default: build_bronze_tables)
    -e, --environment ENV   Target environment (dev|uat|prod) (default: dev)
    -t, --timeout SECONDS  Job timeout in seconds (default: 1800)
    -w, --wait              Wait for job completion
    -d, --dry-run          Extract tables but don't run job
    -h, --help             Display this help message

REQUIRED ENVIRONMENT VARIABLES:
    DATABRICKS_HOST         Databricks workspace URL
    DATABRICKS_TOKEN        Databricks access token

EXAMPLE:
    $0 --environment prod --wait
    $0 --config /path/to/extracts.json --job-name my_bronze_job --timeout 3600

EOF
}

# Parse command line arguments
CONFIG_PATH="$CONFIG_FILE"
ENVIRONMENT="dev"
TIMEOUT="$DEFAULT_TIMEOUT"
WAIT_FOR_COMPLETION=false
DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -c|--config)
            CONFIG_PATH="$2"
            shift 2
            ;;
        -j|--job-name)
            JOB_NAME="$2"
            shift 2
            ;;
        -e|--environment)
            ENVIRONMENT="$2"
            shift 2
            ;;
        -t|--timeout)
            TIMEOUT="$2"
            shift 2
            ;;
        -w|--wait)
            WAIT_FOR_COMPLETION=true
            shift
            ;;
        -d|--dry-run)
            DRY_RUN=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Validate environment variables
if [[ -z "${DATABRICKS_HOST:-}" ]]; then
    print_error "DATABRICKS_HOST environment variable is required"
    exit 1
fi

if [[ -z "${DATABRICKS_TOKEN:-}" ]]; then
    print_error "DATABRICKS_TOKEN environment variable is required"
    exit 1
fi

# Check if Databricks CLI is available
if ! command -v databricks &> /dev/null; then
    print_error "Databricks CLI is not installed or not in PATH"
    print_info "Install with: pip install databricks-cli"
    exit 1
fi

# Configure Databricks CLI
export DATABRICKS_HOST
export DATABRICKS_TOKEN

print_info "Configuring Databricks CLI..."
databricks configure --token <<EOF
${DATABRICKS_HOST}
${DATABRICKS_TOKEN}
EOF

# Validate configuration file exists
if [[ ! -f "$CONFIG_PATH" ]]; then
    print_error "Configuration file not found: $CONFIG_PATH"
    exit 1
fi

print_info "Configuration:"
print_info "  Config File: $CONFIG_PATH"
print_info "  Job Name: $JOB_NAME"
print_info "  Environment: $ENVIRONMENT"
print_info "  Timeout: $TIMEOUT seconds"
print_info "  Wait for completion: $WAIT_FOR_COMPLETION"
print_info "  Dry run: $DRY_RUN"

# Extract table names from extracts.json
print_info "Extracting table names from configuration..."

TABLE_LIST=$(python3 << 'EOF'
import json
import re
import sys

def extract_table_names(config_path):
    """Extract table names from extracts.json configuration"""
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
    except Exception as e:
        print(f"Error reading config file: {e}", file=sys.stderr)
        sys.exit(1)
    
    tables = set()
    
    def find_table_statements(obj):
        """Recursively find TABLE statements in configuration"""
        if isinstance(obj, str):
            # Look for TABLE statements
            table_pattern = r'TABLE\s+([\w\.]+)'
            matches = re.finditer(table_pattern, obj, re.IGNORECASE)
            for match in matches:
                table_ref = match.group(1)
                # Extract table name (remove schema prefix if present)
                if '.' in table_ref:
                    table_name = table_ref.split('.')[-1]
                else:
                    table_name = table_ref
                tables.add(table_name.upper())
        elif isinstance(obj, dict):
            for value in obj.values():
                find_table_statements(value)
        elif isinstance(obj, list):
            for item in obj:
                find_table_statements(item)
    
    # Search through all extracts
    for extract in config.get('extracts', []):
        find_table_statements(extract.get('config', {}))
    
    return sorted(list(tables))

# Extract tables
config_path = sys.argv[1]
tables = extract_table_names(config_path)

if not tables:
    print("No tables found in configuration", file=sys.stderr)
    sys.exit(1)

# Output as comma-separated string
print(','.join(tables))
EOF
"$CONFIG_PATH")

if [[ $? -ne 0 ]]; then
    print_error "Failed to extract table names from configuration"
    exit 1
fi

if [[ -z "$TABLE_LIST" ]]; then
    print_error "No table names found in configuration"
    exit 1
fi

print_success "Found tables: $TABLE_LIST"

if [[ "$DRY_RUN" == "true" ]]; then
    print_success "Dry run completed. Tables that would be processed: $TABLE_LIST"
    exit 0
fi

# Get job ID by name
print_info "Looking up job ID for '$JOB_NAME'..."
JOB_ID=$(databricks jobs list --output json | python3 -c "
import json
import sys
jobs = json.load(sys.stdin)
for job in jobs.get('jobs', []):
    if job.get('settings', {}).get('name') == '$JOB_NAME':
        print(job['job_id'])
        sys.exit(0)
print('Job not found', file=sys.stderr)
sys.exit(1)
")

if [[ $? -ne 0 ]]; then
    print_error "Job '$JOB_NAME' not found in Databricks workspace"
    print_info "Available jobs:"
    databricks jobs list --output json | python3 -c "
import json
import sys
jobs = json.load(sys.stdin)
for job in jobs.get('jobs', []):
    print(f\"  - {job.get('settings', {}).get('name', 'Unknown')} (ID: {job['job_id']})\")
" || true
    exit 1
fi

print_success "Found job '$JOB_NAME' with ID: $JOB_ID"

# Prepare job parameters
JOB_PARAMS=$(cat << EOF
{
  "table_list": "$TABLE_LIST",
  "environment": "$ENVIRONMENT",
  "triggered_by": "goldengate_deployment"
}
EOF
)

# Run the job
print_info "Triggering Databricks job '$JOB_NAME' with parameters:"
echo "$JOB_PARAMS" | python3 -m json.tool

RUN_OUTPUT=$(databricks jobs run-now --job-id "$JOB_ID" --json-params "$JOB_PARAMS" --output json)
RUN_ID=$(echo "$RUN_OUTPUT" | python3 -c "import json, sys; print(json.load(sys.stdin)['run_id'])")

if [[ -z "$RUN_ID" ]]; then
    print_error "Failed to start job run"
    echo "$RUN_OUTPUT"
    exit 1
fi

print_success "Job run started with ID: $RUN_ID"
print_info "Job URL: ${DATABRICKS_HOST}/#job/${JOB_ID}/run/${RUN_ID}"

if [[ "$WAIT_FOR_COMPLETION" == "true" ]]; then
    print_info "Waiting for job completion (timeout: ${TIMEOUT}s)..."
    
    START_TIME=$(date +%s)
    while true; do
        CURRENT_TIME=$(date +%s)
        ELAPSED=$((CURRENT_TIME - START_TIME))
        
        if [[ $ELAPSED -gt $TIMEOUT ]]; then
            print_error "Job execution timed out after ${TIMEOUT} seconds"
            print_info "Job may still be running. Check: ${DATABRICKS_HOST}/#job/${JOB_ID}/run/${RUN_ID}"
            exit 1
        fi
        
        # Get run status
        RUN_STATUS=$(databricks runs get --run-id "$RUN_ID" --output json)
        STATE=$(echo "$RUN_STATUS" | python3 -c "
import json
import sys
run = json.load(sys.stdin)
state = run.get('state', {})
life_cycle_state = state.get('life_cycle_state', '')
result_state = state.get('result_state', '')
print(f'{life_cycle_state}:{result_state}')
")
        
        LIFE_CYCLE_STATE="${STATE%%:*}"
        RESULT_STATE="${STATE##*:}"
        
        print_info "Job status: $LIFE_CYCLE_STATE $([ -n "$RESULT_STATE" ] && echo "($RESULT_STATE)" || echo "")"
        
        case "$LIFE_CYCLE_STATE" in
            "TERMINATED")
                if [[ "$RESULT_STATE" == "SUCCESS" ]]; then
                    print_success "Job completed successfully!"
                    exit 0
                else
                    print_error "Job failed with result: $RESULT_STATE"
                    print_info "Check job logs: ${DATABRICKS_HOST}/#job/${JOB_ID}/run/${RUN_ID}"
                    exit 1
                fi
                ;;
            "INTERNAL_ERROR"|"SKIPPED")
                print_error "Job encountered an error: $LIFE_CYCLE_STATE"
                exit 1
                ;;
            *)
                # Job is still running
                sleep 30
                ;;
        esac
    done
else
    print_success "Job triggered successfully. Run ID: $RUN_ID"
    print_info "Monitor progress at: ${DATABRICKS_HOST}/#job/${JOB_ID}/run/${RUN_ID}"
fi