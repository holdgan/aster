#!/bin/bash

# Aster Trading System - Executor Server (B/C) Startup Script
# This script starts an executor server on Server B or C

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default configuration
ACCOUNT_ID=""
PORT=8080
SERVER_NAME=""
LOG_LEVEL="INFO"
MAX_POSITION=""
MAX_ORDER=""
DRY_RUN=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --account)
            ACCOUNT_ID="$2"
            shift 2
            ;;
        --port)
            PORT="$2"
            shift 2
            ;;
        --name)
            SERVER_NAME="$2"
            shift 2
            ;;
        --log-level)
            LOG_LEVEL="$2"
            shift 2
            ;;
        --max-position)
            MAX_POSITION="$2"
            shift 2
            ;;
        --max-order)
            MAX_ORDER="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 --account ID [options]"
            echo "Options:"
            echo "  --account ID        Account identifier (B or C) [REQUIRED]"
            echo "  --port PORT         Listen port (default: 8080)"
            echo "  --name NAME         Server name for logging"
            echo "  --log-level LEVEL   Log level (DEBUG, INFO, WARNING, ERROR)"
            echo "  --max-position USD  Maximum position size"
            echo "  --max-order USD     Maximum order size"
            echo "  --dry-run          Run without executing trades"
            echo "  -h, --help         Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Validate required arguments
if [[ -z "$ACCOUNT_ID" ]]; then
    echo -e "${RED}❌ Error: --account is required${NC}"
    echo "Use --account B for buy-side executor or --account C for sell-side executor"
    exit 1
fi

if [[ "$ACCOUNT_ID" != "B" ]] && [[ "$ACCOUNT_ID" != "C" ]]; then
    echo -e "${RED}❌ Error: Account ID must be B or C${NC}"
    exit 1
fi

# Set default server name if not provided
if [[ -z "$SERVER_NAME" ]]; then
    SERVER_NAME="executor-$ACCOUNT_ID"
fi

echo -e "${BLUE}🚀 Aster Executor Server Startup${NC}"
echo "================================"

# Check if .env file exists (look in parent directory)
if [[ ! -f "../.env" ]]; then
    echo -e "${RED}❌ Error: .env file not found${NC}"
    echo "Please copy .env.example to .env and configure your API credentials"
    exit 1
fi

# Load environment variables
echo -e "${YELLOW}📁 Loading environment configuration...${NC}"
source ../.env

# Validate required environment variables
REQUIRED_VARS=("ASTER_${ACCOUNT_ID}_KEY" "ASTER_${ACCOUNT_ID}_SECRET")
for var in "${REQUIRED_VARS[@]}"; do
    if [[ -z "${!var}" ]]; then
        echo -e "${RED}❌ Error: $var is not set in .env file${NC}"
        exit 1
    fi
done

# Skip virtual environment - use system Python directly
echo -e "${YELLOW}🐍 Using system Python environment...${NC}"

# Check and install dependencies if needed
echo -e "${YELLOW}📦 Checking dependencies...${NC}"
if ! python3 -c "import aiohttp" &>/dev/null; then
    echo -e "${YELLOW}📦 Installing missing dependencies...${NC}"
    pip3 install -r ../requirements.txt
fi

# Check Python modules
echo -e "${YELLOW}✅ Checking Python modules...${NC}"
python3 -c "
import asyncio
import aiohttp
from aiohttp import web
from dotenv import load_dotenv
print('✅ All required modules available')
"

# Create log directory
LOG_DIR="/var/log/aster"
if [[ ! -d "$LOG_DIR" ]] && [[ -w "/var/log" ]]; then
    echo -e "${YELLOW}📝 Creating log directory: $LOG_DIR${NC}"
    sudo mkdir -p "$LOG_DIR"
    sudo chown "$USER:$USER" "$LOG_DIR"
elif [[ ! -d "logs" ]]; then
    echo -e "${YELLOW}📝 Creating local log directory: ./logs${NC}"
    mkdir -p logs
    LOG_DIR="./logs"
fi

# Pre-flight checks
echo -e "${YELLOW}🔍 Running pre-flight checks...${NC}"

# Check if executor_server.py exists (in current scripts directory)
if [[ ! -f "executor_server.py" ]]; then
    echo -e "${RED}❌ Error: executor_server.py not found${NC}"
    exit 1
fi

# Check if port is available
if [[ "$DRY_RUN" == "false" ]]; then
    if netstat -tuln | grep -q ":$PORT "; then
        echo -e "${RED}❌ Error: Port $PORT is already in use${NC}"
        echo "Please choose a different port or stop the service using this port"
        exit 1
    fi
fi

# Display configuration
echo -e "${BLUE}📋 Configuration Summary:${NC}"
echo "  Account ID: $ACCOUNT_ID"
echo "  Server Name: $SERVER_NAME"
echo "  Listen Port: $PORT"
echo "  Log Level: $LOG_LEVEL"
if [[ -n "$MAX_POSITION" ]]; then
    echo "  Max Position: \$${MAX_POSITION}"
else
    echo "  Max Position: (not enforced)"
fi

if [[ -n "$MAX_ORDER" ]]; then
    echo "  Max Order: \$${MAX_ORDER}"
else
    echo "  Max Order: (not enforced)"
fi
echo "  Dry Run: $DRY_RUN"
echo "  Log Directory: $LOG_DIR"

# Explain role
if [[ "$ACCOUNT_ID" == "B" ]]; then
    echo -e "${GREEN}📈 Role: Maker Buy Orders (Long positions)${NC}"
elif [[ "$ACCOUNT_ID" == "C" ]]; then
    echo -e "${RED}📉 Role: Maker Sell Orders (Short positions)${NC}"
fi

# Start the executor
echo -e "${GREEN}🎯 Starting Aster Executor Server...${NC}"
echo "Press Ctrl+C to stop"
echo ""

# Build command
CMD="python3 executor_server.py --account $ACCOUNT_ID --port $PORT --name $SERVER_NAME --log-level $LOG_LEVEL"

if [[ -n "$MAX_POSITION" ]]; then
    CMD+=" --max-position $MAX_POSITION"
fi

if [[ -n "$MAX_ORDER" ]]; then
    CMD+=" --max-order $MAX_ORDER"
fi

if [[ "$DRY_RUN" == "true" ]]; then
    echo -e "${YELLOW}🧪 DRY RUN MODE - No actual trades will be executed${NC}"
    echo "Command that would be executed:"
    echo "$CMD"
    exit 0
fi

# Set up signal handling for graceful shutdown
trap 'echo -e "\n${YELLOW}🛑 Shutting down executor...${NC}"; kill $!; wait; echo -e "${GREEN}✅ Executor stopped${NC}"; exit 0' SIGINT SIGTERM

# Show startup banner
echo -e "${BLUE}============================================${NC}"
echo -e "${BLUE}  Aster Executor Server ($ACCOUNT_ID) Started${NC}"
echo -e "${BLUE}  Listening on: http://0.0.0.0:$PORT${NC}"
echo -e "${BLUE}  Health Check: http://0.0.0.0:$PORT/health${NC}"
echo -e "${BLUE}  Status Check: http://0.0.0.0:$PORT/status${NC}"
echo -e "${BLUE}============================================${NC}"

# Execute the command
exec $CMD
