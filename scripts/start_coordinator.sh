#!/bin/bash

# Aster Trading System - Coordinator Server (A) Startup Script
# This script starts the coordinator server on Server A

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
ACCOUNT_ID="A"
SERVER_B_URL="http://localhost:8081"
SERVER_C_URL="http://localhost:8082"
LOG_LEVEL="INFO"
DRY_RUN=false
STRATEGY="server"  # "server" or "cycle"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --account)
            ACCOUNT_ID="$2"
            shift 2
            ;;
        --server-b)
            SERVER_B_URL="$2"
            shift 2
            ;;
        --server-c)
            SERVER_C_URL="$2"
            shift 2
            ;;
        --log-level)
            LOG_LEVEL="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --strategy)
            STRATEGY="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [options]"
            echo "Options:"
            echo "  --account ID        Account identifier (default: A)"
            echo "  --server-b URL      Server B endpoint"
            echo "  --server-c URL      Server C endpoint"
            echo "  --log-level LEVEL   Log level (DEBUG, INFO, WARNING, ERROR)"
            echo "  --strategy TYPE     Strategy type: 'server' or 'cycle' (default: server)"
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

echo -e "${BLUE}🚀 Aster Coordinator Server Startup${NC}"
echo "=================================="

# Check if .env file exists (look in parent directory)
if [[ ! -f "../.env" ]]; then
    echo -e "${RED}❌ Error: .env file not found${NC}"
    echo "Please copy .env.example to .env and configure your API credentials"
    exit 1
fi

# Load environment variables
echo -e "${YELLOW}📁 Loading environment configuration...${NC}"
source ../.env

# 如果没有通过命令行指定，使用.env中的配置
if [[ "$SERVER_B_URL" == "http://localhost:8081" && -n "${SERVER_B_URL}" ]]; then
    # 使用.env中的SERVER_B_URL (已经通过source加载)
    echo -e "${GREEN}📡 Using SERVER_B_URL from .env: $SERVER_B_URL${NC}"
fi

if [[ "$SERVER_C_URL" == "http://localhost:8082" && -n "${SERVER_C_URL}" ]]; then
    # 使用.env中的SERVER_C_URL (已经通过source加载)
    echo -e "${GREEN}📡 Using SERVER_C_URL from .env: $SERVER_C_URL${NC}"
fi

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

# Validate strategy selection
if [[ "$STRATEGY" != "server" && "$STRATEGY" != "cycle" ]]; then
    echo -e "${RED}❌ Error: Invalid strategy '$STRATEGY'. Must be 'server' or 'cycle'${NC}"
    exit 1
fi

# Determine coordinator script based on strategy
if [[ "$STRATEGY" == "cycle" ]]; then
    COORDINATOR_SCRIPT="coordinator_cycle.py"
    STRATEGY_NAME="Cyclic Inventory Maximizer"
else
    COORDINATOR_SCRIPT="coordinator_server.py"
    STRATEGY_NAME="Target Position Strategy"
fi

# Pre-flight checks
echo -e "${YELLOW}🔍 Running pre-flight checks...${NC}"

# Check if coordinator script exists
if [[ ! -f "$COORDINATOR_SCRIPT" ]]; then
    echo -e "${RED}❌ Error: $COORDINATOR_SCRIPT not found${NC}"
    exit 1
fi

# Check if original trading module exists (in parent directory)
if [[ ! -f "../aster_btcusdt_genesis.py" ]]; then
    echo -e "${RED}❌ Error: aster_btcusdt_genesis.py not found${NC}"
    echo "This file contains the AsterFuturesClient and other required classes"
    exit 1
fi

# Test server endpoints (if not dry run)
if [[ "$DRY_RUN" == "false" ]]; then
    echo -e "${YELLOW}🌐 Testing server endpoints...${NC}"

    # Test Server B
    if timeout 5 curl -s "$SERVER_B_URL/health" > /dev/null 2>&1; then
        echo -e "${GREEN}✅ Server B is reachable at $SERVER_B_URL${NC}"
    else
        echo -e "${YELLOW}⚠️  Server B not reachable at $SERVER_B_URL (will retry during runtime)${NC}"
    fi

    # Test Server C
    if timeout 5 curl -s "$SERVER_C_URL/health" > /dev/null 2>&1; then
        echo -e "${GREEN}✅ Server C is reachable at $SERVER_C_URL${NC}"
    else
        echo -e "${YELLOW}⚠️  Server C not reachable at $SERVER_C_URL (will retry during runtime)${NC}"
    fi
fi

# Display configuration
echo -e "${BLUE}📋 Configuration Summary:${NC}"
echo "  Strategy: $STRATEGY_NAME ($STRATEGY)"
echo "  Script: $COORDINATOR_SCRIPT"
echo "  Account ID: $ACCOUNT_ID"
echo "  Server B URL: $SERVER_B_URL"
echo "  Server C URL: $SERVER_C_URL"
echo "  Log Level: $LOG_LEVEL"
echo "  Dry Run: $DRY_RUN"
echo "  Log Directory: $LOG_DIR"

# Start the coordinator
echo -e "${GREEN}🎯 Starting Aster Coordinator Server...${NC}"
echo "Press Ctrl+C to stop"
echo ""

# Build command based on strategy
if [[ "$STRATEGY" == "cycle" ]]; then
    # coordinator_cycle.py doesn't accept command line arguments, uses .env only
    CMD="python3 $COORDINATOR_SCRIPT"
else
    CMD="python3 $COORDINATOR_SCRIPT --account $ACCOUNT_ID --server-b $SERVER_B_URL --server-c $SERVER_C_URL --log-level $LOG_LEVEL"
fi

if [[ "$DRY_RUN" == "true" ]]; then
    echo -e "${YELLOW}🧪 DRY RUN MODE - No actual trades will be executed${NC}"
    echo "Command that would be executed:"
    echo "$CMD"
    exit 0
fi

# Set up signal handling for graceful shutdown
trap 'echo -e "\n${YELLOW}🛑 Shutting down coordinator...${NC}"; kill $!; wait; echo -e "${GREEN}✅ Coordinator stopped${NC}"; exit 0' SIGINT SIGTERM

# Execute the command
exec $CMD