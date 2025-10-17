#!/bin/bash
# ============================================
# LendingWise AI - EC2 Deployment Script
# ============================================

set -e

echo "🚀 LendingWise AI Deployment Script"
echo "===================================="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored messages
print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_info() {
    echo -e "ℹ $1"
}

# Check if running on Linux
if [[ "$OSTYPE" != "linux-gnu"* ]]; then
    print_error "This script must be run on Linux (EC2 instance)"
    exit 1
fi

# Check if .env exists
if [ ! -f .env ]; then
    print_error ".env file not found!"
    print_info "Please copy env.example to .env and configure it:"
    echo "  cp env.example .env"
    echo "  nano .env"
    exit 1
fi

print_success ".env file found"

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    print_error "Docker is not installed!"
    print_info "Please install Docker first. See DEPLOYMENT.md for instructions"
    exit 1
fi

print_success "Docker is installed"

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    print_error "Docker Compose is not installed!"
    print_info "Please install Docker Compose first. See DEPLOYMENT.md for instructions"
    exit 1
fi

print_success "Docker Compose is installed"

# Check if user is in docker group
if ! groups | grep -q docker; then
    print_warning "Current user is not in docker group"
    print_info "You may need to run: sudo usermod -a -G docker \$USER"
    print_info "Then logout and login again"
fi

# Create necessary directories
print_info "Creating output directories..."
mkdir -p outputs
mkdir -p Nodes/outputs
mkdir -p Nodes/outputs/temp_tamper_check
mkdir -p cross_validation/reports
mkdir -p result
print_success "Output directories created"

# Check environment variables
print_info "Checking environment variables..."

check_env_var() {
    local var_name=$1
    local var_value=$(grep "^${var_name}=" .env | cut -d '=' -f2-)
    
    if [ -z "$var_value" ] || [ "$var_value" = "your-value-here" ] || [ "$var_value" = "sk-your-openai-api-key-here" ]; then
        print_error "$var_name is not configured in .env"
        return 1
    else
        print_success "$var_name is configured"
        return 0
    fi
}

# Check critical environment variables
all_vars_ok=true

if ! check_env_var "OPENAI_API_KEY"; then
    all_vars_ok=false
fi

if ! check_env_var "DB_HOST"; then
    all_vars_ok=false
fi

if ! check_env_var "DB_PASSWORD"; then
    all_vars_ok=false
fi

if [ "$all_vars_ok" = false ]; then
    print_error "Please configure all required environment variables in .env"
    exit 1
fi

# Ask for confirmation
echo ""
print_info "Ready to deploy LendingWise AI workers"
echo "  - SQS Worker (processes documents from queue)"
echo "  - Cross-Validation Watcher (validates processed documents)"
echo ""
read -p "Do you want to continue? (y/n) " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    print_info "Deployment cancelled"
    exit 0
fi

# Stop existing containers if any
print_info "Stopping existing containers (if any)..."
docker-compose down 2>/dev/null || true
print_success "Existing containers stopped"

# Build and start containers
print_info "Building Docker images (this may take a few minutes)..."
if docker-compose build; then
    print_success "Docker images built successfully"
else
    print_error "Failed to build Docker images"
    exit 1
fi

print_info "Starting containers..."
if docker-compose up -d; then
    print_success "Containers started successfully"
else
    print_error "Failed to start containers"
    exit 1
fi

# Wait for containers to start
print_info "Waiting for containers to initialize..."
sleep 5

# Check container status
print_info "Checking container status..."
if docker-compose ps | grep -q "Up"; then
    print_success "Containers are running"
else
    print_error "Containers failed to start"
    echo ""
    print_info "Showing logs:"
    docker-compose logs --tail=50
    exit 1
fi

# Display container status
echo ""
echo "=========================================="
echo "📊 Container Status"
echo "=========================================="
docker-compose ps
echo ""

# Show recent logs
echo "=========================================="
echo "📝 Recent Logs (last 20 lines)"
echo "=========================================="
docker-compose logs --tail=20
echo ""

# Success message
print_success "Deployment completed successfully!"
echo ""
echo "=========================================="
echo "🎉 LendingWise AI is now running!"
echo "=========================================="
echo ""
print_info "Useful commands:"
echo "  View logs:         docker-compose logs -f"
echo "  View SQS worker:   docker-compose logs -f sqs-worker"
echo "  View validator:    docker-compose logs -f cross-validation-watcher"
echo "  Restart services:  docker-compose restart"
echo "  Stop services:     docker-compose stop"
echo "  Update & restart:  ./deploy.sh"
echo ""
print_info "For more information, see DEPLOYMENT.md"
echo ""

