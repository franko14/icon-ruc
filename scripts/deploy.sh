#!/bin/bash
set -euo pipefail

# Production deployment script for ICON-RUC
# This script handles zero-downtime deployment with health checks

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
ENVIRONMENT="${1:-production}"
IMAGE_TAG="${2:-latest}"

# Configuration
COMPOSE_FILE="docker-compose.yml"
if [ "$ENVIRONMENT" = "staging" ]; then
    COMPOSE_FILE="docker-compose.staging.yml"
fi

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}"
}

warn() {
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}"
}

error() {
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}"
    exit 1
}

# Pre-deployment checks
pre_deployment_checks() {
    log "Running pre-deployment checks..."
    
    # Check if Docker is running
    if ! docker info > /dev/null 2>&1; then
        error "Docker is not running"
    fi
    
    # Check if compose file exists
    if [ ! -f "$PROJECT_DIR/$COMPOSE_FILE" ]; then
        error "Compose file not found: $COMPOSE_FILE"
    fi
    
    # Check if environment file exists
    if [ ! -f "$PROJECT_DIR/.env" ]; then
        warn "No .env file found. Using example configuration."
        cp "$PROJECT_DIR/.env.example" "$PROJECT_DIR/.env"
        warn "Please edit .env with your actual configuration values"
    fi
    
    # Validate environment variables
    source "$PROJECT_DIR/.env"
    if [ -z "${POSTGRES_PASSWORD:-}" ]; then
        error "POSTGRES_PASSWORD not set in .env file"
    fi
    if [ -z "${GRAFANA_PASSWORD:-}" ]; then
        error "GRAFANA_PASSWORD not set in .env file"
    fi
    
    log "Pre-deployment checks passed"
}

# Database backup
backup_database() {
    log "Creating database backup..."
    
    BACKUP_DIR="$PROJECT_DIR/backups"
    mkdir -p "$BACKUP_DIR"
    
    BACKUP_FILE="$BACKUP_DIR/backup-$(date +%Y%m%d-%H%M%S).sql"
    
    if docker-compose -f "$PROJECT_DIR/$COMPOSE_FILE" exec -T db pg_dump -U iconuser iconruc > "$BACKUP_FILE" 2>/dev/null; then
        log "Database backup created: $BACKUP_FILE"
        
        # Keep only last 10 backups
        cd "$BACKUP_DIR"
        ls -t backup-*.sql | tail -n +11 | xargs -r rm --
    else
        warn "Database backup failed or database not running"
    fi
}

# Health check function
health_check() {
    local service_url=$1
    local max_attempts=${2:-30}
    local attempt=1
    
    log "Checking health of $service_url..."
    
    while [ $attempt -le $max_attempts ]; do
        if curl -f -s "$service_url/api/health" > /dev/null 2>&1; then
            log "Health check passed for $service_url"
            return 0
        fi
        
        warn "Health check attempt $attempt/$max_attempts failed, retrying in 5 seconds..."
        sleep 5
        ((attempt++))
    done
    
    error "Health check failed for $service_url after $max_attempts attempts"
}

# Rolling update function
rolling_update() {
    log "Starting rolling update..."
    
    cd "$PROJECT_DIR"
    
    # Pull latest images
    log "Pulling latest images..."
    docker-compose -f "$COMPOSE_FILE" pull
    
    # Update database and Redis first
    log "Updating database and Redis..."
    docker-compose -f "$COMPOSE_FILE" up -d db redis
    sleep 10
    
    # Scale up new application instances
    log "Scaling up new application instances..."
    docker-compose -f "$COMPOSE_FILE" up -d --scale app=2 app
    sleep 30
    
    # Health check
    if [ "$ENVIRONMENT" = "production" ]; then
        health_check "http://localhost:5000"
    else
        health_check "http://localhost:5000"
    fi
    
    # Update load balancer
    log "Updating load balancer..."
    docker-compose -f "$COMPOSE_FILE" up -d nginx
    
    # Scale down to single instance
    log "Scaling down to single instance..."
    docker-compose -f "$COMPOSE_FILE" up -d --scale app=1 app
    
    # Final health check
    sleep 10
    if [ "$ENVIRONMENT" = "production" ]; then
        health_check "http://localhost"
    else
        health_check "http://localhost"
    fi
    
    log "Rolling update completed successfully"
}

# Database migration
run_migrations() {
    log "Running database migrations..."
    
    cd "$PROJECT_DIR"
    
    # Check if migration file exists
    if [ -f "db_migrations_prod.sql" ]; then
        # Run migrations
        if docker-compose -f "$COMPOSE_FILE" exec -T db psql -U iconuser -d iconruc < db_migrations_prod.sql; then
            log "Database migrations completed"
        else
            error "Database migrations failed"
        fi
    else
        warn "No migration file found, skipping migrations"
    fi
}

# Cleanup old resources
cleanup() {
    log "Cleaning up old resources..."
    
    # Remove unused images
    docker image prune -f
    
    # Remove unused volumes (be careful with this in production)
    if [ "$ENVIRONMENT" != "production" ]; then
        docker volume prune -f
    fi
    
    # Remove unused networks
    docker network prune -f
    
    log "Cleanup completed"
}

# Send deployment notification
send_notification() {
    local status=$1
    local webhook_url="${WEBHOOK_URL:-}"
    
    if [ -n "$webhook_url" ]; then
        local message="ICON-RUC deployment to $ENVIRONMENT: $status"
        local payload="{\"text\": \"$message\", \"timestamp\": \"$(date -Iseconds)\"}"
        
        curl -s -X POST -H "Content-Type: application/json" -d "$payload" "$webhook_url" || true
    fi
}

# Main deployment function
deploy() {
    log "Starting deployment to $ENVIRONMENT environment..."
    
    trap 'error "Deployment failed"' ERR
    
    # Run pre-deployment checks
    pre_deployment_checks
    
    # Create database backup
    backup_database
    
    # Run database migrations
    run_migrations
    
    # Perform rolling update
    rolling_update
    
    # Cleanup
    cleanup
    
    # Send success notification
    send_notification "SUCCESS"
    
    log "Deployment to $ENVIRONMENT completed successfully!"
}

# Handle different commands
case "${1:-deploy}" in
    "deploy")
        deploy
        ;;
    "backup")
        backup_database
        ;;
    "health-check")
        if [ "$ENVIRONMENT" = "production" ]; then
            health_check "http://localhost"
        else
            health_check "http://localhost:5000"
        fi
        ;;
    "migrate")
        run_migrations
        ;;
    "cleanup")
        cleanup
        ;;
    *)
        echo "Usage: $0 {deploy|backup|health-check|migrate|cleanup} [environment] [image_tag]"
        echo "  environment: production (default) or staging"
        echo "  image_tag: Docker image tag (default: latest)"
        exit 1
        ;;
esac