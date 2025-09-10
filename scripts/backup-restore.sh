#!/bin/bash
set -euo pipefail

# Backup and restore utilities for ICON-RUC
# Handles database, volumes, and configuration backups

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BACKUP_BASE_DIR="${BACKUP_DIR:-$PROJECT_DIR/backups}"
ENVIRONMENT="${ENVIRONMENT:-production}"

# Configuration
COMPOSE_FILE="docker-compose.yml"
S3_BUCKET="${S3_BACKUP_BUCKET:-}"
RETENTION_DAYS="${BACKUP_RETENTION_DAYS:-30}"

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

# Create backup directory structure
init_backup_dirs() {
    mkdir -p "$BACKUP_BASE_DIR"/{database,volumes,config,logs}
    log "Backup directories initialized"
}

# Database backup
backup_database() {
    local timestamp=$(date +%Y%m%d-%H%M%S)
    local backup_file="$BACKUP_BASE_DIR/database/db-backup-$timestamp.sql"
    local backup_file_compressed="$backup_file.gz"
    
    log "Creating database backup..."
    
    cd "$PROJECT_DIR"
    
    # Create SQL dump
    if docker-compose -f "$COMPOSE_FILE" exec -T db pg_dump -U iconuser --verbose --clean --no-owner --no-acl iconruc > "$backup_file"; then
        
        # Compress the backup
        gzip "$backup_file"
        
        local size=$(du -h "$backup_file_compressed" | cut -f1)
        log "Database backup created: $(basename "$backup_file_compressed") ($size)"
        
        # Verify backup integrity
        if gunzip -t "$backup_file_compressed"; then
            log "Backup integrity verified"
        else
            error "Backup integrity check failed"
        fi
        
        # Upload to S3 if configured
        if [ -n "$S3_BUCKET" ]; then
            upload_to_s3 "$backup_file_compressed" "database/"
        fi
        
        # Clean old backups
        cleanup_old_backups "database" "db-backup-*.sql.gz"
        
        echo "$backup_file_compressed"
    else
        error "Database backup failed"
    fi
}

# Volume backup
backup_volumes() {
    local timestamp=$(date +%Y%m%d-%H%M%S)
    local backup_file="$BACKUP_BASE_DIR/volumes/volumes-backup-$timestamp.tar.gz"
    
    log "Creating volumes backup..."
    
    cd "$PROJECT_DIR"
    
    # Get list of volumes
    local volumes=$(docker-compose -f "$COMPOSE_FILE" config --volumes)
    
    if [ -n "$volumes" ]; then
        # Create temporary container to access volumes
        docker run --rm \
            $(echo "$volumes" | sed 's/^/-v /' | sed 's/$/:\/backup\/&/' | tr '\n' ' ') \
            -v "$BACKUP_BASE_DIR/volumes:/host-backup" \
            alpine:latest sh -c "
                cd /backup && \
                tar czf /host-backup/volumes-backup-$timestamp.tar.gz . && \
                echo 'Volume backup completed'
            "
        
        local size=$(du -h "$backup_file" | cut -f1)
        log "Volume backup created: $(basename "$backup_file") ($size)"
        
        # Upload to S3 if configured
        if [ -n "$S3_BUCKET" ]; then
            upload_to_s3 "$backup_file" "volumes/"
        fi
        
        # Clean old backups
        cleanup_old_backups "volumes" "volumes-backup-*.tar.gz"
        
        echo "$backup_file"
    else
        warn "No volumes found to backup"
    fi
}

# Configuration backup
backup_config() {
    local timestamp=$(date +%Y%m%d-%H%M%S)
    local backup_file="$BACKUP_BASE_DIR/config/config-backup-$timestamp.tar.gz"
    
    log "Creating configuration backup..."
    
    cd "$PROJECT_DIR"
    
    # Backup configuration files (excluding sensitive data)
    tar czf "$backup_file" \
        --exclude='.env' \
        --exclude='*.log' \
        --exclude='backups/' \
        --exclude='.git/' \
        --exclude='data/' \
        --exclude='icon-env/' \
        --exclude='__pycache__/' \
        --exclude='*.pyc' \
        docker-compose*.yml \
        nginx/ \
        monitoring/ \
        scripts/ \
        requirements*.txt \
        *.py \
        *.sql \
        *.md \
        2>/dev/null || true
    
    local size=$(du -h "$backup_file" | cut -f1)
    log "Configuration backup created: $(basename "$backup_file") ($size)"
    
    # Upload to S3 if configured
    if [ -n "$S3_BUCKET" ]; then
        upload_to_s3 "$backup_file" "config/"
    fi
    
    # Clean old backups
    cleanup_old_backups "config" "config-backup-*.tar.gz"
    
    echo "$backup_file"
}

# Full system backup
backup_full() {
    log "Starting full system backup..."
    
    init_backup_dirs
    
    local db_backup=$(backup_database)
    local volume_backup=$(backup_volumes)
    local config_backup=$(backup_config)
    
    # Create manifest
    local timestamp=$(date +%Y%m%d-%H%M%S)
    local manifest="$BACKUP_BASE_DIR/manifest-$timestamp.json"
    
    cat > "$manifest" <<EOF
{
    "timestamp": "$(date -Iseconds)",
    "environment": "$ENVIRONMENT",
    "backup_type": "full",
    "files": {
        "database": "$(basename "$db_backup")",
        "volumes": "$(basename "$volume_backup")",
        "config": "$(basename "$config_backup")"
    },
    "docker_images": [
$(docker-compose -f "$COMPOSE_FILE" config | grep image: | sed 's/.*image: /        "/' | sed 's/$/",/' | sed '$ s/,$//')
    ]
}
EOF
    
    log "Full backup completed. Manifest: $(basename "$manifest")"
}

# Database restore
restore_database() {
    local backup_file="$1"
    
    if [ ! -f "$backup_file" ]; then
        error "Backup file not found: $backup_file"
    fi
    
    log "Restoring database from: $(basename "$backup_file")"
    
    # Stop application to prevent connections
    cd "$PROJECT_DIR"
    docker-compose -f "$COMPOSE_FILE" stop app
    
    # Restore database
    if [[ "$backup_file" == *.gz ]]; then
        gunzip -c "$backup_file" | docker-compose -f "$COMPOSE_FILE" exec -T db psql -U iconuser -d iconruc
    else
        docker-compose -f "$COMPOSE_FILE" exec -T db psql -U iconuser -d iconruc < "$backup_file"
    fi
    
    # Restart services
    docker-compose -f "$COMPOSE_FILE" up -d
    
    log "Database restore completed"
}

# Volume restore
restore_volumes() {
    local backup_file="$1"
    
    if [ ! -f "$backup_file" ]; then
        error "Backup file not found: $backup_file"
    fi
    
    log "Restoring volumes from: $(basename "$backup_file")"
    
    cd "$PROJECT_DIR"
    
    # Stop all services
    docker-compose -f "$COMPOSE_FILE" down
    
    # Get list of volumes and restore
    local volumes=$(docker-compose -f "$COMPOSE_FILE" config --volumes)
    
    if [ -n "$volumes" ]; then
        docker run --rm \
            $(echo "$volumes" | sed 's/^/-v /' | sed 's/$/:\/backup\/&/' | tr '\n' ' ') \
            -v "$backup_file:/backup.tar.gz:ro" \
            alpine:latest sh -c "
                cd /backup && \
                tar xzf /backup.tar.gz && \
                echo 'Volume restore completed'
            "
    fi
    
    # Restart services
    docker-compose -f "$COMPOSE_FILE" up -d
    
    log "Volume restore completed"
}

# Upload to S3
upload_to_s3() {
    local file_path="$1"
    local s3_prefix="$2"
    
    if [ -n "$S3_BUCKET" ] && command -v aws >/dev/null 2>&1; then
        local s3_key="$s3_prefix$(basename "$file_path")"
        
        log "Uploading to S3: s3://$S3_BUCKET/$s3_key"
        
        if aws s3 cp "$file_path" "s3://$S3_BUCKET/$s3_key"; then
            log "S3 upload completed"
        else
            warn "S3 upload failed"
        fi
    fi
}

# Cleanup old backups
cleanup_old_backups() {
    local backup_type="$1"
    local pattern="$2"
    local backup_dir="$BACKUP_BASE_DIR/$backup_type"
    
    if [ -d "$backup_dir" ]; then
        find "$backup_dir" -name "$pattern" -type f -mtime +"$RETENTION_DAYS" -delete
        log "Cleaned old $backup_type backups (older than $RETENTION_DAYS days)"
    fi
}

# List available backups
list_backups() {
    log "Available backups:"
    
    for backup_type in database volumes config; do
        local backup_dir="$BACKUP_BASE_DIR/$backup_type"
        if [ -d "$backup_dir" ]; then
            echo ""
            echo "=== $backup_type backups ==="
            ls -lah "$backup_dir" | tail -n +2 | while read line; do
                echo "  $line"
            done
        fi
    done
    
    echo ""
    echo "=== Manifests ==="
    ls -lah "$BACKUP_BASE_DIR"/manifest-*.json 2>/dev/null | tail -n +2 | while read line; do
        echo "  $line"
    done || echo "  No manifests found"
}

# Main function
case "${1:-help}" in
    "backup-db")
        init_backup_dirs
        backup_database
        ;;
    "backup-volumes")
        init_backup_dirs
        backup_volumes
        ;;
    "backup-config")
        init_backup_dirs
        backup_config
        ;;
    "backup-full")
        backup_full
        ;;
    "restore-db")
        restore_database "${2:-}"
        ;;
    "restore-volumes")
        restore_volumes "${2:-}"
        ;;
    "list")
        list_backups
        ;;
    "cleanup")
        cleanup_old_backups "database" "db-backup-*.sql.gz"
        cleanup_old_backups "volumes" "volumes-backup-*.tar.gz"
        cleanup_old_backups "config" "config-backup-*.tar.gz"
        ;;
    *)
        echo "Usage: $0 {backup-db|backup-volumes|backup-config|backup-full|restore-db|restore-volumes|list|cleanup} [backup_file]"
        echo ""
        echo "Backup commands:"
        echo "  backup-db        - Backup database only"
        echo "  backup-volumes   - Backup Docker volumes"
        echo "  backup-config    - Backup configuration files"
        echo "  backup-full      - Full system backup"
        echo ""
        echo "Restore commands:"
        echo "  restore-db FILE  - Restore database from backup file"
        echo "  restore-volumes FILE - Restore volumes from backup file"
        echo ""
        echo "Utility commands:"
        echo "  list             - List available backups"
        echo "  cleanup          - Clean old backups"
        echo ""
        echo "Environment variables:"
        echo "  BACKUP_DIR              - Base backup directory (default: ./backups)"
        echo "  S3_BACKUP_BUCKET        - S3 bucket for remote backups"
        echo "  BACKUP_RETENTION_DAYS   - Days to keep backups (default: 30)"
        exit 1
        ;;
esac