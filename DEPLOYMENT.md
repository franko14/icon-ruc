# ICON-RUC Production Deployment Guide

This guide provides comprehensive instructions for deploying the ICON-RUC weather data processing application with progress tracking to production environments.

## Overview

The deployment includes:
- **Multi-container application** with Flask API, PostgreSQL, Redis, and Nginx
- **Real-time progress tracking** with WebSocket support and Redis scaling
- **Comprehensive monitoring** with Prometheus, Grafana, and centralized logging
- **Automated CI/CD pipeline** with GitHub Actions
- **Zero-downtime deployment** capabilities
- **Backup and disaster recovery** procedures
- **Security hardening** and environment isolation

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Load Balancer │────│      Nginx      │────│   Application   │
│    (External)   │    │   (Port 80/443) │    │   (Flask API)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │                        │
                                │                        ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│    Monitoring   │    │      Redis      │    │   PostgreSQL    │
│  (Prometheus,   │    │   (Sessions,    │    │   (Main Data)   │
│   Grafana)      │    │   WebSocket)    │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Prerequisites

### System Requirements
- **OS**: Ubuntu 20.04 LTS or newer / CentOS 8+ / RHEL 8+
- **CPU**: 4+ cores recommended
- **RAM**: 8GB+ (16GB recommended for production)
- **Storage**: 100GB+ available space
- **Network**: Internet access for downloading weather data

### Software Dependencies
- Docker 20.10+ and Docker Compose 2.0+
- Git
- curl and wget
- SSL certificates (Let's Encrypt recommended)

### Access Requirements
- SSH access to deployment servers
- Docker registry access (GitHub Container Registry)
- Optional: AWS CLI for S3 backups
- Optional: Slack webhook URL for notifications

## Environment Setup

### 1. Clone Repository
```bash
git clone https://github.com/your-username/icon-ruc.git
cd icon-ruc
```

### 2. Configure Environment
```bash
# Copy environment template
cp .env.example .env

# Edit configuration (REQUIRED)
vim .env
```

**Critical environment variables:**
```bash
# Database
POSTGRES_PASSWORD=your_secure_database_password

# Monitoring  
GRAFANA_PASSWORD=your_grafana_admin_password

# Application
FLASK_SECRET_KEY=your_super_secret_flask_key_32_chars_min
LOG_LEVEL=info

# Optional: SSL certificates
SSL_CERTFILE=/etc/nginx/ssl/cert.pem
SSL_KEYFILE=/etc/nginx/ssl/key.pem

# Optional: Notifications
WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
```

### 3. Create Data Directories
```bash
sudo mkdir -p /opt/icon-ruc/{data/{postgres,redis,app,prometheus,grafana,loki},logs,backups}
sudo chown -R 1000:1000 /opt/icon-ruc
```

### 4. SSL Certificate Setup
```bash
# Using Let's Encrypt (recommended)
sudo certbot certonly --webroot -w /var/www/html -d your-domain.com

# Copy certificates
sudo mkdir -p ssl/
sudo cp /etc/letsencrypt/live/your-domain.com/fullchain.pem ssl/cert.pem
sudo cp /etc/letsencrypt/live/your-domain.com/privkey.pem ssl/key.pem
sudo chown 1000:1000 ssl/*
```

## Deployment Methods

### Method 1: Production Deployment Script (Recommended)

The deployment script handles zero-downtime deployments with health checks:

```bash
# Full production deployment
./scripts/deploy.sh production

# Deploy to staging
./scripts/deploy.sh staging

# Deploy specific image tag
./scripts/deploy.sh production v1.2.3
```

**Deployment features:**
- Pre-deployment validation
- Database backup before changes
- Rolling updates with health checks
- Automatic rollback on failure
- Notification integration

### Method 2: Manual Docker Compose

For direct control over the deployment:

```bash
# Production environment
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d

# Development environment  
docker-compose up -d

# Staging environment
docker-compose -f docker-compose.yml -f docker-compose.staging.yml up -d
```

### Method 3: CI/CD Pipeline

Automated deployment through GitHub Actions:

1. **Push to `develop` branch** → Triggers staging deployment
2. **Create release** → Triggers production deployment
3. **Manual workflow dispatch** → Deploy specific version

## Service Configuration

### Application Service (Flask API)

**Key features:**
- Production WSGI server (Gunicorn)
- WebSocket support with Redis scaling
- Prometheus metrics export
- Comprehensive health checks
- Structured logging

**Configuration:**
- **Replicas**: 2 (production)
- **Resources**: 2 CPU cores, 4GB RAM limit
- **Health check**: `/api/health` endpoint
- **Logs**: JSON structured logging

### Database Service (PostgreSQL 15)

**Optimized for:**
- High-performance read/write operations
- Connection pooling
- Query performance monitoring
- Automated backups

**Configuration:**
- **Shared buffers**: 256MB
- **Max connections**: 100
- **Work memory**: 4MB
- **Effective cache size**: 1GB

### Cache Service (Redis 7)

**Features:**
- Session management
- WebSocket message queuing
- LRU cache eviction
- Persistence with AOF

**Configuration:**
- **Max memory**: 400MB
- **Eviction policy**: allkeys-lru
- **Persistence**: RDB + AOF

### Load Balancer (Nginx)

**Features:**
- SSL termination
- Static file serving
- WebSocket proxying
- Rate limiting
- Security headers

**Configuration:**
- **Rate limits**: 10 req/s for API, 5 req/s for WebSocket
- **SSL**: TLS 1.2+ with secure cipher suites
- **Compression**: Gzip enabled
- **Caching**: Static assets cached for 1 year

## Monitoring and Observability

### Metrics (Prometheus)

**Application metrics:**
- HTTP request rates, duration, errors
- Processing job queues and completion rates
- WebSocket connection counts
- Database query performance

**System metrics:**
- CPU, memory, disk usage
- Container resource utilization
- Network I/O statistics
- Service availability

### Visualization (Grafana)

**Pre-configured dashboards:**
- System overview with key KPIs
- Application performance metrics
- Processing job monitoring
- Infrastructure resource utilization

**Access:** `http://your-domain:3000`
**Default credentials:** admin / (value from GRAFANA_PASSWORD)

### Logging (Loki + Promtail)

**Centralized logging:**
- Application logs (structured JSON)
- Nginx access/error logs
- System logs
- Container logs

**Log retention:** 30 days (configurable)

### Alerting

**Critical alerts:**
- Service downtime
- High error rates
- Database connection issues
- Disk space warnings
- Long-running jobs

**Notification channels:**
- Slack webhooks
- Email (configurable)
- PagerDuty integration (optional)

## Backup and Disaster Recovery

### Automated Backups

```bash
# Full system backup
./scripts/backup-restore.sh backup-full

# Database only
./scripts/backup-restore.sh backup-db

# Configuration backup
./scripts/backup-restore.sh backup-config
```

**Backup features:**
- Compressed SQL dumps
- Volume snapshots
- Configuration archives
- S3 remote storage (optional)
- 30-day retention policy

### Disaster Recovery

**Database restore:**
```bash
./scripts/backup-restore.sh restore-db /path/to/backup.sql.gz
```

**Volume restore:**
```bash
./scripts/backup-restore.sh restore-volumes /path/to/backup.tar.gz
```

**Recovery time objectives:**
- **RTO (Recovery Time Objective)**: 30 minutes
- **RPO (Recovery Point Objective)**: 24 hours

## Security Configuration

### Network Security
- **Firewall**: Only necessary ports exposed (80, 443)
- **Internal communication**: Docker network isolation
- **Rate limiting**: API and WebSocket endpoints protected

### Application Security
- **Authentication**: Session-based with Redis storage
- **Headers**: Security headers configured (CSP, HSTS, etc.)
- **Input validation**: All API inputs validated
- **Secrets management**: Environment variables, no hardcoded secrets

### Container Security
- **Non-root user**: Application runs as UID 1000
- **Read-only filesystem**: Where possible
- **Resource limits**: CPU and memory constraints
- **Security scanning**: Trivy integration in CI/CD

## Performance Optimization

### Application Performance
- **Async processing**: Background job processing
- **Connection pooling**: Database connections optimized
- **Caching**: Redis caching for frequently accessed data
- **Static file serving**: Nginx serves static content

### Database Performance
- **Indexing**: Critical queries indexed
- **Connection pooling**: SQLAlchemy pool configuration
- **Query optimization**: Monitoring slow queries
- **Partitioning**: Time-based partitioning for large tables

### Infrastructure Performance
- **Resource allocation**: Services sized appropriately
- **Load balancing**: Nginx upstream configuration
- **Monitoring**: Real-time performance metrics
- **Scaling**: Horizontal scaling capability

## Troubleshooting

### Common Issues

**Service won't start:**
```bash
# Check logs
docker-compose logs app

# Check environment
docker-compose config

# Validate configuration
docker-compose -f docker-compose.yml -f docker-compose.prod.yml config
```

**Database connection issues:**
```bash
# Check database status
docker-compose exec db pg_isready -U iconuser

# Check connections
docker-compose exec db psql -U iconuser -c "SELECT * FROM pg_stat_activity;"
```

**High memory usage:**
```bash
# Monitor resource usage
docker stats

# Check application metrics
curl http://localhost:5000/metrics | grep memory
```

**WebSocket connection failures:**
```bash
# Check Redis connectivity
docker-compose exec redis redis-cli ping

# Monitor WebSocket connections
curl http://localhost:5000/metrics | grep websocket
```

### Log Analysis

**Application logs:**
```bash
# View structured logs
docker-compose logs app | jq '.'

# Filter by log level
docker-compose logs app | jq 'select(.level=="ERROR")'
```

**Database logs:**
```bash
# Query performance
docker-compose exec db psql -U iconuser -c "SELECT * FROM pg_stat_statements ORDER BY total_time DESC LIMIT 10;"
```

### Performance Debugging

**Slow API responses:**
```bash
# Check Prometheus metrics
curl http://localhost:9090/api/v1/query?query=flask_http_request_duration_seconds

# Analyze database queries
docker-compose exec db psql -U iconuser -c "SELECT query, mean_time, calls FROM pg_stat_statements ORDER BY mean_time DESC LIMIT 10;"
```

## Maintenance

### Regular Tasks

**Weekly:**
- Review monitoring dashboards
- Check backup integrity
- Update security patches
- Review log alerts

**Monthly:**
- Update dependencies
- Performance review
- Capacity planning
- Security audit

**Quarterly:**
- Disaster recovery testing
- Documentation updates
- Architecture review
- Cost optimization

### Updates and Upgrades

**Application updates:**
```bash
# Pull latest images
docker-compose pull

# Rolling update
./scripts/deploy.sh production latest
```

**System updates:**
```bash
# Update system packages
sudo apt update && sudo apt upgrade

# Update Docker
sudo apt install docker-ce docker-ce-cli containerd.io

# Update Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
```

## Support and Contact

### Documentation
- **API Documentation**: `/api/docs` endpoint
- **Monitoring Guides**: Grafana dashboard help
- **Architecture Diagrams**: `docs/` directory

### Monitoring Endpoints
- **Health Check**: `http://your-domain/api/health`
- **Metrics**: `http://your-domain:9090` (Prometheus)
- **Dashboards**: `http://your-domain:3000` (Grafana)
- **Logs**: Grafana Loki integration

### Emergency Procedures

**Critical service failure:**
1. Check health endpoints
2. Review recent deployments
3. Check resource utilization
4. Rollback if necessary
5. Contact on-call team

**Data corruption:**
1. Stop application immediately
2. Assess corruption extent
3. Restore from latest backup
4. Verify data integrity
5. Resume operations

This deployment guide provides a production-ready setup for the ICON-RUC application with comprehensive monitoring, security, and operational procedures.