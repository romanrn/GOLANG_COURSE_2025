#!/bin/bash

# Let's Encrypt Certificate Renewal Script
# Should be run via cron job: 0 3 * * * /path/to/renew_ssl.sh

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Configuration
SSL_DIR="./nginx/ssl"
DOMAIN="${DOMAIN:-localhost}"
LOG_FILE="./logs/ssl_renewal.log"

# Create logs directory
mkdir -p ./logs

echo -e "${GREEN}=== Let's Encrypt Certificate Renewal ===${NC}" | tee -a "$LOG_FILE"
echo "Started at: $(date)" | tee -a "$LOG_FILE"

# Function to renew certificate with Docker
renew_certificate() {
    echo -e "${YELLOW}Checking certificate expiration...${NC}" | tee -a "$LOG_FILE"

    # Check if certificate expires in less than 30 days
    if openssl x509 -checkend 2592000 -noout -in "$SSL_DIR/cert.pem" > /dev/null 2>&1; then
        echo -e "${GREEN}Certificate is still valid for more than 30 days${NC}" | tee -a "$LOG_FILE"
        return 0
    fi

    echo -e "${YELLOW}Certificate expires soon, renewing...${NC}" | tee -a "$LOG_FILE"

    # Renew using Docker
    docker run --rm \
        -v "$(pwd)/nginx/certbot/conf:/etc/letsencrypt" \
        -v "$(pwd)/nginx/certbot/www:/var/www/certbot" \
        certbot/certbot renew --quiet

    # Copy renewed certificates
    if [ -f "./nginx/certbot/conf/live/$DOMAIN/fullchain.pem" ]; then
        cp "./nginx/certbot/conf/live/$DOMAIN/fullchain.pem" "$SSL_DIR/cert.pem"
        cp "./nginx/certbot/conf/live/$DOMAIN/privkey.pem" "$SSL_DIR/key.pem"

        echo -e "${GREEN}✓ Certificate renewed successfully${NC}" | tee -a "$LOG_FILE"

        # Reload nginx
        docker compose exec -T nginx nginx -s reload
        echo -e "${GREEN}✓ Nginx reloaded${NC}" | tee -a "$LOG_FILE"
    else
        echo -e "${RED}✗ Certificate renewal failed${NC}" | tee -a "$LOG_FILE"
        return 1
    fi
}

# Run renewal
if renew_certificate; then
    echo "Completed at: $(date)" | tee -a "$LOG_FILE"
    echo -e "${GREEN}=== Renewal Successful ===${NC}" | tee -a "$LOG_FILE"
else
    echo "Failed at: $(date)" | tee -a "$LOG_FILE"
    echo -e "${RED}=== Renewal Failed ===${NC}" | tee -a "$LOG_FILE"
    exit 1
fi

