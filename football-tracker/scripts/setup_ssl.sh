#!/bin/bash

# SSL Certificate Generation Script for Football Tracker
# Automatically detects if Let's Encrypt is possible, otherwise uses self-signed

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Configuration
SSL_DIR="./nginx/ssl"
DOMAIN="${DOMAIN:-}"
EMAIL="${LETSENCRYPT_EMAIL:-}"

echo -e "${GREEN}=== Football Tracker SSL Certificate Setup ===${NC}\n"

# Validate variables
if [ -z "$DOMAIN" ] || [ -z "$EMAIL" ]; then
    echo -e "${RED}Error: DOMAIN and LETSENCRYPT_EMAIL required${NC}"
    echo -e "Usage: DOMAIN=your-domain.com LETSENCRYPT_EMAIL=your-email@example.com ./setup_ssl.sh"
    exit 1
fi

# Create directories
mkdir -p "$SSL_DIR"
mkdir -p ./nginx/certbot/conf
mkdir -p ./nginx/certbot/www

# Check DNS
check_dns() {
    echo -e "${BLUE}Checking DNS for $DOMAIN...${NC}"
    if host "$DOMAIN" > /dev/null 2>&1; then
        local ip=$(host "$DOMAIN" | grep "has address" | head -1 | awk '{print $4}')
        if [ -n "$ip" ]; then
            echo -e "${GREEN}✓ DNS resolves: $DOMAIN → $ip${NC}"
            return 0
        fi
    fi
    echo -e "${YELLOW}⚠ DNS does not resolve for $DOMAIN${NC}"
    return 1
}

# Generate self-signed certificate
generate_self_signed() {
    echo -e "${YELLOW}Generating self-signed certificate...${NC}"
    echo -e "${YELLOW}⚠ Warning: Self-signed = testing only!${NC}\n"

    openssl req -x509 -nodes -days 365 -newkey rsa:4096 \
        -keyout "$SSL_DIR/key.pem" \
        -out "$SSL_DIR/cert.pem" \
        -subj "/C=UA/ST=Kyiv/L=Kyiv/O=FootballTracker/CN=$DOMAIN" \
        -addext "subjectAltName=DNS:$DOMAIN,DNS:*.$DOMAIN" 2>/dev/null

    chmod 600 "$SSL_DIR/key.pem"
    chmod 644 "$SSL_DIR/cert.pem"

    echo -e "${GREEN}✓ Self-signed certificate created${NC}"
    echo -e "  Certificate: $SSL_DIR/cert.pem"
    echo -e "  Private Key: $SSL_DIR/key.pem"
    echo -e "  Valid for: 365 days"
    echo -e "  Domain: $DOMAIN"
    echo -e "  Region: Kyiv, Ukraine"
    echo -e "\n${YELLOW}Note: For production, fix DNS and regenerate!${NC}\n"
}

# Generate Let's Encrypt certificate
generate_letsencrypt() {
    echo -e "${GREEN}Generating Let's Encrypt certificate...${NC}"
    echo -e "Domain: $DOMAIN"
    echo -e "Email: $EMAIL"
    echo -e "Region: Kyiv, Ukraine\n"

    # Stop nginx if running
    docker compose stop nginx 2>/dev/null || true
    sleep 2

    # Request certificate
    echo -e "${BLUE}Requesting certificate from Let's Encrypt...${NC}"
    if docker run -it --rm \
        -v "$(pwd)/nginx/certbot/conf:/etc/letsencrypt" \
        -v "$(pwd)/nginx/certbot/www:/var/www/certbot" \
        -p 80:80 \
        certbot/certbot certonly --standalone \
        --email "$EMAIL" \
        --agree-tos \
        --no-eff-email \
        --domain "$DOMAIN" \
        --rsa-key-size 4096 \
        --verbose; then

        # Copy certificates
        if [ -f "./nginx/certbot/conf/live/$DOMAIN/fullchain.pem" ]; then
            cp "./nginx/certbot/conf/live/$DOMAIN/fullchain.pem" "$SSL_DIR/cert.pem"
            cp "./nginx/certbot/conf/live/$DOMAIN/privkey.pem" "$SSL_DIR/key.pem"

            chmod 600 "$SSL_DIR/key.pem"
            chmod 644 "$SSL_DIR/cert.pem"

            echo -e "\n${GREEN}✓ Let's Encrypt certificate installed!${NC}"
            echo -e "  Certificate: $SSL_DIR/cert.pem"
            echo -e "  Private Key: $SSL_DIR/key.pem"
            echo -e "  Valid for: 90 days"
            echo -e "  Domain: $DOMAIN"
            echo -e "  Issuer: Let's Encrypt"
            return 0
        fi
    fi

    # If failed
    echo -e "\n${RED}✗ Let's Encrypt failed!${NC}"
    echo -e "${YELLOW}Common issues:${NC}"
    echo -e "  • DNS: $DOMAIN must point to this server's IP"
    echo -e "  • Port 80: Must be accessible from internet"
    echo -e "  • Firewall: Check if port 80 is open"
    return 1
}

# Check existing certificate
if [ -f "$SSL_DIR/cert.pem" ]; then
    echo -e "${GREEN}Existing certificate found:${NC}"
    openssl x509 -in "$SSL_DIR/cert.pem" -noout -subject -dates -issuer 2>/dev/null
    echo ""
    read -p "Regenerate? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Keeping existing certificate"
        exit 0
    fi
fi

# Check DNS
if check_dns; then
    echo -e "\n${BLUE}DNS is configured. Choose method:${NC}"
    echo "1) Let's Encrypt (Production - Recommended)"
    echo "2) Self-signed (Testing)"
    read -p "Choice [1-2] (default: 1): " choice
    choice=${choice:-1}

    if [ "$choice" = "1" ]; then
        if generate_letsencrypt; then
            echo -e "\n${GREEN}=== Success! ===${NC}"
            echo -e "Start services: docker compose up -d"
            echo -e "Setup renewal: ./scripts/renew_ssl.sh\n"
            exit 0
        else
            echo -e "\n${YELLOW}Falling back to self-signed...${NC}"
            generate_self_signed
        fi
    else
        generate_self_signed
    fi
else
    echo -e "\n${YELLOW}⚠ DNS not configured for $DOMAIN${NC}"
    echo -e "${YELLOW}Cannot use Let's Encrypt without valid DNS${NC}\n"
    read -p "Generate self-signed for testing? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        generate_self_signed
    else
        echo "Exiting..."
        exit 1
    fi
fi

echo -e "\n${GREEN}=== Setup Complete ===${NC}"
echo -e "Start services: docker compose up -d\n"

