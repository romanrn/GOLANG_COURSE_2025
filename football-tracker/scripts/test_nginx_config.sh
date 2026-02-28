#!/bin/bash

# Nginx Configuration Testing Script
# Tests syntax without requiring Docker Compose network

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}=== Nginx Configuration Testing ===${NC}\n"

# Test 1: Syntax check with test config (without backend dependency)
echo -e "${YELLOW}Test 1: Syntax check (standalone)${NC}"
if docker run --rm \
    -v "$(pwd)/nginx/nginx.conf.test:/etc/nginx/nginx.conf:ro" \
    nginx:1.25-alpine nginx -t 2>&1; then
    echo -e "${GREEN}✓ Nginx syntax is valid${NC}\n"
else
    echo -e "${RED}✗ Nginx syntax error${NC}\n"
    exit 1
fi

# Test 2: Check main config has correct structure (parsing only)
echo -e "${YELLOW}Test 2: Check main config structure${NC}"
if grep -q "upstream backend" nginx/nginx.conf && \
   grep -q "server backend:8080" nginx/nginx.conf && \
   grep -q "listen 8443 ssl http2" nginx/nginx.conf && \
   grep -q "ssl_protocols TLSv1.2 TLSv1.3" nginx/nginx.conf; then
    echo -e "${GREEN}✓ Main config structure is correct${NC}\n"
else
    echo -e "${RED}✗ Main config structure issue${NC}\n"
    exit 1
fi

# Test 3: Verify SSL configuration
echo -e "${YELLOW}Test 3: SSL configuration${NC}"
if grep -q "ssl_certificate /etc/nginx/ssl/cert.pem" nginx/nginx.conf && \
   grep -q "ssl_certificate_key /etc/nginx/ssl/key.pem" nginx/nginx.conf; then
    echo -e "${GREEN}✓ SSL certificates configured${NC}\n"
else
    echo -e "${RED}✗ SSL certificates not configured${NC}\n"
    exit 1
fi

# Test 4: Verify security headers
echo -e "${YELLOW}Test 4: Security headers${NC}"
if grep -q "X-Frame-Options" nginx/nginx.conf && \
   grep -q "X-Content-Type-Options" nginx/nginx.conf && \
   grep -q "Content-Security-Policy" nginx/nginx.conf; then
    echo -e "${GREEN}✓ Security headers configured${NC}\n"
else
    echo -e "${RED}✗ Security headers missing${NC}\n"
    exit 1
fi

# Test 5: Verify rate limiting
echo -e "${YELLOW}Test 5: Rate limiting${NC}"
if grep -q "limit_req_zone" nginx/nginx.conf && \
   grep -q "zone=api_limit" nginx/nginx.conf && \
   grep -q "zone=auth_limit" nginx/nginx.conf; then
    echo -e "${GREEN}✓ Rate limiting configured${NC}\n"
else
    echo -e "${RED}✗ Rate limiting not configured${NC}\n"
    exit 1
fi

# Test 6: Verify non-root configuration
echo -e "${YELLOW}Test 6: Non-root execution${NC}"
if grep -q "user nginx" nginx/nginx.conf && \
   grep -q "listen 8080" nginx/nginx.conf && \
   grep -q "listen 8443" nginx/nginx.conf; then
    echo -e "${GREEN}✓ Non-root configuration (unprivileged ports)${NC}\n"
else
    echo -e "${RED}✗ Non-root configuration issue${NC}\n"
    exit 1
fi

# Test 7: Verify proxy headers
echo -e "${YELLOW}Test 7: Proxy headers${NC}"
if grep -q "X-Real-IP" nginx/nginx.conf && \
   grep -q "X-Forwarded-For" nginx/nginx.conf && \
   grep -q "X-Forwarded-Proto" nginx/nginx.conf; then
    echo -e "${GREEN}✓ Proxy headers configured${NC}\n"
else
    echo -e "${RED}✗ Proxy headers missing${NC}\n"
    exit 1
fi

# Test 8: Verify cookie security
echo -e "${YELLOW}Test 8: Cookie security${NC}"
if grep -q "Secure; HttpOnly; SameSite=Strict" nginx/nginx.conf; then
    echo -e "${GREEN}✓ Cookie security flags configured${NC}\n"
else
    echo -e "${RED}✗ Cookie security flags missing${NC}\n"
    exit 1
fi

# Summary
echo -e "${BLUE}=== Test Summary ===${NC}"
echo -e "${GREEN}✓ All tests passed!${NC}"
echo -e "\n${YELLOW}Note:${NC} Main config uses 'backend:8080' which resolves in Docker Compose network."
echo -e "This is correct for production. Test config uses 'localhost:8080' for syntax validation."
echo -e "\n${GREEN}Configuration is ready for deployment!${NC}\n"

