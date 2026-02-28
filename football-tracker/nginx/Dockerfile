# Nginx Reverse Proxy with HTTPS support
# Production-ready: runs as non-root user on unprivileged ports

FROM nginx:1.25-alpine

# Install required packages
RUN apk add --no-cache \
    openssl \
    curl \
    tzdata

# Create necessary directories with proper permissions
RUN mkdir -p /var/www/certbot \
    && mkdir -p /etc/nginx/ssl \
    && mkdir -p /var/cache/nginx/client_temp \
    && mkdir -p /var/cache/nginx/proxy_temp \
    && mkdir -p /var/run/nginx \
    && touch /var/run/nginx/nginx.pid \
    && chown -R nginx:nginx /var/cache/nginx \
    && chown -R nginx:nginx /var/log/nginx \
    && chown -R nginx:nginx /etc/nginx/ssl \
    && chown -R nginx:nginx /var/run/nginx \
    && chmod 755 /var/www/certbot

# Copy nginx configuration
COPY nginx/nginx.conf /etc/nginx/nginx.conf

# Health check on unprivileged port 8080
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080 || exit 1

# Expose unprivileged ports (mapped to 80/443 via Docker port mapping)
EXPOSE 8080 8443

# Run as non-root nginx user (security best practice)
USER nginx

CMD ["nginx", "-g", "daemon off;"]

