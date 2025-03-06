FROM python:3.9-slim

WORKDIR /app

# Install PostgreSQL client and required system dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy migration scripts and server
COPY pg_migrate.py pg_migrate.sh server.py ./
RUN chmod +x pg_migrate.py pg_migrate.sh server.py

# Expose port 3000
EXPOSE 3000

# Run the HTTP server
CMD ["python", "server.py"]
