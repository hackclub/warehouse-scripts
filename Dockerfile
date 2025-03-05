FROM debian:bullseye-slim

# Install dependencies and PostgreSQL repository
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    gnupg \
    python3 \
    && rm -rf /var/lib/apt/lists/*

# Add PostgreSQL repository
RUN curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /usr/share/keyrings/postgresql-keyring.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/postgresql-keyring.gpg] http://apt.postgresql.org/pub/repos/apt/ bullseye-pgdg main" > /etc/apt/sources.list.d/postgresql.list

# Install PostgreSQL 16 client
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    postgresql-client-16 \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user to run the script
RUN useradd -m -s /bin/bash worker

# Copy the scripts
COPY . /home/worker/

# Create tmp directory with correct permissions
RUN mkdir -p /home/worker/tmp && \
    chown worker:worker /home/worker/tmp && \
    chmod 755 /home/worker/tmp

# Switch to non-root user
USER worker
WORKDIR /home/worker

# Expose the HTTP port
EXPOSE 3000

# Run the HTTP server
CMD ["python3", "server.py"] 