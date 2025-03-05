FROM debian:bullseye-slim

# Install PostgreSQL client tools, Python, and other dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    postgresql-client \
    ca-certificates \
    python3 \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user to run the script
RUN useradd -m -s /bin/bash worker

# Copy the scripts
COPY copyHackatimeDb.sh server.py /home/worker/
RUN chmod +x /home/worker/copyHackatimeDb.sh

# Switch to non-root user
USER worker
WORKDIR /home/worker

# Expose the HTTP port
EXPOSE 3000

# Create a startup script
COPY <<'EOF' /home/worker/start.sh
#!/bin/bash
# Start the HTTP server in the background
python3 server.py &
# Keep the container running
wait
EOF

RUN chmod +x start.sh

# Run the startup script
ENTRYPOINT ["./start.sh"] 