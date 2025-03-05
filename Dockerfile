FROM debian:bullseye-slim

# Install PostgreSQL client tools and dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    postgresql-client \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user to run the script
RUN useradd -m -s /bin/bash worker

# Copy the script
COPY copyHackatimeDb.sh /home/worker/
RUN chmod +x /home/worker/copyHackatimeDb.sh

# Switch to non-root user
USER worker
WORKDIR /home/worker

# Run the script
ENTRYPOINT ["./copyHackatimeDb.sh"] 