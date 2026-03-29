# ═══════════════════════════════════════════════════
# TRAKR Backend — Unified Dockerfile (Python + Node)
# Runs Flask API + WhatsApp Bot in a single container
# ═══════════════════════════════════════════════════

FROM python:3.10-slim

# Install Node.js 20.x
RUN apt-get update && \
    apt-get install -y curl && \
    curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y nodejs && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt gunicorn

# Node dependencies
COPY whatsapp-bot/package*.json ./whatsapp-bot/
RUN cd whatsapp-bot && npm install --omit=dev

# Copy application code
COPY . .

# Make startup script executable
RUN chmod +x start.sh

# Render provides PORT env variable
ENV PORT=5000

EXPOSE 5000

CMD ["bash", "start.sh"]
