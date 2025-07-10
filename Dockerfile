FROM python:3.9-slim

# Install dependencies including Docker CLI
RUN apt-get update && apt-get install -y \
    build-essential \
    libsnappy-dev \
    cmake \
    postgresql-client \
    curl \
    texlive-latex-base \
    texlive-fonts-recommended \
    texlive-fonts-extra \
    texlive-latex-extra \
    dvipng \
    && curl -fsSL https://get.docker.com | sh \
    && rm -rf /var/lib/apt/lists/*

# Create working directory
WORKDIR /app

# Copy requirements file FIRST and install Python dependencies
# This allows Docker to cache the pip install layer separately
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Then copy the rest of the application
COPY experiments ./experiments
COPY experiments_new ./experiments_new
COPY optimizer ./optimizer

# Create necessary directories
# Create necessary directories and files
RUN mkdir -p experiments_new/res experiments_new/local_measurements experiments_new/local_measurements_new && \
    touch /app/experiments_new/local_measurements_new/xdbc_general_stats.csv && \
    chmod -R a+w /app/experiments_new/local_measurements /app/experiments_new/local_measurements_new

# Set environment variables
ENV PYTHONPATH=/app

# Keep container running without executing anything
CMD ["tail", "-f", "/dev/null"]