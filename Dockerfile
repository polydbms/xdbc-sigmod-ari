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
    bc \
    && curl -fsSL https://get.docker.com | sh \
    && rm -rf /var/lib/apt/lists/*

# Create working directory
WORKDIR /app

# Copy requirements file FIRST and install Python dependencies
# This allows Docker to cache the pip install layer separately
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Then copy the rest of the application
COPY experiment_files ./experiment_files
COPY sql_scripts ./sql_scripts
COPY schemas ./schemas
# COPY experiments_new ./experiments_new
COPY optimizer ./optimizer

RUN chmod +x /app/experiment_files/prepare_postgres.sh
RUN chmod +x /app/experiment_files/prepare_parquet.sh
RUN chmod +x /app/experiment_files/prepare_tbl.sh
RUN chmod +x /app/experiment_files/spark_expt.sh
RUN chmod +x /app/experiment_files/run_experiments_for_datasets.sh

#RUN chmod +x /app/experiment_files/run_experiments.sh

# Create necessary directories
# Create necessary directories and files
RUN mkdir -p experiment_files/res 

# Set environment variables
ENV PYTHONPATH=/app

# Keep container running without executing anything
CMD ["tail", "-f", "/dev/null"]