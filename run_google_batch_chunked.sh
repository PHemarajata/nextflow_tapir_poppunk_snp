#!/bin/bash

# Google Cloud Batch execution script with chunked processing for segfault prevention
# This script processes large datasets in smaller chunks to avoid PopPUNK segmentation faults

set -euo pipefail

# Configuration
PROJECT_ID="erudite-pod-307018"
REGION="us-central1"
INPUT_BUCKET="gs://aphlhq-ngs-gh/nextflow_data/subset_100"
OUTPUT_BUCKET="gs://aphlhq-ngs-gh/nextflow_data/subset_100_results_chunked"
WORK_DIR="gs://aphlhq-ngs-gh/nextflow_work_chunked"
CHUNK_SIZE=150  # Process max 150 files at a time to prevent segfaults

echo "=== Google Cloud Batch Chunked Execution ==="
echo "Project: $PROJECT_ID"
echo "Region: $REGION"
echo "Input: $INPUT_BUCKET"
echo "Output: $OUTPUT_BUCKET"
echo "Work Directory: $WORK_DIR"
echo "Chunk Size: $CHUNK_SIZE files"
echo "============================================="

# Check if gcloud is authenticated
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q .; then
    echo "ERROR: No active gcloud authentication found."
    echo "Please run: gcloud auth login"
    exit 1
fi

# Set the project
gcloud config set project $PROJECT_ID

# Enable required APIs if not already enabled
echo "Ensuring required APIs are enabled..."
gcloud services enable batch.googleapis.com --quiet
gcloud services enable compute.googleapis.com --quiet
gcloud services enable storage.googleapis.com --quiet

# Clean up previous work directory
echo "Cleaning up previous work directory..."
gsutil -m rm -rf $WORK_DIR || echo "Work directory doesn't exist or already clean"

# Create output directory
echo "Creating output directory..."
gsutil -m mkdir -p $OUTPUT_BUCKET || echo "Output directory already exists"

# Run the chunked pipeline
echo "Starting Nextflow pipeline with chunked processing..."
echo "This will process files in chunks of $CHUNK_SIZE to prevent segmentation faults"

./nextflow run nextflow_tapir_poppunk_snp_chunked.nf \
    -profile google_batch \
    --input $INPUT_BUCKET \
    --resultsDir $OUTPUT_BUCKET \
    --chunk_size $CHUNK_SIZE \
    -w $WORK_DIR \
    -with-report "${OUTPUT_BUCKET}/reports/pipeline_report.html" \
    -with-timeline "${OUTPUT_BUCKET}/reports/timeline.html" \
    -with-trace "${OUTPUT_BUCKET}/reports/trace.txt" \
    -with-dag "${OUTPUT_BUCKET}/reports/dag.html" \
    -resume

echo "Pipeline execution completed!"
echo "Results available at: $OUTPUT_BUCKET"
echo "Reports available at: ${OUTPUT_BUCKET}/reports/"