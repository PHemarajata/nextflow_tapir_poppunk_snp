#!/bin/bash

# TAPIR + PopPUNK + Per-Clade SNP Analysis Pipeline
# Google Cloud Batch execution script with fixed GCS file detection
# Project: erudite-pod-307018

# Google Cloud configuration
PROJECT_ID="erudite-pod-307018"
INPUT_BUCKET="gs://aphlhq-ngs-gh/nextflow_data/subset_100"
RESULTS_BUCKET="gs://aphlhq-ngs-gh/nextflow_data/subset_100_results"
WORK_BUCKET="gs://aphlhq-ngs-gh/nextflow_work"

echo "TAPIR + PopPUNK + SNP Analysis Pipeline - Google Cloud Batch"
echo "============================================================"
echo "Project: $PROJECT_ID"
echo "Input: $INPUT_BUCKET"
echo "Results: $RESULTS_BUCKET"
echo "Work directory: $WORK_BUCKET"
echo ""
echo "💡 Tip: If you encounter issues finding FASTA files, run:"
echo "   ./diagnose_gcs.sh"
echo ""

# Check if gcloud is configured
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q .; then
    echo "Error: No active Google Cloud authentication found!"
    echo "Please run: gcloud auth login"
    exit 1
fi

# Check if project is set correctly
CURRENT_PROJECT=$(gcloud config get-value project)
if [ "$CURRENT_PROJECT" != "$PROJECT_ID" ]; then
    echo "Setting Google Cloud project to $PROJECT_ID..."
    gcloud config set project $PROJECT_ID
fi

# Enable required APIs (if not already enabled)
echo "Ensuring required Google Cloud APIs are enabled..."
gcloud services enable batch.googleapis.com --quiet
gcloud services enable compute.googleapis.com --quiet
gcloud services enable storage.googleapis.com --quiet

# Check if input bucket exists and has files
echo "Checking input data..."

# Check if bucket exists first
if ! gsutil ls "$INPUT_BUCKET/" >/dev/null 2>&1; then
    echo "Error: Input bucket $INPUT_BUCKET does not exist or is not accessible!"
    echo "Please check:"
    echo "1. Bucket name is correct"
    echo "2. You have read permissions"
    echo "3. Bucket exists in the project"
    exit 1
fi

# Count FASTA files with individual checks for each extension
echo "Scanning for FASTA files..."
FASTA_COUNT=0

# Check .fasta files
FASTA_FILES=$(gsutil ls "$INPUT_BUCKET/*.fasta" 2>/dev/null | wc -l)
FASTA_COUNT=$((FASTA_COUNT + FASTA_FILES))
if [ $FASTA_FILES -gt 0 ]; then
    echo "Found $FASTA_FILES .fasta files"
fi

# Check .fa files
FA_FILES=$(gsutil ls "$INPUT_BUCKET/*.fa" 2>/dev/null | wc -l)
FASTA_COUNT=$((FASTA_COUNT + FA_FILES))
if [ $FA_FILES -gt 0 ]; then
    echo "Found $FA_FILES .fa files"
fi

# Check .fas files
FAS_FILES=$(gsutil ls "$INPUT_BUCKET/*.fas" 2>/dev/null | wc -l)
FASTA_COUNT=$((FASTA_COUNT + FAS_FILES))
if [ $FAS_FILES -gt 0 ]; then
    echo "Found $FAS_FILES .fas files"
fi

if [ $FASTA_COUNT -eq 0 ]; then
    echo "Error: No FASTA files found in $INPUT_BUCKET"
    echo ""
    echo "Available files in bucket:"
    gsutil ls "$INPUT_BUCKET/" | head -10
    echo ""
    echo "Please ensure your FASTA files are uploaded with supported extensions:"
    echo "- .fasta"
    echo "- .fa" 
    echo "- .fas"
    echo ""
    echo "Upload examples:"
    echo "gsutil -m cp ./local_assemblies/*.fasta $INPUT_BUCKET/"
    echo "gsutil -m cp ./local_assemblies/*.fa $INPUT_BUCKET/"
    echo "gsutil -m cp ./local_assemblies/*.fas $INPUT_BUCKET/"
    echo ""
    echo "🔧 For detailed troubleshooting, run:"
    echo "   ./diagnose_gcs.sh"
    echo ""
    echo "📋 To check what's currently in the bucket:"
    echo "   gsutil ls -l $INPUT_BUCKET/"
    exit 1
fi

echo "Total: Found $FASTA_COUNT FASTA files in input bucket"

# Create results bucket if it doesn't exist
echo "Ensuring results bucket exists..."
gsutil mb "$RESULTS_BUCKET" 2>/dev/null || echo "Results bucket already exists"

# Run the pipeline with Google Batch profile
echo "Starting pipeline execution on Google Cloud Batch..."
echo "This may take several hours for large datasets."
echo ""

./nextflow run nextflow_tapir_poppunk_snp.nf \
    -profile google_batch \
    --input "$INPUT_BUCKET" \
    --resultsDir "$RESULTS_BUCKET" \
    -w "$WORK_BUCKET" \
    -resume

# Check if pipeline completed successfully
if [ $? -eq 0 ]; then
    echo ""
    echo "Pipeline completed successfully!"
    echo "Results are available in: $RESULTS_BUCKET"
    echo ""
    echo "You can view the results with:"
    echo "gsutil ls -r $RESULTS_BUCKET"
    echo ""
    echo "Download results locally with:"
    echo "gsutil -m cp -r $RESULTS_BUCKET ./local_results"
else
    echo ""
    echo "Pipeline failed! Check the error messages above."
    echo "Common issues:"
    echo "- Insufficient Google Cloud quotas"
    echo "- Authentication problems"
    echo "- Invalid input data"
    echo ""
    echo "Check the Nextflow log for details:"
    echo "cat .nextflow.log"
fi
