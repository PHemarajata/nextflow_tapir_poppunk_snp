#!/bin/bash

# Test script for chunked processing approach
# This script tests the chunked Google Batch execution

set -euo pipefail

echo "=== Testing Chunked Processing Approach ==="
echo "This script will test the chunked processing with a small chunk size"
echo "to verify the segfault prevention works correctly."
echo ""

# Configuration for testing
PROJECT_ID="erudite-pod-307018"
INPUT_BUCKET="gs://aphlhq-ngs-gh/nextflow_data/subset_100"
OUTPUT_BUCKET="gs://aphlhq-ngs-gh/nextflow_data/test_chunked_results"
WORK_DIR="gs://aphlhq-ngs-gh/nextflow_work_test_chunked"
CHUNK_SIZE=50  # Small chunk size for testing

echo "Test Configuration:"
echo "- Project: $PROJECT_ID"
echo "- Input: $INPUT_BUCKET"
echo "- Output: $OUTPUT_BUCKET"
echo "- Work Dir: $WORK_DIR"
echo "- Chunk Size: $CHUNK_SIZE files"
echo ""

# Check authentication
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q .; then
    echo "ERROR: No active gcloud authentication found."
    echo "Please run: gcloud auth login"
    exit 1
fi

# Set project
gcloud config set project $PROJECT_ID

# Clean up previous test results
echo "Cleaning up previous test results..."
gsutil -m rm -rf $OUTPUT_BUCKET $WORK_DIR 2>/dev/null || echo "No previous results to clean"

# Create output directory
gsutil -m mkdir -p $OUTPUT_BUCKET

# Count input files
echo "Checking input files..."
FILE_COUNT=$(gsutil ls "$INPUT_BUCKET/*.{fasta,fa,fas}" 2>/dev/null | wc -l || echo "0")
echo "Found $FILE_COUNT input files"

if [ "$FILE_COUNT" -eq 0 ]; then
    echo "ERROR: No input files found!"
    exit 1
fi

EXPECTED_CHUNKS=$(( (FILE_COUNT + CHUNK_SIZE - 1) / CHUNK_SIZE ))
echo "Expected number of chunks: $EXPECTED_CHUNKS"
echo ""

# Run the chunked pipeline
echo "Starting chunked pipeline test..."
echo "This should process files in chunks of $CHUNK_SIZE to prevent segfaults"
echo ""

./nextflow run nextflow_tapir_poppunk_snp.nf \
    -profile google_batch \
    --input $INPUT_BUCKET \
    --resultsDir $OUTPUT_BUCKET \
    --chunk_size $CHUNK_SIZE \
    -w $WORK_DIR \
    -with-report "${OUTPUT_BUCKET}/reports/test_report.html" \
    -with-timeline "${OUTPUT_BUCKET}/reports/test_timeline.html" \
    -resume

# Check results
if [ $? -eq 0 ]; then
    echo ""
    echo "✅ CHUNKED PROCESSING TEST COMPLETED SUCCESSFULLY!"
    echo ""
    echo "Results available at: $OUTPUT_BUCKET"
    echo "Reports available at: ${OUTPUT_BUCKET}/reports/"
    echo ""
    echo "To view results:"
    echo "gsutil ls -r $OUTPUT_BUCKET"
    echo ""
    echo "To download results:"
    echo "gsutil -m cp -r $OUTPUT_BUCKET ./test_chunked_results"
else
    echo ""
    echo "❌ CHUNKED PROCESSING TEST FAILED!"
    echo ""
    echo "Check the Nextflow log for details:"
    echo "gsutil cat ${WORK_DIR}/.nextflow.log"
    echo ""
    echo "Common issues:"
    echo "- PopPUNK still segfaulting (try smaller chunk size)"
    echo "- Google Cloud quota limits"
    echo "- Authentication problems"
fi

echo ""
echo "=== Test Complete ==="