#!/bin/bash

# TAPIR + PopPUNK + Per-Clade SNP Analysis Pipeline - CHUNKED LOCAL EXECUTION
# This script processes large datasets in smaller chunks to prevent PopPUNK segmentation faults

set -euo pipefail

# Configuration
INPUT_DIR="${1:-./assemblies}"
OUTPUT_DIR="${2:-./results_chunked}"
CHUNK_SIZE="${3:-150}"  # Process max 150 files at a time

echo "=== TAPIR + PopPUNK Pipeline - Chunked Local Execution ==="
echo "Input Directory: $INPUT_DIR"
echo "Output Directory: $OUTPUT_DIR"
echo "Chunk Size: $CHUNK_SIZE files"
echo "=========================================================="

# Check if input directory exists
if [ ! -d "$INPUT_DIR" ]; then
    echo "ERROR: Input directory $INPUT_DIR does not exist!"
    echo "Usage: $0 [input_dir] [output_dir] [chunk_size]"
    echo "Example: $0 ./assemblies ./results_chunked 150"
    exit 1
fi

# Count FASTA files
FASTA_COUNT=$(find "$INPUT_DIR" -name "*.fasta" -o -name "*.fa" -o -name "*.fas" | wc -l)
echo "Found $FASTA_COUNT FASTA files in $INPUT_DIR"

if [ $FASTA_COUNT -eq 0 ]; then
    echo "ERROR: No FASTA files found in $INPUT_DIR"
    echo "Supported extensions: .fasta, .fa, .fas"
    exit 1
fi

# Calculate number of chunks
NUM_CHUNKS=$(( (FASTA_COUNT + CHUNK_SIZE - 1) / CHUNK_SIZE ))
echo "Will process in $NUM_CHUNKS chunks of max $CHUNK_SIZE files each"

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Setup environment
echo "Setting up environment..."
./setup_ubuntu_docker.sh

# Run the chunked pipeline
echo "Starting chunked pipeline execution..."
echo "This approach prevents PopPUNK segmentation faults by processing smaller batches"
echo ""

./nextflow run nextflow_tapir_poppunk_snp_chunked.nf \
    -profile ubuntu_docker \
    --input "$INPUT_DIR" \
    --resultsDir "$OUTPUT_DIR" \
    --chunk_size "$CHUNK_SIZE" \
    --poppunk_threads 8 \
    --panaroo_threads 16 \
    --gubbins_threads 8 \
    --iqtree_threads 4 \
    -with-report "${OUTPUT_DIR}/reports/pipeline_report.html" \
    -with-timeline "${OUTPUT_DIR}/reports/timeline.html" \
    -with-trace "${OUTPUT_DIR}/reports/trace.txt" \
    -with-dag "${OUTPUT_DIR}/reports/dag.html" \
    -resume

# Check if pipeline completed successfully
if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Chunked pipeline completed successfully!"
    echo "Results are in: $OUTPUT_DIR"
    echo ""
    echo "Output structure:"
    echo "├── poppunk_chunks/        # Individual chunk results"
    echo "├── poppunk/              # Merged clustering results"
    echo "├── cluster_*/            # Per-cluster analysis results"
    echo "└── reports/              # Pipeline execution reports"
    echo ""
    echo "📊 View reports:"
    echo "   - Pipeline report: ${OUTPUT_DIR}/reports/pipeline_report.html"
    echo "   - Timeline: ${OUTPUT_DIR}/reports/timeline.html"
    echo "   - Trace: ${OUTPUT_DIR}/reports/trace.txt"
else
    echo ""
    echo "❌ Pipeline failed! Check the error messages above."
    echo ""
    echo "Common troubleshooting steps:"
    echo "1. Check Docker is running: docker ps"
    echo "2. Verify input files: ls -la $INPUT_DIR/*.{fasta,fa,fas}"
    echo "3. Check available memory: free -h"
    echo "4. Review Nextflow log: cat .nextflow.log"
    echo ""
    echo "💡 Try reducing chunk size if memory issues persist:"
    echo "   $0 $INPUT_DIR $OUTPUT_DIR 100"
    exit 1
fi