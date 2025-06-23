#!/bin/bash

# Example script to run the TAPIR + PopPUNK + SNP Analysis Pipeline
# Make sure Docker is running before executing this script

# Set input and output directories
INPUT_DIR="./assemblies"
OUTPUT_DIR="./results"

# Create directories if they don't exist
mkdir -p "$INPUT_DIR"
mkdir -p "$OUTPUT_DIR"

echo "Running TAPIR + PopPUNK + SNP Analysis Pipeline"
echo "Input directory: $INPUT_DIR"
echo "Output directory: $OUTPUT_DIR"
echo "Expected input: FASTA files (.fasta, .fa, .fas)"
echo ""

# Check if input directory has FASTA files
if [ -z "$(find "$INPUT_DIR" -name "*.fasta" -o -name "*.fa" -o -name "*.fas" 2>/dev/null)" ]; then
    echo "WARNING: No FASTA files found in $INPUT_DIR"
    echo "Please add your bacterial genome assemblies to this directory"
    echo "Supported formats: .fasta, .fa, .fas"
    exit 1
fi

# Count input files
FASTA_COUNT=$(find "$INPUT_DIR" -name "*.fasta" -o -name "*.fa" -o -name "*.fas" | wc -l)
echo "Found $FASTA_COUNT FASTA files"

# Run the pipeline
echo "Starting pipeline execution..."
./nextflow run nextflow_tapir_poppunk_snp.nf \
    --input "$INPUT_DIR" \
    --resultsDir "$OUTPUT_DIR" \
    -with-report "$OUTPUT_DIR/execution_report.html" \
    -with-timeline "$OUTPUT_DIR/execution_timeline.html" \
    -with-dag "$OUTPUT_DIR/workflow_dag.html"

echo "Pipeline completed!"
echo "Check results in: $OUTPUT_DIR"