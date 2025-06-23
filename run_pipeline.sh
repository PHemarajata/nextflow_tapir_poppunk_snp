#!/bin/bash

# TAPIR + PopPUNK + Per-Clade SNP Analysis Pipeline
# Local execution script optimized for Ubuntu Docker environment

# Set your input and output directories
INPUT_DIR="./assemblies"
OUTPUT_DIR="./results"

echo "TAPIR + PopPUNK + SNP Analysis Pipeline - Local Execution"
echo "========================================================="

# Check if input directory exists
if [ ! -d "$INPUT_DIR" ]; then
    echo "Error: Input directory $INPUT_DIR does not exist!"
    echo "Please create it and add your FASTA files (.fasta, .fa, or .fas)"
    exit 1
fi

# Count FASTA files
FASTA_COUNT=$(find "$INPUT_DIR" -name "*.fasta" -o -name "*.fa" -o -name "*.fas" | wc -l)
echo "Found $FASTA_COUNT FASTA files in $INPUT_DIR"

if [ $FASTA_COUNT -eq 0 ]; then
    echo "Error: No FASTA files found in $INPUT_DIR"
    echo "Supported extensions: .fasta, .fa, .fas"
    exit 1
fi

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Setup Ubuntu Docker environment
echo "Setting up Ubuntu Docker environment..."
./setup_ubuntu_docker.sh

# Run the pipeline
echo "Starting TAPIR + PopPUNK + SNP Analysis Pipeline..."
echo "Input: $INPUT_DIR ($FASTA_COUNT files)"
echo "Output: $OUTPUT_DIR"
echo "System: Optimized for large datasets with ultra-conservative PopPUNK settings"
echo "Containers: StaPH-B Docker images from DockerHub"
echo ""

./nextflow run nextflow_tapir_poppunk_snp.nf \
    -profile ubuntu_docker \
    --input "$INPUT_DIR" \
    --resultsDir "$OUTPUT_DIR" \
    --poppunk_threads 8 \
    --panaroo_threads 16 \
    --gubbins_threads 8 \
    --iqtree_threads 4

# Check if pipeline completed successfully
if [ $? -eq 0 ]; then
    echo ""
    echo "Pipeline completed successfully!"
    echo "Results are in: $OUTPUT_DIR"
    echo ""
    echo "Output structure:"
    echo "├── poppunk/           # PopPUNK clustering results"
    echo "├── cluster_*/         # Per-cluster analysis results"
    echo "├── pipeline_report.html"
    echo "├── timeline.html"
    echo "└── trace.txt"
else
    echo ""
    echo "Pipeline failed! Check the error messages above."
    echo "Common issues:"
    echo "- Docker not running"
    echo "- Insufficient memory"
    echo "- Invalid FASTA files"
    echo "- PopPUNK segmentation fault (use monitor script to track)"
    exit 1
fi