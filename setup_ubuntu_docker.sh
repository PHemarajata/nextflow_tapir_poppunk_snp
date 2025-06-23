#!/bin/bash

# Setup script for Ubuntu Docker environment
# Fixes Docker file sharing issues with Nextflow

echo "Setting up Ubuntu Docker environment for Nextflow pipeline..."

# Create necessary directories
echo "Creating work and temp directories..."
mkdir -p ./work
mkdir -p ./tmp
mkdir -p ./results

# Set proper permissions
echo "Setting permissions..."
chmod 755 ./work
chmod 755 ./tmp
chmod 755 ./results

# Check Docker permissions
echo "Checking Docker permissions..."
if ! docker ps >/dev/null 2>&1; then
    echo "WARNING: Docker permission issue detected!"
    echo "You may need to add your user to the docker group:"
    echo "  sudo usermod -aG docker \$USER"
    echo "  newgrp docker"
    echo "Or run with sudo (not recommended for production)"
fi

# Check available disk space
echo "Checking disk space..."
df -h .

# Set environment variables for this session
export TMPDIR="$(pwd)/tmp"
export NXF_TEMP="$(pwd)/tmp"

echo "Setup complete!"
echo ""
echo "Available profiles to fix Docker mount issues:"
echo "1. Ubuntu Docker (recommended):"
echo "   ./nextflow run nextflow_tapir_poppunk_snp.nf -profile ubuntu_docker --input ./assemblies --resultsDir ./results"
echo ""
echo "2. Local temp directory:"
echo "   ./nextflow run nextflow_tapir_poppunk_snp.nf -profile local_tmp --input ./assemblies --resultsDir ./results"
echo ""
echo "3. Standard configuration:"
echo "   nextflow run nextflow_tapir_poppunk_snp.nf --input ./assemblies --resultsDir ./results"