#!/bin/bash

# Quick script to enable ultra-high memory machines for PopPUNK segfault prevention

set -euo pipefail

echo "=== Enabling Ultra-High Memory Configuration ==="

# Backup current config
cp nextflow.config nextflow.config.backup
echo "✅ Backed up current config to nextflow.config.backup"

# Replace machine type with ultra-high memory
sed -i 's/machineType = .n1-highmem-16./machineType = "n1-ultramem-40"   \/\/ 961 GB RAM, 40 vCPUs/' nextflow.config

# Update memory allocation
sed -i 's/memory = .104 GB./memory = "200 GB"   \/\/ Ultra-high memory allocation/' nextflow.config

# Update CPU allocation to match machine
sed -i 's/cpus = 8            \/\/ REDUCED/cpus = 16           \/\/ Increased for ultramem/' nextflow.config

echo "✅ Updated configuration for ultra-high memory machines"
echo ""
echo "Changes made:"
echo "- Machine type: n1-ultramem-40 (961 GB RAM, 40 vCPUs)"
echo "- Memory allocation: 200 GB"
echo "- CPU allocation: 16 cores"
echo ""
echo "⚠️  WARNING: Ultra-high memory machines are expensive!"
echo "   Estimated cost: ~$30-50/hour per machine"
echo ""
echo "To revert changes: cp nextflow.config.backup nextflow.config"
echo ""
echo "Now run: ./run_google_batch.sh"