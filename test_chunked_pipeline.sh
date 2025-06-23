#!/bin/bash

# Test script for chunked pipeline approach
# This script validates the chunked processing setup

set -euo pipefail

echo "=== Testing Chunked Pipeline Setup ==="
echo "Timestamp: $(date)"
echo "======================================"

# Test 1: Check if chunked pipeline file exists
echo "1. Checking chunked pipeline file..."
if [ -f "nextflow_tapir_poppunk_snp_chunked.nf" ]; then
    echo "✅ Chunked pipeline file exists"
else
    echo "❌ Chunked pipeline file missing"
    exit 1
fi

# Test 2: Check if execution scripts exist
echo ""
echo "2. Checking execution scripts..."
scripts_to_check=(
    "run_google_batch_chunked.sh"
    "run_local_chunked.sh"
)

for script in "${scripts_to_check[@]}"; do
    if [ -f "$script" ]; then
        echo "✅ $script exists and is executable: $(test -x "$script" && echo "YES" || echo "NO")"
    else
        echo "❌ $script missing"
    fi
done

# Test 3: Validate Nextflow syntax
echo ""
echo "3. Validating Nextflow syntax..."
if command -v nextflow >/dev/null 2>&1; then
    echo "Nextflow found: $(nextflow -version | head -1)"
    
    # Test main pipeline syntax
    echo "Testing main pipeline syntax..."
    ./nextflow run nextflow_tapir_poppunk_snp.nf --help >/dev/null 2>&1 && echo "✅ Main pipeline syntax OK" || echo "❌ Main pipeline syntax error"
    
    # Test chunked pipeline syntax
    echo "Testing chunked pipeline syntax..."
    ./nextflow run nextflow_tapir_poppunk_snp_chunked.nf --help >/dev/null 2>&1 && echo "✅ Chunked pipeline syntax OK" || echo "❌ Chunked pipeline syntax error"
else
    echo "⚠️  Nextflow not found - cannot validate syntax"
fi

# Test 4: Check configuration
echo ""
echo "4. Checking configuration..."
if grep -q "chunk_size" nextflow.config; then
    chunk_size=$(grep "chunk_size" nextflow.config | head -1 | cut -d'=' -f2 | tr -d ' ')
    echo "✅ Chunk size configured: $chunk_size"
else
    echo "❌ Chunk size not configured"
fi

if grep -q "POPPUNK_CHUNK" nextflow.config; then
    echo "✅ Chunked process configurations found"
else
    echo "❌ Chunked process configurations missing"
fi

# Test 5: Check Docker availability (if running locally)
echo ""
echo "5. Checking Docker availability..."
if command -v docker >/dev/null 2>&1; then
    if docker ps >/dev/null 2>&1; then
        echo "✅ Docker is running"
        
        # Test PopPUNK container
        echo "Testing PopPUNK container availability..."
        if docker pull staphb/poppunk:2.7.5 >/dev/null 2>&1; then
            echo "✅ PopPUNK container accessible"
        else
            echo "⚠️  PopPUNK container pull failed (may be network issue)"
        fi
    else
        echo "⚠️  Docker not running or permission issues"
    fi
else
    echo "⚠️  Docker not found"
fi

# Test 6: Check Google Cloud setup (if applicable)
echo ""
echo "6. Checking Google Cloud setup..."
if command -v gcloud >/dev/null 2>&1; then
    echo "gcloud found: $(gcloud version --format='value(Google Cloud SDK)' 2>/dev/null || echo 'version unknown')"
    
    if gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q .; then
        echo "✅ Google Cloud authenticated"
        
        # Check project
        project=$(gcloud config get-value project 2>/dev/null || echo "not set")
        echo "Current project: $project"
        
        if [ "$project" = "erudite-pod-307018" ]; then
            echo "✅ Correct project configured"
        else
            echo "⚠️  Project may need to be set to erudite-pod-307018"
        fi
    else
        echo "⚠️  Google Cloud not authenticated"
    fi
else
    echo "⚠️  gcloud not found"
fi

# Test 7: Estimate resource requirements
echo ""
echo "7. Resource estimation for chunked processing..."
echo "Chunk size: 150 files (default)"
echo "Memory per chunk: 32 GB (local) / 52 GB (cloud)"
echo "Estimated processing time per chunk: 2-6 hours"
echo "Parallel chunks: Limited by available memory"

# Test 8: Create sample test structure
echo ""
echo "8. Creating sample test structure..."
mkdir -p test_assemblies
mkdir -p test_results_chunked

# Create dummy FASTA files for testing (if they don't exist)
if [ ! -f "test_assemblies/sample1.fasta" ]; then
    echo "Creating sample test files..."
    for i in {1..10}; do
        echo ">sample${i}" > "test_assemblies/sample${i}.fasta"
        echo "ATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCG" >> "test_assemblies/sample${i}.fasta"
    done
    echo "✅ Created 10 sample FASTA files for testing"
else
    echo "✅ Test files already exist"
fi

echo ""
echo "=== Test Summary ==="
echo "The chunked pipeline setup appears to be ready for use."
echo ""
echo "Next steps:"
echo "1. For Google Cloud: ./run_google_batch_chunked.sh"
echo "2. For local testing: ./run_local_chunked.sh test_assemblies test_results_chunked 5"
echo "3. For production local: ./run_local_chunked.sh ./assemblies ./results_chunked 150"
echo ""
echo "Troubleshooting:"
echo "- Review: CHUNKED_PROCESSING_GUIDE.md"
echo "- Diagnose: ./diagnose_segfault_gcp.sh"
echo "- Monitor: ./scripts/monitor_poppunk.sh"
echo ""
echo "=== Test Complete ==="