#!/bin/bash

# Google Cloud PopPUNK Segfault Diagnostic Script
# This script helps diagnose and troubleshoot PopPUNK segmentation faults on GCP

set -euo pipefail

echo "=== PopPUNK Segfault Diagnostic for Google Cloud ==="
echo "Timestamp: $(date)"
echo "=================================================="

# Check system resources
echo "1. SYSTEM RESOURCES:"
echo "   CPU Info:"
nproc || echo "   Cannot get CPU count"
cat /proc/cpuinfo | grep "model name" | head -1 || echo "   Cannot get CPU model"

echo "   Memory Info:"
free -h || echo "   Cannot get memory info"
cat /proc/meminfo | grep -E "(MemTotal|MemAvailable|SwapTotal)" || echo "   Cannot get detailed memory info"

echo "   Disk Space:"
df -h || echo "   Cannot get disk info"

echo ""

# Check system limits
echo "2. SYSTEM LIMITS:"
echo "   Current ulimits:"
ulimit -a || echo "   Cannot get ulimits"

echo "   Memory overcommit settings:"
cat /proc/sys/vm/overcommit_memory 2>/dev/null || echo "   Cannot read overcommit_memory"
cat /proc/sys/vm/overcommit_ratio 2>/dev/null || echo "   Cannot read overcommit_ratio"

echo ""

# Check for previous segfaults
echo "3. SEGFAULT HISTORY:"
echo "   Checking dmesg for segfaults:"
dmesg | grep -i "segfault\|killed\|oom" | tail -10 || echo "   No recent segfaults found in dmesg"

echo "   Checking system logs:"
journalctl --no-pager -n 20 | grep -i "segfault\|killed\|oom" || echo "   No recent segfaults found in journal"

echo ""

# Check PopPUNK container
echo "4. POPPUNK CONTAINER CHECK:"
echo "   Testing PopPUNK container availability:"
docker pull staphb/poppunk:2.7.5 || echo "   Cannot pull PopPUNK container"

echo "   Testing PopPUNK basic functionality:"
docker run --rm staphb/poppunk:2.7.5 poppunk --version || echo "   Cannot run PopPUNK version check"

echo ""

# Check input data
echo "5. INPUT DATA ANALYSIS:"
INPUT_DIR="${1:-gs://aphlhq-ngs-gh/nextflow_data/subset_100}"
echo "   Input directory: $INPUT_DIR"

if [[ $INPUT_DIR == gs://* ]]; then
    echo "   Counting files in GCS bucket:"
    gsutil ls "$INPUT_DIR/*.{fasta,fa,fas}" 2>/dev/null | wc -l || echo "   Cannot count files in GCS bucket"
    
    echo "   Sample file sizes:"
    gsutil ls -l "$INPUT_DIR/*.{fasta,fa,fas}" 2>/dev/null | head -5 || echo "   Cannot get file sizes"
else
    echo "   Counting local files:"
    find "$INPUT_DIR" -name "*.fasta" -o -name "*.fa" -o -name "*.fas" | wc -l || echo "   Cannot count local files"
    
    echo "   Sample file sizes:"
    find "$INPUT_DIR" -name "*.fasta" -o -name "*.fa" -o -name "*.fas" -exec ls -lh {} \; | head -5 || echo "   Cannot get local file sizes"
fi

echo ""

# Recommendations
echo "6. RECOMMENDATIONS:"
FILE_COUNT=$(gsutil ls "$INPUT_DIR/*.{fasta,fa,fas}" 2>/dev/null | wc -l || echo "0")
echo "   Estimated file count: $FILE_COUNT"

if [ "$FILE_COUNT" -gt 300 ]; then
    echo "   ⚠️  LARGE DATASET DETECTED ($FILE_COUNT files)"
    echo "   Recommendations:"
    echo "   - Use chunked processing: ./run_google_batch_chunked.sh"
    echo "   - Consider n1-ultramem-40 machine type (961 GB RAM)"
    echo "   - Process in batches of 100-150 files maximum"
    echo "   - Use ultra-conservative PopPUNK parameters"
elif [ "$FILE_COUNT" -gt 200 ]; then
    echo "   ⚠️  MEDIUM-LARGE DATASET ($FILE_COUNT files)"
    echo "   Recommendations:"
    echo "   - Use conservative parameters (already enabled)"
    echo "   - Monitor memory usage closely"
    echo "   - Consider chunked processing if segfaults persist"
else
    echo "   ✅ MANAGEABLE DATASET SIZE ($FILE_COUNT files)"
    echo "   Standard processing should work with current settings"
fi

echo ""
echo "7. NEXT STEPS:"
echo "   If segfaults persist:"
echo "   1. Try: ./run_google_batch_chunked.sh (processes in smaller batches)"
echo "   2. Edit nextflow.config to use n1-ultramem-40 machine type"
echo "   3. Reduce dataset size for testing"
echo "   4. Check Nextflow logs: gsutil cat gs://aphlhq-ngs-gh/nextflow_work/.nextflow.log"

echo ""
echo "=== Diagnostic Complete ==="