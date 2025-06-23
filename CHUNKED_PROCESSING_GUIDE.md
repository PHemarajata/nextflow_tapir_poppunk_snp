# Chunked Processing Guide - PopPUNK Segfault Prevention

This guide explains how to use the chunked processing approach to prevent PopPUNK segmentation faults when processing large datasets.

## Overview

The chunked processing approach splits large datasets into smaller chunks, processes each chunk separately with PopPUNK, then merges the results. This prevents memory-related segmentation faults that occur with very large datasets.

## When to Use Chunked Processing

Use chunked processing when:
- You have >200 genome assemblies
- You're experiencing PopPUNK segmentation faults
- You want to process large datasets cost-effectively on cloud platforms
- You have limited memory resources

## Available Scripts

### 1. Google Cloud Chunked Processing
```bash
./run_google_batch_chunked.sh
```
- Processes files in chunks of 150 (configurable)
- Uses smaller, cost-effective Google Cloud machines
- Prevents segmentation faults on large datasets

### 2. Local Chunked Processing
```bash
./run_local_chunked.sh [input_dir] [output_dir] [chunk_size]
```
- Example: `./run_local_chunked.sh ./assemblies ./results_chunked 100`
- Processes files locally in smaller batches
- Reduces memory requirements

### 3. Direct Pipeline Execution
```bash
./nextflow run nextflow_tapir_poppunk_snp_chunked.nf --input ./assemblies --resultsDir ./results --chunk_size 150
```

## Configuration

### Chunk Size Guidelines
- **Small datasets (50-100 files)**: chunk_size = 50
- **Medium datasets (100-200 files)**: chunk_size = 100  
- **Large datasets (200-400 files)**: chunk_size = 150
- **Very large datasets (400+ files)**: chunk_size = 100-120

### Memory Requirements per Chunk
- **Local execution**: 32 GB RAM per chunk
- **Google Cloud**: 52 GB RAM per chunk (n1-highmem-8)

## How It Works

1. **File Splitting**: Input files are divided into chunks of specified size
2. **Parallel PopPUNK**: Each chunk is processed independently with PopPUNK
3. **Cluster Merging**: Results from all chunks are merged into final clusters
4. **Downstream Analysis**: Standard Panaroo → Gubbins → IQ-TREE per cluster

## Advantages

### Cost Benefits
- Uses smaller, cheaper cloud instances
- Parallel processing reduces total runtime
- Spot instances can be used safely

### Reliability Benefits
- Eliminates PopPUNK segmentation faults
- Individual chunk failures don't affect entire run
- Better memory management

### Scalability Benefits
- Can process datasets of any size
- Linear scaling with dataset size
- Automatic parallelization

## Output Structure

```
results_chunked/
├── poppunk_chunks/           # Individual chunk results
│   ├── chunk_1_clusters.csv
│   ├── chunk_2_clusters.csv
│   └── ...
├── poppunk/                  # Merged results
│   └── merged_clusters.csv   # Final cluster assignments
├── cluster_1/                # Per-cluster analysis
│   ├── panaroo/
│   ├── gubbins/
│   └── iqtree/
├── cluster_2/
│   └── ...
└── reports/                  # Pipeline reports
    ├── pipeline_report.html
    ├── timeline.html
    ├── trace.txt
    └── dag.html
```

## Troubleshooting

### Common Issues

1. **Chunk too large causing segfaults**
   - Reduce chunk_size parameter
   - Try: `--chunk_size 100` or `--chunk_size 75`

2. **Memory issues during merging**
   - Increase memory for MERGE_CLUSTERS process
   - Process fewer chunks simultaneously

3. **File path issues**
   - Ensure all file paths are accessible
   - Check file permissions

### Monitoring Progress

```bash
# Watch pipeline progress
watch -n 30 'ls -la work/*/POPPUNK_CHUNK*'

# Check memory usage
watch -n 10 'free -h'

# Monitor Google Cloud jobs
gcloud batch jobs list --location=us-central1
```

### Performance Tuning

#### For Speed
- Increase chunk_size (but watch for segfaults)
- Use more parallel processes
- Use faster storage (SSD)

#### For Memory Efficiency
- Decrease chunk_size
- Reduce thread counts
- Use conservative PopPUNK parameters

#### For Cost Efficiency (Google Cloud)
- Use spot instances (already enabled)
- Optimize chunk_size for machine types
- Use regional persistent disks

## Example Workflows

### Large Dataset (500 files)
```bash
# Google Cloud - cost-effective
./run_google_batch_chunked.sh

# Local - if you have sufficient RAM
./run_local_chunked.sh ./assemblies ./results 120
```

### Very Large Dataset (1000+ files)
```bash
# Use smaller chunks
./nextflow run nextflow_tapir_poppunk_snp_chunked.nf \
    --input gs://bucket/assemblies \
    --resultsDir gs://bucket/results \
    --chunk_size 100 \
    -profile google_batch
```

### Testing/Development
```bash
# Small chunks for testing
./run_local_chunked.sh ./test_assemblies ./test_results 25
```

## Comparison: Standard vs Chunked

| Aspect | Standard Pipeline | Chunked Pipeline |
|--------|------------------|------------------|
| Memory Usage | High (60-100GB) | Moderate (32-52GB per chunk) |
| Segfault Risk | High for >200 files | Very Low |
| Processing Time | Faster for small datasets | Faster for large datasets |
| Cost (Cloud) | High memory instances | Standard instances |
| Complexity | Simple | Moderate |
| Scalability | Limited by memory | Unlimited |

## Best Practices

1. **Start with chunked approach** for datasets >200 files
2. **Test with small chunks** first to validate your data
3. **Monitor memory usage** during execution
4. **Use appropriate machine types** for your chunk size
5. **Keep chunk results** for debugging if needed
6. **Validate merged results** against expectations

## Support

If you encounter issues with chunked processing:

1. Run diagnostics: `./diagnose_segfault_gcp.sh`
2. Check pipeline logs: `cat .nextflow.log`
3. Verify chunk sizes are appropriate for your system
4. Consider reducing chunk_size further

The chunked approach should resolve most PopPUNK segmentation fault issues while providing better scalability and cost-effectiveness.