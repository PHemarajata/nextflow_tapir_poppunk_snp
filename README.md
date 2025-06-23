# TAPIR + PopPUNK + Per-Clade SNP Analysis Pipeline

A Nextflow DSL2 pipeline optimized for phylogenetic analysis of bacterial genomes using PopPUNK clustering followed by per-cluster SNP analysis. Supports both local execution and Google Cloud Batch for scalable processing.

## Overview

This pipeline performs:
1. **PopPUNK clustering** - Groups assembled genomes into clusters based on genetic similarity
2. **Per-cluster analysis**:
   - **Panaroo** - Pan-genome analysis to identify core genes
   - **Gubbins** - Removes recombination and identifies SNPs
   - **IQ-TREE** - Builds phylogenetic trees from filtered SNPs

**Note**: This pipeline uses StaPH-B (State Public Health Bioinformatics) Docker containers, which are standardized, well-maintained containers specifically designed for public health bioinformatics workflows.

## System Requirements

### Local Execution
- **Hardware**: Multi-core system with 64GB+ RAM (optimized for large datasets)
- **Software**: 
  - Nextflow (v23+)
  - Docker (running locally)
  - StaPH-B Docker images (pulled automatically):
    - `staphb/poppunk:2.7.5` - PopPUNK clustering
    - `staphb/panaroo:1.5.2` - Pan-genome analysis
    - `staphb/gubbins:3.3.5` - Recombination removal
    - `staphb/iqtree2:2.4.0` - Phylogenetic tree building

### Google Cloud Execution
- **Google Cloud Project**: `erudite-pod-307018`
- **Required APIs**: Batch API, Compute Engine API, Cloud Storage API
- **Permissions**: Batch Job Editor, Compute Instance Admin, Storage Object Admin

## Usage

### Basic Usage

**For local Ubuntu users (recommended):**
```bash
# First, run the setup script to configure Docker properly
./setup_ubuntu_docker.sh

# Then run the pipeline with Ubuntu Docker profile
./nextflow run nextflow_tapir_poppunk_snp.nf -profile ubuntu_docker --input ./assemblies --resultsDir ./results
```

**For Google Cloud Batch execution:**
```bash
# Setup and run on Google Cloud (see GOOGLE_CLOUD_SETUP.md for details)
./run_google_batch.sh

# Or manually:
./nextflow run nextflow_tapir_poppunk_snp.nf -profile google_batch \
    --input gs://aphlhq-ngs-gh/nextflow_data/subset_100 \
    --resultsDir gs://aphlhq-ngs-gh/nextflow_data/subset_100_results \
    -w gs://aphlhq-ngs-gh/nextflow_work
```

**Standard local usage:**
```bash
./nextflow run nextflow_tapir_poppunk_snp.nf --input ./assemblies --resultsDir ./results
```

### Input Requirements
- Directory containing FASTA assembly files
- Supported extensions: `.fasta`, `.fa`, `.fas`
- Minimum 3 genomes per cluster for meaningful analysis

### Parameters
- `--input`: Path to directory containing FASTA assemblies (required)
  - Local: `./assemblies` 
  - Google Cloud: `gs://bucket-name/path/to/assemblies`
- `--resultsDir`: Path to output directory (required)
  - Local: `./results`
  - Google Cloud: `gs://bucket-name/path/to/results`
- `--poppunk_threads`: Threads for PopPUNK (default: 8 local, 16 cloud)
- `--panaroo_threads`: Threads for Panaroo (default: 16 local, 8 cloud) 
- `--gubbins_threads`: Threads for Gubbins (default: 8 local, 4 cloud)
- `--iqtree_threads`: Threads for IQ-TREE (default: 4 local, 4 cloud)
- `--large_dataset_threshold`: Threshold for conservative PopPUNK parameters (default: 400)
- `--very_large_dataset_threshold`: Threshold for ultra-conservative PopPUNK parameters (default: 450)

### Example
```bash
# Run with custom thread allocation
./nextflow run nextflow_tapir_poppunk_snp.nf \
    --input ./my_assemblies \
    --resultsDir ./my_results \
    --poppunk_threads 12 \
    --panaroo_threads 8
```

## Execution Profiles

The pipeline supports multiple execution environments:

### Local Execution
- **`ubuntu_docker`**: Optimized for Ubuntu systems with Docker
- **`local_tmp`**: Alternative local configuration with explicit temp directories
- **`standard`**: Default local configuration

### Cloud Execution  
- **`google_batch`**: Google Cloud Batch execution with automatic scaling
  - Project: `erudite-pod-307018`
  - Input: `gs://aphlhq-ngs-gh/nextflow_data/subset_100`
  - Results: `gs://aphlhq-ngs-gh/nextflow_data/subset_100_results`
  - Work: `gs://aphlhq-ngs-gh/nextflow_work`

### Profile Usage
```bash
# Local Ubuntu
./nextflow run nextflow_tapir_poppunk_snp.nf -profile ubuntu_docker --input ./assemblies --resultsDir ./results

# Google Cloud
./nextflow run nextflow_tapir_poppunk_snp.nf -profile google_batch --input gs://bucket/input --resultsDir gs://bucket/results
```

## Output Structure
```
results/
├── poppunk/                    # PopPUNK clustering results
│   └── clusters.csv           # Cluster assignments
├── cluster_1/                 # Results for cluster 1
│   ├── panaroo/              # Pan-genome analysis
│   ├── gubbins/              # Recombination removal
│   └── iqtree/               # Phylogenetic tree
├── cluster_2/                 # Results for cluster 2
│   └── ...
├── pipeline_report.html       # Execution report
├── timeline.html             # Timeline report
└── trace.txt                 # Process trace
```

## Resource Optimization

### Local Execution
The pipeline is optimized for large datasets with ultra-conservative settings:
- **PopPUNK**: 8 threads, 60GB RAM (ultra-conservative for stability)
- **Panaroo**: 16 threads, 24GB RAM per cluster
- **Gubbins**: 8 threads, 12GB RAM per cluster  
- **IQ-TREE**: 4 threads, 6GB RAM per cluster
- **Queue limit**: 10 concurrent processes to manage memory

### Google Cloud Execution
- **PopPUNK**: 16 vCPUs, 64GB RAM (n1-highmem-16)
- **Panaroo**: 8 vCPUs, 32GB RAM (n1-standard-8)
- **Gubbins**: 4 vCPUs, 16GB RAM (n1-standard-4)
- **IQ-TREE**: 4 vCPUs, 8GB RAM (n1-standard-4)
- **Spot instances**: Enabled for cost savings

## Troubleshooting

### Common Issues
1. **No FASTA files found**: Check file extensions and input path
2. **Docker permission errors**: Ensure Docker is running and user has permissions
3. **Memory issues**: Reduce thread counts or process fewer files at once
4. **Small clusters skipped**: Clusters with <3 genomes are automatically filtered out

### PopPUNK Issues

#### Input Format Error
If you encounter "Input reference list is misformatted" error:
- This has been fixed in the latest version
- PopPUNK requires tab-separated input (sample_name<TAB>file_path)
- The pipeline now automatically creates properly formatted input files

#### Segmentation Fault with Large Datasets
If PopPUNK crashes with "Segmentation fault" when processing many files (>400):
- **Ultra-conservative fix implemented**: Optimized for datasets up to 500+ files
- **Automatic detection**: Pipeline detects very large datasets (>450) and uses ultra-conservative parameters
- **Resource optimization**: 8 threads, 60GB memory, 48-hour time limit

**For persistent segmentation faults:**
```bash
# Use ultra-conservative settings (automatically applied for >450 files)
./nextflow run nextflow_tapir_poppunk_snp.nf --input ./assemblies --resultsDir ./results

# Alternative: Use chunked approach for extremely large datasets
./nextflow run poppunk_chunked_alternative.nf --input ./assemblies --resultsDir ./results --chunk_size 200
```

**Monitor progress:**
```bash
# Track PopPUNK progress in real-time
./monitor_poppunk.sh
```

### Ubuntu Docker Issues
If you encounter Docker mount errors like "Mounts denied", "path not shared", or "Duplicate mount point":

1. **Run the setup script first:**
   ```bash
   ./setup_ubuntu_docker.sh
   ```

2. **Choose the appropriate profile:**
   ```bash
   # Recommended for Ubuntu
   ./nextflow run nextflow_tapir_poppunk_snp.nf -profile ubuntu_docker --input ./assemblies --resultsDir ./results
   
   # Alternative if still having mount issues
   ./nextflow run nextflow_tapir_poppunk_snp.nf -profile local_tmp --input ./assemblies --resultsDir ./results
   ```

3. **Check Docker permissions:**
   ```bash
   # Add user to docker group (requires logout/login)
   sudo usermod -aG docker $USER
   newgrp docker
   
   # Test Docker access
   docker ps
   ```

### Google Cloud Issues
For Google Cloud execution problems, see `GOOGLE_CLOUD_SETUP.md` for detailed troubleshooting.

### Help
```bash
./nextflow run nextflow_tapir_poppunk_snp.nf --help
```

## Performance Notes

### Local Execution
- Optimized for large datasets (400+ FASTA files) with ultra-conservative PopPUNK settings
- PopPUNK clustering is the most memory-intensive step
- Per-cluster analyses run in parallel after clustering
- Total runtime depends on number of clusters and cluster sizes

### Google Cloud Execution
- Automatic scaling based on workload
- Parallel processing of multiple clusters
- Cost-optimized with spot instances
- Estimated cost: $20-50 for 100 genomes, $80-150 for 400+ genomes

## Files Included

### Core Pipeline Files
- `nextflow_tapir_poppunk_snp.nf` - Main Nextflow pipeline
- `nextflow.config` - Configuration with multiple execution profiles
- `run_pipeline.sh` - Local execution script
- `run_google_batch.sh` - Google Cloud execution script
- `setup_ubuntu_docker.sh` - Docker environment setup
- `monitor_poppunk.sh` - PopPUNK progress monitoring

### Documentation
- `README.md` - This comprehensive guide
- `GOOGLE_CLOUD_SETUP.md` - Google Cloud setup instructions
- Additional troubleshooting guides for specific issues

The pipeline is ready for production use on both local systems and Google Cloud Platform!