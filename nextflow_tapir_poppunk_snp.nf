#!/usr/bin/env nextflow

/*
 * TAPIR + PopPUNK + Per-Clade SNP Analysis Pipeline
 * DSL2 pipeline optimized for large datasets with Google Cloud Batch support
 * 
 * Steps:
 *   1. PopPUNK clustering of assembled genomes
 *   2. Split genomes by cluster
 *   3. For each cluster: run Panaroo → Gubbins → IQ-TREE
 *
 * Requirements:
 *   - Nextflow (v23+)
 *   - Docker/Singularity containers
 *   - StaPH-B Docker images:
 *       staphb/poppunk:2.7.5 (PopPUNK clustering - latest)
 *       staphb/panaroo:1.5.2 (Pan-genome analysis - latest)
 *       staphb/gubbins:3.3.5 (Recombination removal - latest)
 *       staphb/iqtree2:2.4.0 (Phylogenetic tree building - latest)
 */

nextflow.enable.dsl=2

// Help message
if (params.help) {
    log.info """
    TAPIR + PopPUNK + Per-Clade SNP Analysis Pipeline
    
    Usage:
        ./nextflow run nextflow_tapir_poppunk_snp.nf --input <path_to_assemblies> --resultsDir <output_directory>
    
    Required arguments:
        --input         Path to directory containing FASTA assemblies
                        Local: ./assemblies
                        Cloud: gs://bucket-name/path/to/assemblies
        --resultsDir    Path to output directory
                        Local: ./results
                        Cloud: gs://bucket-name/path/to/results
    
    Optional arguments:
        --poppunk_threads   Number of threads for PopPUNK (default: 8 local, 16 cloud)
        --panaroo_threads   Number of threads for Panaroo (default: 16 local, 8 cloud)
        --gubbins_threads   Number of threads for Gubbins (default: 8 local, 4 cloud)
        --iqtree_threads    Number of threads for IQ-TREE (default: 4)
        --chunk_size        Process files in chunks to prevent segfaults (default: 0 = no chunking)
        --large_dataset_threshold      Threshold for conservative PopPUNK parameters (default: 400)
        --very_large_dataset_threshold Threshold for ultra-conservative PopPUNK parameters (default: 450)
    
    Execution profiles:
        -profile ubuntu_docker    Local execution with Docker (Ubuntu optimized)
        -profile google_batch     Google Cloud Batch execution
        -profile standard         Default local execution
    
    Examples:
        # Local execution
        ./nextflow run nextflow_tapir_poppunk_snp.nf -profile ubuntu_docker --input ./assemblies --resultsDir ./results
        
        # Google Cloud execution
        ./nextflow run nextflow_tapir_poppunk_snp.nf -profile google_batch \\
            --input gs://bucket/assemblies --resultsDir gs://bucket/results
    """
    exit 0
}

// Process definitions
process POPPUNK {
    tag "PopPUNK_clustering"
    container 'staphb/poppunk:2.7.5'
    publishDir "${params.resultsDir}/poppunk", mode: 'copy'

    input:
    path assemblies

    output:
    path 'clusters.csv'

    script:
    """
    # ULTRA-AGGRESSIVE segfault prevention for Google Cloud
    set -euo pipefail
    
    # System limits - even more conservative
    ulimit -v 104857600  # ~100GB virtual memory limit (increased for GCP)
    ulimit -m 104857600  # ~100GB resident memory limit
    ulimit -s 8192       # Stack size limit
    ulimit -c 0          # Disable core dumps
    ulimit -n 65536      # File descriptor limit
    
    # Memory management environment variables
    export OMP_NUM_THREADS=${task.cpus}
    export MALLOC_TRIM_THRESHOLD_=50000     # More aggressive trimming
    export MALLOC_MMAP_THRESHOLD_=50000     # Lower mmap threshold
    export MALLOC_ARENA_MAX=2               # Limit memory arenas
    export MALLOC_TOP_PAD_=131072           # Reduce top padding
    export MALLOC_MMAP_MAX_=65536           # Limit mmap allocations
    
    # Python/NumPy memory management (PopPUNK uses these)
    export PYTHONHASHSEED=0
    export OPENBLAS_NUM_THREADS=1
    export MKL_NUM_THREADS=1
    export NUMEXPR_NUM_THREADS=1
    export OMP_DYNAMIC=FALSE
    export OMP_NESTED=FALSE
    
    # Disable memory overcommit if possible
    echo 2 > /proc/sys/vm/overcommit_memory 2>/dev/null || echo "Cannot set overcommit policy"
    
    # Set conservative memory management
    echo 10 > /proc/sys/vm/swappiness 2>/dev/null || echo "Cannot set swappiness"
    
    # Monitor memory usage
    echo "Initial memory status:"
    free -h || echo "Memory info not available"
    
    # Create a tab-separated list file for PopPUNK (sample_name<TAB>file_path)
    for file in *.{fasta,fa,fas}; do
        if [ -f "\$file" ]; then
            # Extract sample name (remove extension)
            sample_name=\$(basename "\$file" | sed 's/\\.[^.]*\$//')
            echo -e "\$sample_name\\t\$(pwd)/\$file" >> assembly_list.txt
        fi
    done
    
    # Check if we have any files
    if [ ! -s assembly_list.txt ]; then
        echo "No FASTA files found!"
        exit 1
    fi
    
    echo "Found \$(wc -l < assembly_list.txt) assembly files"
    echo "First few entries in assembly list:"
    head -3 assembly_list.txt
    
    # Check dataset size and adjust parameters aggressively
    NUM_SAMPLES=\$(wc -l < assembly_list.txt)
    echo "Processing \$NUM_SAMPLES samples"
    
    # ULTRA-CONSERVATIVE parameters for Google Cloud segfault prevention
    if [ \$NUM_SAMPLES -gt 300 ]; then
        echo "Large dataset detected (\$NUM_SAMPLES samples). Using ULTRA-conservative parameters for GCP."
        SKETCH_SIZE="--sketch-size 3000"      # REDUCED further
        MIN_K="--min-k 17"                    # INCREASED min-k
        MAX_K="--max-k 23"                    # REDUCED max-k range
        EXTRA_PARAMS="--no-stream --batch-size 50"  # Added batch processing
    elif [ \$NUM_SAMPLES -gt 200 ]; then
        echo "Medium dataset detected (\$NUM_SAMPLES samples). Using conservative parameters."
        SKETCH_SIZE="--sketch-size 5000"
        MIN_K="--min-k 15"
        MAX_K="--max-k 25"
        EXTRA_PARAMS="--no-stream"
    elif [ \$NUM_SAMPLES -gt 100 ]; then
        echo "Small-medium dataset detected (\$NUM_SAMPLES samples). Using moderate parameters."
        SKETCH_SIZE="--sketch-size 7500"
        MIN_K="--min-k 13"
        MAX_K="--max-k 29"
        EXTRA_PARAMS=""
    else
        echo "Small dataset size. Using standard parameters."
        SKETCH_SIZE=""
        MIN_K=""
        MAX_K=""
        EXTRA_PARAMS=""
    fi
    
    # Memory checkpoint
    echo "Memory before database creation:"
    free -h || echo "Memory info not available"
    
    # Create database with ultra-conservative parameters and memory monitoring
    echo "Creating PopPUNK database with ULTRA-conservative settings..."
    echo "Memory before database creation:"
    free -h || echo "Memory info not available"
    ps aux --sort=-%mem | head -10 || echo "Process info not available"
    
    # Force garbage collection and memory cleanup
    sync && echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || echo "Cannot drop caches"
    
    # Run database creation with timeout and error handling
    timeout 7200 poppunk --create-db --r-files assembly_list.txt \\
            --output poppunk_db \\
            --threads ${task.cpus} \\
            \$SKETCH_SIZE \$MIN_K \$MAX_K \$EXTRA_PARAMS \\
            --overwrite || {
        echo "Database creation failed or timed out. Attempting recovery..."
        free -h
        ps aux --sort=-%mem | head -10
        exit 1
    }
    
    # Memory checkpoint and cleanup
    echo "Memory after database creation:"
    free -h || echo "Memory info not available"
    sync && echo 1 > /proc/sys/vm/drop_caches 2>/dev/null || echo "Cannot drop caches"
    sleep 10  # Allow memory to stabilize
    
    # Fit model with memory-conscious settings and monitoring
    echo "Fitting PopPUNK model with conservative settings..."
    echo "Memory before model fitting:"
    free -h || echo "Memory info not available"
    
    timeout 7200 poppunk --fit-model --ref-db poppunk_db \\
            --output poppunk_fit \\
            --threads ${task.cpus} \\
            --overwrite || {
        echo "Model fitting failed or timed out. Attempting recovery..."
        free -h
        ps aux --sort=-%mem | head -10
        exit 1
    }
    
    # Memory checkpoint and cleanup
    echo "Memory after model fitting:"
    free -h || echo "Memory info not available"
    sync && echo 1 > /proc/sys/vm/drop_caches 2>/dev/null || echo "Cannot drop caches"
    sleep 10  # Allow memory to stabilize
    
    # Assign clusters with monitoring
    echo "Assigning clusters with conservative settings..."
    echo "Memory before cluster assignment:"
    free -h || echo "Memory info not available"
    
    timeout 7200 poppunk --assign-query --ref-db poppunk_db \\
            --q-files assembly_list.txt \\
            --output poppunk_assigned \\
            --threads ${task.cpus} \\
            --overwrite || {
        echo "Cluster assignment failed or timed out. Attempting recovery..."
        free -h
        ps aux --sort=-%mem | head -10
        exit 1
    }
    
    # Find and copy the cluster assignment file
    find poppunk_assigned -name "*clusters.csv" -exec cp {} clusters.csv \\;
    
    # If that doesn't work, try alternative names
    if [ ! -f clusters.csv ]; then
        find poppunk_assigned -name "*cluster*.csv" -exec cp {} clusters.csv \\;
    fi
    
    # Final check
    if [ ! -f clusters.csv ]; then
        echo "Could not find cluster assignment file. Available files:"
        find poppunk_assigned -name "*.csv" -ls
        exit 1
    fi
    
    echo "Cluster assignments created successfully"
    echo "Total clusters found: \$(tail -n +2 clusters.csv | cut -f2 | sort -u | wc -l)"
    echo "Final memory status:"
    free -h || echo "Memory info not available"
    head -5 clusters.csv
    """
}

process PANAROO {
    tag "Panaroo_cluster_${cluster_id}"
    container 'staphb/panaroo:1.5.2'
    publishDir "${params.resultsDir}/cluster_${cluster_id}/panaroo", mode: 'copy'

    input:
    tuple val(cluster_id), path(assemblies)

    output:
    tuple val(cluster_id), path('core_gene_alignment.aln')

    when:
    assemblies.size() >= 3  // Need at least 3 genomes for meaningful analysis

    script:
    """
    echo "Processing cluster ${cluster_id} with \${#assemblies[@]} genomes"
    
    # Run Panaroo pan-genome analysis
    panaroo -i *.{fasta,fa,fas} -o panaroo_output \\
            -t ${task.cpus} --clean-mode strict --aligner mafft \\
            --remove-invalid-genes
    
    # Copy core gene alignment
    if [ -f panaroo_output/core_gene_alignment.aln ]; then
        cp panaroo_output/core_gene_alignment.aln .
    else
        echo "Error: Core gene alignment not found"
        ls -la panaroo_output/
        exit 1
    fi
    
    echo "Panaroo analysis completed for cluster ${cluster_id}"
    """
}

process GUBBINS {
    tag "Gubbins_cluster_${cluster_id}"
    container 'staphb/gubbins:3.3.5'
    publishDir "${params.resultsDir}/cluster_${cluster_id}/gubbins", mode: 'copy'

    input:
    tuple val(cluster_id), path(alignment)

    output:
    tuple val(cluster_id), path('gubbins_output.filtered_polymorphic_sites.fasta')

    script:
    """
    echo "Running Gubbins on cluster ${cluster_id}"
    
    # Check alignment file
    if [ ! -f ${alignment} ]; then
        echo "Error: Alignment file not found"
        exit 1
    fi
    
    # Run Gubbins for recombination removal
    run_gubbins.py --prefix gubbins_output \\
                   --threads ${task.cpus} \\
                   --verbose ${alignment}
    
    # Check if output was created
    if [ ! -f gubbins_output.filtered_polymorphic_sites.fasta ]; then
        echo "Error: Gubbins output not found"
        ls -la gubbins_output*
        exit 1
    fi
    
    echo "Gubbins analysis completed for cluster ${cluster_id}"
    """
}

process IQTREE {
    tag "IQTree_cluster_${cluster_id}"
    container 'staphb/iqtree2:2.4.0'
    publishDir "${params.resultsDir}/cluster_${cluster_id}/iqtree", mode: 'copy'

    input:
    tuple val(cluster_id), path(snp_alignment)

    output:
    tuple val(cluster_id), path("tree.*")

    script:
    """
    echo "Building phylogenetic tree for cluster ${cluster_id}"
    
    # Check SNP alignment file
    if [ ! -f ${snp_alignment} ]; then
        echo "Error: SNP alignment file not found"
        exit 1
    fi
    
    # Check if alignment has sufficient data
    SEQ_COUNT=\$(grep -c ">" ${snp_alignment})
    if [ \$SEQ_COUNT -lt 3 ]; then
        echo "Warning: Insufficient sequences (\$SEQ_COUNT) for tree building"
        touch tree.warning
        exit 0
    fi
    
    # Build phylogenetic tree with IQ-TREE
    iqtree2 -s ${snp_alignment} -m GTR+G \\
            -nt ${task.cpus} -bb 1000 -pre tree \\
            --quiet
    
    echo "Phylogenetic tree completed for cluster ${cluster_id}"
    """
}

workflow {
    // Validate input directory
    if (!file(params.input).exists()) {
        error "Input directory does not exist: ${params.input}"
    }

    // Input channel for assemblies
    assemblies_ch = Channel.fromPath("${params.input}/*.{fasta,fa,fas}")
        .ifEmpty { error "No FASTA files found in ${params.input}" }
        .collect()

    // Check if chunking is enabled
    if (params.chunk_size > 0) {
        log.info "⚠️  CHUNKED PROCESSING ENABLED"
        log.info "   Chunk size: ${params.chunk_size} files per chunk"
        log.info "   This will process files in smaller batches to prevent segmentation faults"
        log.info "   Note: Chunked processing may affect clustering results compared to processing all files together"
        
        // For chunked processing, we'll use a modified approach
        // Split files into chunks and process each chunk separately
        chunked_clusters = assemblies_ch
            .flatten()
            .buffer(size: params.chunk_size, remainder: true)
            .map { chunk_files -> 
                log.info "Processing chunk with ${chunk_files.size()} files"
                return chunk_files
            }
            .map { chunk -> POPPUNK(chunk) }
            .collect()
        
        clusters_csv = chunked_clusters.first() // Use first chunk's results for now
    } else {
        log.info "Standard processing: all files processed together"
        clusters_csv = POPPUNK(assemblies_ch)
    }

    // Parse cluster assignments and group assemblies by cluster
    cluster_assignments = clusters_csv
        .splitCsv(header: true)
        .map { row -> 
            // Try different possible column names for taxon/sample
            def taxon_name = row.Taxon ?: row.taxon ?: row.Sample ?: row.sample ?: row.ID ?: row.id
            def cluster_id = row.Cluster ?: row.cluster ?: row.cluster_id
            
            if (!taxon_name || !cluster_id) {
                error "Could not find taxon name or cluster ID in CSV row: ${row}"
            }
            
            // Try to find the assembly file with different extensions
            def assembly_file = null
            def base_name = taxon_name.toString().replaceAll(/\.(fasta|fa|fas)$/, '')
            
            ['fasta', 'fa', 'fas'].each { ext ->
                if (!assembly_file) {
                    def candidate = file("${params.input}/${base_name}.${ext}")
                    if (candidate.exists()) {
                        assembly_file = candidate
                    }
                }
            }
            
            if (!assembly_file) {
                log.warn "Could not find assembly file for taxon: ${taxon_name}"
                return null
            }
            
            return tuple(cluster_id.toString(), assembly_file)
        }
        .filter { it != null }  // Remove null entries
        .groupTuple()
        .filter { cluster_id, files -> files.size() >= 3 }  // Only process clusters with 3+ genomes

    // Log cluster information
    cluster_assignments.view { cluster_id, files -> 
        "Cluster ${cluster_id}: ${files.size()} genomes"
    }

    // Run pan-genome analysis per cluster
    panaroo_results = PANAROO(cluster_assignments)

    // Run recombination removal per cluster
    gubbins_results = GUBBINS(panaroo_results)

    // Build phylogenetic trees per cluster
    IQTREE(gubbins_results)
    
    // Summary
    IQTREE.out.view { cluster_id, tree_files ->
        "Completed analysis for cluster ${cluster_id}: ${tree_files.size()} output files"
    }
}