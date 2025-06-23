#!/usr/bin/env nextflow

/*
 * TAPIR + PopPUNK + Per-Clade SNP Analysis Pipeline - CHUNKED VERSION
 * DSL2 pipeline optimized for large datasets with chunked processing to prevent segfaults
 * 
 * This version processes files in smaller chunks to avoid PopPUNK segmentation faults
 * 
 * Steps:
 *   1. Split input files into chunks
 *   2. PopPUNK clustering per chunk
 *   3. Merge cluster results
 *   4. For each final cluster: run Panaroo → Gubbins → IQ-TREE
 */

nextflow.enable.dsl=2

// Help message
if (params.help) {
    log.info """
    TAPIR + PopPUNK + Per-Clade SNP Analysis Pipeline - CHUNKED VERSION
    
    Usage:
        ./nextflow run nextflow_tapir_poppunk_snp_chunked.nf --input <path_to_assemblies> --resultsDir <output_directory>
    
    Required arguments:
        --input         Path to directory containing FASTA assemblies
        --resultsDir    Path to output directory
    
    Optional arguments:
        --chunk_size        Number of files per chunk (default: 150)
        --poppunk_threads   Number of threads for PopPUNK (default: 8)
        --panaroo_threads   Number of threads for Panaroo (default: 16)
        --gubbins_threads   Number of threads for Gubbins (default: 8)
        --iqtree_threads    Number of threads for IQ-TREE (default: 4)
    
    Examples:
        # Local execution with chunking
        ./nextflow run nextflow_tapir_poppunk_snp_chunked.nf --input ./assemblies --resultsDir ./results --chunk_size 100
        
        # Google Cloud execution with chunking
        ./nextflow run nextflow_tapir_poppunk_snp_chunked.nf -profile google_batch \\
            --input gs://bucket/assemblies --resultsDir gs://bucket/results --chunk_size 150
    """
    exit 0
}

// Default parameters
params.chunk_size = 150  // Process max 150 files per chunk to prevent segfaults

// Process to split files into chunks
process SPLIT_FILES {
    tag "Splitting_files_into_chunks"
    
    input:
    path assemblies
    
    output:
    path "chunk_*", emit: chunks
    
    script:
    """
    # Create chunks directory
    mkdir -p chunks
    
    # Get all assembly files
    find . -name "*.fasta" -o -name "*.fa" -o -name "*.fas" > all_files.txt
    
    # Split files into chunks
    split -l ${params.chunk_size} all_files.txt chunk_list_
    
    # Create chunk directories and copy files
    chunk_num=1
    for chunk_list in chunk_list_*; do
        chunk_dir="chunk_\${chunk_num}"
        mkdir -p "\$chunk_dir"
        
        while IFS= read -r file; do
            if [ -f "\$file" ]; then
                cp "\$file" "\$chunk_dir/"
            fi
        done < "\$chunk_list"
        
        # Only keep chunks with files
        if [ \$(ls "\$chunk_dir" | wc -l) -gt 0 ]; then
            echo "Created \$chunk_dir with \$(ls \$chunk_dir | wc -l) files"
            chunk_num=\$((chunk_num + 1))
        else
            rm -rf "\$chunk_dir"
        fi
    done
    
    # Clean up
    rm chunk_list_* all_files.txt
    """
}

// Process PopPUNK on each chunk
process POPPUNK_CHUNK {
    tag "PopPUNK_chunk_${chunk_id}"
    container 'staphb/poppunk:2.7.5'
    publishDir "${params.resultsDir}/poppunk_chunks", mode: 'copy'

    input:
    tuple val(chunk_id), path(chunk_assemblies)

    output:
    tuple val(chunk_id), path("chunk_${chunk_id}_clusters.csv")

    script:
    """
    # ULTRA-AGGRESSIVE segfault prevention
    set -euo pipefail
    
    # System limits - conservative for chunked processing
    ulimit -v 52428800   # ~50GB virtual memory limit
    ulimit -m 52428800   # ~50GB resident memory limit
    ulimit -s 8192       # Stack size limit
    ulimit -c 0          # Disable core dumps
    ulimit -n 65536      # File descriptor limit
    
    # Memory management environment variables
    export OMP_NUM_THREADS=${task.cpus}
    export MALLOC_TRIM_THRESHOLD_=50000
    export MALLOC_MMAP_THRESHOLD_=50000
    export MALLOC_ARENA_MAX=2
    export PYTHONHASHSEED=0
    export OPENBLAS_NUM_THREADS=1
    export MKL_NUM_THREADS=1
    export NUMEXPR_NUM_THREADS=1
    
    echo "Processing chunk ${chunk_id}"
    echo "Initial memory status:"
    free -h || echo "Memory info not available"
    
    # Create assembly list for PopPUNK
    for file in *.{fasta,fa,fas}; do
        if [ -f "\$file" ]; then
            sample_name=\$(basename "\$file" | sed 's/\\.[^.]*\$//')
            echo -e "\$sample_name\\t\$(pwd)/\$file" >> assembly_list.txt
        fi
    done
    
    if [ ! -s assembly_list.txt ]; then
        echo "No FASTA files found in chunk!"
        exit 1
    fi
    
    NUM_SAMPLES=\$(wc -l < assembly_list.txt)
    echo "Processing \$NUM_SAMPLES samples in chunk ${chunk_id}"
    
    # Use conservative parameters for all chunks
    SKETCH_SIZE="--sketch-size 7500"
    MIN_K="--min-k 15"
    MAX_K="--max-k 25"
    EXTRA_PARAMS="--no-stream"
    
    # Create database
    echo "Creating PopPUNK database for chunk ${chunk_id}..."
    timeout 3600 poppunk --create-db --r-files assembly_list.txt \\
            --output poppunk_db_${chunk_id} \\
            --threads ${task.cpus} \\
            \$SKETCH_SIZE \$MIN_K \$MAX_K \$EXTRA_PARAMS \\
            --overwrite || exit 1
    
    # Memory cleanup
    sync && echo 1 > /proc/sys/vm/drop_caches 2>/dev/null || echo "Cannot drop caches"
    sleep 5
    
    # Fit model
    echo "Fitting PopPUNK model for chunk ${chunk_id}..."
    timeout 3600 poppunk --fit-model --ref-db poppunk_db_${chunk_id} \\
            --output poppunk_fit_${chunk_id} \\
            --threads ${task.cpus} \\
            --overwrite || exit 1
    
    # Memory cleanup
    sync && echo 1 > /proc/sys/vm/drop_caches 2>/dev/null || echo "Cannot drop caches"
    sleep 5
    
    # Assign clusters
    echo "Assigning clusters for chunk ${chunk_id}..."
    timeout 3600 poppunk --assign-query --ref-db poppunk_db_${chunk_id} \\
            --q-files assembly_list.txt \\
            --output poppunk_assigned_${chunk_id} \\
            --threads ${task.cpus} \\
            --overwrite || exit 1
    
    # Find and copy cluster file
    find poppunk_assigned_${chunk_id} -name "*clusters.csv" -exec cp {} chunk_${chunk_id}_clusters.csv \\;
    
    if [ ! -f chunk_${chunk_id}_clusters.csv ]; then
        find poppunk_assigned_${chunk_id} -name "*cluster*.csv" -exec cp {} chunk_${chunk_id}_clusters.csv \\;
    fi
    
    if [ ! -f chunk_${chunk_id}_clusters.csv ]; then
        echo "Could not find cluster file for chunk ${chunk_id}"
        exit 1
    fi
    
    echo "Chunk ${chunk_id} completed successfully"
    """
}

// Process to merge cluster results from all chunks
process MERGE_CLUSTERS {
    tag "Merging_cluster_results"
    publishDir "${params.resultsDir}/poppunk", mode: 'copy'
    
    input:
    path cluster_files
    
    output:
    path 'merged_clusters.csv'
    
    script:
    """
    echo "Merging cluster results from chunks..."
    
    # Create header
    head -1 ${cluster_files[0]} > merged_clusters.csv
    
    # Merge all cluster files (skip headers)
    for file in ${cluster_files.join(' ')}; do
        tail -n +2 "\$file" >> merged_clusters.csv
    done
    
    echo "Merged \$(tail -n +2 merged_clusters.csv | wc -l) cluster assignments"
    echo "Total unique clusters: \$(tail -n +2 merged_clusters.csv | cut -f2 | sort -u | wc -l)"
    """
}

// Reuse existing processes from main pipeline
process PANAROO {
    tag "Panaroo_cluster_${cluster_id}"
    container 'staphb/panaroo:1.5.2'
    publishDir "${params.resultsDir}/cluster_${cluster_id}/panaroo", mode: 'copy'

    input:
    tuple val(cluster_id), path(assemblies)

    output:
    tuple val(cluster_id), path('core_gene_alignment.aln')

    when:
    assemblies.size() >= 3

    script:
    """
    echo "Processing cluster ${cluster_id} with \${#assemblies[@]} genomes"
    
    panaroo -i *.{fasta,fa,fas} -o panaroo_output \\
            -t ${task.cpus} --clean-mode strict --aligner mafft \\
            --remove-invalid-genes
    
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
    
    if [ ! -f ${alignment} ]; then
        echo "Error: Alignment file not found"
        exit 1
    fi
    
    run_gubbins.py --prefix gubbins_output \\
                   --threads ${task.cpus} \\
                   --verbose ${alignment}
    
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
    
    if [ ! -f ${snp_alignment} ]; then
        echo "Error: SNP alignment file not found"
        exit 1
    fi
    
    SEQ_COUNT=\$(grep -c ">" ${snp_alignment})
    if [ \$SEQ_COUNT -lt 3 ]; then
        echo "Warning: Insufficient sequences (\$SEQ_COUNT) for tree building"
        touch tree.warning
        exit 0
    fi
    
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

    // Split files into chunks
    chunks_ch = SPLIT_FILES(assemblies_ch)
        .chunks
        .flatten()
        .map { chunk_dir -> 
            def chunk_id = chunk_dir.name.replaceAll('chunk_', '')
            def chunk_files = file("${chunk_dir}/*.{fasta,fa,fas}")
            return tuple(chunk_id, chunk_files)
        }

    // Run PopPUNK on each chunk
    chunk_clusters = POPPUNK_CHUNK(chunks_ch)

    // Merge cluster results
    merged_clusters = MERGE_CLUSTERS(chunk_clusters.map { it[1] }.collect())

    // Parse merged cluster assignments and group assemblies by cluster
    cluster_assignments = merged_clusters
        .splitCsv(header: true)
        .map { row -> 
            def taxon_name = row.Taxon ?: row.taxon ?: row.Sample ?: row.sample ?: row.ID ?: row.id
            def cluster_id = row.Cluster ?: row.cluster ?: row.cluster_id
            
            if (!taxon_name || !cluster_id) {
                error "Could not find taxon name or cluster ID in CSV row: ${row}"
            }
            
            // Find the assembly file
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
        .filter { it != null }
        .groupTuple()
        .filter { cluster_id, files -> files.size() >= 3 }

    // Log cluster information
    cluster_assignments.view { cluster_id, files -> 
        "Final cluster ${cluster_id}: ${files.size()} genomes"
    }

    // Run downstream analysis per cluster
    panaroo_results = PANAROO(cluster_assignments)
    gubbins_results = GUBBINS(panaroo_results)
    IQTREE(gubbins_results)
    
    // Summary
    IQTREE.out.view { cluster_id, tree_files ->
        "Completed analysis for cluster ${cluster_id}: ${tree_files.size()} output files"
    }
}