#!/usr/bin/env nextflow

// cptools2 — multi-stage Cell Painting pipeline
// DSL2 pipeline for illumination correction, segmentation, and feature extraction
//
// Usage:
//   nextflow run nextflow/main.nf -profile test
//   nextflow run nextflow/main.nf -profile eddie --input_dir /path/to/images
//   nextflow run nextflow/main.nf -profile eddie --stages illum,segment

nextflow.enable.dsl = 2

// ---------------------------------------------------------------------------
// Default parameters
// ---------------------------------------------------------------------------

params.input_dir        = null      // root directory containing plate subdirs
params.output_dir       = null      // root output directory
params.stages           = 'all'     // comma-separated: illum,segment,extract  or 'all'
params.plates           = null      // comma-separated plate IDs, or null to auto-detect

// Channel configuration (Cell Painting defaults)
params.channels         = ['DNA', 'RNA', 'ER', 'AGP', 'Mito']

// Illumination correction
params.illum_smoothing_filter_size = 200
params.illum_pipeline_calculate    = "${projectDir}/cptools2/templates/illum_calculate.cppipe"
params.illum_pipeline_apply        = "${projectDir}/cptools2/templates/illum_apply.cppipe"

// Segmentation
params.seg_pipeline     = "${projectDir}/cptools2/templates/nuclear_segmentation.cppipe"
params.seg_diameter_min = 20
params.seg_diameter_max = 80

// Feature extraction
params.feature_extraction_tool      = 'deepprofiler'   // 'deepprofiler' or 'dinov2'
params.feature_extraction_config    = "${projectDir}/cptools2/templates/deepprofiler_config.json"
params.feature_extraction_weights   = null              // path to model checkpoint
params.feature_extraction_batch_size = 128

// ---------------------------------------------------------------------------
// Resolve active stages
// ---------------------------------------------------------------------------

// Handle both List (from params-file JSON) and String (from CLI/config)
def active_stages = params.stages instanceof List
    ? params.stages.collect { it.trim().toLowerCase() }
    : params.stages.tokenize(',').collect { it.trim().toLowerCase() }

def run_illum_calc = active_stages.contains('all') || active_stages.contains('illum_calculate')
def run_illum_app  = active_stages.contains('all') || active_stages.contains('illum_apply')
def run_segment    = active_stages.contains('all') || active_stages.contains('segmentation')
def run_extract    = active_stages.contains('all') || active_stages.contains('feature_extract')

// Convenience: if either illum sub-stage requested, run both
def run_illum = run_illum_calc || run_illum_app

// ---------------------------------------------------------------------------
// Module includes
// ---------------------------------------------------------------------------

include { ILLUM_CALCULATE } from './modules/illum_calculate'
include { ILLUM_APPLY     } from './modules/illum_apply'
include { SEGMENTATION    } from './modules/segmentation'
include { FEATURE_EXTRACT } from './modules/feature_extract'

// ---------------------------------------------------------------------------
// Input channel: one entry per plate
// ---------------------------------------------------------------------------

def build_plate_channel() {
    if (params.plates) {
        return Channel.of(params.plates.tokenize(',').collect { it.trim() })
                      .flatten()
                      .map { plate_id ->
                          def plate_dir = file("${params.input_dir}/${plate_id}")
                          tuple(plate_id, plate_dir)
                      }
    } else {
        return Channel.fromPath("${params.input_dir}/*", type: 'dir')
                      .map { dir -> tuple(dir.name, dir) }
    }
}

// ---------------------------------------------------------------------------
// Workflow
// ---------------------------------------------------------------------------

workflow {

    // Build per-plate input channel
    ch_plates = build_plate_channel()

    // Stage 1: Illumination correction — calculate
    if (run_illum) {
        ILLUM_CALCULATE(ch_plates)

        // Stage 2: Illumination correction — apply
        // Join calculate output with original plates for apply step
        ILLUM_APPLY(ILLUM_CALCULATE.out.illum_functions)

        ch_corrected = ILLUM_APPLY.out.corrected_images
    }

    // Stage 3: Segmentation
    if (run_segment) {
        if (run_illum) {
            SEGMENTATION(ch_corrected)
        } else {
            // If skipping illum, assume input images are already corrected
            SEGMENTATION(ch_plates)
        }

        ch_segmented = SEGMENTATION.out.locations
    }

    // Stage 4: Feature extraction
    // FEATURE_EXTRACT expects: tuple(plate_id, corrected_dir, locations_dir)
    // SEGMENTATION.out.locations already emits: tuple(plate_id, corrected_dir, locations_dir)
    if (run_extract) {
        if (run_segment) {
            // Segmentation output already carries corrected images and locations
            FEATURE_EXTRACT(ch_segmented)
        } else if (run_illum) {
            // Have corrected images but no segmentation — need pre-existing locations
            ch_extract_input = ch_corrected.map { plate_id, corrected_dir ->
                tuple(plate_id, corrected_dir, file("${params.output_dir}/${plate_id}/segmentation"))
            }
            FEATURE_EXTRACT(ch_extract_input)
        } else {
            // Running extraction alone — expect pre-existing corrected images and locations
            ch_extract_input = ch_plates.map { plate_id, plate_dir ->
                tuple(plate_id, plate_dir, file("${params.output_dir}/${plate_id}/segmentation"))
            }
            FEATURE_EXTRACT(ch_extract_input)
        }
    }
}
