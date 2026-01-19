import os
import tempfile
import polars as pl

from cptools2.file_tools import (
    _discover_plates_from_raw_data,
    _discover_chunks_for_plate,
    merge_loaddata_metadata,
    enrich_chunks_with_metadata
)


def test_discover_plates_preserves_underscores(tmp_path):
    """
    Plate discovery should not truncate plate names that contain underscores.
    Expected raw_data dir format: "<plate_name>_<job_id>/".
    """
    raw_data = tmp_path / "raw_data"
    raw_data.mkdir()

    # Plate name contains underscores; job id is numeric.
    (raw_data / "my_plate_name_with_underscores_0001").mkdir()
    (raw_data / "my_plate_name_with_underscores_0002").mkdir()

    plates = _discover_plates_from_raw_data(str(raw_data))
    assert plates == ["my_plate_name_with_underscores"]


def test_discover_chunks_for_plate(tmp_path):
    """Test discovering chunk directories for a specific plate."""
    raw_data = tmp_path / "raw_data"
    raw_data.mkdir()
    
    # Create multiple chunks for a plate
    (raw_data / "plate_A_0").mkdir()
    (raw_data / "plate_A_1").mkdir()
    (raw_data / "plate_A_2").mkdir()
    (raw_data / "plate_B_0").mkdir()  # Different plate
    
    chunks = _discover_chunks_for_plate(str(raw_data), "plate_A")
    chunk_names = [os.path.basename(c) for c in chunks]
    
    assert chunk_names == ["plate_A_0", "plate_A_1", "plate_A_2"]


def test_merge_loaddata_metadata_basic(tmp_path):
    """Test basic metadata merge on ImageNumber."""
    # Create LoadData CSV
    loaddata_csv = tmp_path / "loaddata.csv"
    loaddata_data = pl.DataFrame({
        "ImageNumber": [1, 2, 3],
        "Metadata_well": ["A01", "A02", "A03"],
        "Metadata_site": [1, 1, 1],
        "Metadata_platename": ["plate1", "plate1", "plate1"],
    })
    loaddata_data.write_csv(loaddata_csv)
    
    # Create output CSV
    output_csv = tmp_path / "output.csv"
    output_data = pl.DataFrame({
        "ImageNumber": [1, 2, 3],
        "AreaShape_Area": [100, 200, 300],
        "Intensity_Mean": [50.5, 60.3, 70.1],
    })
    output_data.write_csv(output_csv)
    
    # Merge metadata
    result = merge_loaddata_metadata(str(output_csv), str(loaddata_csv))
    
    assert result['success'] is True
    assert result['rows_before'] == 3
    assert result['rows_after'] == 3
    assert set(result['metadata_columns_added']) == {"Metadata_well", "Metadata_site", "Metadata_platename"}
    assert result['null_metadata_count'] == 0
    
    # Verify enriched CSV has metadata columns
    enriched = pl.read_csv(str(output_csv))
    assert "Metadata_well" in enriched.columns
    assert "Metadata_site" in enriched.columns
    assert enriched["Metadata_well"].to_list() == ["A01", "A02", "A03"]


def test_merge_loaddata_metadata_missing_imagenumber(tmp_path):
    """Test error handling when ImageNumber column is missing."""
    # Create LoadData CSV without ImageNumber
    loaddata_csv = tmp_path / "loaddata.csv"
    loaddata_data = pl.DataFrame({
        "Metadata_well": ["A01", "A02"],
    })
    loaddata_data.write_csv(loaddata_csv)
    
    # Create output CSV with ImageNumber
    output_csv = tmp_path / "output.csv"
    output_data = pl.DataFrame({
        "ImageNumber": [1, 2],
        "AreaShape_Area": [100, 200],
    })
    output_data.write_csv(output_csv)
    
    # Merge should fail gracefully
    result = merge_loaddata_metadata(str(output_csv), str(loaddata_csv))
    
    assert result['success'] is False
    assert "ImageNumber" in result['error']


def test_merge_loaddata_metadata_mismatched_imagenumber(tmp_path):
    """Test detection of ImageNumber mismatches (different row counts)."""
    # Create LoadData CSV with 3 rows
    loaddata_csv = tmp_path / "loaddata.csv"
    loaddata_data = pl.DataFrame({
        "ImageNumber": [1, 2, 3],
        "Metadata_well": ["A01", "A02", "A03"],
    })
    loaddata_data.write_csv(loaddata_csv)
    
    # Create output CSV with 5 rows (mismatch)
    output_csv = tmp_path / "output.csv"
    output_data = pl.DataFrame({
        "ImageNumber": [1, 2, 3, 4, 5],
        "AreaShape_Area": [100, 200, 300, 400, 500],
    })
    output_data.write_csv(output_csv)
    
    # Merge will result in null metadata for rows 4-5
    result = merge_loaddata_metadata(str(output_csv), str(loaddata_csv))
    
    assert result['success'] is True
    assert result['null_metadata_count'] == 2  # Rows 4 and 5 have null metadata


def test_merge_loaddata_metadata_output_path(tmp_path):
    """Test writing enriched CSV to a different output path."""
    # Create LoadData and output CSVs
    loaddata_csv = tmp_path / "loaddata.csv"
    loaddata_data = pl.DataFrame({
        "ImageNumber": [1, 2],
        "Metadata_well": ["A01", "A02"],
    })
    loaddata_data.write_csv(loaddata_csv)
    
    output_csv = tmp_path / "output.csv"
    output_data = pl.DataFrame({
        "ImageNumber": [1, 2],
        "AreaShape_Area": [100, 200],
    })
    output_data.write_csv(output_csv)
    
    # Merge to different output path
    enriched_csv = tmp_path / "enriched_output.csv"
    result = merge_loaddata_metadata(str(output_csv), str(loaddata_csv), str(enriched_csv))
    
    assert result['success'] is True
    assert result['output_file'] == str(enriched_csv)
    assert enriched_csv.exists()
    
    # Original should be unchanged
    original = pl.read_csv(str(output_csv))
    assert "Metadata_well" not in original.columns


def test_enrich_chunks_with_metadata_full_workflow(tmp_path):
    """Test full enrichment workflow with multiple chunks and plates."""
    # Create directory structure
    location = tmp_path / "project"
    location.mkdir()
    raw_data = location / "raw_data"
    raw_data.mkdir()
    loaddata_dir = location / "loaddata"
    loaddata_dir.mkdir()
    
    # Create chunks for plate1
    chunk1 = raw_data / "plate1_0"
    chunk1.mkdir()
    chunk2 = raw_data / "plate1_1"
    chunk2.mkdir()
    
    # Create LoadData CSVs for each chunk
    for i, chunk_dir in enumerate([chunk1, chunk2]):
        chunk_name = os.path.basename(str(chunk_dir))
        loaddata_csv = loaddata_dir / f"{chunk_name}.csv"
        loaddata_data = pl.DataFrame({
            "ImageNumber": [1, 2, 3],
            "Metadata_well": [f"A0{j+1}" for j in range(3)],
            "Metadata_site": [1, 1, 1],
        })
        loaddata_data.write_csv(loaddata_csv)
        
        # Create output CSVs in chunk directory
        image_csv = chunk_dir / "Image.csv"
        image_data = pl.DataFrame({
            "ImageNumber": [1, 2, 3],
            "Image_Area": [100, 200, 300],
        })
        image_data.write_csv(image_csv)
        
        cells_csv = chunk_dir / "Cells.csv"
        cells_data = pl.DataFrame({
            "ImageNumber": [1, 1, 2, 2, 3, 3],
            "ObjectNumber": [1, 2, 1, 2, 1, 2],
            "AreaShape_Area": [10, 20, 30, 40, 50, 60],
        })
        cells_data.write_csv(cells_csv)
    
    # Run enrichment
    result = enrich_chunks_with_metadata(str(location), patterns=["Image.csv", "Cells.csv"])
    
    assert result['total_chunks_processed'] == 2
    assert result['total_files_enriched'] == 4  # 2 chunks * 2 patterns
    assert len(result['errors']) == 0
    
    # Verify enriched files have metadata
    for chunk_name in ["plate1_0", "plate1_1"]:
        chunk_dir = raw_data / chunk_name
        
        image_enriched = pl.read_csv(chunk_dir / "Image.csv")
        assert "Metadata_well" in image_enriched.columns
        assert "Metadata_site" in image_enriched.columns
        
        cells_enriched = pl.read_csv(chunk_dir / "Cells.csv")
        assert "Metadata_well" in cells_enriched.columns
        assert "Metadata_site" in cells_enriched.columns


def test_enrich_chunks_missing_loaddata(tmp_path):
    """Test enrichment handles missing LoadData gracefully."""
    location = tmp_path / "project"
    location.mkdir()
    raw_data = location / "raw_data"
    raw_data.mkdir()
    loaddata_dir = location / "loaddata"
    loaddata_dir.mkdir()
    
    # Create chunk but no corresponding LoadData
    chunk = raw_data / "plate1_0"
    chunk.mkdir()
    
    output_csv = chunk / "Image.csv"
    output_data = pl.DataFrame({
        "ImageNumber": [1, 2],
        "Image_Area": [100, 200],
    })
    output_data.write_csv(output_csv)
    
    # Run enrichment - should handle gracefully
    result = enrich_chunks_with_metadata(str(location), patterns=["Image.csv"])
    
    # Should have error but not crash
    assert len(result['errors']) > 0
    assert result['total_chunks_processed'] == 0


def test_enrich_chunks_no_loaddata_directory(tmp_path):
    """Test enrichment handles missing loaddata directory gracefully."""
    location = tmp_path / "project"
    location.mkdir()
    raw_data = location / "raw_data"
    raw_data.mkdir()
    # No loaddata directory
    
    chunk = raw_data / "plate1_0"
    chunk.mkdir()
    
    result = enrich_chunks_with_metadata(str(location), patterns=["Image.csv"])
    
    # Should handle gracefully
    assert len(result['errors']) == 0  # Graceful degradation - no errors, just returns
