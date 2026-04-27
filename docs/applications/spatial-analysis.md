# Spatial Analysis

> **Source:** <https://www.wiki.ed.ac.uk/display/ResearchServices/Spatial+analysis>

Eddie includes most of the **OSGeo suite** of libraries and applications for geospatial analysis, including GDAL/OGR and GRASS GIS.

```bash
module load osgeo
```

## Pre-built Packages

The most commonly used spatial analysis packages are provided pre-built:

- **R:** Modules `R/4.3.0` and `R/4.4` include `sf`, `sp`, `rgdal`, and `maptools`. Other packages can be built — required dependencies should be available once the `osgeo` module is loaded.
- **Python:** Module `python/3.11.4` includes `osgeo_tools` and `geopandas`.
