# Attribute Extractor

A Python tool to extract attributes from various file types (shapefiles, CSV, etc.) with sample data where all attributes contain values.

## Features

- ✅ Extract attributes from shapefiles (.shp)
- ✅ Extract attributes from CSV files (.csv)
- ✅ Skip duplicate filenames across different folders
- ✅ Find sample data where all attributes have non-null values
- ✅ Extensible design for adding new file type handlers
- ✅ Separate output files for different file types

## Setup

### 1. Create Virtual Environment

```powershell
python -m venv venv
```

### 2. Activate Virtual Environment

```powershell
.\venv\Scripts\Activate.ps1
```

If you encounter an execution policy error, run:
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### 3. Install Dependencies

```powershell
pip install -r requirements.txt
```

## Usage

### 1. Extract Attributes

**Optional: Configure Filters**

Edit [config.txt](config.txt) to filter files by filename. Each line is a filter - files will be processed only if their filename contains ANY of the specified texts:

```
# config.txt example
boundary
population
road
```

Leave empty or comment all lines (with `#`) to process all files.

**Run the extraction:**

```powershell
python extract_attributes.py
```

When prompted, enter the root folder path containing your shapefiles and CSV files, or press Enter to use the current directory.

The script will:
1. Ask if you want to use config.txt filters (if found)
2. Recursively search for all supported files
3. Filter files based on config (if enabled)
4. Extract attributes and find a sample row with all non-null values
5. Skip duplicate filenames
6. Save results to the `output` folder:
   - `shapefile_attributes.txt` - Shapefile results
   - `csv_attributes.txt` - CSV file results

### 2. Visualize Attribute Overlaps

```powershell
python visualize_attributes.py
```

When prompted, enter the path to the output folder (default: `./output`).

The visualization tool will:
1. Parse the extracted attribute files
2. Analyze attribute overlaps across files
3. Generate visualizations in `output/visualizations/`:
   - **UpSet Plot** - Shows which attributes are common/unique across files
   - **Frequency Chart** - Top 30 most common attributes
   - **Summary Report** - Detailed text analysis

## Output Format

Each output file contains:
- File name and path
- Number of attributes
- List of all attributes
- Sample data with values for each attribute
- Summary statistics

## Extending for Other File Types

To add support for new file types:

1. Create a new handler class inheriting from `FileHandler`:

```python
class GeoJSONHandler(FileHandler):
    def can_handle(self, file_path: Path) -> bool:
        return file_path.suffix.lower() == '.geojson'
    
    def extract_data(self, file_path: Path) -> Optional[Tuple[List[str], Dict[str, Any]]]:
        # Implementation here
        pass
```

2. Add the handler to the `main()` function:

```python
handlers = [
    ShapefileHandler(),
    CSVHandler(),
    GeoJSONHandler(),  # Add your new handler
]
```

## Requirements

- Python 3.8 or higher
- pandas
- geopandas

## License

Free to use and modify.
