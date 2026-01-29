"""
Extract attributes from various file types (shapefiles, CSV, etc.) with sample data.
Extensible design for adding new file type handlers.
"""
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from abc import ABC, abstractmethod
import pandas as pd
import geopandas as gpd


class FileHandler(ABC):
    """Abstract base class for file type handlers."""
    
    @abstractmethod
    def can_handle(self, file_path: Path) -> bool:
        """Check if this handler can process the given file."""
        pass
    
    @abstractmethod
    def extract_data(self, file_path: Path) -> Optional[Tuple[List[str], Dict[str, Any]]]:
        """
        Extract attributes and sample data from file.
        
        Returns:
            Tuple of (attribute_names, sample_data_dict) or None if no valid sample found
        """
        pass


class ShapefileHandler(FileHandler):
    """Handler for shapefile extraction."""
    
    def can_handle(self, file_path: Path) -> bool:
        return file_path.suffix.lower() == '.shp'
    
    def extract_data(self, file_path: Path) -> Optional[Tuple[List[str], Dict[str, Any]]]:
        try:
            gdf = gpd.read_file(file_path)
            
            if gdf.empty:
                return None
            
            # Limit to first 10,000 records
            gdf_sample = gdf.head(10000)
            
            # Get column names (attributes)
            attributes = list(gdf_sample.columns)
            
            best_row = None
            best_count = -1
            
            # Find first row where all attributes have data (no nulls)
            # Or track the row with most non-null values
            for idx, row in gdf_sample.iterrows():
                null_count = row.isnull().sum()
                non_null_count = len(row) - null_count
                
                if null_count == 0:
                    # Found perfect row with all data
                    sample_data = row.to_dict()
                    if 'geometry' in sample_data:
                        sample_data['geometry'] = str(sample_data['geometry'])
                    return attributes, sample_data
                
                # Track best row (most complete)
                if non_null_count > best_count:
                    best_count = non_null_count
                    best_row = row
            
            # Use the best row found (most complete data)
            if best_row is not None:
                sample_data = best_row.to_dict()
                if 'geometry' in sample_data:
                    sample_data['geometry'] = str(sample_data['geometry'])
                return attributes, sample_data
            
            return None
            
        except Exception as e:
            print(f"Error reading shapefile {file_path}: {e}")
            return None


class CSVHandler(FileHandler):
    """Handler for CSV file extraction."""
    
    def can_handle(self, file_path: Path) -> bool:
        return file_path.suffix.lower() == '.csv'
    
    def extract_data(self, file_path: Path) -> Optional[Tuple[List[str], Dict[str, Any]]]:
        try:
            # Check file size to determine strategy
            file_size = file_path.stat().st_size
            
            # For files larger than 100MB, use chunked reading
            if file_size > 100 * 1024 * 1024:
                print(f"    💾 Large file detected ({file_size / (1024**3):.2f} GB), using chunked reading...")
                return self._extract_chunked(file_path)
            else:
                # For smaller files, read directly with row limit
                df = pd.read_csv(file_path, nrows=10000)
                
                if df.empty:
                    return None
                
                attributes = list(df.columns)
                
                best_row = None
                best_count = -1
                
                for idx, row in df.iterrows():
                    null_count = row.isnull().sum()
                    non_null_count = len(row) - null_count
                    
                    if null_count == 0:
                        return attributes, row.to_dict()
                    
                    if non_null_count > best_count:
                        best_count = non_null_count
                        best_row = row
                
                if best_row is not None:
                    return attributes, best_row.to_dict()
                
                return None
            
        except Exception as e:
            print(f"Error reading CSV {file_path}: {e}")
            return None
    
    def _extract_chunked(self, file_path: Path) -> Optional[Tuple[List[str], Dict[str, Any]]]:
        """Extract data using chunked reading for large files."""
        try:
            chunk_size = 1000
            max_rows_to_check = 10000
            rows_checked = 0
            
            best_row = None
            best_count = -1
            attributes = None
            
            for chunk in pd.read_csv(file_path, chunksize=chunk_size):
                if attributes is None:
                    attributes = list(chunk.columns)
                
                for idx, row in chunk.iterrows():
                    null_count = row.isnull().sum()
                    non_null_count = len(row) - null_count
                    
                    if null_count == 0:
                        # Found perfect row, return immediately
                        return attributes, row.to_dict()
                    
                    if non_null_count > best_count:
                        best_count = non_null_count
                        best_row = row
                
                rows_checked += len(chunk)
                if rows_checked % 5000 == 0:
                    print(f"    🔍 Checked {rows_checked} rows...", end='\r')
                
                if rows_checked >= max_rows_to_check:
                    break
            
            if best_row is not None:
                return attributes, best_row.to_dict()
            
            return None
            
        except Exception as e:
            print(f"Error reading CSV in chunks {file_path}: {e}")
            return None


# To extend: Add more handlers here (e.g., GeoJSONHandler, ExcelHandler, etc.)


class TXTHandler(FileHandler):
    """Handler for TXT files that contain CSV-like data."""
    
    def can_handle(self, file_path: Path) -> bool:
        return file_path.suffix.lower() == '.txt'
    
    def extract_data(self, file_path: Path) -> Optional[Tuple[List[str], Dict[str, Any]]]:
        try:
            # Check file size
            file_size = file_path.stat().st_size
            print(f"    💾 File size: {file_size / (1024**3):.2f} GB")
            
            # For very large files, use chunked reading with auto-delimiter detection
            if file_size > 100 * 1024 * 1024:
                print(f"    🔍 Large TXT file detected, using chunked reading...")
                return self._extract_chunked(file_path)
            else:
                # Try to detect delimiter and read sample
                return self._extract_small(file_path)
            
        except Exception as e:
            print(f"Error reading TXT file {file_path}: {e}")
            return None
    
    def _detect_delimiter(self, file_path: Path) -> str:
        """Detect the delimiter used in the file."""
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                first_line = f.readline()
                
                # Count potential delimiters
                delimiters = [',', '\t', '|', ';', ' ']
                counts = {delim: first_line.count(delim) for delim in delimiters}
                
                # Return delimiter with highest count (if > 0)
                best_delim = max(counts, key=counts.get)
                if counts[best_delim] > 0:
                    return best_delim
                
                return ','  # Default to comma
        except:
            return ','
    
    def _extract_small(self, file_path: Path) -> Optional[Tuple[List[str], Dict[str, Any]]]:
        """Extract data from smaller TXT files."""
        try:
            delimiter = self._detect_delimiter(file_path)
            print(f"    🔍 Detected delimiter: '{delimiter}' (tab)" if delimiter == '\t' else f"    🔍 Detected delimiter: '{delimiter}'")
            
            df = pd.read_csv(file_path, sep=delimiter, nrows=10000, encoding='utf-8', 
                           on_bad_lines='skip', engine='python')
            
            if df.empty:
                return None
            
            attributes = list(df.columns)
            best_row = None
            best_count = -1
            
            for idx, row in df.iterrows():
                null_count = row.isnull().sum()
                non_null_count = len(row) - null_count
                
                if null_count == 0:
                    return attributes, row.to_dict()
                
                if non_null_count > best_count:
                    best_count = non_null_count
                    best_row = row
            
            if best_row is not None:
                return attributes, best_row.to_dict()
            
            return None
            
        except Exception as e:
            print(f"    ⚠️  Error reading as delimited file: {e}")
            return None
    
    def _extract_chunked(self, file_path: Path) -> Optional[Tuple[List[str], Dict[str, Any]]]:
        """Extract data using chunked reading for large TXT files."""
        try:
            delimiter = self._detect_delimiter(file_path)
            print(f"    🔍 Detected delimiter: '{delimiter}' (tab)" if delimiter == '\t' else f"    🔍 Detected delimiter: '{delimiter}'")
            
            chunk_size = 1000
            max_rows_to_check = 10000
            rows_checked = 0
            
            best_row = None
            best_count = -1
            attributes = None
            
            reader = pd.read_csv(file_path, sep=delimiter, chunksize=chunk_size, 
                               encoding='utf-8', on_bad_lines='skip', engine='python')
            
            for chunk in reader:
                if attributes is None:
                    attributes = list(chunk.columns)
                
                for idx, row in chunk.iterrows():
                    null_count = row.isnull().sum()
                    non_null_count = len(row) - null_count
                    
                    if null_count == 0:
                        return attributes, row.to_dict()
                    
                    if non_null_count > best_count:
                        best_count = non_null_count
                        best_row = row
                
                rows_checked += len(chunk)
                if rows_checked % 2000 == 0:
                    print(f"    🔍 Checked {rows_checked}/{max_rows_to_check} rows...", end='\r')
                
                if rows_checked >= max_rows_to_check:
                    print(f"    ✓ Completed checking {rows_checked} rows" + " " * 20)
                    break
            
            if best_row is not None:
                return attributes, best_row.to_dict()
            
            return None
            
        except Exception as e:
            print(f"    ⚠️  Error reading TXT file in chunks: {e}")
            return None


class AttributeExtractor:
    """Main class for extracting attributes from files."""
    
    def __init__(self, root_folder: str, handlers: List[FileHandler], config_file: Optional[str] = None):
        self.root_folder = Path(root_folder)
        self.handlers = handlers
        self.processed_filenames = set()
        self.attribute_filters = self._load_config(config_file) if config_file else []
    
    def _load_config(self, config_file: str) -> List[str]:
        """Load filename filters from config file."""
        filters = []
        config_path = Path(config_file)
        
        if not config_path.exists():
            print(f"⚠️  Warning: Config file '{config_file}' not found. Processing all files.")
            return filters
        
        with open(config_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                # Skip empty lines and comments
                if line and not line.startswith('#'):
                    filters.append(line)  # Case-sensitive matching
        
        if filters:
            print(f"\n📋 Loaded {len(filters)} filename filter(s) from config:")
            for f in filters:
                print(f"  • {f}")
        else:
            print(f"\n📋 No filters in config file. Processing all files.")
        
        return filters
    
    def _matches_filters(self, filename: str) -> bool:
        """Check if filename matches any filter."""
        if not self.attribute_filters:
            return True  # No filters = process all
        
        # Check if filename contains any filter text (case-sensitive)
        for filter_text in self.attribute_filters:
            if filter_text in filename:
                return True
        
        return False
    
    def find_files(self) -> Dict[str, List[Path]]:
        """Find all supported files in the root folder and subfolders."""
        print(f"\n🔍 Scanning directory: {self.root_folder}")
        print("Please wait, searching for files...")
        
        files_by_handler = {handler.__class__.__name__: [] for handler in self.handlers}
        file_count = 0
        
        for file_path in self.root_folder.rglob('*'):
            if file_path.is_file():
                file_count += 1
                if file_count % 100 == 0:
                    print(f"  Found {file_count} files so far...", end='\r')
                
                for handler in self.handlers:
                    if handler.can_handle(file_path):
                        files_by_handler[handler.__class__.__name__].append(file_path)
                        break
        
        print(f"  ✓ Scan complete: {file_count} files found" + " " * 20)
        return files_by_handler
    
    def extract_and_save(self, output_folder: str = "."):
        """Extract attributes and save to individual text files."""
        output_path = Path(output_folder)
        output_path.mkdir(exist_ok=True)
        
        files_by_handler = self.find_files()
        
        # Track overall statistics
        overall_processed = 0
        overall_skipped = 0
        overall_failed = 0
        
        # Process each handler type separately
        for handler in self.handlers:
            handler_name = handler.__class__.__name__
            file_paths = files_by_handler.get(handler_name, [])
            
            if not file_paths:
                print(f"No files found for {handler_name}")
                continue
            
            total_files = len(file_paths)
            print(f"\n{'='*80}")
            print(f"Processing {total_files} files with {handler_name}")
            print(f"{'='*80}")
            
            processed_count = 0
            skipped_count = 0
            failed_count = 0
            
            for idx, file_path in enumerate(sorted(file_paths), 1):
                filename = file_path.name
                
                # Show progress
                progress_pct = (idx / total_files) * 100
                print(f"\n[{idx}/{total_files}] ({progress_pct:.1f}%)")
                print(f"  File: {filename}")
                print(f"  Path: {file_path}")
                
                # Check if output file already exists
                file_stem = Path(filename).stem
                output_file = output_path / f"{file_stem}_attributes.txt"
                if output_file.exists():
                    skipped_count += 1
                    print(f"  ⊘ Status: SKIPPED (output already exists)")
                    print(f"  📊 Stats: ✓ {processed_count} | ⊘ {skipped_count} | ✗ {failed_count}")
                    continue
                
                # Check if filename matches the config filters
                if not self._matches_filters(filename):
                    skipped_count += 1
                    print(f"  ⊘ Status: SKIPPED (filename not in config filters)")
                    print(f"  📊 Stats: ✓ {processed_count} | ⊘ {skipped_count} | ✗ {failed_count}")
                    continue
                
                # Skip if filename already processed
                if filename in self.processed_filenames:
                    skipped_count += 1
                    print(f"  ⊘ Status: SKIPPED (duplicate filename)")
                    print(f"  📊 Stats: ✓ {processed_count} | ⊘ {skipped_count} | ✗ {failed_count}")
                    continue
                
                result = handler.extract_data(file_path)
                
                if result is None:
                    failed_count += 1
                    print(f"  ✗ Status: FAILED (no data found)")
                    print(f"  📊 Stats: ✓ {processed_count} | ⊘ {skipped_count} | ✗ {failed_count}")
                    continue
                
                attributes, sample_data = result
                
                # Check data completeness
                null_count = sum(1 for v in sample_data.values() if pd.isna(v))
                completeness_pct = ((len(attributes) - null_count) / len(attributes)) * 100 if attributes else 0
                
                processed_count += 1
                
                # Show detailed info in console
                print(f"  ✓ Status: SUCCESS")
                print(f"  📋 Attributes: {len(attributes)}")
                print(f"  📊 Completeness: {completeness_pct:.1f}% ({len(attributes) - null_count}/{len(attributes)} fields)")
                print(f"  📈 Stats: ✓ {processed_count} | ⊘ {skipped_count} | ✗ {failed_count}")
                
                # Write to individual file (output_file already defined above)
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(f"[FILE_START]\n")
                    f.write(f"Filename: {filename}\n")
                    f.write(f"Path: {file_path}\n")
                    f.write(f"Type: {handler_name}\n")
                    f.write(f"Completeness: {completeness_pct:.1f}%\n\n")
                    
                    f.write("[ATTRIBUTES_START]\n")
                    for attr in attributes:
                        f.write(f"{attr}\n")
                    f.write("[ATTRIBUTES_END]\n\n")
                    
                    f.write("[SAMPLE_DATA_START]\n")
                    for attr in attributes:
                        value = sample_data.get(attr, 'N/A')
                        value_str = str(value)
                        # Use ||| as delimiter for easy parsing
                        f.write(f"{attr}|||{value_str}\n")
                    f.write("[SAMPLE_DATA_END]\n")
                    f.write("[FILE_END]\n")
                
                print(f"  💾 Saved to: {output_file.name}")
                
                self.processed_filenames.add(filename)
            
            print(f"\n{'='*80}")
            print(f"SUMMARY - {handler_name}")
            print(f"{'='*80}")
            print(f"  Total files found:       {total_files}")
            print(f"  ✓ Successfully processed: {processed_count}")
            print(f"  ⊘ Skipped (duplicates):   {skipped_count}")
            print(f"  ✗ Failed (no valid data): {failed_count}")
            print(f"{'='*80}")
            
            overall_processed += processed_count
            overall_skipped += skipped_count
            overall_failed += failed_count
        
        # Print overall summary
        print(f"\n{'='*80}")
        print(f"OVERALL SUMMARY")
        print(f"{'='*80}")
        print(f"  ✓ Total processed: {overall_processed}")
        print(f"  ⊘ Total skipped:   {overall_skipped}")
        print(f"  ✗ Total failed:    {overall_failed}")
        print(f"  📁 Output folder:  {output_path.absolute()}")
        print(f"{'='*80}")


def main():
    """Main entry point."""
    # Get the root folder from user or use current directory
    root_folder = input("Enter the root folder path (or press Enter for current directory): ").strip()
    if not root_folder:
        root_folder = "."
    
    if not os.path.exists(root_folder):
        print(f"Error: Folder '{root_folder}' does not exist!")
        return
    
    # Check for config file
    config_file = "config.txt"
    if os.path.exists(config_file):
        use_config = input(f"\nFound '{config_file}'. Use it to filter files? (y/n, default=y): ").strip().lower()
        if use_config in ['n', 'no']:
            config_file = None
    else:
        print(f"\nℹ️  No '{config_file}' found. Processing all files.")
        config_file = None
    
    # Initialize handlers - easily extensible by adding more handlers
    handlers = [
        ShapefileHandler(),
        CSVHandler(),
        TXTHandler(),
        # Add more handlers here as needed:
        # GeoJSONHandler(),
        # ExcelHandler(),
    ]
    
    # Create extractor and process files
    extractor = AttributeExtractor(root_folder, handlers, config_file=config_file)
    extractor.extract_and_save(output_folder="output")
    
    print("\n✓ Extraction complete!")
    print("Check the 'output' folder for results.")


if __name__ == "__main__":
    main()
