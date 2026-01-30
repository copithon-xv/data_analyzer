"""
Extract attributes from various file types (shapefiles, CSV, etc.) with sample data.
Extensible design for adding new file type handlers.
"""
import os
import sys
import csv
import argparse
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
            file_size = file_path.stat().st_size
            print(f"    💾 File size: {file_size / (1024**3):.2f} GB")
            
            max_rows_to_check = 10000
            rows_checked = 0
            
            best_row = None
            best_count = -1
            attributes = None
            
            # Read as text and manually parse to avoid field size limits
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                # Get headers from first line
                header_line = f.readline().strip()
                if not header_line:
                    return None
                
                # Split on comma (simple split for CSV)
                attributes = [col.strip('"').strip() for col in header_line.split(',')]
                
                # Process rows
                for line in f:
                    rows_checked += 1
                    line = line.strip()
                    
                    if not line:
                        continue
                    
                    # Simple split - this avoids csv field size limits
                    row = [val.strip('"').strip() for val in line.split(',')]
                    
                    # Skip rows with wrong column count
                    if len(row) != len(attributes):
                        continue
                    
                    # Count non-empty values
                    non_null_count = sum(1 for val in row if val)
                    
                    # Perfect row found
                    if non_null_count == len(attributes):
                        row_dict = {attributes[i]: row[i] for i in range(len(attributes))}
                        return attributes, row_dict
                    
                    # Track best row
                    if non_null_count > best_count:
                        best_count = non_null_count
                        best_row = row
                    
                    # Progress indicator
                    if rows_checked % 5000 == 0:
                        print(f"    🔍 Checked {rows_checked} rows...", end='\r')
                    
                    # Stop after checking enough rows
                    if rows_checked >= max_rows_to_check:
                        print(f"    ✓ Checked {rows_checked} rows" + " " * 20)
                        break
            
            if best_row is not None:
                row_dict = {attributes[i]: best_row[i] if i < len(best_row) else '' for i in range(len(attributes))}
                return attributes, row_dict
            
            return None
            
        except Exception as e:
            print(f"    ⚠️  Error reading CSV: {e}")
            return None


# To extend: Add more handlers here (e.g., GeoJSONHandler, ExcelHandler, etc.)


class TXTHandler(FileHandler):
    """Handler for TXT files that contain CSV-like data."""
    
    def can_handle(self, file_path: Path) -> bool:
        return file_path.suffix.lower() == '.txt'
    
    def extract_data(self, file_path: Path) -> Optional[Tuple[List[str], Dict[str, Any]]]:
        try:
            file_size = file_path.stat().st_size
            print(f"    💾 File size: {file_size / (1024**3):.2f} GB")
            
            # Detect delimiter
            delimiter = self._detect_delimiter(file_path)
            print(f"    🔍 Detected delimiter: 'tab'" if delimiter == '\t' else f"    🔍 Detected delimiter: '{delimiter}'")
            
            max_rows_to_check = 10000
            rows_checked = 0
            
            best_row = None
            best_count = -1
            attributes = None
            
            # Read as text and manually parse to avoid field size limits
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                # Get headers from first line
                header_line = f.readline().strip()
                if not header_line:
                    return None
                
                # Split on detected delimiter
                attributes = [col.strip('"').strip() for col in header_line.split(delimiter)]
                
                # Process rows
                for line in f:
                    rows_checked += 1
                    line = line.strip()
                    
                    if not line:
                        continue
                    
                    # Simple split - this avoids csv field size limits
                    row = [val.strip('"').strip() for val in line.split(delimiter)]
                    
                    # Skip rows with wrong column count
                    if len(row) != len(attributes):
                        continue
                    
                    # Count non-empty values
                    non_null_count = sum(1 for val in row if val)
                    
                    # Perfect row found
                    if non_null_count == len(attributes):
                        row_dict = {attributes[i]: row[i] for i in range(len(attributes))}
                        return attributes, row_dict
                    
                    # Track best row
                    if non_null_count > best_count:
                        best_count = non_null_count
                        best_row = row
                    
                    # Progress indicator
                    if rows_checked % 5000 == 0:
                        print(f"    🔍 Checked {rows_checked} rows...", end='\r')
                    
                    # Stop after checking enough rows
                    if rows_checked >= max_rows_to_check:
                        print(f"    ✓ Checked {rows_checked} rows" + " " * 20)
                        break
            
            if best_row is not None:
                row_dict = {attributes[i]: best_row[i] if i < len(best_row) else '' for i in range(len(attributes))}
                return attributes, row_dict
            
            return None
            
        except Exception as e:
            print(f"    ⚠️  Error reading TXT file: {e}")
            return None
    
    def _detect_delimiter(self, file_path: Path) -> str:
        """Detect the delimiter used in the file."""
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                first_line = f.readline()
                
                # Count potential delimiters
                delimiters = [',', '\t', '|', ';']
                counts = {delim: first_line.count(delim) for delim in delimiters}
                
                # Return delimiter with highest count (if > 0)
                best_delim = max(counts, key=counts.get)
                if counts[best_delim] > 0:
                    return best_delim
                
                return ','  # Default to comma
        except:
            return ','


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
        
        if self.attribute_filters:
            print(f"Searching for .shp, .csv, .txt files matching config filters...")
        else:
            print("Searching for all .shp, .csv, and .txt files...")
        
        files_by_handler = {handler.__class__.__name__: [] for handler in self.handlers}
        
        # Search for specific extensions only (much faster than checking every file)
        extensions = ['*.shp', '*.csv', '*.txt']
        total_found = 0
        total_matched = 0
        
        for ext in extensions:
            print(f"  Looking for {ext} files...", end='\r')
            for file_path in self.root_folder.rglob(ext):
                if file_path.is_file():
                    total_found += 1
                    
                    # Check config filter immediately (if filters exist)
                    if self.attribute_filters and not self._matches_filters(file_path.name):
                        continue  # Skip files that don't match config
                    
                    total_matched += 1
                    
                    # Match with appropriate handler
                    for handler in self.handlers:
                        if handler.can_handle(file_path):
                            files_by_handler[handler.__class__.__name__].append(file_path)
                            break
        
        if self.attribute_filters:
            print(f"  ✓ Scan complete: {total_matched} matched files (from {total_found} total)" + " " * 30)
        else:
            print(f"  ✓ Scan complete: {total_found} files found" + " " * 30)
        
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


def process_single_file(file_path: str, handlers: List[FileHandler], output_folder: str = "output"):
    """Process a single file directly."""
    file_path = Path(file_path)
    
    if not file_path.exists():
        print(f"❌ Error: File '{file_path}' does not exist!")
        return
    
    if not file_path.is_file():
        print(f"❌ Error: '{file_path}' is not a file!")
        return
    
    # Find appropriate handler
    handler = None
    for h in handlers:
        if h.can_handle(file_path):
            handler = h
            break
    
    if handler is None:
        print(f"❌ Error: No handler available for file type '{file_path.suffix}'")
        print("Supported types: .shp, .csv, .txt")
        return
    
    print(f"\n{'='*80}")
    print(f"SINGLE FILE PROCESSING")
    print(f"{'='*80}")
    print(f"File: {file_path.name}")
    print(f"Path: {file_path.absolute()}")
    print(f"Type: {handler.__class__.__name__}")
    print(f"{'='*80}\n")
    
    # Extract data
    print("🔍 Extracting attributes...")
    result = handler.extract_data(file_path)
    
    if result is None:
        print("❌ FAILED: No valid data found in file")
        return
    
    attributes, sample_data = result
    
    # Check data completeness
    null_count = sum(1 for v in sample_data.values() if pd.isna(v))
    completeness_pct = ((len(attributes) - null_count) / len(attributes)) * 100 if attributes else 0
    
    print(f"✓ SUCCESS")
    print(f"  📋 Attributes: {len(attributes)}")
    print(f"  📊 Completeness: {completeness_pct:.1f}% ({len(attributes) - null_count}/{len(attributes)} fields)")
    
    # Create output
    output_path = Path(output_folder)
    output_path.mkdir(exist_ok=True)
    
    file_stem = file_path.stem
    output_file = output_path / f"{file_stem}_attributes.txt"
    
    # Write to file
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(f"[FILE_START]\n")
        f.write(f"Filename: {file_path.name}\n")
        f.write(f"Path: {file_path.absolute()}\n")
        f.write(f"Type: {handler.__class__.__name__}\n")
        f.write(f"Completeness: {completeness_pct:.1f}%\n\n")
        
        f.write("[ATTRIBUTES_START]\n")
        for attr in attributes:
            f.write(f"{attr}\n")
        f.write("[ATTRIBUTES_END]\n\n")
        
        f.write("[SAMPLE_DATA_START]\n")
        for attr in attributes:
            value = sample_data.get(attr, 'N/A')
            value_str = str(value)
            f.write(f"{attr}|||{value_str}\n")
        f.write("[SAMPLE_DATA_END]\n")
        f.write("[FILE_END]\n")
    
    print(f"\n💾 Output saved to: {output_file.absolute()}")
    print(f"\n{'='*80}")


def main():
    """Main entry point."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Extract attributes from shapefiles, CSV, and TXT files.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process all files in a directory
  python extract_attributes.py
  
  # Process a single specific file
  python extract_attributes.py --file data/boundary.shp
  python extract_attributes.py -f data/population.csv
        """
    )
    parser.add_argument('-f', '--file', type=str, help='Process a single file instead of directory scan')
    parser.add_argument('-o', '--output', type=str, default='output', help='Output folder (default: output)')
    
    args = parser.parse_args()
    
    # Initialize handlers
    handlers = [
        ShapefileHandler(),
        CSVHandler(),
        TXTHandler(),
    ]
    
    # If single file mode
    if args.file:
        process_single_file(args.file, handlers, args.output)
        return
    
    # Directory mode (original behavior)
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
    
    # Create extractor and process files
    extractor = AttributeExtractor(root_folder, handlers, config_file=config_file)
    extractor.extract_and_save(output_folder=args.output)
    
    print("\n✓ Extraction complete!")
    print("Check the 'output' folder for results.")


if __name__ == "__main__":
    main()
