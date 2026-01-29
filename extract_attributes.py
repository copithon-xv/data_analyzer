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
            # Read only first 10,000 rows for efficiency
            df = pd.read_csv(file_path, nrows=10000)
            
            if df.empty:
                return None
            
            # Get column names (attributes)
            attributes = list(df.columns)
            
            best_row = None
            best_count = -1
            
            # Find first row where all attributes have data (no nulls)
            # Or track the row with most non-null values
            for idx, row in df.iterrows():
                null_count = row.isnull().sum()
                non_null_count = len(row) - null_count
                
                if null_count == 0:
                    # Found perfect row with all data
                    return attributes, row.to_dict()
                
                # Track best row (most complete)
                if non_null_count > best_count:
                    best_count = non_null_count
                    best_row = row
            
            # Use the best row found (most complete data)
            if best_row is not None:
                return attributes, best_row.to_dict()
            
            return None
            
        except Exception as e:
            print(f"Error reading CSV {file_path}: {e}")
            return None


# To extend: Add more handlers here (e.g., GeoJSONHandler, ExcelHandler, etc.)


class AttributeExtractor:
    """Main class for extracting attributes from files."""
    
    def __init__(self, root_folder: str, handlers: List[FileHandler]):
        self.root_folder = Path(root_folder)
        self.handlers = handlers
        self.processed_filenames = set()
    
    def find_files(self) -> Dict[str, List[Path]]:
        """Find all supported files in the root folder and subfolders."""
        files_by_handler = {handler.__class__.__name__: [] for handler in self.handlers}
        
        for file_path in self.root_folder.rglob('*'):
            if file_path.is_file():
                for handler in self.handlers:
                    if handler.can_handle(file_path):
                        files_by_handler[handler.__class__.__name__].append(file_path)
                        break
        
        return files_by_handler
    
    def extract_and_save(self, output_folder: str = "."):
        """Extract attributes and save to text files."""
        output_path = Path(output_folder)
        output_path.mkdir(exist_ok=True)
        
        files_by_handler = self.find_files()
        
        # Process each handler type separately
        for handler in self.handlers:
            handler_name = handler.__class__.__name__
            file_paths = files_by_handler.get(handler_name, [])
            
            if not file_paths:
                print(f"No files found for {handler_name}")
                continue
            
            # Determine output filename based on handler
            if isinstance(handler, ShapefileHandler):
                output_file = output_path / "shapefile_attributes.txt"
            elif isinstance(handler, CSVHandler):
                output_file = output_path / "csv_attributes.txt"
            else:
                # Generic name for other handlers
                output_file = output_path / f"{handler_name.lower()}_attributes.txt"
            
            total_files = len(file_paths)
            print(f"\n{'='*80}")
            print(f"Processing {total_files} files with {handler_name}")
            print(f"{'='*80}")
            
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(f"=== {handler_name} Attribute Extraction ===\n")
                f.write(f"Total files found: {len(file_paths)}\n")
                f.write(f"Generated on: {pd.Timestamp.now()}\n")
                f.write("=" * 80 + "\n\n")
                
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
                    
                    # Skip if filename already processed
                    if filename in self.processed_filenames:
                        print(f"  ⊘ Status: SKIPPED (duplicate filename)")
                        print(f"  📊 Stats: ✓ {processed_count} | ⊘ {skipped_count + 1} | ✗ {failed_count}")
                        skipped_count += 1
                        continue
                    
                    result = handler.extract_data(file_path)
                    
                    if result is None:
                        print(f"  ✗ Status: FAILED (no data found)")
                        print(f"  📊 Stats: ✓ {processed_count} | ⊘ {skipped_count} | ✗ {failed_count + 1}")
                        failed_count += 1
                        continue
                    
                    attributes, sample_data = result
                    
                    # Check data completeness
                    null_count = sum(1 for v in sample_data.values() if pd.isna(v))
                    completeness_pct = ((len(attributes) - null_count) / len(attributes)) * 100 if attributes else 0
                    
                    # Show detailed info in console
                    print(f"  ✓ Status: SUCCESS")
                    print(f"  📋 Attributes: {len(attributes)}")
                    print(f"  📊 Completeness: {completeness_pct:.1f}% ({len(attributes) - null_count}/{len(attributes)} fields)")
                    print(f"  📈 Stats: ✓ {processed_count + 1} | ⊘ {skipped_count} | ✗ {failed_count}")
                    
                    # Write to file with simple structured format (only attributes and sample data)
                    f.write(f"[FILE_START]\n")
                    f.write(f"Filename: {filename}\n\n")
                    
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
                    f.write("=" * 80 + "\n\n")
                    
                    self.processed_filenames.add(filename)
                    processed_count += 1
                
                # Write summary
                f.write("\n" + "=" * 80 + "\n")
                f.write("SUMMARY\n")
                f.write("=" * 80 + "\n")
                f.write(f"Total files found: {len(file_paths)}\n")
                f.write(f"Successfully processed: {processed_count}\n")
                f.write(f"Skipped (duplicates): {skipped_count}\n")
                f.write(f"Failed (no valid sample): {failed_count}\n")
            
            print(f"\n{'='*80}")
            print(f"SUMMARY - {handler_name}")
            print(f"{'='*80}")
            print(f"  Total files found:       {total_files}")
            print(f"  ✓ Successfully processed: {processed_count}")
            print(f"  ⊘ Skipped (duplicates):   {skipped_count}")
            print(f"  ✗ Failed (no valid data): {failed_count}")
            print(f"  📄 Output saved to:       {output_file}")
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
    
    # Initialize handlers - easily extensible by adding more handlers
    handlers = [
        ShapefileHandler(),
        CSVHandler(),
        # Add more handlers here as needed:
        # GeoJSONHandler(),
        # ExcelHandler(),
    ]
    
    # Create extractor and process files
    extractor = AttributeExtractor(root_folder, handlers)
    extractor.extract_and_save(output_folder="output")
    
    print("\n✓ Extraction complete!")
    print("Check the 'output' folder for results.")


if __name__ == "__main__":
    main()
