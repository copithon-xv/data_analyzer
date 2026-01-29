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
            
            # Get column names (attributes)
            attributes = list(gdf.columns)
            
            # Find first row where all attributes have data (no nulls)
            for idx, row in gdf.iterrows():
                if not row.isnull().any():
                    sample_data = row.to_dict()
                    # Convert geometry to WKT string for better readability
                    if 'geometry' in sample_data:
                        sample_data['geometry'] = str(sample_data['geometry'])
                    return attributes, sample_data
            
            return None  # No row with all non-null values found
            
        except Exception as e:
            print(f"Error reading shapefile {file_path}: {e}")
            return None


class CSVHandler(FileHandler):
    """Handler for CSV file extraction."""
    
    def can_handle(self, file_path: Path) -> bool:
        return file_path.suffix.lower() == '.csv'
    
    def extract_data(self, file_path: Path) -> Optional[Tuple[List[str], Dict[str, Any]]]:
        try:
            df = pd.read_csv(file_path)
            
            if df.empty:
                return None
            
            # Get column names (attributes)
            attributes = list(df.columns)
            
            # Find first row where all attributes have data (no nulls)
            for idx, row in df.iterrows():
                if not row.isnull().any():
                    sample_data = row.to_dict()
                    return attributes, sample_data
            
            return None  # No row with all non-null values found
            
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
            
            print(f"\nProcessing {len(file_paths)} files with {handler_name}...")
            
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(f"=== {handler_name} Attribute Extraction ===\n")
                f.write(f"Total files found: {len(file_paths)}\n")
                f.write(f"Generated on: {pd.Timestamp.now()}\n")
                f.write("=" * 80 + "\n\n")
                
                processed_count = 0
                skipped_count = 0
                failed_count = 0
                
                for file_path in sorted(file_paths):
                    filename = file_path.name
                    
                    # Skip if filename already processed
                    if filename in self.processed_filenames:
                        print(f"  Skipped (duplicate): {filename}")
                        skipped_count += 1
                        continue
                    
                    result = handler.extract_data(file_path)
                    
                    if result is None:
                        print(f"  Failed (no valid sample): {filename}")
                        f.write(f"File: {filename}\n")
                        f.write(f"Path: {file_path}\n")
                        f.write(f"Status: No row with all non-null values found\n")
                        f.write("-" * 80 + "\n\n")
                        failed_count += 1
                        continue
                    
                    attributes, sample_data = result
                    
                    # Write to file
                    f.write(f"File: {filename}\n")
                    f.write(f"Path: {file_path}\n")
                    f.write(f"Number of Attributes: {len(attributes)}\n\n")
                    
                    f.write("Attributes:\n")
                    for i, attr in enumerate(attributes, 1):
                        f.write(f"  {i}. {attr}\n")
                    
                    f.write("\nSample Data:\n")
                    for attr in attributes:
                        value = sample_data.get(attr, 'N/A')
                        # Truncate long values
                        value_str = str(value)
                        if len(value_str) > 100:
                            value_str = value_str[:97] + "..."
                        f.write(f"  {attr}: {value_str}\n")
                    
                    f.write("-" * 80 + "\n\n")
                    
                    self.processed_filenames.add(filename)
                    processed_count += 1
                    print(f"  Processed: {filename}")
                
                # Write summary
                f.write("\n" + "=" * 80 + "\n")
                f.write("SUMMARY\n")
                f.write("=" * 80 + "\n")
                f.write(f"Total files found: {len(file_paths)}\n")
                f.write(f"Successfully processed: {processed_count}\n")
                f.write(f"Skipped (duplicates): {skipped_count}\n")
                f.write(f"Failed (no valid sample): {failed_count}\n")
            
            print(f"\nResults saved to: {output_file}")
            print(f"  Processed: {processed_count}, Skipped: {skipped_count}, Failed: {failed_count}")


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
