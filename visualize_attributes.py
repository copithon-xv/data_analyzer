"""
Visualize attribute relationships and overlaps from extracted data.
Creates UpSet plots and summary statistics for attribute analysis.
"""
import os
from pathlib import Path
from typing import Dict, List, Set
from collections import defaultdict
import matplotlib.pyplot as plt
from upsetplot import from_contents, UpSet
import pandas as pd


def parse_output_file(file_path: Path) -> Dict[str, Set[str]]:
    """
    Parse the structured output file and extract attributes per file.
    
    Returns:
        Dictionary mapping filename to set of attributes
    """
    file_attributes = {}
    current_filename = None
    in_attributes_section = False
    current_attributes = []
    
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            
            if line.startswith('Filename:'):
                current_filename = line.replace('Filename:', '').strip()
                current_attributes = []
                
            elif line == '[ATTRIBUTES_START]':
                in_attributes_section = True
                
            elif line == '[ATTRIBUTES_END]':
                in_attributes_section = False
                if current_filename:
                    file_attributes[current_filename] = set(current_attributes)
                
            elif in_attributes_section and line:
                current_attributes.append(line)
    
    return file_attributes


def analyze_attributes(file_attributes: Dict[str, Set[str]]) -> Dict:
    """Analyze attribute overlap and generate statistics."""
    all_attributes = set()
    for attrs in file_attributes.values():
        all_attributes.update(attrs)
    
    # Count how many files have each attribute
    attribute_counts = defaultdict(int)
    for attrs in file_attributes.values():
        for attr in attrs:
            attribute_counts[attr] += 1
    
    # Find common attributes (in all files)
    common_attributes = set(all_attributes)
    for attrs in file_attributes.values():
        common_attributes &= attrs
    
    # Find unique attributes (in only one file)
    unique_attributes = {attr: [] for attr in all_attributes}
    for filename, attrs in file_attributes.items():
        for attr in attrs:
            if attribute_counts[attr] == 1:
                unique_attributes[attr].append(filename)
    
    unique_attributes = {k: v for k, v in unique_attributes.items() if v}
    
    return {
        'total_files': len(file_attributes),
        'total_unique_attributes': len(all_attributes),
        'common_attributes': common_attributes,
        'unique_attributes': unique_attributes,
        'attribute_counts': attribute_counts
    }


def create_upset_plot(file_attributes: Dict[str, Set[str]], output_path: Path, max_sets: int = 20):
    """Create an UpSet plot showing attribute overlaps."""
    
    # Limit to max_sets files for readability
    if len(file_attributes) > max_sets:
        print(f"⚠️  Warning: {len(file_attributes)} files found. Showing top {max_sets} with most attributes.")
        sorted_files = sorted(file_attributes.items(), key=lambda x: len(x[1]), reverse=True)
        file_attributes = dict(sorted_files[:max_sets])
    
    # Create UpSet data structure
    # Map each attribute to the set of files that contain it
    attribute_to_files = defaultdict(list)
    for filename, attrs in file_attributes.items():
        for attr in attrs:
            attribute_to_files[attr].append(filename)
    
    # Create data for UpSet plot
    data = from_contents({filename: attrs for filename, attrs in file_attributes.items()})
    
    # Create the plot
    fig = plt.figure(figsize=(16, 10))
    upset = UpSet(data, 
                  subset_size='count',
                  intersection_plot_elements=10,
                  show_counts=True,
                  sort_by='cardinality',
                  element_size=40)
    upset.plot(fig=fig)
    
    plt.suptitle('Attribute Overlap Analysis Across Files', 
                fontsize=16, fontweight='bold', y=0.98)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"  ✓ UpSet plot saved to: {output_path}")
    
    return fig


def create_frequency_chart(attribute_counts: Dict[str, int], output_path: Path, top_n: int = 30):
    """Create a bar chart showing most common attributes."""
    
    # Get top N attributes
    sorted_attrs = sorted(attribute_counts.items(), key=lambda x: x[1], reverse=True)[:top_n]
    attrs, counts = zip(*sorted_attrs) if sorted_attrs else ([], [])
    
    fig, ax = plt.subplots(figsize=(12, 8))
    bars = ax.barh(range(len(attrs)), counts, color='steelblue')
    ax.set_yticks(range(len(attrs)))
    ax.set_yticklabels(attrs)
    ax.set_xlabel('Number of Files Containing Attribute', fontsize=12)
    ax.set_title(f'Top {top_n} Most Common Attributes', fontsize=14, fontweight='bold')
    ax.invert_yaxis()
    
    # Add value labels
    for i, (bar, count) in enumerate(zip(bars, counts)):
        ax.text(count + 0.1, i, str(count), va='center', fontsize=10)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"  ✓ Frequency chart saved to: {output_path}")
    
    return fig


def generate_summary_report(analysis: Dict, output_path: Path):
    """Generate a text summary report of the analysis."""
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write("=" * 80 + "\n")
        f.write("ATTRIBUTE OVERLAP ANALYSIS SUMMARY\n")
        f.write("=" * 80 + "\n\n")
        
        f.write(f"Total Files Analyzed: {analysis['total_files']}\n")
        f.write(f"Total Unique Attributes: {analysis['total_unique_attributes']}\n")
        f.write(f"Common Attributes (in ALL files): {len(analysis['common_attributes'])}\n")
        f.write(f"Unique Attributes (in ONE file only): {len(analysis['unique_attributes'])}\n\n")
        
        if analysis['common_attributes']:
            f.write("=" * 80 + "\n")
            f.write("COMMON ATTRIBUTES (Present in ALL files)\n")
            f.write("=" * 80 + "\n")
            for attr in sorted(analysis['common_attributes']):
                f.write(f"  • {attr}\n")
            f.write("\n")
        else:
            f.write("No attributes are common to all files.\n\n")
        
        if analysis['unique_attributes']:
            f.write("=" * 80 + "\n")
            f.write("UNIQUE ATTRIBUTES (Present in only ONE file)\n")
            f.write("=" * 80 + "\n")
            for attr, files in sorted(analysis['unique_attributes'].items()):
                f.write(f"  • {attr}\n")
                for filename in files:
                    f.write(f"      └─ {filename}\n")
            f.write("\n")
        
        f.write("=" * 80 + "\n")
        f.write("ATTRIBUTE FREQUENCY DISTRIBUTION\n")
        f.write("=" * 80 + "\n")
        sorted_counts = sorted(analysis['attribute_counts'].items(), 
                              key=lambda x: x[1], reverse=True)
        
        f.write(f"{'Attribute':<50} {'Files':<10} {'Percentage':<10}\n")
        f.write("-" * 80 + "\n")
        
        total_files = analysis['total_files']
        for attr, count in sorted_counts:
            percentage = (count / total_files) * 100
            attr_truncated = attr[:47] + "..." if len(attr) > 50 else attr
            f.write(f"{attr_truncated:<50} {count:<10} {percentage:.1f}%\n")
    
    print(f"  ✓ Summary report saved to: {output_path}")


def main():
    """Main entry point."""
    print("\n" + "=" * 80)
    print("ATTRIBUTE OVERLAP VISUALIZATION TOOL")
    print("=" * 80 + "\n")
    
    # Get output folder path
    output_folder = input("Enter path to output folder (or press Enter for './output'): ").strip()
    if not output_folder:
        output_folder = "output"
    
    output_path = Path(output_folder)
    
    if not output_path.exists():
        print(f"❌ Error: Folder '{output_folder}' does not exist!")
        return
    
    # Find all *_attributes.txt files
    txt_files = list(output_path.glob("*_attributes.txt"))
    
    if not txt_files:
        print(f"❌ Error: No *_attributes.txt files found in '{output_folder}'")
        return
    
    print(f"📁 Found {len(txt_files)} attribute file(s)\n")
    
    # Create visualization output folder
    viz_output = output_path / "visualizations"
    viz_output.mkdir(exist_ok=True)
    
    # Process each file type
    for txt_file in txt_files:
        file_type = txt_file.stem.replace('_attributes', '')
        print(f"\n{'='*80}")
        print(f"Processing: {txt_file.name} ({file_type.upper()})")
        print(f"{'='*80}")
        
        # Parse the file
        print("  📖 Parsing attribute data...")
        file_attributes = parse_output_file(txt_file)
        
        if not file_attributes:
            print("  ⚠️  No data found in file")
            continue
        
        print(f"  ✓ Found {len(file_attributes)} files with attributes")
        
        # Analyze
        print("  🔍 Analyzing attribute overlaps...")
        analysis = analyze_attributes(file_attributes)
        
        # Generate visualizations
        print("  📊 Generating visualizations...")
        
        # 1. UpSet plot
        try:
            upset_path = viz_output / f"{file_type}_upset_plot.png"
            create_upset_plot(file_attributes, upset_path)
        except Exception as e:
            print(f"  ⚠️  Could not create UpSet plot: {e}")
        
        # 2. Frequency chart
        try:
            freq_path = viz_output / f"{file_type}_frequency.png"
            create_frequency_chart(analysis['attribute_counts'], freq_path)
        except Exception as e:
            print(f"  ⚠️  Could not create frequency chart: {e}")
        
        # 3. Summary report
        summary_path = viz_output / f"{file_type}_summary.txt"
        generate_summary_report(analysis, summary_path)
        
        print(f"\n  📈 Summary Statistics:")
        print(f"    Total files:                {analysis['total_files']}")
        print(f"    Unique attributes:          {analysis['total_unique_attributes']}")
        print(f"    Common to all:              {len(analysis['common_attributes'])}")
        print(f"    Unique to one file:         {len(analysis['unique_attributes'])}")
    
    print("\n" + "=" * 80)
    print("✓ VISUALIZATION COMPLETE!")
    print(f"📁 Check the '{viz_output}' folder for results")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
