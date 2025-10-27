"""
Schema Inspector for S3 Parquet Files
Compare historical and incremental data schemas with smart matching
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Add project root
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

# Import config
sys.path.append(str(Path(__file__).parent.parent / 'etl'))
from config_loader import ETLConfig

# Import polars
import polars as pl


# Column mapping rules
COLUMN_MAPPINGS = {
    # Incremental → Historical
    'SIC': 'sic',  # Case normalization
    'section_item': 'section_name',  # Special case: section_item becomes section_name
}

# Derived columns (can be computed, not critical for merge)
DERIVED_COLUMNS = {
    'cik_int',  # Derived from: CAST(cik AS INT)
    'has_comparison',  # Derived from: sentence text analysis
    'has_numbers',  # Derived from: sentence text analysis
    'likely_kpi',  # Derived from: sentence text analysis
    'row_hash',  # Derived from: MD5(sentenceID || sentence)
    'tickers',  # Derived from: company lookup/mapping
    'sentence_index',  # Derived from: ROW_NUMBER() or ordering
}


def normalize_column_name(col):
    """Normalize column name: trim whitespace, uppercase"""
    return col.strip().upper()


def apply_mapping(col, mappings):
    """Apply column mapping rules"""
    return mappings.get(col, col)


def inspect_schemas():
    """Load and display schemas with smart comparison"""
    
    # Load config
    config = ETLConfig()
    
    print("=" * 100)
    print("SCHEMA INSPECTOR - Historical vs Incremental (COMPARISON)")
    print("=" * 100)
    
    # Get S3 URIs
    hist_uri = config.s3_uri(config.hist_path)
    incr_uri = config.s3_uri(config.incr_path)
    
    print(f"\n📁 Historical: {config.hist_path}")
    print(f"📁 Incremental: {config.incr_path}")
    
    # Load AWS credentials
    secrets_path = Path(__file__).parent.parent / '.aws_secrets' / 'aws_credentials.env'
    load_dotenv(secrets_path)
    
    storage_options = {
        'aws_region': os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
    }
    
    # Read schemas
    print("\n⏳ Reading schemas...")
    hist_df = pl.read_parquet(hist_uri, n_rows=1, storage_options=storage_options)
    incr_df = pl.read_parquet(incr_uri, n_rows=1, storage_options=storage_options)
    
    hist_schema = hist_df.schema
    incr_schema = incr_df.schema
    
    print(f"✓ Historical: {len(hist_schema)} columns")
    print(f"✓ Incremental: {len(incr_schema)} columns")
    
    # Create normalized mapping
    hist_normalized = {normalize_column_name(col): (col, dtype) for col, dtype in hist_schema.items()}
    incr_normalized = {normalize_column_name(col): (col, dtype) for col, dtype in incr_schema.items()}
    
    # Apply column mappings to incremental
    incr_mapped = {}
    for col, dtype in incr_schema.items():
        mapped_col = apply_mapping(col, COLUMN_MAPPINGS)
        incr_mapped[mapped_col] = (col, dtype)  # Store original name and type
    
    # Get all unique columns (after mapping)
    all_cols = sorted(set(hist_schema.keys()) | set(incr_mapped.keys()))
    
    print("\n" + "=" * 100)
    print("COLUMN MAPPING RULES APPLIED")
    print("=" * 100)
    for incr_col, hist_col in COLUMN_MAPPINGS.items():
        if incr_col in incr_schema:
            print(f"  {incr_col:20s} → {hist_col:20s} (Mapped)")
    
    print("\n" + "=" * 100)
    print("COLUMN COMPARISON (Side-by-Side with Smart Matching)")
    print("=" * 100)
    print(f"{'Column Name':<30} | {'Historical Type':<35} | {'Incremental Type':<35} | {'Status':<15}")
    print("-" * 100)
    
    matches = []
    hist_only = []
    incr_only = []
    type_diffs = []
    
    for col in all_cols:
        hist_type = str(hist_schema.get(col, "MISSING"))
        
        # Check if incremental has this column (after mapping)
        incr_orig_col, incr_type_val = incr_mapped.get(col, (None, None))
        incr_type = str(incr_type_val) if incr_type_val else "MISSING"
        
        # Normalize datetime types for comparison (ignore microsecond vs nanosecond)
        hist_type_norm = hist_type.replace('time_unit=\'us\'', 'TIME_UNIT').replace('time_unit=\'ns\'', 'TIME_UNIT')
        incr_type_norm = incr_type.replace('time_unit=\'us\'', 'TIME_UNIT').replace('time_unit=\'ns\'', 'TIME_UNIT')
        
        # Ignore timezone differences for datetime
        hist_type_norm = hist_type_norm.replace('time_zone=\'UTC\'', 'TZ').replace('time_zone=None', 'TZ')
        incr_type_norm = incr_type_norm.replace('time_zone=\'UTC\'', 'TZ').replace('time_zone=None', 'TZ')
        
        # Determine status
        if col not in hist_schema:
            if col in DERIVED_COLUMNS:
                status = "ℹ️ Derived"
                incr_only.append(col)
            else:
                status = "⚠️ Incr Only"
                incr_only.append(col)
        elif incr_orig_col is None:
            if col in DERIVED_COLUMNS:
                status = "ℹ️ Derived"
                hist_only.append(col)
            else:
                status = "⚠️ Hist Only"
                hist_only.append(col)
        elif hist_type_norm == incr_type_norm:
            status = "✅ Match"
            matches.append(col)
        else:
            # Check if it's just datetime precision difference
            if 'Datetime' in hist_type and 'Datetime' in incr_type:
                status = "⚠️ DT Precision"
                type_diffs.append(col)
            else:
                status = "❌ Type Diff"
                type_diffs.append(col)
        
        # Show mapped name if different
        display_col = col
        if incr_orig_col and incr_orig_col != col:
            display_col = f"{col} ({incr_orig_col}→)"
        
        print(f"{display_col:<30} | {hist_type:<35} | {incr_type:<35} | {status:<15}")
    
    # Summary
    print("\n" + "=" * 100)
    print("SUMMARY")
    print("=" * 100)
    
    print(f"\n📊 Column Counts:")
    print(f"  Matching columns: {len(matches)}")
    print(f"  Historical only: {len([c for c in hist_only if c not in DERIVED_COLUMNS])}")
    print(f"  Incremental only: {len([c for c in incr_only if c not in DERIVED_COLUMNS])}")
    print(f"  Derived (OK to be missing): {len([c for c in (hist_only + incr_only) if c in DERIVED_COLUMNS])}")
    print(f"  Type differences: {len(type_diffs)}")
    
    # Critical issues
    critical_hist_only = [c for c in hist_only if c not in DERIVED_COLUMNS]
    critical_incr_only = [c for c in incr_only if c not in DERIVED_COLUMNS]
    
    if critical_hist_only:
        print(f"\n⚠️  Critical: Columns only in Historical (not derived):")
        for col in sorted(critical_hist_only):
            print(f"  - {col:<30} ({hist_schema[col]})")
    
    if critical_incr_only:
        print(f"\n⚠️  Critical: Columns only in Incremental (not derived):")
        for col in sorted(critical_incr_only):
            orig_col, dtype = incr_mapped.get(col, (col, None))
            print(f"  - {col:<30} ({dtype})")
    
    # Derived columns info
    derived_present = [c for c in (hist_only + incr_only) if c in DERIVED_COLUMNS]
    if derived_present:
        print(f"\nℹ️  Derived Columns (can be computed during merge):")
        for col in sorted(derived_present):
            if col == 'cik_int':
                print(f"  - {col:<30} → CAST(cik AS INT)")
            elif col == 'row_hash':
                print(f"  - {col:<30} → MD5(sentenceID || sentence)")
            elif col == 'tickers':
                print(f"  - {col:<30} → Company lookup/mapping")
            elif col in ['has_comparison', 'has_numbers', 'likely_kpi']:
                print(f"  - {col:<30} → Text analysis (regex/NLP)")
            elif col == 'sentence_index':
                print(f"  - {col:<30} → ROW_NUMBER() or position")
    
    # Datetime precision issues
    dt_precision_issues = [c for c in type_diffs if 'Datetime' in str(hist_schema.get(c, ''))]
    if dt_precision_issues:
        print(f"\n⚠️  Datetime Precision Differences (can be normalized):")
        for col in sorted(dt_precision_issues):
            print(f"  - {col:<30}")
            print(f"      Historical: {hist_schema[col]}")
            orig_col, incr_type_val = incr_mapped.get(col, (None, None))
            if incr_type_val:
                print(f"      Incremental: {incr_type_val}")
    
    # Final verdict
    print("\n" + "=" * 100)
    print("MERGE COMPATIBILITY ASSESSMENT")
    print("=" * 100)
    
    critical_issues = len(critical_hist_only) + len(critical_incr_only) + len([c for c in type_diffs if 'Datetime' not in str(hist_schema.get(c, ''))])
    
    if critical_issues == 0:
        print("\n✅ SCHEMAS ARE COMPATIBLE FOR MERGE!")
        print("   - All critical columns present")
        print("   - Derived columns can be computed")
        print("   - Datetime precision differences can be normalized")
        print("\n🚀 Ready to proceed with merge pipeline!")
    else:
        print(f"\n⚠️  {critical_issues} CRITICAL ISSUE(S) NEED ATTENTION")
        print("   Review columns marked as 'Critical' above")
    
    print("=" * 100)


if __name__ == "__main__":
    try:
        inspect_schemas()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)