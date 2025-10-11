
from pathlib import Path
import shutil
from datasets import load_dataset
import polars as pl
from dotenv import load_dotenv
import os

# Setup paths
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
CACHE_DIR = DATA_DIR / "hf_cache"
EXPORT_DIR = DATA_DIR / "exports"
TEMP_DIR = DATA_DIR / "temp_large_download"

# Ensure directories exist
for dir_path in [DATA_DIR, CACHE_DIR, EXPORT_DIR, TEMP_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

# Load environment variables
load_dotenv(PROJECT_ROOT / "assets" / "config.env")

def download_and_convert_large_full():
    """
    Download large_full config from HuggingFace, convert to Parquet, cleanup.
    """
    
    HF_DATASET = os.getenv("HF_DATASET_NAME", "JanosAudran/financial-reports-sec")
    CONFIG_NAME = "large_full"  # Hardcoded for this task
    SPLIT = "train"
    
    print(f"Starting download: {HF_DATASET} ({CONFIG_NAME})")
    print(f"This may take 10-30 minutes depending on size...\n")
    
    # Step 1: Download dataset (will cache in TEMP_DIR to control cleanup)
    print("Step 1/4: Downloading from HuggingFace...")
    os.environ["HF_DATASETS_CACHE"] = str(TEMP_DIR)
    
    try:
        ds = load_dataset(
            HF_DATASET,
            CONFIG_NAME,
            split=SPLIT,
            streaming=False,  # Must be False to convert to Parquet efficiently
            trust_remote_code=True,
            cache_dir=TEMP_DIR
        )
        print(f"✓ Downloaded: {len(ds):,} rows")
    except Exception as e:
        print(f"✗ Download failed: {e}")
        return None
    
    # Step 2: Quick metadata check
    print("\nStep 2/4: Checking metadata...")
    df_pl = pl.from_arrow(ds.data.table)
    
    meta = {
        "rows": len(df_pl),
        "columns": df_pl.shape[1],
        "size_mb": df_pl.estimated_size('mb'),
        "companies": df_pl.select(pl.n_unique("cik")).item(),
        "sections": df_pl.select(pl.n_unique("section")).item(),
        "date_range": (
            df_pl.select(pl.col("reportDate").min()).item(),
            df_pl.select(pl.col("reportDate").max()).item()
        )
    }
    
    print(f"  Rows: {meta['rows']:,}")
    print(f"  Companies: {meta['companies']}")
    print(f"  Sections: {meta['sections']}")
    print(f"  Date range: {meta['date_range'][0]} to {meta['date_range'][1]}")
    print(f"  Memory: {meta['size_mb']:.1f} MB")
    
    # Step 3: Save to Parquet
    print("\nStep 3/4: Converting to Parquet...")
    output_path = EXPORT_DIR / f"sec_filings_{CONFIG_NAME}.parquet"
    
    df_pl.write_parquet(output_path, compression="snappy")
    
    output_size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"✓ Saved to: {output_path}")
    print(f"  File size: {output_size_mb:.1f} MB")
    
    # Step 4: Cleanup temp files
    print("\nStep 4/4: Cleaning up temporary files...")
    try:
        shutil.rmtree(TEMP_DIR)
        print(f"✓ Deleted temp cache: {TEMP_DIR}")
    except Exception as e:
        print(f"⚠ Cleanup warning: {e}")
        print(f"  You may manually delete: {TEMP_DIR}")
    
    print("\n" + "="*60)
    print("COMPLETE")
    print("="*60)
    print(f"Parquet file: {output_path}")
    print(f"Final size: {output_size_mb:.1f} MB")
    print(f"\nTo load in notebook:")
    print(f'  df = pl.read_parquet("{output_path.relative_to(PROJECT_ROOT)}")')
    
    return output_path, meta


if __name__ == "__main__":
    result = download_and_convert_large_full()
    if result:
        print("\n✓ Success!")
    else:
        print("\n✗ Failed - check errors above")