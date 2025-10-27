"""
S3 Connection Test - FinRAG AWS ETL Pipeline
Purpose: Verify AWS credentials and S3 bucket access
Author: Joel Markapudi

python src_aws_etl\tests\test_s3_conn.py
"""



"""
S3 Connection Test - FinRAG AWS ETL Pipeline
Purpose: Verify AWS credentials and S3 bucket access with proper directory traversal
Author: Joel Markapudi
"""

import os
import sys
from pathlib import Path
from collections import defaultdict

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

# Load environment variables
from dotenv import load_dotenv

# Load credentials from .aws_secrets
secrets_path = Path(__file__).parent.parent / '.aws_secrets' / 'aws_credentials.env'

if not secrets_path.exists():
    print("❌ ERROR: Credentials file not found!")
    print(f"   Expected location: {secrets_path.absolute()}")
    sys.exit(1)

load_dotenv(secrets_path)

# Import AWS SDK
import boto3
from botocore.exceptions import ClientError, NoCredentialsError


def list_s3_structure(s3_client, bucket_name, max_keys=1000):
    """
    List S3 bucket structure with proper folder/file organization
    Returns organized structure without duplicates
    """
    try:
        # Use paginator to handle large buckets
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, PaginationConfig={'MaxItems': max_keys})
        
        # Organize by folder structure
        structure = defaultdict(list)
        all_objects = []
        
        for page in pages:
            if 'Contents' not in page:
                continue
            
            for obj in page['Contents']:
                key = obj['Key']
                size_mb = obj['Size'] / (1024 * 1024)
                all_objects.append((key, size_mb))
                
                # Determine folder path
                if '/' in key:
                    folder = '/'.join(key.split('/')[:-1]) + '/'
                    file_name = key.split('/')[-1]
                    if file_name:  # Ignore folder markers (keys ending with /)
                        structure[folder].append((file_name, size_mb))
                else:
                    structure['[ROOT]'].append((key, size_mb))
        
        return structure, all_objects
    
    except ClientError as e:
        print(f"❌ Error listing objects: {e.response['Error']['Code']}")
        return None, None


def print_tree_structure(structure):
    """Print S3 bucket structure in a tree format"""
    print("\n📂 Bucket Structure:")
    print("=" * 70)
    
    # Sort folders for consistent output
    sorted_folders = sorted(structure.keys())
    
    for folder in sorted_folders:
        if folder == '[ROOT]':
            print("\n📁 [ROOT LEVEL]")
        else:
            # Calculate folder depth for indentation
            depth = folder.count('/')
            indent = "  " * (depth - 1)
            folder_name = folder.rstrip('/').split('/')[-1]
            print(f"\n{indent}📁 {folder}/")
        
        # List files in this folder
        files = structure[folder]
        for file_name, size_mb in sorted(files):
            file_indent = "  " * folder.count('/')
            if size_mb < 0.01:
                size_str = f"{size_mb * 1024:.2f} KB"
            else:
                size_str = f"{size_mb:.2f} MB"
            print(f"{file_indent}  📄 {file_name} ({size_str})")


def get_folder_summary(structure):
    """Get summary statistics by top-level folder"""
    summary = defaultdict(lambda: {'count': 0, 'size_mb': 0})
    
    for folder, files in structure.items():
        if folder == '[ROOT]':
            top_folder = '[ROOT]'
        else:
            top_folder = folder.split('/')[0]
        
        for _, size_mb in files:
            summary[top_folder]['count'] += 1
            summary[top_folder]['size_mb'] += size_mb
    
    return summary


def test_s3_connection():
    """Test AWS S3 connection and credentials"""
    
    print("=" * 70)
    print("FinRAG AWS ETL - S3 Connection Test")
    print("=" * 70)
    
    # Step 1: Verify credentials file
    print(f"\n📁 Credentials file: {secrets_path.name}")
    print(f"✓ Location: {secrets_path.parent.absolute()}")
    
    # Step 2: Load and verify environment variables
    print("\n📋 Step 1: Checking environment variables...")
    
    access_key = os.getenv('AWS_ACCESS_KEY_ID')
    secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
    bucket_name = os.getenv('S3_BUCKET_NAME')
    
    if not all([access_key, secret_key, bucket_name]):
        print("❌ Missing required environment variables!")
        return False
    
    print(f"✓ AWS_ACCESS_KEY_ID: {access_key[:8]}... (masked)")
    print(f"✓ AWS_SECRET_ACCESS_KEY: ****** (hidden)")
    print(f"✓ AWS_DEFAULT_REGION: {region}")
    print(f"✓ S3_BUCKET_NAME: {bucket_name}")
    
    # Step 3: Initialize S3 client
    print("\n🔌 Step 2: Initializing S3 client...")
    
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region
        )
        print(f"✓ S3 client initialized for region: {region}")
    except Exception as e:
        print(f"❌ Error initializing S3 client: {e}")
        return False
    
    # Step 4: Test connection by listing buckets
    print("\n🪣 Step 3: Testing AWS connection...")
    
    try:
        response = s3_client.list_buckets()
        buckets = [b['Name'] for b in response['Buckets']]
        print(f"✓ Successfully connected to AWS!")
        print(f"✓ Found {len(buckets)} accessible bucket(s)")
        
        if bucket_name in buckets:
            print(f"✓ Target bucket '{bucket_name}' is accessible ✅")
        else:
            print(f"⚠️  Target bucket '{bucket_name}' not found in accessible buckets")
            print(f"   Available: {', '.join(buckets)}")
            return False
            
    except ClientError as e:
        print(f"❌ Connection failed: {e.response['Error']['Code']}")
        return False
    
    # Step 5: List bucket contents with proper structure
    print(f"\n📂 Step 4: Analyzing bucket structure...")
    
    structure, all_objects = list_s3_structure(s3_client, bucket_name)
    
    if structure is None:
        return False
    
    if not all_objects:
        print("ℹ️  Bucket is empty (no objects found)")
        return True
    
    # Print statistics
    print(f"✓ Total objects found: {len(all_objects)}")
    total_size_mb = sum(size for _, size in all_objects)
    print(f"✓ Total size: {total_size_mb:.2f} MB")
    
    # Print folder summary
    summary = get_folder_summary(structure)
    print("\n📊 Folder Summary:")
    print("-" * 70)
    for folder, stats in sorted(summary.items()):
        print(f"  {folder:30s} | {stats['count']:3d} files | {stats['size_mb']:8.2f} MB")
    
    # Print detailed tree structure
    print_tree_structure(structure)
    
    # Success!
    print("\n" + "=" * 70)
    print("✅ SUCCESS! AWS S3 connection fully verified!")
    print("=" * 70)
    print("\n📋 Next Steps:")
    print("1. ✓ S3 connection working")
    print("2. ✓ Bucket structure analyzed")
    print("3. → Ready to build ETL merge pipeline")
    
    return True


if __name__ == "__main__":
    try:
        success = test_s3_connection()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n⚠️  Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

        