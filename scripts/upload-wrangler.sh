#!/bin/bash
# Upload index files to R2 using wrangler

BUCKET="parquedb"
LOCAL_DIR="data-v3"
DATASET="${1:-imdb-1m}"

echo "Uploading indexes for $DATASET to R2 bucket: $BUCKET"

# Find all index files (excluding .parquet data files which are already uploaded)
find "$LOCAL_DIR/$DATASET" -type f \( -name "*.idx" -o -name "*.json" -o -name "*.bin" \) | while read file; do
  # Get relative path from data-v3
  r2_path="${file#$LOCAL_DIR/}"

  echo "Uploading: $r2_path"
  npx wrangler r2 object put "$BUCKET/$r2_path" --file="$file" --content-type="application/octet-stream" 2>/dev/null
done

echo "Done!"
