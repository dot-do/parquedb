#!/bin/bash
# Upload index files to R2 using wrangler
# Uploads to benchmark-data/ prefix to match where benchmarks expect data

BUCKET="parquedb"
LOCAL_DIR="data-v3"
DATASET="${1:-imdb-1m}"
# Upload to benchmark-data/ prefix to match benchmark expectations
R2_PREFIX="benchmark-data"

echo "Uploading indexes for $DATASET to R2 bucket: $BUCKET (prefix: $R2_PREFIX)"

# Find all index files (excluding .parquet data files which are already uploaded)
find "$LOCAL_DIR/$DATASET" -type f \( -name "*.idx" -o -name "*.json" -o -name "*.bin" \) | while read file; do
  # Get relative path from data-v3 and prepend benchmark-data prefix
  local_rel="${file#$LOCAL_DIR/}"
  r2_path="$R2_PREFIX/$local_rel"

  echo "Uploading: $r2_path"
  npx wrangler r2 object put "$BUCKET/$r2_path" --file="$file" --content-type="application/octet-stream" 2>/dev/null
done

echo "Done!"
