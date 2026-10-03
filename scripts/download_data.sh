#!/usr/bin/env bash
# Downloads one month of NYC TLC yellow taxi trips (about 50 MB, ~3M rows).
set -euo pipefail
MONTH="${1:-2024-01}"
DEST="$(cd "$(dirname "$0")/.." && pwd)/data"
FILE="yellow_tripdata_${MONTH}.parquet"
mkdir -p "$DEST"
if [[ -f "$DEST/$FILE" ]]; then
  echo "$FILE already present"
  exit 0
fi
curl -fL --retry 3 -o "$DEST/$FILE.part" "https://d37ci6vzurychx.cloudfront.net/trip-data/$FILE"
mv "$DEST/$FILE.part" "$DEST/$FILE"
echo "saved $DEST/$FILE"
