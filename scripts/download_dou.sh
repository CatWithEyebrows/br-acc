#!/usr/bin/env bash
# Download DOU (Diario Oficial da Uniao) gazette data from Querido Diário.
#
# The Querido Diário API (queridodiario.ok.org.br/api/gazettes) is behind
# Cloudflare bot protection and cannot be accessed via curl/wget.
#
# Options to obtain data:
#
# 1. BROWSER EXPORT: Visit the Querido Diário website in a browser,
#    search for gazettes, and use browser DevTools to capture the API
#    response JSON. Save as data/dou/gazettes_<territory>.json
#
# 2. QUERIDO DIÁRIO DATA PACKAGE: The project publishes periodic data
#    dumps. Check https://github.com/okfn-brasil/querido-diario for
#    download links.
#
# 3. MANUAL CSV: Create a CSV with columns:
#    date,territory_id,territory_name,edition,is_extra_edition,url,excerpt
#
# Expected output directory: data/dou/
# Supported file formats: *.json (QD API format) or *.csv
#
# Example QD API JSON format:
# {
#   "gazettes": [
#     {
#       "date": "2024-01-15",
#       "territory_id": "3550308",
#       "territory_name": "São Paulo",
#       "edition": "123",
#       "is_extra_edition": false,
#       "url": "https://...",
#       "excerpts": ["...text mentioning CNPJs..."]
#     }
#   ]
# }

set -euo pipefail

DATA_DIR="${1:-./data/dou}"
mkdir -p "$DATA_DIR"

echo "=== DOU Data Download ==="
echo ""
echo "The Querido Diario API is behind Cloudflare bot protection."
echo "Automated download is not possible via curl/wget."
echo ""
echo "Please obtain gazette data manually using one of these methods:"
echo ""
echo "1. Browser: Visit https://queridodiario.ok.org.br"
echo "   Search for gazettes, capture API responses from DevTools"
echo "   Save JSON files to: $DATA_DIR/"
echo ""
echo "2. Data dumps: Check https://github.com/okfn-brasil/querido-diario"
echo ""
echo "3. Create CSV files with columns:"
echo "   date,territory_id,territory_name,edition,is_extra_edition,url,excerpt"
echo "   Save to: $DATA_DIR/"
echo ""
echo "Target directory: $DATA_DIR"
echo ""

# Check if any data already exists
json_count=$(find "$DATA_DIR" -name "*.json" 2>/dev/null | wc -l | tr -d ' ')
csv_count=$(find "$DATA_DIR" -name "*.csv" 2>/dev/null | wc -l | tr -d ' ')

if [ "$json_count" -gt 0 ] || [ "$csv_count" -gt 0 ]; then
    echo "Found existing data:"
    echo "  JSON files: $json_count"
    echo "  CSV files: $csv_count"
else
    echo "No data files found yet in $DATA_DIR"
fi
