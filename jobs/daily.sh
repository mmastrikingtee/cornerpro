set -euo pipefail

python -m src.ingest.mma_ingest --days-ahead 120
python -m src.features.elo
python -m src.predict.score_upcoming
python -m src.publish.build_site

# publish to /docs for GitHub Pages
rm -rf docs/*
cp -r site/public/* docs/

# keep these helpful files
echo > docs/.nojekyll
echo "<!doctype html><title>404</title>Not found" > docs/404.html
date -u +"%Y-%m-%dT%H:%M:%SZ" > docs/_pages_heartbeat.txt
