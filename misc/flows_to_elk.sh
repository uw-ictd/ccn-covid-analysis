pip install elasticsearch_loader
pip install "elasticsearch_loader[parquet]"

elasticsearch_loader --index flows parquet data/clean/flows/*.parquet
