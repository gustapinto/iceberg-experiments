CREATE CATALOG nessie USING iceberg
WITH (
    "iceberg.catalog.type" = 'rest',
    "iceberg.file-format" = 'parquet',
    "iceberg.rest-catalog.uri" = 'http://localhost:19120/iceberg/main',
    "iceberg.rest-catalog.vended-credentials-enabled" = 'true',
    "fs.native-s3.enabled" = 'true',
    "s3.endpoint" = 'http://localhost:9000/',
    "s3.path-style-access" = 'true',
    "s3.region" = 'us-east-1',
    "s3.aws-access-key" = 'admin',
    "s3.aws-secret-key" = 'password'
);
