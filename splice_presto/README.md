# Splice Machine Presto Shading

This is the shading of Presto for use in Splice Machine

## Key Capabilities

S3 Reading: HDFS S3FileSystem is a mess
ORC Reader: Spark's ORC reader is row based (yep)
Parquet Reader: Just looking at the code, hard to believe this is not much faster than Spark's Implementation.

TODO ITEMS:

Wrap the Parquet and ORC readers into the Splice Spark Adapter (Coming Soon)