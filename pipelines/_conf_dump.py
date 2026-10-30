"""Print the Spark configuration the driver actually resolved.

Run when a job hangs with "Initial job has not accepted any resources". The
submit container's logs do not say what resource request reached the master, so
this is the first thing to check: if `spark.executor.cores` or
`spark.executor.instances` did not come from spark-defaults.conf, the request
the master sees will not match what the worker has free.

    docker compose --profile compute run --rm spark-jobs \
        /opt/pipelines/_conf_dump.py
"""

import sys

from pyspark.sql import SparkSession

KEYS = (
    "spark.master",
    "spark.executor.instances",
    "spark.executor.cores",
    "spark.executor.memory",
    "spark.driver.memory",
    "spark.default.parallelism",
    "spark.sql.catalogImplementation",
    "spark.hadoop.hive.metastore.uris",
    "spark.sql.warehouse.dir",
    "spark.hadoop.fs.s3a.endpoint",
    "spark.hadoop.fs.s3a.path.style.access",
)


def main() -> int:
    """Print the resolved configuration and the cluster's view of it."""
    spark = SparkSession.builder.appName("edai-conf-dump").getOrCreate()
    conf = spark.sparkContext.getConf()

    print("=== resolved configuration")
    for key in KEYS:
        print(f"  {key} = {conf.get(key, '<unset>')}")

    master = conf.get("spark.master")
    print("=== master:", master)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
