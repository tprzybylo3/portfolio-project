# Built from scratch on an actively-maintained official base (eclipse-temurin,
# the canonical OpenJDK image) instead of a pre-packaged Spark image -- both
# Bitnami's free tags and the official apache/spark-py image turned out to
# be unreliable/stale (Bitnami dropped free version tags in 2025; apache/
# spark-py hasn't been updated in ~3 years, tops out at v3.4.0). Building
# from JDK + `pip install pyspark` is itself a standard, well-documented
# way to run PySpark -- not a workaround.
FROM eclipse-temurin:17-jdk-jammy

RUN apt-get update && apt-get install -y --no-install-recommends \
      python3 python3-pip curl \
    && rm -rf /var/lib/apt/lists/*

# pyspark on PyPI bundles the actual Spark distribution (jars, scripts) --
# this single pip install gives us a complete, working Spark, not just a
# thin Python wrapper. Version pinned to match the Iceberg runtime jar
# below exactly (both must agree on the Spark minor version: 3.5).
RUN pip3 install --no-cache-dir pyspark==3.5.5 boto3

# Iceberg's Spark integration ships as two separate jars, not bundled with
# plain Spark: the core Iceberg-for-Spark runtime, and the AWS bundle
# (S3 + Glue Data Catalog clients). Both dropped straight into PySpark's
# own jars/ directory, wherever pip installed it -- found dynamically
# rather than hardcoding a path that could differ between environments.
RUN PYSPARK_JARS_DIR=$(python3 -c "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))") \
    && curl -fsSL -o "$PYSPARK_JARS_DIR/iceberg-spark-runtime-3.5_2.12-1.9.1.jar" \
         https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.9.1/iceberg-spark-runtime-3.5_2.12-1.9.1.jar \
    && curl -fsSL -o "$PYSPARK_JARS_DIR/iceberg-aws-bundle-1.9.1.jar" \
         https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.9.1/iceberg-aws-bundle-1.9.1.jar

ENV PYSPARK_PYTHON=python3
WORKDIR /opt/spark-scripts

CMD ["tail", "-f", "/dev/null"]