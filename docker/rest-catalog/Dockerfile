# Apache's Iceberg REST catalog server only bundles the SQLite JDBC driver.
# Add the Postgres driver so catalog metadata lives in the postgres service.
ARG ICEBERG_VERSION=1.10.1
FROM apache/iceberg-rest-fixture:${ICEBERG_VERSION}

ARG POSTGRES_JDBC_VERSION=42.7.13
ADD --chown=iceberg:iceberg \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar \
    /usr/lib/iceberg-rest/postgresql.jar

CMD ["java", "-cp", "iceberg-rest-adapter.jar:postgresql.jar", "org.apache.iceberg.rest.RESTCatalogServer"]
