package io.openlineage.spark3.agent.utils;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession$;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.collection.immutable.HashMap;

@Slf4j
public class ColumnLevelLineageUtilsTest {

  SparkSession spark;

  @BeforeEach
  @SneakyThrows
  public void setUp() {
    SparkSession$.MODULE$.cleanupAnyExistingSession();
    spark =
        SparkSession.builder()
            .master("local[*]")
            .appName("ColumnLevelLineage")
            .config("spark.extraListeners", LastJobIdSparkEventListener.class.getName())
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.spark_catalog.type", "hive")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", "/tmp/column_level_lineage/")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/derby")
            .getOrCreate();
    FileSystem.get(spark.sparkContext().hadoopConfiguration())
        .delete(new Path("spark-warehouse"), true);
    FileSystem.get(spark.sparkContext().hadoopConfiguration())
        .delete(new Path("/tmp/column_level_lineage/"), true);

    spark.sql("DROP TABLE IF EXISTS local.db.t1");
    spark.sql("DROP TABLE IF EXISTS local.db.t2");
    spark.sql("DROP TABLE IF EXISTS local.db.t");
  }

  @Test
  @SneakyThrows
  public void testIcebergLineageWithUnion() {
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("a", StringType$.MODULE$, false, new Metadata(new HashMap<>())),
              new StructField("b", StringType$.MODULE$, false, new Metadata(new HashMap<>()))
            });

    spark
        .createDataFrame(Arrays.asList(new GenericRow(new Object[] {"one", "two"})), schema)
        .createTempView("temp");
    spark.sql("CREATE TABLE local.db.t1 USING iceberg AS SELECT * FROM temp");
    spark.sql("CREATE TABLE local.db.t2 USING iceberg AS SELECT * FROM temp");
    spark.sql(
        "CREATE TABLE local.db.t USING iceberg AS (SELECT * FROM local.db.t1 UNION SELECT * FROM local.db.t2)");

    LogicalPlan plan = LastJobIdSparkEventListener.getLogicalPlan().get();

    ColumnLevelLineageBuilder cl = ColumnLevelLineageUtils.includeColumnLevelLineage(schema, plan);

    assertColumnDependsOn(cl, "a", "local.db.t1", "a");
    assertColumnDependsOn(cl, "a", "local.db.t2", "a");
    assertColumnDependsOn(cl, "b", "local.db.t1src/test/spark3/java/io/openlineage/spark3/agent/utils/ColumnLevelLineageUtilsTest.java", "b");
    assertColumnDependsOn(cl, "b", "local.db.t2", "b");
  }

  private void assertColumnDependsOn(
      ColumnLevelLineageBuilder cl,
      String outputColumn,
      String expectedInputTable,
      String expectedInputField) {
    List<Pair<DataSourceV2Relation, String>> inputs = cl.getInputsUsedFor(outputColumn);

    inputs.stream()
        .filter(pair -> pair.getLeft().name().equalsIgnoreCase(expectedInputTable))
        .filter(pair -> pair.getRight().equalsIgnoreCase(expectedInputField))
        .findAny();

    assertTrue(!inputs.isEmpty());
  }
}
