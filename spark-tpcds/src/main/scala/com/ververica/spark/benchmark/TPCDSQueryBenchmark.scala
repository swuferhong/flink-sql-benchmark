/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.spark.benchmark

import org.apache.spark.{BenchmarkHelper, SparkConf}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.catalyst.util.DateTimeConstants.NANOS_PER_SECOND
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.benchmark.SqlBasedBenchmark
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal.SQLConf

import java.io.File
import java.nio.file.Files
import scala.concurrent.duration.DurationInt
import scala.util.Try

/**
 * Benchmark to measure TPCDS query performance.
 * To run this:
 * {{{
 *   1. without sbt:
 *        bin/spark-submit --jars <spark core test jar>,<spark catalyst test jar>
 *          --class <this class> <spark sql test jar> --data-location <location>
 *   2. build/sbt "sql/test:runMain <this class> --data-location <TPCDS data location>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt
 *        "sql/test:runMain <this class> --data-location <location>"
 *      Results will be written to "benchmarks/TPCDSQueryBenchmark-results.txt".
 * }}}
 */
object TPCDSQueryBenchmark extends SqlBasedBenchmark with Logging {

  override def getSparkSession: SparkSession = {
    SparkSession.builder.enableHiveSupport().config(new SparkConf()).getOrCreate()
  }

  val tables = Seq("catalog_page", "catalog_returns", "customer", "customer_address",
    "customer_demographics", "date_dim", "household_demographics", "inventory", "item",
    "promotion", "store", "store_returns", "catalog_sales", "web_sales", "store_sales",
    "web_returns", "web_site", "reason", "call_center", "warehouse", "ship_mode", "income_band",
    "time_dim", "web_page")

  def setupTables(
      dataLocation: String,
      fileFormat: String,
      database: String,
      createTempView: Boolean,
      recoverPartition: Boolean): Map[String, Long] = {
    val useDatabase = database != null && database.nonEmpty
    if (useDatabase) spark.sql(s"USE $database")

    tables.map { tableName =>
      if (!useDatabase) {
        if (createTempView) {
          fileFormat match {
            case "parquet" =>
              spark.read.parquet(s"$dataLocation/$tableName").createOrReplaceTempView(tableName)
            case "orc" =>
              spark.read.orc(s"$dataLocation/$tableName").createOrReplaceTempView(tableName)
            case _ =>
              spark.read.text(s"$dataLocation/$tableName").createOrReplaceTempView(tableName)
          }
        } else {
          spark.sql(s"DROP TABLE IF EXISTS $tableName")
          spark.catalog.createTable(tableName, s"$dataLocation/$tableName", fileFormat)
          if (recoverPartition) {
            // Recover partitions but don't fail if a table is not partitioned.
            Try {
              spark.sql(s"ALTER TABLE $tableName RECOVER PARTITIONS")
            }.getOrElse {
              logInfo(s"Recovering partitions of table $tableName failed")
            }
          }
        }
      }
      tableName -> spark.table(tableName).count()
    }.toMap
  }

  def explainTpcdsQueries(queryLocation: String, queries: Seq[String]): Unit = {
    queries.foreach { name =>
      val queryString = readQuery(queryLocation, name)
      print(s"explain query: $name\n")
      spark.sql(queryString).explain(true)
    }
  }

  def runTpcdsQueries(
      queryLocation: String,
      queries: Seq[String],
      tableSizes: Map[String, Long],
      nameSuffix: String = ""): Unit = {
    queries.foreach { name =>
      val startTime = System.currentTimeMillis();
      println(s"$name start at ${startTime}")
      val queryString = readQuery(queryLocation, name)

      // This is an indirect hack to estimate the size of each query's input by traversing the
      // logical plan and adding up the sizes of all tables that appear in the plan.
      val queryRelations = scala.collection.mutable.HashSet[String]()
      spark.sql(queryString).queryExecution.analyzed.foreach {
        case SubqueryAlias(alias, _: LogicalRelation) =>
          queryRelations.add(alias.name)
        case LogicalRelation(_, _, Some(catalogTable), _) =>
          queryRelations.add(catalogTable.identifier.table)
        case HiveTableRelation(tableMeta, _, _, _, _) =>
          queryRelations.add(tableMeta.identifier.table)
        case _ =>
      }
      val numRows = queryRelations.map(tableSizes.getOrElse(_, 0L)).sum

      println(s"$name prepare cost ${System.currentTimeMillis() - startTime}")
      // do not warm up, iters=1
      val benchmark = new BenchmarkHelper(s"TPCDS Snappy", numRows, 1, warmupTime = -1.seconds, output = output)
      benchmark.addCase(s"$name$nameSuffix") { _ =>
        spark.sql(s"SET spark.app.name=$name")
        println(s"$name start run at ${System.currentTimeMillis()}")
        spark.sparkContext.setJobDescription(s"query $name")
        spark.sql(queryString).noop()
        println(s"$name finish at ${System.currentTimeMillis()}")
      }
      benchmark.run()

      println(s"$name total cost ${System.currentTimeMillis() - startTime}")
    }
  }

  private def filterQueries(
      origQueries: Seq[String],
      queryFilter: Set[String],
      nameSuffix: String = ""): Seq[String] = {
    if (queryFilter.nonEmpty) {
      if (nameSuffix.nonEmpty) {
        origQueries.filter { name => queryFilter.contains(s"$name$nameSuffix") }
      } else {
        origQueries.filter(queryFilter.contains)
      }
    } else {
      origQueries
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val benchmarkArgs = new TPCDSQueryBenchmarkArguments(mainArgs)
    logInfo(s"args: " + benchmarkArgs)

    val tpcdsQueries = Seq(
      "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10", "q11",
      "q12", "q13", "q14a", "q14b", "q15", "q16", "q17", "q18", "q19", "q20",
      "q21", "q22", "q23a", "q23b", "q24", "q25", "q26", "q27", "q28", "q29", "q30",
      "q31", "q32", "q33", "q34", "q35", "q36", "q37", "q38", "q39a", "q39b", "q40",
      "q41", "q42", "q43", "q44", "q45", "q46", "q47", "q48", "q49", "q50",
      "q51", "q52", "q53", "q54", "q55", "q56", "q57", "q58", "q59", "q60",
      "q61", "q62", "q63", "q64", "q65", "q66", "q67", "q68", "q69", "q70",
      "q71", "q72", "q73", "q74", "q75", "q76", "q77", "q78", "q79", "q80",
      "q81", "q82", "q83", "q84", "q85", "q86", "q87", "q88", "q89", "q90",
      "q91", "q92", "q93", "q94", "q95", "q96", "q97", "q98", "q99")

    val queries = filterQueries(tpcdsQueries, benchmarkArgs.queryFilter)

    val tableSizes = setupTables(
      benchmarkArgs.dataLocation,
      benchmarkArgs.fileFormat,
      benchmarkArgs.database,
      createTempView = !benchmarkArgs.cboEnabled,
      benchmarkArgs.recoverPartition)
    if (benchmarkArgs.cboEnabled) {
      spark.sql(s"SET ${SQLConf.CBO_ENABLED.key}=true")
      spark.sql(s"SET ${SQLConf.PLAN_STATS_ENABLED.key}=true")
      spark.sql(s"SET ${SQLConf.JOIN_REORDER_ENABLED.key}=true")
      if (benchmarkArgs.histogram) {
        spark.sql(s"SET ${SQLConf.HISTOGRAM_ENABLED.key}=true")
      } else {
        spark.sql(s"SET ${SQLConf.HISTOGRAM_ENABLED.key}=false")
      }
    } else {
      spark.sql(s"SET ${SQLConf.CBO_ENABLED.key}=false")
    }

    if (benchmarkArgs.collectStats) {
      // Analyze all the tables before running TPCDS queries
      val startTime = System.nanoTime()
      tables.foreach { tableName =>
        print(s"ANALYZE TABLE $tableName")
        spark.sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS FOR ALL COLUMNS")
      }
      logInfo("The elapsed time to analyze all the tables is " +
        s"${(System.nanoTime() - startTime) / NANOS_PER_SECOND.toDouble} seconds")
    }

    val queryLocation = if (benchmarkArgs.queryLocation != null &&
      benchmarkArgs.queryLocation.nonEmpty) {
      benchmarkArgs.queryLocation
    } else {
      "spark_queries"
    }

    benchmarkArgs.mode match {
      case "explain" =>
        explainTpcdsQueries(queryLocation, queries)
      case "execute" =>
        runTpcdsQueries(queryLocation, queries, tableSizes)
      case m =>
        throw new RuntimeException(s"Illegal mode: $m")
    }

    if (benchmarkArgs.debug) {
      // hold the application
      Thread.sleep(Long.MaxValue)
    }
  }

  private def readQuery(queryLocation: String, name: String): String = {
    val query = s"$queryLocation/$name.sql"
    print(s"read query: $query\n")
    val file = new File(query)
    if (file.exists()) {
      new String(Files.readAllBytes(file.toPath))
    } else {
      resourceToString(query, classLoader = Thread.currentThread().getContextClassLoader)
    }
  }
}
