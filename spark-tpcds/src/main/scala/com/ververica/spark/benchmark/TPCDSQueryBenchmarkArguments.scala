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

import java.util.Locale

class TPCDSQueryBenchmarkArguments(val args: Array[String]) {
  var queryLocation: String = null
  var database: String = null
  var dataLocation: String = null
  var queryFilter: Set[String] = Set.empty
  var cboEnabled: Boolean = false
  var collectStats: Boolean = false
  var fileFormat: String = "orc"
  var mode: String = "explain" // explain or execute
  var recoverPartition: Boolean = false
  var histogram: Boolean = false

  parseArgs(args.toList)
  validateArguments()

  private def optionMatch(optionName: String, s: String): Boolean = {
    optionName == s.toLowerCase(Locale.ROOT)
  }

  private def parseArgs(inputArgs: List[String]): Unit = {
    var args = inputArgs

    while (args.nonEmpty) {
      args match {
        case optName :: value :: tail if optionMatch("--database", optName) =>
          database = value
          args = tail

        case optName :: value :: tail if optionMatch("--query-location", optName) =>
          queryLocation = value
          args = tail

        case optName :: value :: tail if optionMatch("--data-location", optName) =>
          dataLocation = value
          args = tail

        case optName :: value :: tail if optionMatch("--file-format", optName) =>
          fileFormat = value
          args = tail

        case optName :: value :: tail if optionMatch("--query-filter", optName) =>
          queryFilter = value.toLowerCase(Locale.ROOT).split(",").map(_.trim).toSet
          args = tail

        case optName :: tail if optionMatch("--cbo", optName) =>
          cboEnabled = true
          args = tail

        case optName :: tail if optionMatch("--collect-stats", optName) =>
          collectStats = true
          args = tail

        case optName :: value :: tail if optionMatch("--mode", optName) =>
          mode = value
          args = tail

        case optName :: tail if optionMatch("--recover-partition", optName) =>
          recoverPartition = true
          args = tail

        case optName :: tail if optionMatch("--histogram", optName) =>
          histogram = true
          args = tail

        case _ =>
          // scalastyle:off println
          System.err.println("Unknown/unsupported param " + args)
          // scalastyle:on println
          printUsageAndExit(1)
      }
    }
  }

  private def printUsageAndExit(exitCode: Int): Unit = {
    // scalastyle:off
    System.err.println("""
      |Usage: spark-submit --class <this class> <spark sql test jar> [Options]
      |Options:
      |  --database           Database name
      |  --data-location      Path to TPCDS queries
      |  --data-location      Path to TPCDS data
      |  --file-format        File format, e.g. parquet,orc
      |  --query-filter       Queries to filter, e.g., q3,q5,q13
      |  --cbo                Whether to enable cost-based optimization
      |  --collect-stats      Whether to collect table statistics
      |  --recover-partition  Whether to enable recover-partition
      |  --histogram          Whether to enable histogram
      |
      |------------------------------------------------------------------------------------------------------------------
      |In order to run this benchmark, please follow the instructions at
      |https://github.com/databricks/spark-sql-perf/blob/master/README.md
      |to generate the TPCDS data locally (preferably with a scale factor of 5 for benchmarking).
      |Thereafter, the value of <TPCDS data location> needs to be set to the location where the generated data is stored.
      """.stripMargin)
    // scalastyle:on
    System.exit(exitCode)
  }

  private def validateArguments(): Unit = {
    if (dataLocation == null) {
      // scalastyle:off println
      System.err.println("Must specify a data location")
      // scalastyle:on println
      printUsageAndExit(-1)
    }
    if (fileFormat == null) {
      // scalastyle:off println
      System.err.println("Must specify file format")
      // scalastyle:on println
      printUsageAndExit(-1)
    }
    if (mode == null || !Seq("explain", "execute").contains(mode)) {
      // scalastyle:off println
      System.err.println("Must specify mode to explain or execute")
      // scalastyle:on println
      printUsageAndExit(-1)
    }
  }

  override def toString: String =
    s"\ndatabase=$database, " +
    s"\nqueryLocation=$queryLocation, " +
    s"\ndataLocation=$dataLocation, " +
    s"\nqueryFilter=$queryFilter, " +
    s"\ncboEnabled=$cboEnabled," +
    s"\ncollectStats=$collectStats, " +
    s"\nfileFormat=$fileFormat" +
    s"\nrecoverPartition=$recoverPartition" +
    s"\nmode=$mode" +
    s"\nhistogram=$histogram"
}
