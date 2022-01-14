package org.apache.spark

import org.apache.spark.benchmark.Benchmark

import java.io.OutputStream
import scala.concurrent.duration.{DurationInt, FiniteDuration}

class BenchmarkHelper(
        name: String,
        valuesPerIteration: Long,
        minNumIters: Int = 2,
        warmupTime: FiniteDuration = 2.seconds,
        minTime: FiniteDuration = 2.seconds,
        outputPerIteration: Boolean = false,
        output: Option[OutputStream] = None)
  extends Benchmark(name, valuesPerIteration, minNumIters, warmupTime, minTime, outputPerIteration, output) {
}
