package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingApp {
  def main(args: Array[String]): Unit = {
    // Check for the correct number of arguments
    if (args.length < 2 ) {
      System.err.println("Usage: SparkStreamingApp <inputDirectory> <outputDirectory>")
      System.exit(1)
    }

    // Extract input and output directories from arguments
    val inputDirectory = args(0)
    val outputDirectory = args(1)

    // Set up the Spark configuration
    val sparkConf = new SparkConf().setAppName("SparkStreamingApp").setMaster("local[*]")

    // Create the streaming context with a batch interval of 3 seconds
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // Set checkpoint directory
    ssc.checkpoint(".")

    // Read input data
    val lines = ssc.textFileStream(inputDirectory)

    // Set counter for each task
    var numA = 1
    var numB = 1
    var numC = 1

    // Task A: Word Frequency Count
    lines.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {

        // Filter words based on criteria
        val words = rdd.flatMap(_.split(" "))
          .filter(_.matches("[a-zA-Z]+"))
          .filter(_.length >= 3)

        // Count the word frequency
        val wordCounts = words.map(word => (word, 1)).reduceByKey(_ + _)

        // Save the output to HDFS
        wordCounts.saveAsTextFile(s"$outputDirectory/taskA-${"%03d".format(numA)}")
        numA += 1
      }
    }

    // Task B: Co-occurrence Frequency Count
    lines.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {

        // Extract words from the RDD
        val words = rdd.flatMap(_.split(" "))
          .filter(_.matches("[a-zA-Z]+"))
          .filter(_.length >= 3)
          .collect()

        // Generate co-occurrence pairs
        val coOccurrences = rdd.flatMap { line =>
          for {
            i <- words.indices
            j <- words.indices
            if i != j
          } yield ((words(i), words(j)), 1)
        }

        // Reduce by key to get co-occurrence frequency
        val coOccurrencesReduced = coOccurrences.reduceByKey(_ + _)
          .map { case ((w1, w2), count) => s"($w1, $w2) $count" }

        // Save the output to HDFS
        coOccurrencesReduced.saveAsTextFile(s"$outputDirectory/taskB-${"%03d".format(numB)}")
        numB += 1
      }
    }

    // Task C: Co-occurrence Frequency Count with updateStateByKey
    var state: org.apache.spark.rdd.RDD[(String, String)] = ssc.sparkContext.emptyRDD

    lines.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {

        // Extract words from the RDD
        val words = rdd.flatMap(_.split(" "))
          .filter(_.matches("[a-zA-Z]+"))
          .filter(_.length >= 3)
          .collect()

        // Generate co-occurrence pairs
        val coOccurrences = rdd.flatMap { line =>
          for {
            i <- words.indices
            j <- words.indices
            if i != j
          } yield (words(i), words(j))
        }

        // Update the state with co-occurrence pairs
        state = state.union(coOccurrences)

        val updateFunc = (values: Seq[Int], state: Option[Int]) => {
          val currentCount = values.sum
          val previousCount = state.getOrElse(0)
          Some(currentCount + previousCount)
        }

        // Perform the required operations on the co-occurrence data
        val coOccurrencesReduced = state.map(pair => (pair, 1))
          .reduceByKey(_ + _)
          .map { case ((w1, w2), count) => s"($w1, $w2) $count" }

        // Save the output to HDFS
        coOccurrencesReduced.saveAsTextFile(s"$outputDirectory/taskC-${"%03d".format(numC)}")
        numC += 1
      }
    }
    // Start the streaming context
    ssc.start()
    ssc.awaitTermination()
  }
}