package com.awd.blooming

import org.apache.spark.rdd.RDD

object BloomFilter {

  private[this] val falsePositiveRate = 0.1

  def buildBloom(
                  rdd: RDD[String],
                  expectedValues: Long): bloomfilter.mutable.BloomFilter[String] = {

    val bloomFilter = bloomfilter.mutable.BloomFilter[String](expectedValues, falsePositiveRate)

    // From our RDD add each value to the BloomFilter
    rdd.collect.map { x =>
      bloomFilter.add(x)
    }
    bloomFilter
  }

  def checkBloom(rdd: RDD[String], bloomFilter: bloomfilter.mutable.BloomFilter[String]): Boolean = {
    // Check to see whether we have already processed the new values received
    val checks: Array[Boolean] = rdd.map { x =>
      bloomFilter.mightContain(x)
    }.distinct.filter(_ == false).collect

    // Check our results, false indicates we have received new data that needs to be processed
    checks.contains(false) match {
      case true =>
        println(s"Unprocessed values found. Will process new values")
        true
      case false =>
        println(s"No unprocessed values found.")
        false
    }

  }

  def updateBloom(rdd: RDD[String], bloomFilter: bloomfilter.mutable.BloomFilter[String]) = {
    // Filter our RDD for new values
    val filteredRDD: RDD[String] = rdd
      .filter(bloomFilter.mightContain(_) == false)

    // Add our new values to our BloomFilter
    filteredRDD.collect.map { x =>
      bloomFilter.add(x)
    }
    bloomFilter
  }

}
