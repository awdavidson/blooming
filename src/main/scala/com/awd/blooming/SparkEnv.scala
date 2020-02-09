package com.awd.blooming

import org.apache.spark.sql.SparkSession

trait SparkEnv {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("BloomFilter")
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .getOrCreate()
  }

}