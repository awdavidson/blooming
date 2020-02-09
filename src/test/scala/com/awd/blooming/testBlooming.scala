package com.awd.blooming

import com.awd.blooming.BloomFilter.{buildBloom, checkBloom, updateBloom}

import org.scalatest.{FunSuite, Matchers}

class testBlooming extends FunSuite
  with Matchers
  with SparkEnv {

  import spark.implicits._


  def randomInt(n: Int = 100) = scala.util.Random.nextInt(n) + 1

  // Randomly generate DF of 100,000 rows
  val randomDF = spark.sparkContext.parallelize(
    Seq.fill(100000) {
      (randomInt(1000).toString)
    }
  ).toDF("A").as[String]

  // Insure the following values are added to our randomDF
  val valuesToCheck = Seq("11", "20").toDF("A").as[String]

  // Create out testRdd
  val testRdd = randomDF.union(valuesToCheck).distinct.rdd

  // Build our bloomFilter from testRdd
  val bFilter = buildBloom(testRdd, 1000000)

  test("Check our bloomFilter has be created with values we want") {
    // Check for forced values
    bFilter.mightContain("20") shouldBe true
    bFilter.mightContain("11") shouldBe true

    // Check for value completely out of our range
    bFilter.mightContain("13000") shouldBe false
  }

  test("Check bloomFilter for multiple values from our new data - force new values") {
    // New values - we have introduced two new values
    val newValues = Seq("11", "20000", "13000").toDF("A").as[String]
    val checkedNew = checkBloom(newValues.rdd, bFilter)

    checkedNew shouldBe true
  }

  test("Check bloomFilter for multiple values from our new data - forced no new values") {
    // New values - we have not introduce any new values here
    val newValues = Seq("11", "20").toDF("A").as[String]
    val checkedNew = checkBloom(newValues.rdd, bFilter)

    checkedNew shouldBe false
  }

  test("Update bloomFilter with new values") {
    // New values - we have introduced two new values
    val newValues = Seq("11", "20000", "13000").toDF("A").as[String]

    // This bloomFilter will consist of the original elements as well as the new ones
    val updatedBloom = updateBloom(newValues.rdd, bFilter)

    // Check for values added
    updatedBloom.mightContain("13000") shouldBe true
    updatedBloom.mightContain("20000") shouldBe true

    // Check for original values
    updatedBloom.mightContain("20") shouldBe true
    updatedBloom.mightContain("11") shouldBe true
  }


}
