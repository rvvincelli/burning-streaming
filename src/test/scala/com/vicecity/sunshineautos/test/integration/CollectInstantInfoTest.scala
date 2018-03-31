package com.vicecity.sunshineautos.test.integration

import java.time.Instant

import scala.reflect.ClassTag

import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream
import org.scalatest.FunSuite

import com.holdenkarau.spark.testing.StreamingSuiteBase
import com.vicecity.sunshineautos.domain.DriverState
import com.vicecity.sunshineautos.domain.InstantInfo
import com.vicecity.sunshineautos.pipeline.CollectInstantInfo

import Parsers._
import org.scalactic.Equality
import com.vicecity.sunshineautos.pipeline.CarSpyTestDataGenerator
import java.time.temporal.ChronoUnit

class CollectInstantInfoTest extends FunSuite with StreamingSuiteBase {

  //Even by sticking to Spark libs, our Seq's carry the seed of unserializability, because of toLocalIterator I guess. To solve this well just disable serialization altogether.
  override lazy val checkpointDir = null
  
  //val fileMask = "src/test/resources/%s_full.csv"
  
  // Set these lazy because Spark context won't be readily ready
  private lazy val input: Seq[(Int, InstantInfo)] = parseFile(inputFile("src/test/resources/input_minussix.csv"), parseInstantInfo)
  private lazy val expected: Seq[(Int, DriverState)] = parseFile(outputFile("src/test/resources/output.csv"), parseDriverState)

  // Function composition is read from right to left - the rightmost function is applied first.
  // Of course this addition does not do anything, it is just cool to show how a chain of stream transformations is composed, giving birth to a full integration test.
  val x = CollectInstantInfo.createStream _
  def y(hb: DStream[(Int, InstantInfo)]) = hb

  implicit lazy val _ = sc
  
  private val operation = x compose y

  test("The pipeline should compute the detection values correctly, starting from the hour behavior inputs on an empty memory with proper burn-in and tolerance") {

    def equality(burnInDate: Instant = Instant.EPOCH): Equality[(Int, DriverState)] = new Equality[(Int, DriverState)] {
      def areEqual(a: (Int, DriverState), b: Any): Boolean = (a, b) match {
        case ((fna: Int, dsa: DriverState), (fnb: Int, dsb: DriverState)) => fna == fnb && DriverState.driverStatesEq(burnInDate).areEqual(dsa, dsb)
        case _ => false
      }
    }
  
    // The argument defines how much of the sequence head we are going to discard. The value should be consistent with the memory of our pipeline process, which should be eight days.
    val eq = equality(CarSpyTestDataGenerator.startingDay.plus(1,ChronoUnit.DAYS))
    testOperation(Seq(input.drop(4)), operation, Seq(expected), ordered = true)(implicitly[ClassTag[(Int, InstantInfo)]], implicitly[ClassTag[(Int, DriverState)]], eq)
  
  }
  
}
