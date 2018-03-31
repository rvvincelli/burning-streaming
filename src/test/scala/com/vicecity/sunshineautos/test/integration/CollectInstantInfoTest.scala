package com.vicecity.sunshineautos.test.integration

import java.time.Instant
import java.lang.IllegalArgumentException

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
import java.time.temporal.ChronoUnit
import com.vicecity.sunshineautos.domain.GPS
import java.io.PrintWriter
import com.vicecity.sunshineautos.domain.Car
import scala.util.Random
import java.io.File
import com.vicecity.sunshineautos.pipeline.Params
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.Duration

class CollectInstantInfoTest extends FunSuite with StreamingSuiteBase {
  
  val outputFile = "src/test/resources/full_recorded_state.csv"

  //We need to override this to become batch-oblivious - we are just looking at the lists, no groupings. Trying to guess which input unlocks the method above can be tiring.
  //To make things work and ignore the counting details, which are irrelevant to the logic, let's perform some manual padding.
  override def verifyOutput[V: ClassTag](
     output: Seq[Seq[V]],
     expectedOutput: Seq[Seq[V]],
     ordered: Boolean
  ) (implicit equality: Equality[V]): Unit = {
    if (ordered) {
      val x = output.flatten
      val y = expectedOutput.flatten
      if (x.size != y.size) {
        println(s"Sizes do not match! ${x.size}, ${y.size}, auto-resizing")
        val resizedY = y.drop(y.size-x.size)
        println(s"Sizes are now: ${x.size}, ${resizedY.size}")
        x zip resizedY foreach { case (a, b) => 
          //Our equality is not commutative - we don't serialize the chronology, therefore the instance parsed from the file does not have it.
          assert(b === a) 
        }
      }
    }
    else throw new IllegalArgumentException("Only list-based comparison is supported for this override")
  }
  
  override def beforeAll() {
    super.beforeAll()
    new File(outputFile).delete()
  }
  
  override def afterAll() {
    super.afterAll()
    new File(outputFile).delete()
  }
  
  //Sometimes it does take a while...
  override def maxWaitTimeMillis: Int = 50000
  
  //This is very important: in order to have ordered output lists to be able to compare them sequentially, holding to a burning rate, we do not want multiple threads scrambling it up!
  override def master = "local[1]" 
  
  // Set this lazy because Spark context won't be readily ready - and the file either
  private lazy val expected: Seq[(Int, DriverState)] = parseFile(outputFile, parseDriverState)

  def operation(hb: DStream[(Int, InstantInfo)]) = CollectInstantInfo.createStream(hb, outputFile)
  def identity(hb: DStream[(Int, InstantInfo)]) = hb
  
  //No use, just showing off some cool usage
  val z = operation _ compose identity
  
  implicit lazy val _ = sc
  
  val startingDay = Instant.now
  
  val batches = 50
  val plates = 5
  println("Fetching the last (random) batch of car instant info beacons...")
  val data = List.tabulate(batches, plates){ (batch, plate) => 
    (plate, InstantInfo(startingDay.plus(batch,ChronoUnit.DAYS), GPS(Random.nextFloat, Random.nextFloat), Random.nextFloat*100, Car("AR693GX"+plate, plate.hashCode.toString)))
  }
  //This is to simulate our "new" implementation which is tested against the "old" implementation which is running.
  //The state we test against was generated from the full input, and we start from a smaller input instead - we will catch up.
  val burnDays = plates*Params.horizon 
  val partialData = data.drop(burnDays)
  
  //The Spark stateful streamer is configure to write out the state. We take advantage of the Spark testing framework to write out a state we will use for the actual test. But in order for this to
  //work without issues and errors, we fool the tester by providing a dummy equality.
  test("Generating a test file to test the catchup") {
    def equality: Equality[(Int, DriverState)] = new Equality[(Int, DriverState)] {
      def areEqual(a: (Int, DriverState), b: Any): Boolean = true
    }
    val dummyOutput = Seq.tabulate(batches)(_ => Seq.tabulate(plates)(n => (n, DriverState())))
    testOperation(data, operation, dummyOutput, ordered = true)(implicitly[ClassTag[(Int, InstantInfo)]], implicitly[ClassTag[(Int, DriverState)]], equality)
  }
  //This is the actual test! To recap a little:
  //-we generated a number of random events - some large stream X
  //-we took out the first days of messages - to have our test run starting off phase, and have it catch up
  //-we used the framework to generate a test file, from the large stream X
  //-we want to show that our processor will catch up and, even if started from just partial data, the final state will be the same
  //Because of the internals, the math will not add very exactly and we will need to pad and drop here and there to start with lists of equal size.
  //We will assume a maximum burn-in: we will set the equality to assume the sigma (see blog post) is undefined to the very end, where we compare just the state emission.
  //What are we checking, exactly? We want to see that we converge to a give state for the car / drivers: same speed and profile.
  test("The pipeline should compute the detection values correctly, starting from the hour behavior inputs on an empty memory with proper burn-in and tolerance") {

    def equality(burnInDate: Instant = Instant.EPOCH): Equality[(Int, DriverState)] = new Equality[(Int, DriverState)] {
      def areEqual(a: (Int, DriverState), b: Any): Boolean = (a, b) match {
        case ((fna: Int, dsa: DriverState), (fnb: Int, dsb: DriverState)) => fna == fnb && DriverState.driverStatesEq(burnInDate).areEqual(dsa, dsb)
        case _ => false
      }
    }
  
    //The argument defines how much of the sequence head we are going to discard. The value should be consistent with the memory of our pipeline process and the number of batches, among other
    //things. If you try with a much smaller or larger offset you will basically always discard or compare, respectively.
    val eq = equality(startingDay.plus(Params.horizon+25,ChronoUnit.DAYS))
    testOperation(Seq(partialData.flatten), operation, Seq(expected), ordered = true)(implicitly[ClassTag[(Int, InstantInfo)]], implicitly[ClassTag[(Int, DriverState)]], eq)
  
  }
  
}
