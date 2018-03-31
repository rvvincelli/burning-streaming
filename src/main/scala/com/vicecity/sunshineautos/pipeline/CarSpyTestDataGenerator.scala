package com.vicecity.sunshineautos.pipeline

import java.io._

import scala.util.Random

import com.vicecity.sunshineautos.domain.DriverState
import com.vicecity.sunshineautos.domain.InstantInfo
import com.vicecity.sunshineautos.domain.Car
import com.vicecity.sunshineautos.domain.GPS
import java.time.Instant
import java.time.temporal.ChronoUnit

object CarSpyTestDataGenerator {

  val startingDay = Instant.now
  
  def main(args: Array[String]) { 

    val inputFile = args(0)
    val outputFile = args(1)
    
    println("Fetching the last (random) batch of car instant info beacons...")
    val data = List.tabulate(50, 5){ (batch, plate) => 
      (plate, InstantInfo(startingDay.plus(batch,ChronoUnit.DAYS), GPS(Random.nextFloat, Random.nextFloat), Random.nextFloat*100, Car("AR693GX"+plate, plate.hashCode.toString)))
    }
    println(s"Writing the second half of it out on file $inputFile, to simulate a recording on the old application.")
    val pwi = new PrintWriter(new File(inputFile))
    data.foreach{_.foreach{case (fn, InstantInfo(timestamp, GPS(x, y), speed, Car(plate, driverSSN))) => pwi.write(s"$fn,${timestamp.toEpochMilli},$x,$y,$speed,$plate,$driverSSN\n")}}
    pwi.close
    
    //Why aren't we using just Spark for this? It is possible, but it would be an overkill - we have the state updation function at hand.
//    println(s"Simulating the stateful processing to create a test file for the state; this the state to which the new application will have to catch up after reading the input extract above.")
//    println(s"Writing it out on file $outputFile")
//    val pwo = new PrintWriter(new File(outputFile))
//    val state = data.map(xs => xs.foldRight(Map.empty[Int, DriverState]){case ((fn, info), st) => 
//      val updatedDriverState = {
//        if (st.contains(fn)) st(fn).update(info)
//        else DriverState().update(info)
//      }
//      pwo.write(s"$fn,${updatedDriverState.avgSpeed},${updatedDriverState.profile}\n")
//      st + (fn -> updatedDriverState)
//    })
//    pwo.close
//    
  }
  
}
