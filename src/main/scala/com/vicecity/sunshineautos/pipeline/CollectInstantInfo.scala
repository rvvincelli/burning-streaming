package com.vicecity.sunshineautos.pipeline

import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{State, StateSpec}
import com.vicecity.sunshineautos.domain.InstantInfo
import com.vicecity.sunshineautos.domain.DriverState
import java.io.PrintWriter
import java.io.File
import java.io.FileOutputStream
import java.time.Instant
import java.io.FileWriter

object CollectInstantInfo {
  
  type FrameNumber = Int
  
  //Please do note that this is a quick and dirty implementation; to make sense of all these Option's around, please refer to the documentation.
  def updateDriverState(fn: FrameNumber, value: Option[InstantInfo], state: State[DriverState]): Option[(FrameNumber, DriverState)] = Some(
    fn ->
    { 
      if (state.exists) {
        val newState = 
          if (value.isDefined) state.get.update(value.get)
          else state.get
        state.update(newState)
        newState
      }
      else {
        val newState =
          if (value.isDefined) DriverState().update(value.get)
          else DriverState()
        state.update(newState)
        newState
      }
    }
  )

  def createStream(ds: DStream[(Int, InstantInfo)], outputFile: String): DStream[(CollectInstantInfo.FrameNumber, DriverState)] = {
    val spec = StateSpec.function(updateDriverState _)
    val mapped = ds.mapWithState(spec)
    val mappedFlat = mapped.flatMap{ x => x }
    mappedFlat.foreachRDD{_.foreach{case (x, y) =>
      //Don't try this at home - this is just a quick and very dirty way to create the output state we need for the real test.
      //This is rather safe if local mode is on just one thread, otherwise expect dirty output files.
      val fw = new FileWriter(outputFile, true)
      fw.write(s"$x,${y.avgSpeed},${y.profile}\n")
      fw.close()
    }}
    mappedFlat
  }

}
