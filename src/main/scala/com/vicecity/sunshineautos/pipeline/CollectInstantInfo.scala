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

  def createStream(ds: DStream[(FrameNumber, InstantInfo)]): DStream[(CollectInstantInfo.FrameNumber, DriverState)] = {
    val spec = StateSpec.function(updateDriverState _)
    val mapped = ds.mapWithState(spec)
    val mappedSorted = mapped.mapPartitions(_.toList.sortBy(_.get._1).toIterator)
    mappedSorted.flatMap(x => x).foreachRDD{x => 
      val when = Instant.now.toEpochMilli()
      x.foreach{case (x, y) =>
      val pwo = new PrintWriter(new FileOutputStream(new File(s"/Users/ricky/Documents/projects/burning_streaming/src/test/resources/output_full_$when.csv"), true))  
      pwo.write(s"\n$x,${y.avgSpeed},${y.profile}\n")
      pwo.close
    }}
    mappedSorted.flatMap(x => x)
  }

}
