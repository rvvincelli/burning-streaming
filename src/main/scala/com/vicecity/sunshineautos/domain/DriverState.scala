package com.vicecity.sunshineautos.domain

import java.time.Instant
import scala.language.implicitConversions

import scala.collection.immutable.SortedMap
import java.time.Duration
import org.scalactic.Equality
import org.scalactic.Tolerance._
import org.scalactic.TripleEquals._
import java.time.temporal.ChronoUnit
import com.vicecity.sunshineautos.pipeline.Params

sealed trait Profile
case object Commuter extends Profile
case object Racer extends Profile
case object Unknown extends Profile

case class DriverState(chronology: SortedMap[Instant, InstantInfo], avgSpeed: Double, profile: Profile) {
  //Assumption: we get one measurement per day - let's think of this as the average speed and position. All is incremental, no out of orders, happy flow.
  def update(info: InstantInfo) = {
    val updatedChronology = chronology + ((info.timestamp.truncatedTo(ChronoUnit.HOURS), info))
    val avgSpeed = (updatedChronology.values.map(_.speed).sum)/updatedChronology.values.size
    if (updatedChronology.size <= Params.horizon) copy(chronology = updatedChronology, avgSpeed = avgSpeed, profile = Unknown)
    else {
      val filteredChronology = updatedChronology.drop(1)
      if (avgSpeed < 60) copy(chronology = filteredChronology, avgSpeed = avgSpeed, profile = Commuter)
      else copy(chronology = filteredChronology, avgSpeed = avgSpeed, profile = Racer)
    }
  }
}

object DriverState {
  
  def apply(): DriverState = DriverState(SortedMap.empty[Instant, InstantInfo], 0, Unknown)

  implicit def driverStatesEq(burnInDate: Instant = Instant.EPOCH): Equality[DriverState] = new Equality[DriverState] {
    private val tolerance = 0.0005f
    def areEqual(a: DriverState, b: Any): Boolean = (a, b) match {
      case (_, ds: DriverState) if ds.chronology.isEmpty => 
        println("Skipping comparison (stream record with empty chronology)")
        true
      case (_, ds: DriverState) if ds.chronology.last._1 isBefore burnInDate => 
        println("Skipping comparison (burn-in)")
        true
      //To be consistent, the Equality should be provided for every sub-class, e.g. Car too.
      //Also, the current history of measurements is ignored because we do not serialize it - this simplifies things a lot.
      case (DriverState(chronologyA, avgSpeedA, profileA), DriverState(chronologyB, avgSpeedB, profileB)) =>
        println("Comparing!")
        (avgSpeedA === avgSpeedB +- tolerance) &&
        profileA == profileB
      case _ => false
    }
  }

}
