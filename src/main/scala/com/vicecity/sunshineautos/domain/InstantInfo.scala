package com.vicecity.sunshineautos.domain

import java.time.Instant
import scala.util.Random

case class InstantInfo(timestamp: Instant, gps: GPS, speed: Double, car: Car)

object InstantInfo {
  def apply(): InstantInfo = InstantInfo(Instant.now, GPS(), Random.nextDouble, Car())
}