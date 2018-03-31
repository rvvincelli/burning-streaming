package com.vicecity.sunshineautos.domain

import scala.util.Random

case class GPS(x: Double, y: Double) {
  def isInCity = Math.abs(x + y) <= 1
}

object GPS {
  def apply(): GPS = GPS(Random.nextDouble, Random.nextDouble)
}
