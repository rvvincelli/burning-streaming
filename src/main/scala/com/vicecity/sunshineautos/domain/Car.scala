package com.vicecity.sunshineautos.domain

import scala.util.Random

case class Car(plate: String, driverSSN: String)

object Car {
  def apply(): Car = Car(Random.nextInt.toString, Random.nextInt.toString)
}