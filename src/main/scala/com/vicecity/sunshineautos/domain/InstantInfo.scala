package com.vicecity.sunshineautos.domain

import java.time.Instant

case class InstantInfo(timestamp: Instant, gps: GPS, speed: Double, car: Car)
