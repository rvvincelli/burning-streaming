package com.vicecity.sunshineautos.test.integration

import com.vicecity.sunshineautos.domain.InstantInfo
import com.vicecity.sunshineautos.domain.Car
import com.vicecity.sunshineautos.domain.GPS
import com.vicecity.sunshineautos.domain.DriverState
import java.time.Instant
import scala.collection.immutable.SortedMap
import com.vicecity.sunshineautos.domain.Racer
import com.vicecity.sunshineautos.domain.Unknown
import com.vicecity.sunshineautos.domain.Commuter
import scala.io.Source
import scala.util.Try

object Parsers {
  
  def inputFile(fileMask: String) = fileMask format "input"
  def outputFile(fileMask: String) = fileMask format "output"
  
  def parseInstant(instant: String) = Instant.ofEpochMilli(instant.toLong)
  
  def parseFile[A](file: String, parser: String => Option[A], header: Boolean = true): Seq[A] = {
    val dropLines = if (header) 1 else 0
    Source.fromFile(file).getLines().drop(dropLines).toSeq.flatMap(parser(_))
  }
  
  def parseInstantInfo(line: String): Option[(Int, InstantInfo)] = {
    val fields = line.split(',')
    Try((fields(0).toInt, InstantInfo(parseInstant(fields(1)), GPS(fields(2).toDouble, fields(3).toDouble), fields(4).toDouble, Car(fields(5), fields(6))))).toOption 
  }
  
  def parseDriverState(line: String): Option[(Int, DriverState)] = {
    val fields = line.split(',')
    Try((fields(0).toInt, DriverState(SortedMap.empty[Instant, InstantInfo], fields(1).toDouble, parseProfile(fields(2))))).toOption
  }
  
  def parseProfile(profile: String) = profile match {
    case "Racer" => Racer
    case "Commuter" => Commuter
    case _ => Unknown
  }
  
}