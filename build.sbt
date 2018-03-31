// Needed for spark-testing-base, see https://github.com/holdenk/spark-testing-base
fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
parallelExecution in Test := false

lazy val commonSettings = Seq(
  organization := "com.vicecity.sunshineautos",
  scalaVersion := "2.11.11",
  version := "0.1.0-SNAPSHOT"
)

val sparkVersion = "2.1.0"


lazy val root = (project in file("."))
  .settings(
    inThisBuild(commonSettings),

    name := "CarProfileSpy",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
      "org.scalactic" %% "scalactic" % "3.0.4",
      "org.scalatest" %% "scalatest" % "3.0.4" % "test",
      "com.holdenkarau" %% "spark-testing-base" % "2.1.0_0.8.0" % "test"
    )

  )
