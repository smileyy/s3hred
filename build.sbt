name := "s3hred"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "ch.qos.logback" %  "logback-classic" % "1.1.7",
  "com.google.guava" % "guava" % "19.0",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",

  "org.apache.commons" % "commons-csv" % "1.2",

  "org.scala-lang" % "scala-reflect" % "2.11.8",
  "org.scala-lang.modules" % "scala-xml_2.11" % "1.0.4",

  "org.scalactic" %% "scalactic" % "2.2.6",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test"
)