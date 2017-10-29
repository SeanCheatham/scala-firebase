import sbt._

object Dependencies {

  object Versions {
    val play = "2.6.6"
    val akka = "2.5.6"
  }

  val playJson =
    Seq(
      "com.typesafe.play" %% "play-json" % Versions.play
    )

  val typesafe =
    Seq(
      "com.typesafe" % "config" % "1.3.2"
    )

  val test =
    Seq(
      "org.scalatest" %% "scalatest" % "3.0.4" % "test"
    )

  val logging =
    Seq(
      "ch.qos.logback" % "logback-classic" % "1.2.3",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2"
    )

  val akka =
    Seq(
      "com.typesafe.akka" %% "akka-actor" % Versions.akka
    )

  val akkaStreams =
    Seq(
      "com.typesafe.akka" %% "akka-stream" % Versions.akka
    )

  val firebaseAdmin =
    Seq(
      "com.google.firebase" % "firebase-admin" % "5.4.0"
    )
}
