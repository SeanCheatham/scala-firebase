lazy val commonSettings =
  Seq(
    organization := "com.seancheatham",
    scalaVersion := "2.12.4",
    libraryDependencies ++=
      Dependencies.typesafe ++
        Dependencies.test ++
        Dependencies.logging
  ) ++ Publish.settings

lazy val firebaseAkkaStreams =
  project
    .in(file("firebaseAkkaStreams"))
    .settings(commonSettings: _*)
    .settings(
      name := "firebase-akka-streams",
      libraryDependencies ++=
        Dependencies.akka ++
          Dependencies.akkaStreams ++
          Dependencies.playJson ++
          Dependencies.firebaseAdmin
    )