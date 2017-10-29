A collection of libraries for using Firebase in Scala

[![Build Status](https://travis-ci.org/SeanCheatham/scala-firebase.svg?branch=master)](https://travis-ci.org/SeanCheatham/scala-firebase)

# Overview

Use this library to leverage Akka Streams combined with Firebase.  This library provides Sources, Sinks, and Flows to be
dropped inside of your application.

# Usage
This library is written in Scala.  It _might_ interoperate with other JVM languages, but I make no guarantees.

This library uses Typesafe's Play JSON library for serialization of content.

## Include the library in your project.
*Note* May not be on Maven Central yet.
In your build.sbt:
`libraryDependencies += "com.seancheatham" %% "firebase-akka-streams" % "0.0.1"`