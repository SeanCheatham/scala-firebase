package com.seancheatham.firebase.streams

import java.nio.file.{Files, Path}

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.google.auth.oauth2.GoogleCredentials
import com.google.firebase.{FirebaseApp, FirebaseOptions}
import play.api.libs.json.JsValue

import scala.util.Try

/**
  * A base client which provides an API for generating Sources, Sinks, and Flows for working with
  * Firebase Realtime Database.
  *
  * @param app The FirebaseApp SDK client
  */
class FirebaseClient(private val app: FirebaseApp) {

  /**
    * Constructs a value-change Source at the given Firebase Ref.  This will attach a listener that pipes value changes
    * downstream.  If [[Some]] value is provided, it is the updated value.  If [[None]] is provided,
    * the value was deleted.
    *
    * @param path The Firebase Ref path (i.e. /threads/1/body)
    */
  def valueSource(path: String,
                  bufferSize: Int = 100): Source[Option[JsValue], NotUsed] =
    Source.fromGraph(new ValueListenerSource(path, app, bufferSize))

  /**
    * Constructs a child-change Source at the given Firebase Ref.  This will attach a listener that pipes value changes
    * downstream.  If [[Some]] value is provided, it is the updated value.  If [[None]] is provided,
    * the value was deleted.
    *
    * @param path The Firebase Ref path (i.e. /threads/1/comments)
    */
  def childSource(path: String,
                  bufferSize: Int = 100): Source[ChildListenerSource.ChildEvent, NotUsed] =
    Source.fromGraph(new ChildListenerSource(path, app, bufferSize))

  /**
    * Constructs a sink which writes/deletes values at the given Firebase Ref.
    *
    * @param path The Firebase Ref path (i.e. /threads/1/body)
    */
  def valueSink(path: String): Sink[Option[JsValue], NotUsed] =
    Sink.fromGraph(new ValueSink(path, app))

  /**
    * Constructs a flow which appends values to the given Firebase reference.  Emitted values are
    * the keys of the newly appeded items.
    *
    * @param path The Firebase Ref path (i.e. /threads/1/comments)
    */
  def push(path: String): Flow[JsValue, String, NotUsed] =
    Flow.fromGraph(new PushFlow(path, app))

}

object FirebaseClient {

  /**
    * Construct a FirebaseClient using the given URL and Credentials Path.  The Path
    * should point to a google-services.json file, which is read in by the Firebase SDK.
    *
    * @param databaseUrl     The Database URL
    * @param credentialsJson The Path to a local google-services.json
    * @return a FirebaseClient
    */
  def fromGoogleServicesJson(databaseUrl: String,
                             credentialsJson: Path): Try[FirebaseClient] =
    Try(GoogleCredentials.fromStream(Files.newInputStream(credentialsJson)))
      .flatMap(credentials =>
        Try(
          FirebaseApp.initializeApp(
            new FirebaseOptions.Builder()
              .setCredentials(credentials)
              .setDatabaseUrl(databaseUrl)
              .build()
          )
        )
      )
      .map(new FirebaseClient(_))

  /**
    * Construct a FirebaseClient using the given URL.  The Firebase SDK will read in the credentials
    * file on its own from a pre-defined environment variable.  Use this implementation if running on
    * Google Cloud hardware, as it'll be included by default.
    *
    * @param databaseUrl The Database URL
    * @return a FirebaseClient
    */
  def default(databaseUrl: String): Try[FirebaseClient] =
    Try(GoogleCredentials.getApplicationDefault)
      .flatMap(credentials =>
        Try(
          FirebaseApp.initializeApp(
            new FirebaseOptions.Builder()
              .setCredentials(credentials)
              .setDatabaseUrl(databaseUrl)
              .build()
          )
        )
      )
      .map(new FirebaseClient(_))

}
