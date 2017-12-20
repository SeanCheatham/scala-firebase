package com.seancheatham.firebase.streams

import java.io.ByteArrayInputStream
import java.nio.file.{Path, Paths}

import akka.stream.Materializer
import akka.stream.scaladsl.{FileIO, Flow, Source}
import akka.{Done, NotUsed}
import com.google.auth.oauth2.GoogleCredentials
import com.google.firebase.database.FirebaseDatabase
import com.google.firebase.{FirebaseApp, FirebaseOptions}
import play.api.libs.json.{Reads, Writes}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}

/**
  * A base client which provides an API for generating Sources, Sinks, and Flows for working with
  * Firebase Realtime Database.
  *
  * @param app The FirebaseApp SDK client
  */
class FirebaseClient()(private implicit val app: FirebaseApp) {

  /**
    * Constructs a value-change Source at the given Firebase Ref.  This will attach a listener that pipes value changes
    * downstream.  If [[Some]] value is provided, it is the updated value.  If [[None]] is provided,
    * the value was deleted.
    *
    * @param path The Firebase Ref path (i.e. /threads/1/body)
    */
  def valueSource[T](path: String)
                    (implicit r: Reads[T]): Source[Option[T], NotUsed] =
    Source.fromGraph(new ValueListenerSource(path, app))

  /**
    * Constructs a child-change Source at the given Firebase Ref.  This will attach a listener that pipes value changes
    * downstream.  If [[Some]] value is provided, it is the updated value.  If [[None]] is provided,
    * the value was deleted.
    *
    * NOTE: This GraphStage assumes that the child keys will be "in order".  It is recommended
    * that realtime data inserts be created using the Firebase "push" mechanism to ensure the
    * generated keys follow a particular order.
    *
    * @param path The Firebase Ref path (i.e. /threads/1/comments)
    * @param bufferSize Rather than leaving an unbounded listener to attached, this Source simulates
    *                   backpressure by buffering X elements and detaching the listener until more are needed.
    * @param openEnded When true, this Source will not complete, and will instead emit elements until downstream cancels.
    *                  When false, only elements existing at the time the "last" element is read will be pushed downstream.
    *                  Once the final element is read, the stage will complete.
    */
  def childSource[T](path: String,
                     bufferSize: Int = 100,
                     openEnded: Boolean = true)
                    (implicit r: Reads[T]): Source[ChildListenerSource.ChildEvent[T], NotUsed] =
    Source.fromGraph(new ChildListenerSource(path, app, bufferSize, openEnded))

  /**
    * Perform a one-time read of the given path, producing an optional JsValue.
    *
    * @param path The path to read
    * @return a Future with an optional JsValue.  None indicates no value existed.
    */
  def readOnce[T](path: String)(implicit r: Reads[T]): Future[Option[T]] =
    SingleValueListener(path)

  /**
    * Write a value to the Firebase Database
    *
    * @param path  The full path to the value to be written
    * @param value The JSON value to write
    * @return a Future indicating success
    */
  def write[T](path: String, value: T)(implicit mat: Materializer, w: Writes[T]): Future[Done] = {
    implicit val ec: ExecutionContextExecutor = mat.executionContext
    FirebaseDatabase
      .getInstance(app)
      .getReference(path)
      .setValueAsync(jsonToAny(w.writes(value)))
      .asFuture
      .map(_ => Done)
  }

  /**
    * Deletes a value from the Firebase Database
    *
    * @param path The full path to the value to be deleted
    * @return a Future indicating success
    */
  def remove(path: String)(implicit mat: Materializer): Future[Done] = {
    implicit val ec: ExecutionContextExecutor = mat.executionContext
    FirebaseDatabase
      .getInstance(app)
      .getReference(path)
      .removeValueAsync()
      .asFuture
      .map(_ => Done)
  }

  /**
    * Constructs a flow which appends values to the given Firebase reference.  Emitted values are
    * the keys of the newly appeded items.
    *
    * @param path The Firebase Ref path (i.e. /threads/1/comments)
    */
  def push[T](path: String)(implicit w: Writes[T], mat: Materializer): Flow[T, String, NotUsed] =
    Flow[T]
      .mapAsync(1) { t =>
        implicit val ec: ExecutionContextExecutor = mat.executionContext
        val ref =
          FirebaseDatabase.getInstance(app)
            .getReference(path)
            .push()
        ref.setValueAsync(jsonToAny(w.writes(t)))
          .asFuture
          .map(_ => ref.getKey)
      }

}

object FirebaseClient {

  /**
    * Construct a FirebaseClient using the given URL and Credentials Path.  The Path
    * should point to a google-services.json file, which is read in by the Firebase SDK.
    *
    * Rather than permit the Firebase SDK to use blocking mechanisms to read in the credentials file, we use
    * Akka Streams FileIO to buffer the contents in memory, and then create a bytestream from the buffered contents.
    *
    * @param databaseUrl     The Database URL
    * @param credentialsJson The Path to a local google-services.json
    * @return a FirebaseClient
    */
  def fromGoogleServicesJson(databaseUrl: String, credentialsJson: Path)
                            (implicit mat: Materializer, ec: ExecutionContext): Future[FirebaseClient] =
    FileIO.fromPath(credentialsJson)
      .runReduce(_ ++ _)
      .map(bs => new ByteArrayInputStream(bs.toArray))
      .flatMap(is =>
        Future.fromTry(
          Try(GoogleCredentials.fromStream(is))
            .flatMap(credentials =>
              Try(
                FirebaseApp.initializeApp(
                  new FirebaseOptions.Builder()
                    .setCredentials(credentials)
                    .setDatabaseUrl(databaseUrl)
                    .build()
                )
              )
                .map(new FirebaseClient()(_))
            )
        )
      )

  /**
    * This variable is specific to Google.  It is a private value declared in their configuration class, so we'll simply
    * duplicate it here.
    *
    * @see [[com.google.auth.oauth2.DefaultCredentialsProvider#CREDENTIAL_ENV_VAR]]
    */
  private val defaultCredentialsEnvVariable =
    "GOOGLE_APPLICATION_CREDENTIALS"

  /**
    * Construct a FirebaseClient using the given URL.  The Firebase SDK will read in the credentials
    * file on its own from a pre-defined environment variable.  Use this implementation if running on
    * Google Cloud hardware, as it'll be included by default.
    *
    * @param databaseUrl The Database URL
    * @return a FirebaseClient
    */
  def default(databaseUrl: String)
             (implicit mat: Materializer, ec: ExecutionContext): Future[FirebaseClient] =
    Future.fromTry(
      Try(
        Option(System.getenv(defaultCredentialsEnvVariable))
      )
        .flatMap {
          case Some(v) =>
            Success(Paths.get(v))
          case _ =>
            Failure(new IllegalArgumentException(s"No env variable set at $defaultCredentialsEnvVariable"))
        }
    )
      .flatMap(fromGoogleServicesJson(databaseUrl, _))

}
