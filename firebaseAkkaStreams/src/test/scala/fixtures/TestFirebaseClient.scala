package fixtures

import java.nio.file.{Path, Paths}

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.seancheatham.firebase.streams.FirebaseClient
import com.typesafe.config.Config

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

object TestFirebaseClient {

  implicit val system: ActorSystem =
    ActorSystem()

  implicit val mat: ActorMaterializer =
    ActorMaterializer()

  implicit val ec: ExecutionContext =
    system.dispatcher

  implicit val config: Config =
    system.settings.config

  val databaseUrl: String =
    config.getString("firebase.database-url")

  val serviceAccountKeyPath: Path =
    Paths.get(
      config.getString("firebase.service-account-key-path")
    )

  val client: FirebaseClient =
    Await.result(
      FirebaseClient.fromGoogleServicesJson(
        databaseUrl,
        serviceAccountKeyPath
      ),
      10.seconds
    )

  val basePath: String =
    "/FIREBASE_AKKA_STREAMS_TEST"

  implicit val timeout: FiniteDuration =
    100.seconds

}
