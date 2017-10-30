package com.seancheatham.firebase.streams

import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.typesafe.config.Config
import org.scalatest.{BeforeAndAfterAll, WordSpec}
import play.api.libs.json.JsString

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

class FirebaseClientSpec extends WordSpec with BeforeAndAfterAll {

  private implicit val system: ActorSystem = ActorSystem()
  implicit private val mat: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher

  private implicit val config: Config =
    system.settings.config

  private val databaseUrl =
    config.getString("firebase.database-url")

  private val serviceAccountKeyPath =
    Paths.get(
      config.getString("firebase.service-account-key-path")
    )

  private val client: FirebaseClient =
    Await.result(
      FirebaseClient.fromGoogleServicesJson(
        databaseUrl,
        serviceAccountKeyPath
      ),
      10.seconds
    )

  private val basePath =
    "/FIREBASE_AKKA_STREAMS_TEST"

  implicit private val timeout: FiniteDuration = 100.seconds

  "The Firebase client" can {
    "write a value to the database" in {
      val path =
        basePath + "/basicWrite"
      val flow =
        client.valueFlow(path)

      val runResult =
        Await.result(
          Source.single(Some(JsString("Test Value")))
            .via(flow)
            .runWith(Sink.head),
          timeout
        )

      assert(runResult == "basicWrite")

    }

//    "read that value back" in {
//      val path =
//        basePath + "/basicWrite"
//      val source =
//        client.valueSource(path)
//
//      val runResult =
//        Await.result(
//          source
//            .runWith(Sink.head),
//          timeout
//        )
//
//      assert(runResult.map(_.as[String]).contains("Test Value"))
//    }
//
//    "delete that value" in {
//      val path =
//        basePath + "/basicWrite"
//      val flow =
//        client.valueFlow(path)
//
//      val runResult =
//        Await.result(
//          Source.single(None)
//            .via(flow)
//            .runWith(Sink.head),
//          timeout
//        )
//
//      assert(runResult == "basicWrite")
//    }
//
//    "verify the item's deletion" in {
//      val path =
//        basePath + "/basicWrite"
//      val source =
//        client.valueSource(path)
//
//      val runResult =
//        Await.result(
//          source
//            .runWith(Sink.head),
//          timeout
//        )
//
//      assert(runResult.isEmpty)
//    }
  }

}
