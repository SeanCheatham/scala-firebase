package com.seancheatham.firebase.streams

import akka.stream.{KillSwitches, UniqueKillSwitch}
import akka.stream.scaladsl.{Keep, Sink, Source}
import org.scalatest.{BeforeAndAfterAll, WordSpec}
import play.api.libs.json.{JsNumber, JsValue}

import scala.concurrent.Await

class ChildSourceSpec extends WordSpec with BeforeAndAfterAll {

  import fixtures.TestFirebaseClient._

  override def beforeAll(): Unit = {
    Await.result(
      client.remove(basePath + "/childTest"),
      timeout
    )
  }

  override def afterAll(): Unit = {
    Await.result(
      client.remove(basePath + "/childTest"),
      timeout
    )
  }

  "A Child Source" can {

    "observe child added while adding elements" in {
      var lastRead: Option[(String, JsValue)] =
        None
      val source =
        client.childSource[JsValue](basePath + "/childTest", 5)
          .collect {
            case c: ChildListenerSource.ChildAdded[JsValue] =>
              c
          }

      val sink =
        Sink.foreach[ChildListenerSource.ChildAdded[JsValue]](e =>
          lastRead = Some((e.key, e.value))
        )

      val (killSwitch: UniqueKillSwitch, _) =
        source
          .viaMat(KillSwitches.single)(Keep.right)
          .toMat(sink)(Keep.both)
          .run()

      val k0 =
        Await.result(
          Source.single(JsNumber(0))
            .via(client.push(basePath + "/childTest"))
            .runWith(Sink.head),
          timeout
        )

      Thread.sleep(3000)

      assert(lastRead.contains((k0, JsNumber(0))))

      val k1 =
        Await.result(
          Source.single(JsNumber(1))
            .via(client.push(basePath + "/childTest"))
            .runWith(Sink.head),
          timeout
        )

      Thread.sleep(3000)

      assert(lastRead.contains((k1, JsNumber(1))))

      val k2 =
        Await.result(
          Source.single(JsNumber(2))
            .via(client.push(basePath + "/childTest"))
            .runWith(Sink.head),
          timeout
        )

      Thread.sleep(3000)

      assert(lastRead.contains((k2, JsNumber(2))))

      killSwitch.shutdown()

      assert(true)

    }
  }

}
