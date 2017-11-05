package com.seancheatham.firebase.streams

import akka.Done
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{KillSwitches, UniqueKillSwitch}
import org.scalatest.{BeforeAndAfterAll, WordSpec}
import play.api.libs.json.{JsNumber, JsValue}

import scala.concurrent.Await

class PushSpec extends WordSpec with BeforeAndAfterAll {

  import fixtures.TestFirebaseClient._

  override def beforeAll(): Unit = {
    Await.result(
      client.remove(basePath + "/basicPush"),
      timeout
    )
  }

  override def afterAll(): Unit = {
    Await.result(
      client.remove(basePath + "/basicPush"),
      timeout
    )
  }

  "The Firebase client" can {

    "push values" in {
      val path = basePath + "/basicPush"

      val runResult =
        Await.result(
          Source(0 to 10)
            .via(client.push(path))
            .runWith(Sink.seq),
          timeout
        )

      assert(runResult.length == 10)

      runResult.zipWithIndex
        .foreach {
          case (id, value) =>
            val fValue =
              Await.result(
                client.readOnce[Int](path + "/" + id),
                timeout
              )
            assert(fValue.contains(value))
        }

    }

    "fetch child values" in {
      val path = basePath + "/basicPush"

      val runResult =
        Await.result(
          client.childSource[Int](path, 5)
            .take(10)
            .runWith(Sink.seq),
          timeout
        )

      runResult
        .collect {
          case ChildListenerSource.ChildAdded(_, v, _) => v
        }
        .zipWithIndex
        .foreach {
          case (v, idx) =>
            assert(v == idx)
        }

      assert(runResult.size == 10)
    }

    "delete child values" in {
      val result =
        Await.result(
          client.remove(basePath + "/basicPush"),
          timeout
        )
      assert(result == Done)
    }

    "observe child added while adding elements" in {
      var lastRead: Option[(String, JsValue)] =
        None
      val source =
        client.childSource[JsValue](basePath + "/pushTest", 5)
          .collect {
            case c: ChildListenerSource.ChildAdded[JsValue] =>
              c
          }

      val sink =
        Sink.foreach[ChildListenerSource.ChildAdded[JsValue]](e =>
          lastRead = Some((e.key, e.value))
        )

      val (killSwitch: UniqueKillSwitch, x) =
        source
          .viaMat(KillSwitches.single)(Keep.right)
          .toMat(sink)(Keep.both)
          .run()

      val k0 =
        Await.result(
          Source.single(JsNumber(0))
            .via(client.push(basePath + "/pushTest"))
            .runWith(Sink.head),
          timeout
        )

      Thread.sleep(3000)

      assert(lastRead.contains((k0, JsNumber(0))))

      val k1 =
        Await.result(
          Source.single(JsNumber(1))
            .via(client.push(basePath + "/pushTest"))
            .runWith(Sink.head),
          timeout
        )

      Thread.sleep(3000)

      assert(lastRead.contains((k1, JsNumber(1))))

      val k2 =
        Await.result(
          Source.single(JsNumber(2))
            .via(client.push(basePath + "/pushTest"))
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
