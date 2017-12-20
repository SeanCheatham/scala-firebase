package com.seancheatham.firebase.streams

import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.{KillSwitches, UniqueKillSwitch}
import org.scalatest.{BeforeAndAfterAll, WordSpec}
import play.api.libs.json.{JsString, JsValue}

import scala.concurrent.Await

class ValueSourceSpec extends WordSpec with BeforeAndAfterAll {

  import fixtures.TestFirebaseClient._

  override def beforeAll(): Unit = {
    Await.result(
      client.remove(basePath + "/valueTest"),
      timeout
    )
  }

  override def afterAll(): Unit = {
    Await.result(
      client.remove(basePath + "/valueTest"),
      timeout
    )
  }

  "A Value Source" can {
    var lastRead: Option[Option[JsValue]] =
      None
    val source =
      client.valueSource[JsValue](basePath + "/valueTest")

    val sink =
      Sink.foreach[Option[JsValue]](e => lastRead = Some(e))

    val (killSwitch: UniqueKillSwitch, _) =
      source
        .viaMat(KillSwitches.single)(Keep.right)
        .toMat(sink)(Keep.both)
        .run()

    "observe an initially empty value" in {
      Thread.sleep(3000)

      assert(lastRead.contains(None))
    }

    "observe a value write" in {
      Await.result(
        client.write(basePath + "/valueTest", JsString("t0")),
        timeout
      )

      Thread.sleep(3000)

      assert(lastRead.contains(Some(JsString("t0"))))
    }

    "observe another value write" in {
      Await.result(
        client.write(basePath + "/valueTest", JsString("t1")),
        timeout
      )

      Thread.sleep(3000)

      assert(lastRead.contains(Some(JsString("t1"))))
    }

    "observe a value deletion" in {
      Await.result(
        client.remove(basePath + "/valueTest"),
        timeout
      )

      Thread.sleep(3000)

      assert(lastRead.contains(None))
    }

    "observe a value being written again" in {
      Await.result(
        client.write(basePath + "/valueTest", JsString("t2")),
        timeout
      )

      Thread.sleep(3000)

      assert(lastRead.contains(Some(JsString("t2"))))
    }

    "observe another deletion" in {
      Await.result(
        client.remove(basePath + "/valueTest"),
        timeout
      )

      Thread.sleep(3000)

      assert(lastRead.contains(None))
    }

    "shutdown" in {
      killSwitch.shutdown()

      assert(true)
    }
  }

}
