package com.seancheatham.firebase.streams

import akka.Done
import akka.stream.scaladsl.{Sink, Source}
import org.scalatest.{BeforeAndAfterAll, WordSpec}

import scala.concurrent.Await

class PushSpec extends WordSpec with BeforeAndAfterAll {

  import fixtures.TestFirebaseClient._

  override def beforeAll(): Unit = {
    Await.result(
      client.remove(basePath + "/pushTest"),
      timeout
    )
  }

  override def afterAll(): Unit = {
    Await.result(
      client.remove(basePath + "/pushTest"),
      timeout
    )
  }

  "The Firebase client" can {
    "push values" in {
      val path = basePath + "/pushTest"

      val runResult =
        Await.result(
          Source(0 until 10)
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
      val path = basePath + "/pushTest"

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
          client.remove(basePath + "/pushTest"),
          timeout
        )
      assert(result == Done)
    }

  }

}
