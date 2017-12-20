package com.seancheatham.firebase.streams

import akka.Done
import org.scalatest.{BeforeAndAfterAll, WordSpec}
import play.api.libs.json.{JsObject, JsString, Json}

import scala.concurrent.Await

class BasicOperationsSpec extends WordSpec with BeforeAndAfterAll {

  import fixtures.TestFirebaseClient._

  override def beforeAll(): Unit = {
    Await.result(
      client.remove(basePath + "/writeTest"),
      timeout
    )
    Await.result(
      client.remove(basePath + "/objectWrite"),
      timeout
    )
  }

  override def afterAll(): Unit = {
    Await.result(
      client.remove(basePath + "/writeTest"),
      timeout
    )
    Await.result(
      client.remove(basePath + "/objectWrite"),
      timeout
    )
  }

  "The Firebase client" can {
    "write a value to the database" in {
      val path =
        basePath + "/writeTest"

      val runResult =
        Await.result(
          client.write(path, JsString("Test Value")),
          timeout
        )

      assert(runResult == Done)

    }

    "read that value back" in {
      val path =
        basePath + "/writeTest"

      val runResult =
        Await.result(
          client.readOnce[String](path),
          timeout
        )

      assert(runResult.contains("Test Value"))
    }

    "delete that value" in {
      val path =
        basePath + "/writeTest"

      val runResult =
        Await.result(
          client.remove(path),
          timeout
        )

      assert(runResult == Done)

      val checkResult =
        Await.result(
          client.readOnce[String](path),
          timeout
        )

      assert(checkResult.isEmpty)
    }

    "write nested JSON values" in {
      val path =
        basePath + "/objectWrite"
      val json =
        Json.obj(
          "v1" -> Json.obj(
            "v11" -> Json.obj(
              "v111" -> 4,
              "v112" -> "Test"
            ),
            "v12" -> Json.obj(
              "v121" -> true,
              "v122" -> Seq(1, 2, 3)
            )
          )
        )

      val runResult =
        Await.result(
          client.write(path, json),
          timeout
        )

      assert(runResult == Done)
    }

    "read nested json values" in {
      val v111Read =
        Await.result(
          client.readOnce[Int](basePath + "/objectWrite/v1/v11/v111"),
          timeout
        )
      assert(v111Read.contains(4))

      val v12Read =
        Await.result(
          client.readOnce[JsObject](basePath + "/objectWrite/v1/v12"),
          timeout
        )
      v12Read match {
        case Some(o) =>
          assert((o \ "v121").as[Boolean])
          assert((o \ "v122").as[Seq[Int]] == Seq(1, 2, 3))
        case _ =>
          assert(false)
      }

      val objectRead =
        Await.result(
          client.readOnce[JsObject](basePath + "/objectWrite"),
          timeout
        )
      objectRead match {
        case Some(o) =>
          assert((o \ "v1" \ "v11" \ "v112").as[String] == "Test")
        case _ =>
          assert(false)
      }
    }

    "delete individual nested values" in {
      val v111Delete =
        Await.result(
          client.remove(basePath + "/objectWrite/v1/v11/v111"),
          timeout
        )

      assert(v111Delete == Done)

      val v111Read =
        Await.result(
          client.readOnce[String](basePath + "/objectWrite/v1/v11/v111"),
          timeout
        )

      assert(v111Read.isEmpty)
    }

    "delete entire objects" in {
      val objectDelete =
        Await.result(
          client.remove(basePath + "/objectWrite"),
          timeout
        )

      assert(objectDelete == Done)

      val objectRead =
        Await.result(
          client.readOnce[JsObject](basePath + "/objectWrite"),
          timeout
        )

      assert(objectRead.isEmpty)
    }
  }
}