package com.seancheatham.firebase.streams

import akka.stream._
import akka.stream.stage._
import com.google.firebase.FirebaseApp
import com.google.firebase.database._
import play.api.libs.json.JsValue

import scala.concurrent.ExecutionContextExecutor
import scala.util.{Failure, Success, Try}

/**
  * A Flow GraphStage which writes/deletes to the given Firebase reference, and returns the key of the written value.  A value of None
  * indicates that the value should be deleted.  Otherwise, the given value will be written to the database.
  *
  * @param path        A Firebase reference path
  * @param firebaseApp An initialized Firebase App
  */
class ValueFlow(path: String,
                firebaseApp: => FirebaseApp) extends GraphStage[FlowShape[Option[JsValue], String]] {

  private val in =
    Inlet[Option[JsValue]]("FirebaseValue.in")

  private val out =
    Outlet[String]("FirebaseValue.out")

  override val shape =
    FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      private val database =
        FirebaseDatabase.getInstance(firebaseApp)

      private val reference =
        database.getReference(path)

      implicit var ec: ExecutionContextExecutor =
        _

      override def preStart(): Unit = {
        super.preStart()
        ec = materializer.executionContext
      }

      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            grab(in).map(jsonToAny) match {
              case Some(value) =>
                val callback =
                  getAsyncCallback[Try[_]] {
                    case Success(_) =>
                      push(out, reference.getKey)
                    case Failure(e) =>
                      failStage(e)
                  }
                reference.setValueAsync(value)
                  .asFuture
                  .onComplete(callback.invoke)
              case _ =>
                val callback =
                  getAsyncCallback[Try[_]] {
                    case Success(_) =>
                      push(out, reference.getKey)
                    case Failure(e) =>
                      failStage(e)
                  }
                reference.removeValueAsync()
                  .asFuture
                  .onComplete(callback.invoke)
            }
          }
        }
      )

      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit =
            pull(in)
        }
      )

    }

}
