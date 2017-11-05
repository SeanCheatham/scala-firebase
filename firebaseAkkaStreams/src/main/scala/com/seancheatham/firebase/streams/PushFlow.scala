package com.seancheatham.firebase.streams

import akka.stream._
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import com.google.firebase.FirebaseApp
import com.google.firebase.database.DatabaseReference.CompletionListener
import com.google.firebase.database._
import play.api.libs.json.Writes

/**
  * A GraphStage which appends values to the given Firebase reference.  Values will be written
  * via Firebase's push method.  The generated key for the appended value is emitted downstream.
  *
  * @param path        A Firebase reference path
  * @param firebaseApp An initialized Firebase App
  */
class PushFlow[T](path: String,
                  firebaseApp: => FirebaseApp)(implicit w: Writes[T]) extends GraphStage[FlowShape[T, String]] {

  private val in =
    Inlet[T]("FirebasePush.in")

  private val out =
    Outlet[String]("FirebasePush.out")

  override val shape =
    FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      private val database =
        FirebaseDatabase.getInstance(firebaseApp)

      private val reference =
        database.getReference(path)

      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            val value = jsonToAny(w.writes(grab(in)))
            val pushRef = reference.push()
            val asyncCallbck = getAsyncCallback[String](push(out, _))
            pushRef.setValue(
              value,
              new CompletionListener {
                override def onComplete(error: DatabaseError, ref: DatabaseReference): Unit =
                  Option(error) match {
                    case Some(e) =>
                      failStage(e.toException)
                    case None =>
                      asyncCallbck.invoke(ref.getKey)
                  }
              }
            )
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
