package com.seancheatham.firebase.streams

import akka.stream._
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler}
import com.google.firebase.FirebaseApp
import com.google.firebase.database.DatabaseReference.CompletionListener
import com.google.firebase.database._
import play.api.libs.json.JsValue

/**
  * A GraphStage which sinks value writes/deletes to the given Firebase reference.  A value of None
  * indicates that the value should be deleted.  Otherwise, the given value will be written to the database.
  *
  * @param path        A Firebase reference path
  * @param firebaseApp An initialized Firebase App
  */
class ValueSink(path: String,
                firebaseApp: => FirebaseApp) extends GraphStage[SinkShape[Option[JsValue]]] {

  private val in =
    Inlet[Option[JsValue]]("FirebaseValue.in")

  override val shape =
    SinkShape(in)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      private val database =
        FirebaseDatabase.getInstance(firebaseApp)

      private val reference =
        database.getReference(path)

      private val defaultCompletionListener =
        new CompletionListener {
          override def onComplete(error: DatabaseError, ref: DatabaseReference): Unit =
            Option(error) match {
              case Some(e) =>
                failStage(e.toException)
              case None =>
                tryPull(in)
            }
        }

      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            val value = grab(in)
            value match {
              case Some(v) =>
                reference.setValue(jsonToAny(v), defaultCompletionListener)
              case _ =>
                reference.removeValue(defaultCompletionListener)
            }
          }
        }
      )

    }

}
