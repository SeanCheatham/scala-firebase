package com.seancheatham.firebase.streams

import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.google.firebase.FirebaseApp
import com.google.firebase.database.{DataSnapshot, DatabaseError, FirebaseDatabase, ValueEventListener}
import play.api.libs.json.JsValue

/**
  * A GraphStage which attaches a Firebase Value Event Listener to the given path.  Elements emitted by Firebase
  * are queued up, with old entries being dropped if they exceed the buffer size.  When demand is ready, the values
  * are pushed downstream.
  *
  * @param path        A Firebase reference path
  * @param firebaseApp An initialized Firebase App
  * @param bufferSize  The internal buffer size
  */
class ValueListenerSource(path: String,
                          firebaseApp: => FirebaseApp,
                          bufferSize: Int) extends GraphStage[SourceShape[Option[JsValue]]] {

  private val out =
    Outlet[Option[JsValue]]("FirebaseValue.out")

  override val shape =
    SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      private val database =
        FirebaseDatabase.getInstance(firebaseApp)

      private val reference =
        database.getReference(path)

      private val buffer =
        scala.collection.mutable.Queue.empty[Option[JsValue]]

      private val listener =
        new ValueEventListener {
          override def onCancelled(error: DatabaseError): Unit =
            fail(out, error.toException)

          override def onDataChange(snapshot: DataSnapshot): Unit = {
            if (buffer.size >= bufferSize)
              buffer.dequeue()
            buffer.enqueue(
              if (snapshot.exists())
                Some(
                  anyToJson(snapshot.getValue)
                )
              else
                None
            )
            pushAvailable()
          }
        }

      override def preStart(): Unit = {
        super.preStart()
        reference.addValueEventListener(listener)
      }

      override def postStop(): Unit = {
        reference.removeEventListener(listener)
        super.postStop()
      }

      def pushAvailable(): Unit =
        while (buffer.nonEmpty && isAvailable(out))
          push(out, buffer.dequeue())

      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit =
            pushAvailable()
        }
      )

    }

}