package com.seancheatham.firebase.streams

import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.google.firebase.FirebaseApp
import com.google.firebase.database._
import play.api.libs.json.JsValue

/**
  * A GraphStage which attaches a Firebase Child Event Listener to the given path.  Any changes
  * to child elements at that path will be piped via this stage.  Elements emitted by Firebase
  * are queued up, with old entries being dropped if they exceed the buffer size.  When demand is ready, the values
  * are pushed downstream.
  *
  * @param path        A Firebase reference path
  * @param firebaseApp An initialized Firebase App
  * @param bufferSize  The internal buffer size
  */
class ChildListenerSource(path: String,
                          firebaseApp: => FirebaseApp,
                          bufferSize: Int) extends GraphStage[SourceShape[ChildListenerSource.ChildEvent]] {

  private val out =
    Outlet[ChildListenerSource.ChildEvent]("FirebaseChild.out")

  override val shape =
    SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      private val database =
        FirebaseDatabase.getInstance(firebaseApp)

      private val reference =
        database.getReference(path)

      // TODO: Concurrency trainwreck waiting to happen.  Perhaps wrap this functionality in an actor
      private val buffer =
        scala.collection.mutable.Queue.empty[ChildListenerSource.ChildEvent]

      private def insertIntoBuffer(item: ChildListenerSource.ChildEvent): Unit = {
        if (buffer.size >= bufferSize)
          buffer.dequeue()
        buffer.enqueue(item)
        pushAvailable()
      }

      private val listener =
        new ChildEventListener {
          override def onCancelled(error: DatabaseError): Unit =
            fail(out, error.toException)

          override def onChildChanged(snapshot: DataSnapshot, previousChildName: String): Unit =
            insertIntoBuffer(
              ChildListenerSource.ChildChanged(anyToJson(snapshot.getValue), Option(previousChildName))
            )

          override def onChildMoved(snapshot: DataSnapshot, previousChildName: String): Unit =
            insertIntoBuffer(
              ChildListenerSource.ChildMoved(anyToJson(snapshot.getValue), Option(previousChildName))
            )

          override def onChildAdded(snapshot: DataSnapshot, previousChildName: String): Unit =
            insertIntoBuffer(
              ChildListenerSource.ChildAdded(anyToJson(snapshot.getValue), Option(previousChildName))
            )

          override def onChildRemoved(snapshot: DataSnapshot): Unit =
            insertIntoBuffer(
              ChildListenerSource.ChildRemoved(snapshot.getKey)
            )
        }

      override def preStart(): Unit = {
        super.preStart()
        reference.addChildEventListener(listener)
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

object ChildListenerSource {
  sealed trait ChildEvent
  case class ChildAdded(value: JsValue, previousName: Option[String]) extends ChildEvent
  case class ChildChanged(value: JsValue, previousName: Option[String]) extends ChildEvent
  case class ChildMoved(value: JsValue, previousName: Option[String]) extends ChildEvent
  case class ChildRemoved(name: String) extends ChildEvent
}