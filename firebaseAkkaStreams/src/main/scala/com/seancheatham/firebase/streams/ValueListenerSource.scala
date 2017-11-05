package com.seancheatham.firebase.streams

import akka.stream.stage.{AsyncCallback, GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.google.firebase.FirebaseApp
import com.google.firebase.database.{DataSnapshot, DatabaseError, FirebaseDatabase, ValueEventListener}
import play.api.libs.json.{JsValue, Reads}

/**
  * A GraphStage which attaches a Firebase Value Event Listener to the given path.  Elements emitted by Firebase
  * are queued up, with old entries being dropped if they exceed the buffer size.  When demand is ready, the values
  * are pushed downstream.
  *
  * @param path        A Firebase reference path
  * @param firebaseApp An initialized Firebase App
  */
class ValueListenerSource[T](path: String,
                             firebaseApp: => FirebaseApp)
                            (implicit r: Reads[T]) extends GraphStage[SourceShape[Option[T]]] {

  private val out =
    Outlet[Option[T]]("FirebaseValue.out")

  override val shape =
    SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with OutHandler {
      private val database =
        FirebaseDatabase.getInstance(firebaseApp)

      private val reference =
        database.getReference(path)

      private var readBuffer: Option[Option[JsValue]] =
        None

      private var listenerCallback: AsyncCallback[Option[JsValue]] = _

      private var errorCallback: AsyncCallback[Throwable] = _

      private val listener =
        new ValueEventListener {
          override def onCancelled(error: DatabaseError): Unit =
            errorCallback.invoke(error.toException)

          override def onDataChange(snapshot: DataSnapshot): Unit = {
            listenerCallback
              .invoke(
                Option(snapshot.getValue())
                  .map(anyToJson)
              )
          }
        }

      private var listenerAttached =
        false

      override def preStart(): Unit = {
        super.preStart()
        reference.addValueEventListener(listener)
        listenerCallback =
          getAsyncCallback { v =>
            if (!readBuffer.contains(v)) {
              readBuffer = Some(v)
              pushAvailable()
            }
          }
        errorCallback =
          getAsyncCallback { error =>
            if (listenerAttached) {
              reference.removeEventListener(listener)
              listenerAttached = false
            }
            fail(out, error)
          }
      }

      override def postStop(): Unit = {
        detachListener()
        super.postStop()
      }

      def pushAvailable(): Unit =
        readBuffer
          .foreach { v =>
            // Before pushing out, validate that the item can
            // be read as a [[T]].  If not, it'll be dropped anyway.
            push(out, v.flatMap(r.reads(_).asOpt))
            readBuffer = None
          }

      override def onDownstreamFinish(): Unit = {
        super.onDownstreamFinish()
        detachListener()
      }

      private def detachListener(): Unit =
        if(listenerAttached)
          reference.removeEventListener(listener)


      override def onPull(): Unit = {
        if (!listenerAttached) {
          reference.addValueEventListener(listener)
          listenerAttached = true
        }
        pushAvailable()
      }

      setHandler(out, this)
    }

}