package com.seancheatham.firebase.streams

import akka.stream.stage._
import akka.stream.{Attributes, Outlet, SourceShape}
import com.google.firebase.FirebaseApp
import com.google.firebase.database._
import play.api.libs.json.Reads

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
class ChildListenerSource[T](path: String,
                             firebaseApp: => FirebaseApp,
                             bufferSize: Int)(implicit r: Reads[T]) extends GraphStage[SourceShape[ChildListenerSource.ChildEvent[T]]] {

  private val out =
    Outlet[ChildListenerSource.ChildEvent[T]]("FirebaseChild.out")

  override val shape =
    SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogicWithLogging(shape) with OutHandler {
      private val database =
        FirebaseDatabase.getInstance(firebaseApp)

      private val reference =
        database.getReference(path)

      private val buffer =
        scala.collection.mutable.Queue.empty[ChildListenerSource.ChildEvent[T]]

      private var lastReadKey: Option[String] =
        None

      private var batchReadCount: Int =
        0

      private var lastQuery: Option[Query] =
        None

      private val listener: ChildEventListener =
        new ChildEventListener {
          override def onCancelled(error: DatabaseError): Unit =
            errorCallback.invoke(error.toException)

          override def onChildChanged(snapshot: DataSnapshot, previousChildName: String): Unit =
            r.reads(anyToJson(snapshot.getValue)).asOpt
              .foreach { v =>
                listenerCallback.invoke(
                  (
                    ChildListenerSource.ChildChanged(snapshot.getKey, v, Option(previousChildName)),
                    snapshot.getKey
                  )
                )
              }

          override def onChildMoved(snapshot: DataSnapshot, previousChildName: String): Unit =
            r.reads(anyToJson(snapshot.getValue)).asOpt
              .foreach { v =>
                listenerCallback.invoke(
                  (
                    ChildListenerSource.ChildMoved(snapshot.getKey, v, Option(previousChildName)),
                    snapshot.getKey
                  )
                )
              }

          override def onChildAdded(snapshot: DataSnapshot, previousChildName: String): Unit = {
            if(!lastReadKey.contains(snapshot.getKey))
              r.reads(anyToJson(snapshot.getValue)).asOpt
                .foreach { v =>
                  listenerCallback.invoke(
                    (
                      ChildListenerSource.ChildAdded(snapshot.getKey, v, Option(previousChildName)),
                      snapshot.getKey
                    )
                  )
                }
          }

          override def onChildRemoved(snapshot: DataSnapshot): Unit = {
            listenerCallback.invoke(
              (
                ChildListenerSource.ChildRemoved(snapshot.getKey, snapshot.getKey),
                snapshot.getKey
              )
            )
          }
        }

      private var listenerCallback: AsyncCallback[(ChildListenerSource.ChildEvent[T], String)] = _

      private var errorCallback: AsyncCallback[Throwable] = _

      private def updateKeyState(key: String): Unit = {
        log.info(s"Updating key state to $key. Previous as $lastReadKey")
        batchReadCount += 1
        lastReadKey = Some(key)
        if (batchReadCount >= bufferSize) {
          lastQuery.foreach(_.removeEventListener(listener))
          lastQuery = None
          batchReadCount = 0
        }
      }

      private def insertIntoBuffer(item: ChildListenerSource.ChildEvent[T]): Unit = {
        if (buffer.size >= bufferSize)
          buffer.dequeue()
        buffer.enqueue(item)
        pushAvailable()
      }

      override def preStart(): Unit = {
        listenerCallback =
          getAsyncCallback {
            case (event, key) =>
              updateKeyState(key)
              insertIntoBuffer(event)
          }
        errorCallback =
          getAsyncCallback { error =>
            lastQuery.foreach(_.removeEventListener(listener))
            lastQuery = None
            fail(out, error)
          }
      }

      override def postStop(): Unit = {
        lastQuery.foreach(_.removeEventListener(listener))
        super.postStop()
      }

      private def constructQuery(): Query = {
        log.info(s"Creating query starting at $lastReadKey with limit ${bufferSize + lastReadKey.size}")
        lastReadKey
          .fold[Query](reference.orderByKey())(reference.orderByKey().startAt)
          .limitToFirst(bufferSize + lastReadKey.size)
      }

      def pushAvailable(): Unit =
        if (isAvailable(out) && buffer.nonEmpty) {
          val item = buffer.dequeue()
          log.info(s"Pushing $item")
          push(out, item)
        }

      override def onPull(): Unit =
        if (buffer.nonEmpty) {
          pushAvailable()
        } else if (lastQuery.isEmpty) {
          lastQuery = Some(constructQuery())
          lastQuery.foreach(_.addChildEventListener(listener))
        }

      override def onDownstreamFinish(): Unit = {
        super.onDownstreamFinish()
        lastQuery.foreach(_.removeEventListener(listener))
      }

      setHandler(out, this)

    }

}

object ChildListenerSource {

  sealed trait ChildEvent[T] {
    val key: String
  }

  case class ChildAdded[T](key: String, value: T, previousName: Option[String]) extends ChildEvent[T]

  case class ChildChanged[T](key: String, value: T, previousName: Option[String]) extends ChildEvent[T]

  case class ChildMoved[T](key: String, value: T, previousName: Option[String]) extends ChildEvent[T]

  case class ChildRemoved[T](key: String, name: String) extends ChildEvent[T]

}