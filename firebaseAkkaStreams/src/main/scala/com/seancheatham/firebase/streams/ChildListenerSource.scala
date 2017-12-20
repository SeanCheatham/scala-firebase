package com.seancheatham.firebase.streams

import java.util.concurrent.TimeoutException

import akka.stream.stage._
import akka.stream.{Attributes, Outlet, SourceShape}
import com.google.firebase.FirebaseApp
import com.google.firebase.database._
import play.api.libs.json.Reads

import scala.concurrent.{ExecutionContextExecutor, Future, Promise}
import scala.util.Success

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
                             bufferSize: Int,
                             openEnded: Boolean)(implicit r: Reads[T]) extends GraphStage[SourceShape[ChildListenerSource.ChildEvent[T]]] {

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

      private var finalKey: Option[String] =
        None

      private var setFinalKeyAttempted: Boolean =
        false

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
            if (!lastReadKey.contains(snapshot.getKey))
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

      private var shouldComplete: Boolean = false

      private def updateKeyState(key: String): Unit = {
        log.info(s"Updating key state to $key. Previous as $lastReadKey")
        batchReadCount += 1
        lastReadKey = Some(key)
        if (batchReadCount >= bufferSize) {
          detachCurrentListener()
          lastQuery = None
          batchReadCount = 0
        }

        if (finalKey.contains(key)) {
          shouldComplete = true
          done()
        }
      }

      private def insertIntoBuffer(item: ChildListenerSource.ChildEvent[T]): Unit = {
        if (buffer.size >= bufferSize)
          buffer.dequeue()
        buffer.enqueue(item)
        pushAvailable()
      }

      private def setFinalKey(): Future[String] = {
        setFinalKeyAttempted = true
        val query =
          reference.orderByKey().limitToLast(1)
        val promise =
          Promise[String]()
        val finalKeyListener =
          new ChildEventListener {
            override def onCancelled(error: DatabaseError): Unit =
              promise.tryFailure(error.toException)

            override def onChildChanged(snapshot: DataSnapshot, previousChildName: String): Unit = ()

            override def onChildMoved(snapshot: DataSnapshot, previousChildName: String): Unit = ()

            override def onChildAdded(snapshot: DataSnapshot, previousChildName: String): Unit = {
              promise.trySuccess(snapshot.getKey)
            }

            override def onChildRemoved(snapshot: DataSnapshot): Unit = ()
          }
        query.addChildEventListener(finalKeyListener)
        import scala.concurrent.duration._
        materializer.scheduleOnce(
          2.seconds,
          () => if (!promise.isCompleted)
            promise.tryFailure(
              new TimeoutException("Could not fetch last key, likely an empty collection.")
            )
        )
        implicit val ec: ExecutionContextExecutor = materializer.executionContext
        promise.future
          .andThen {
            case Success(k) =>
              finalKey = Some(k)
          }
          .andThen {
            case _ =>
              query.removeEventListener(finalKeyListener)
          }
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
        detachCurrentListener()
        super.postStop()
      }

      private def constructQuery(): Query = {
        log.info(s"Creating query starting at $lastReadKey with limit ${bufferSize + lastReadKey.size}")
        lastReadKey
          .fold[Query](reference.orderByKey())(reference.orderByKey().startAt)
          .limitToFirst(bufferSize + lastReadKey.size)
      }

      def pushAvailable(): Unit = {
        if (isAvailable(out) && buffer.nonEmpty) {
          val item = buffer.dequeue()
          log.info(s"Pushing $item")
          push(out, item)
        }
        if (shouldComplete && buffer.isEmpty)
          done()
      }

      override def onPull(): Unit =
        if (openEnded && !setFinalKeyAttempted) {
          implicit val ec: ExecutionContextExecutor = materializer.executionContext
          val cb = getAsyncCallback[Unit](_ => onPull())
          setFinalKey()
            .onComplete(_ => cb.invoke())
        } else {
          if (buffer.nonEmpty) {
            pushAvailable()
          } else if (lastQuery.isEmpty) {
            lastQuery = Some(constructQuery())
            lastQuery.foreach(_.addChildEventListener(listener))
          }
        }

      override def onDownstreamFinish(): Unit = {
        super.onDownstreamFinish()
        detachCurrentListener()
      }

      private def detachCurrentListener(): Unit =
        lastQuery.foreach(_.removeEventListener(listener))

      private def done(): Unit = {
        detachCurrentListener()
        if (buffer.isEmpty)
          completeStage()
        else
          pushAvailable()
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