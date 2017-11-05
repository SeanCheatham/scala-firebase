package com.seancheatham.firebase.streams

import com.google.firebase.FirebaseApp
import com.google.firebase.database.{DataSnapshot, DatabaseError, FirebaseDatabase, ValueEventListener}
import play.api.libs.json.Reads

import scala.concurrent.{Future, Promise}

object SingleValueListener {

  def apply[T](path: String)(implicit app: FirebaseApp, r: Reads[T]): Future[Option[T]] = {
    val reference =
      FirebaseDatabase.getInstance(app)
        .getReference(path)

    val promise =
      Promise[Option[T]]()

    val listener =
      new ValueEventListener {
        override def onCancelled(error: DatabaseError): Unit =
          promise.failure(error.toException)

        override def onDataChange(snapshot: DataSnapshot): Unit =
          promise.success(
            Option(snapshot.getValue)
              .flatMap(v => r.reads(anyToJson(v)).asOpt)
          )
      }

    reference.addListenerForSingleValueEvent(listener)

    promise.future
  }

}
