package com.seancheatham.firebase

import java.util.concurrent.Executor

import play.api.libs.json._

import scala.concurrent.{Future, Promise}
import scala.util.Try

package object streams {

  implicit class JsHelper(v: JsValue) {
    def toOption: Option[JsValue] =
      v match {
        case JsNull =>
          None
        case x =>
          Some(x)
      }
  }

  implicit class GoogleFutureHelper[T](f: com.google.api.core.ApiFuture[T]) {
    def asFuture(implicit ec: Executor): Future[T] = {
      val promise =
        Promise[T]()
      f.addListener(
        new Runnable {
          override def run(): Unit = {
            if (!f.isDone)
              promise.failure(new IllegalStateException("Google Listener returned but task not marked done"))
            else
              promise.complete(
                Try(f.get())
              )
          }
        },
        ec
      )
      promise.future
    }
  }

  /**
    * Converts a value returned by Firebase into a [[JsValue]]
    *
    * @param any The value returned by Firebase
    * @return a JsValue
    */
  def anyToJson(any: Any): JsValue =
    any match {
      case null =>
        JsNull
      case v: Double =>
        JsNumber(v)
      case v: Long =>
        JsNumber(v)
      case s: String =>
        JsString(s)
      case v: Boolean =>
        JsBoolean(v)
      case v: java.util.HashMap[String@unchecked, _] =>
        import scala.collection.JavaConverters._
        JsObject(v.asScala.mapValues(anyToJson))
      case v: java.util.ArrayList[_] =>
        import scala.collection.JavaConverters._
        JsArray(v.asScala.map(anyToJson))
    }

  /**
    * Converts the given [[JsValue]] into a consumable format by the Firebase API
    *
    * @param json The JSON value to convert
    * @return a value consumable by the Firebase API
    */
  def jsonToAny(json: JsValue): Any =
    json match {
      case JsNull =>
        null
      case v: JsNumber =>
        val long =
          v.value.longValue()
        val double =
          v.value.doubleValue()
        if (long == double)
          long
        else
          double
      case v: JsString =>
        v.value
      case v: JsBoolean =>
        v.value
      case v: JsArray =>
        import scala.collection.JavaConverters._
        v.value.toVector.map(jsonToAny).asJava
      case v: JsObject =>
        import scala.collection.JavaConverters._
        v.value.mapValues(jsonToAny).asJava
    }
}
