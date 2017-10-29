package com.seancheatham.firebase

import play.api.libs.json._

package object streams {

  implicit class KeyHelper(key: Seq[String]) {
    def keyify: String =
      key mkString "/"
  }

  implicit class JsHelper(v: JsValue) {
    def toOption: Option[JsValue] =
      v match {
        case JsNull =>
          None
        case x =>
          Some(x)
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
