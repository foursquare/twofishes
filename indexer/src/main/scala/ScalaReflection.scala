// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.common.reflection

import java.lang.reflect.{Field, Method}

/** Utilities for performing reflection on Scala classes.
  */
object ScalaReflection {
  /** Given a name, return the instance of the singleton object for
    * that name.
    */
  def objectFromName(name: String, classLoaderOpt: Option[ClassLoader] = None): AnyRef = {
    val clazz = classLoaderOpt match {
      case None => Class.forName(name + "$")
      case Some(loader) => Class.forName(name + "$", true, loader)
    }
    val moduleGetter = clazz.getDeclaredField("MODULE$")
    moduleGetter.get()
  }

  /** Get all the superclasses of a given Class
   */
  def getAncestors(clazz: Class[_]): Seq[Class[_]] = clazz match {
    case null => Vector.empty
    case c => c +: getAncestors(c.getSuperclass)
  }

  /** Get all the methods for a given Class
   */
  def getAllMethods(clazz: Class[_]): Seq[Method] = {
    getAncestors(clazz).flatMap(_.getDeclaredMethods)
  }

  def getAllFields(clazz: Class[_]): Seq[Field] = {
    getAncestors(clazz).flatMap(_.getDeclaredFields)
  }

  /** Call a private method on any instance with the given arguments
   */
  def privateMethodCall(instance: AnyRef)(methodName: String)(_args: Any*): Any = {
    val args = _args.map(_.asInstanceOf[AnyRef])
    val clazz = instance.getClass
    val methods = getAllMethods(clazz)
    val fields = getAllFields(clazz)
    val methodOpt = methods.find(_.getName == methodName)
    val fieldOpt = fields.find(_.getName == methodName)
    val hiddenFieldOpt = fields.find(_.getName.endsWith("$$" + methodName))

    methodOpt.orElse(fieldOpt).orElse(hiddenFieldOpt) match {
      case Some(method: Method) =>
        method.setAccessible(true)
        method.invoke(instance, args: _*)
      case Some(field: Field) if args.isEmpty =>
        field.setAccessible(true)
        field.get(instance)
      case None =>
        val errorMsg = "Method %s valid for arguments %s not found"
        throw new IllegalArgumentException(errorMsg.format(methodName, args.mkString("(", ", ", ")")))
    }
  }

  /** Check to see if A is instance of B by checking classOf[A] against classOf[B]. Example:
    *
    * trait Foo
    * abstract class Bar extends Foo
    * case class Baz(i: Int) extends Bar
    * isClassInstanceOf[Bar](classOf[Foo]) == false
    * isClassInstanceOf[Foo](classOf[Bar]) == true
    */
  def isClassInstanceOf[T: Manifest](clazz: Class[_]): Boolean = {
    val m = manifest[T]
    val instanceOfClass = m.runtimeClass
    if (clazz == instanceOfClass) true
    else if (clazz.getInterfaces.contains(instanceOfClass)) true
    else {
      val superClass = clazz.getSuperclass
      if (superClass == null) false
      else isClassInstanceOf[T](superClass)
    }
  }

}
