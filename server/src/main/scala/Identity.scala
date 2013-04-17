package com.foursquare.twofishes

sealed class Identity[A](protected val _value: A) {
  def =?(other: A) = _value == other
  def !=?(other: A) = _value != other
}

object Identity {
  implicit def wrapIdentity[A](anything: A): Identity[A] = new Identity(anything)
  implicit def unwrapIdentity[A](id: Identity[A]): A = id._value
}