package com.foursquare.twofishes

sealed class Identity[A](protected val _value: A) {
  def =?(other: A) = _value == other
  def !=?(other: A) = _value != other
  def applyIf[B >: A](pred: Boolean, f: A => B): B = if (pred) f(_value) else _value
}

object Identity {
  implicit def wrapIdentity[A](anything: A): Identity[A] = new Identity(anything)
  implicit def unwrapIdentity[A](id: Identity[A]): A = id._value
}
