package com.foursquare.twofishes.util

import scala.collection.TraversableLike
import scala.collection.generic.GenericTraversableTemplate

object Lists {
  trait Implicits {
    implicit def seq2FSTraversable[CC[X] <: Traversable[X], T, Repr <: TraversableLike[T, Repr]](xs: TraversableLike[T, Repr] with GenericTraversableTemplate[T, CC]): FSTraversable[CC, T, Repr] = new FSTraversable[CC, T, Repr](xs)
  }

  object Implicits extends Implicits
}

class FSTraversableOnce[T](xs: TraversableOnce[T]) {
  def toVector: Vector[T] =
    if (xs.isInstanceOf[Vector[_]])
      xs.asInstanceOf[Vector[T]]
    else
      (Vector.newBuilder[T] ++= xs).result
}


class FSTraversable[CC[X] <: Traversable[X], T, Repr <: TraversableLike[T, Repr]](xs: TraversableLike[T, Repr] with GenericTraversableTemplate[T, CC]) extends FSTraversableOnce(xs) {
  def has(e: T): Boolean = xs match {
    case xSet: Set[_] => xSet.asInstanceOf[Set[T]].contains(e)
    case xMap: Map[_,_] => {
      val p = e.asInstanceOf[Pair[Any,Any]]
      xMap.asInstanceOf[Map[Any,Any]].get(p._1) == Some(p._2)
    }
    case _ => xs.exists(_ == e)
  }
}