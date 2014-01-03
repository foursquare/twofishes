package com.foursquare.twofishes.util

import scala.collection.TraversableLike
import scala.collection.generic.GenericTraversableTemplate

object Lists {
  trait Implicits {
    implicit def seq2FSTraversable[CC[X] <: Traversable[X], T, Repr <: TraversableLike[T, Repr]](xs: TraversableLike[T, Repr] with GenericTraversableTemplate[T, CC]): FSTraversable[CC, T, Repr] = new FSTraversable[CC, T, Repr](xs)

    implicit def opt2FSOpt[T](o: Option[T]) = new FSOption(o)
    implicit def fsopt2Opt[T](fso: FSOption[T]) = fso.opt
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

  /**
   * Return the smallest element from the Seq according to the supplied sort function.
   */
  def minByOption[U](f: T => U)(implicit ord: Ordering[U]): Option[T] = {
    var first = true
    var min: Option[T] = None
    var minValue: Option[U] = None

    for (x <- xs) {
      if (first) {
        min = Some(x)
        minValue = Some(f(x))
        first = false
      }

      val value = f(x)

      if (ord.lt(value, minValue.get)) {
        min = Some(x)
        minValue = Some(value)
      }
    }

    min
  }
}

class FSOption[T](val opt: Option[T]) {
  def has(e: T): Boolean = opt.exists(_ == e)
  def isEmptyOr(pred: T => Boolean): Boolean = opt.forall(pred)
  def unzipped[T1, T2](implicit asPair: (T) => (T1, T2)): (Option[T1], Option[T2]) = opt match {
    case Some(x) => {
      val pair = asPair(x)
      (Some(pair._1), Some(pair._2))
    }
    case None => (None, None)
  }
}
