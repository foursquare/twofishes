package com.foursquare.twofishes.util

import com.foursquare.twofishes._
import org.bson.types.ObjectId

case class StoredFeatureId(
  namespace: String,
  id: String) {
  override def toString = "%s:%s".format(namespace, id)
}

object StoredFeatureId {
  def fromString(s: String, defaultNamespace: Option[String] = None) = {
    val parts = s.split(":")
    val k = if (parts.size == 2) {
      parts(0)
    } else {
      defaultNamespace.get
    }

    val v = if (parts.size == 2) {
      parts(1)
    } else {
      parts(0)
    }

    StoredFeatureId(k, v)
  }
}

object GeocodeFeatureIdUtils {
  def objectIdFromLong(n: Long) = {
    val bytes = BigInt(n).toByteArray
    val arr = bytes.reverse.padTo(12, 0: Byte).reverse
    new ObjectId(arr)
  }

  def objectIdFromFeatureId(geonameId: StoredFeatureId, providerMapping: Map[String, Int]) = {
    for {
      idInt <- Helpers.TryO(geonameId.id.toInt)
      providerId <- providerMapping.get(geonameId.namespace)
    } yield {
      objectIdFromLong((providerId.toLong << 32) + idInt)
    }
  }
}