// Copyright 2014 Foursquare Labs Inc. All Rights Reserved.
package com.foursquare.twofishes.scalding

import java.io.{DataOutput, IOException, DataInput}

import com.foursquare.twofishes._
import org.apache.hadoop.io.WritableComparable

class PolygonMatchingKeyWritable(value: PolygonMatchingKey) extends WritableComparable[PolygonMatchingKeyWritable] {

  var s2CellIdValue = value.s2CellIdOption.getOrElse(0L)
  var woeTypeValue = value.woeTypeOption.map(_.getValue).getOrElse(0)

  def this() = this(PolygonMatchingKey(0L, YahooWoeType.UNKNOWN))

  def getKey(): PolygonMatchingKey = {
    PolygonMatchingKey(s2CellIdValue, YahooWoeType.findByIdOrUnknown(woeTypeValue))
  }

  @throws(classOf[IOException])
  override def readFields(in: DataInput) {
    s2CellIdValue = in.readLong
    woeTypeValue = in.readInt
  }

  @throws(classOf[IOException])
  override def write(out: DataOutput) {
    out.writeLong(s2CellIdValue)
    out.writeInt(woeTypeValue)
  }

  override def compareTo(o: PolygonMatchingKeyWritable): Int = {
    getKey().compare(o.getKey())
  }

  override def hashCode: Int = getKey().hashCode
  override def toString: String = getKey().toString
}