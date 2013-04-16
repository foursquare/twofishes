package com.foursquare.twofishes.util

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

object ByteUtils {
  def longToBytes(l: Long): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(baos)
    dos.writeLong(l)
    baos.toByteArray()
  }

  def intToBytes(l: Int): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(baos)
    dos.writeInt(l)
    baos.toByteArray()
  }

  def getLongFromBytes(bytes: Array[Byte]): Long = {
    val bais = new ByteArrayInputStream(bytes)
    (new DataInputStream(bais)).readLong()
  }
}
