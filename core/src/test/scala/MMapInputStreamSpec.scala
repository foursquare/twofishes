package com.foursquare.twofishes

import java.io.{EOFException, File, FileOutputStream}
import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{LocalFileSystem, Path}
import org.specs2.mutable.Specification

class MMapInputStreamSpec extends Specification {
  val random = new scala.util.Random(7)

  def createTempFile(size: Int): (File, Array[Byte]) = {
    val temp = File.createTempFile("mmapinputstreamtest", null)
    val os = new FileOutputStream(temp)
    val randomBuf = new Array[Byte](size)
    random.nextBytes(randomBuf)
    os.write(randomBuf)
    os.close
    (temp, randomBuf)
  }

  def openWithHadoop(file: File) = {
    val fs = new LocalFileSystem
    val conf = new Configuration
    fs.initialize(URI.create("file:///"), conf)
    fs.open(new Path(file.toString))
  }

  def openWithMMap(file: File) = {
    new MMapInputStream(file.toString)
  }

  "should be able to read empty files" in {
    val (file, buf) = createTempFile(0)
    try {
      val mmapStream = openWithMMap(file)
      mmapStream.read() must_== -1
    } finally {
      file.delete
    }
  }

  "should be able to do read byte at a time" in {
    val Size = 1024
    val (file, buf) = createTempFile(Size)
    try {
      val mmapStream = openWithMMap(file)
      val hadoopStream = openWithHadoop(file)

      for {
        i <- 1 to Size
      } {
        mmapStream.read() must_== hadoopStream.read()
        // Position should be advanced.
        hadoopStream.getPos must_== i
        mmapStream.getPos must_== i
      }

      // EOF should be signaled.
      hadoopStream.read() must_== -1
      mmapStream.read() must_== -1
    } finally {
      file.delete
    }
  }

  "should be able to do reads and seeks" in {
    val (file, buf) = createTempFile(100*1024)
    try {
      val mmapStream = openWithMMap(file)
      val hadoopStream = openWithHadoop(file)
      val mmapReadBuf = new Array[Byte](1024)
      val hadoopReadBuf = new Array[Byte](1024)

      // Read 0-99
      mmapStream.getPos must_== 0
      mmapStream.getPos must_== hadoopStream.getPos
      mmapStream.read(mmapReadBuf, 0, 100) must_== 100
      hadoopStream.read(hadoopReadBuf, 0, 100) must_== 100
      mmapReadBuf.toList.take(100) must haveTheSameElementsAs(buf.view(0, 100))
      mmapReadBuf.toList.take(100) must haveTheSameElementsAs(hadoopReadBuf.take(100))

      // Read 100-199
      mmapStream.getPos must_== 100
      mmapStream.getPos must_== hadoopStream.getPos
      mmapStream.read(mmapReadBuf, 0, 100) must_== 100
      hadoopStream.read(hadoopReadBuf, 0, 100) must_== 100
      mmapReadBuf.toList.take(100) must haveTheSameElementsAs(buf.view(100, 200))
      mmapReadBuf.toList.take(100) must haveTheSameElementsAs(hadoopReadBuf.take(100))

      // Seek to 150, then read 150-250
      mmapStream.seek(150)
      hadoopStream.seek(150)
      mmapStream.getPos must_== 150
      mmapStream.getPos must_== hadoopStream.getPos
      mmapStream.read(mmapReadBuf, 0, 100) must_== 100
      hadoopStream.read(hadoopReadBuf, 0, 100) must_== 100
      mmapReadBuf.toList.take(100) must haveTheSameElementsAs(buf.view(150, 250))
      mmapReadBuf.toList.take(100) must haveTheSameElementsAs(hadoopReadBuf.take(100))

      mmapStream.getPos must_== 250
      mmapStream.getPos must_== hadoopStream.getPos
    } finally {
      file.delete
    }
  }

  "should be able to read across blocks" in {
    val (file, buf) = createTempFile(100*1024)
    try {
      val mmapStream = openWithMMap(file)
      val hadoopStream = openWithHadoop(file)
      val mmapReadBuf = new Array[Byte](1024)
      val hadoopReadBuf = new Array[Byte](1024)
      val pos = 64*1024 - 5

      mmapStream.getPos must_== 0
      mmapStream.readFully(pos, mmapReadBuf)
      hadoopStream.readFully(pos, hadoopReadBuf)
      mmapReadBuf.toList must haveTheSameElementsAs(buf.view(pos, pos + mmapReadBuf.length))
      mmapReadBuf.toList must haveTheSameElementsAs(hadoopReadBuf)
      mmapStream.getPos must_== 0
    } finally {
      file.delete
    }
  }

  "should throw if read past end of file" in {
    val (file, buf) = createTempFile(100)
    try {
      val mmapStream = openWithMMap(file)
      val hadoopStream = openWithHadoop(file)
      val mmapReadBuf = new Array[Byte](1024)
      val hadoopReadBuf = new Array[Byte](1024)

      val pos = 0
      mmapStream.readFully(pos, mmapReadBuf) must throwA[EOFException]
      hadoopStream.readFully(pos, hadoopReadBuf) must throwA[EOFException]
    } finally {
      file.delete
    }
  }
}
