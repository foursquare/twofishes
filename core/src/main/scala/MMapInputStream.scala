package com.foursquare.twofishes

import java.io.{EOFException, File, FileInputStream, IOException, InputStream}
import java.nio.{ByteBuffer, MappedByteBuffer}
import java.nio.channels.FileChannel
import org.apache.hadoop.fs.{PositionedReadable, Seekable}

class ByteBufferReader(val buf: ByteBuffer) {
  def read(position: Int): Int = synchronized {
    buf.position(position)
    if (!buf.hasRemaining) {
      -1
    } else {
      buf.get & 0xFF
    }
  }

  def readAtPosition(pos: Int, buffer: Array[Byte], offset: Int, length: Int): Int = synchronized {
    buf.position(pos)
    if (!buf.hasRemaining) {
      -1
    } else {
      val readLen = math.min(length, buf.remaining)
      buf.get(buffer, offset, readLen)
      readLen
    }
  }

  /** Makes a best-effort attempt to preload the mmap'd data. */
  def preload: Unit = buf match {
    case mbb: MappedByteBuffer => mbb.load()
    case _ =>
  }
}

/** Positionless implementation for MMapInputStream that uses mmap under the
  * covers to be faster. All methods are thread-safe and can be used by
  * multiple MMapInputStream's so we can have multiple readers hitting the same
  * input stream, sharing the same mmap'd memory. */
class MMapInputStreamImpl(filename: String, mmapChunkSize: Int = 64*1024) {
  val (fileChannel, readers, fileSize) = {
    val file: File = new File(filename)
    val fileChannel: FileChannel = (new FileInputStream(file)).getChannel
    val fileSize: Long = file.length()
    // Chunk the file into blocks. This is 1), because FileChannel.map only
    // allows a max length of 2G so we have to chunk into something and 2), so
    // we can have finer-grained locks with less contention.
    val byteBuffers = (0L to fileSize by mmapChunkSize).map((offset: Long) =>
      fileChannel.map(FileChannel.MapMode.READ_ONLY, offset, math.min(mmapChunkSize, fileSize - offset))
    ).toArray
    val readers = byteBuffers.map(bb => new ByteBufferReader(bb))
    (fileChannel, readers, fileSize)
  }

  private def getStream(pos: Long): Option[(ByteBufferReader, Int)] = {
    val index = (pos / mmapChunkSize).toInt
    if (index < readers.length) {
      val reader = readers(index)
      val positionInBuffer = (pos % mmapChunkSize).toInt
      Some((reader, positionInBuffer))
    } else {
      None
    }
  }

  def preload = readers.foreach(_.preload)

  def close = fileChannel.close

  def readByteAtPosition(pos: Long): (Int, Long) = {
    getStream(pos) match {
      case Some((reader, posInStream)) =>
        val result = reader.read(posInStream)
        if (result >= 0) {
          (result, pos + 1)
        } else {
          (result, pos)
        }
      case None => (-1, pos)
    }
  }

  def read(pos: Long, buffer: Array[Byte], offset: Int, length: Int): Int = {
    var soFar = 0
    var exhausted = false
    while (!exhausted && soFar < length) {
      getStream(pos + soFar) match {
        case Some((reader, posInStream)) =>
          val numRead = reader.readAtPosition(posInStream, buffer, offset + soFar, length - soFar)
          if (numRead < 0) {
            throw new EOFException("reached EOF while trying to read %d bytes".format(length))
          }
          soFar += numRead
        case None =>
          exhausted = true
      }
    }
    soFar
  }
}

object MMapInputStreamImpl {
  var streams: Map[String, MMapInputStreamImpl] = Map.empty

  def findOrOpen(filename: String) = synchronized {
    streams.get(filename).getOrElse({
      val ret = new MMapInputStreamImpl(filename)
      streams = streams.updated(filename, ret)
      ret
    })
  }
}

/** A FSDataInputStream-compatible InputStream that uses mmap under the covers
  * to be faster. All methods are thread-safe, but the methods that assume a
  * shared position will have worse concurrency because there will be a per-file
  * lock on the position. If possible, use readFully and the read() overload
  * that accepts a position, because those don't acquire a lock until you get
  * down to the individual memory-mapped blocks.
  *
  * This uses MMapInputStreamImpl for the actual implementation so multiple
  * people can read the same file and share the same mmap'd data.
  */
class MMapInputStream(filename: String) extends InputStream with Seekable with PositionedReadable {

  private val impl = MMapInputStreamImpl.findOrOpen(filename)

  private var activePosition: Long = 0

  def preload = impl.preload

  override def close = impl.close
  override def read(): Int = synchronized {
    val (result, newPos) = impl.readByteAtPosition(activePosition)
    activePosition = newPos
    result
  }
  override final def read(buffer: Array[Byte], off: Int, len: Int): Int = synchronized {
    val bytesRead = read(activePosition, buffer, off, len)
    if (bytesRead > 0) {
      activePosition += bytesRead
    }
    bytesRead
  }
  override def getPos: Long = synchronized {
    activePosition
  }
  override def seek(pos: Long) = synchronized {
    if (pos > impl.fileSize) {
      throw new IOException("can't seek past end of file")
    }
    activePosition = pos
  }
  override def seekToNewSource(targetPos: Long): Boolean = {
    // Same as what RawLocalFileSystem's impl does.
    false
  }

  override def read(pos: Long, buffer: Array[Byte], offset: Int, length: Int): Int = {
    impl.read(pos, buffer, offset, length)
  }

  override def readFully(pos: Long, buffer: Array[Byte]) = {
    read(pos, buffer, 0, buffer.length)
  }
  override def readFully(pos: Long, buffer: Array[Byte], offset: Int, length: Int) = {
    read(pos, buffer, offset, length)
  }
}
