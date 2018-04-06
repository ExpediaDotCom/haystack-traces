/*
 *  Copyright 2017 Expedia, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.expedia.www.haystack.trace.commons.packer

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream}
import java.nio.ByteBuffer
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import com.expedia.open.tracing.buffer.SpanBuffer
import com.expedia.www.haystack.trace.commons.packer.PackerType.PackerType
import com.google.protobuf.GeneratedMessageV3
import org.apache.commons.io.IOUtils
import org.json4s.jackson.Serialization
import org.json4s.{DefaultFormats, Formats}
import org.xerial.snappy.{SnappyInputStream, SnappyOutputStream}

object PackerType extends Enumeration {
  type PackerType = Value
  val GZIP, SNAPPY, NONE = Value
}

object PackedMessage {
  implicit val formats: Formats = DefaultFormats + new org.json4s.ext.EnumSerializer(PackerType)
  val MAGIC_BYTES: Array[Byte] = "hytc".getBytes("utf-8")
}

case class PackedMessage[T <: GeneratedMessageV3](protoObj: T,
                                                  private val pack: (T => Array[Byte]),
                                                  private val metadata: PackedMetadata) {
  import PackedMessage._
  private lazy val metadataBytes: Array[Byte] = Serialization.write(metadata).getBytes("utf-8")

  val packedDataBytes: Array[Byte] = {
    val packedDataBytes = pack(protoObj)
    if (PackerType.NONE == metadata.t) {
      packedDataBytes
    } else {
      ByteBuffer
        .allocate(MAGIC_BYTES.length + 4 + metadataBytes.length + packedDataBytes.length)
        .put(MAGIC_BYTES)
        .putInt(metadataBytes.length)
        .put(metadataBytes)
        .put(packedDataBytes).array()
    }
  }
}

case class PackedMetadata(t: PackerType)

object Unpacker {
  import PackedMessage._

  private def readMetadata(packedDataBytes: Array[Byte]): Array[Byte] = {
    val byteBuffer = ByteBuffer.wrap(packedDataBytes)
    val magicBytesExist = MAGIC_BYTES.indices forall { idx => byteBuffer.get() == MAGIC_BYTES.apply(idx) }
    if (magicBytesExist) {
      val headerLength = byteBuffer.getInt
      val metadataBytes = new Array[Byte](headerLength)
      byteBuffer.get(metadataBytes, 0, headerLength)
      metadataBytes
    } else {
      null
    }
  }

  private def unpack(compressedStream: InputStream) = {
    val outputStream = new ByteArrayOutputStream()
    IOUtils.copy(compressedStream, outputStream)
    outputStream.toByteArray
  }

  def readSpanBuffer(packedDataBytes: Array[Byte]): SpanBuffer = {
    var parsedDataBytes: Array[Byte] = null
    val metadataBytes = readMetadata(packedDataBytes)
    if (metadataBytes != null) {
      val packedMetadata = Serialization.read[PackedMetadata](new String(metadataBytes))
      val compressedDataOffset = MAGIC_BYTES.length + 4 + metadataBytes.length
      packedMetadata.t match {
        case PackerType.SNAPPY =>
          parsedDataBytes = unpack(new SnappyInputStream(new ByteArrayInputStream(packedDataBytes, compressedDataOffset, packedDataBytes.length - compressedDataOffset)))
        case PackerType.GZIP =>
          parsedDataBytes = unpack(new GZIPInputStream(new ByteArrayInputStream(packedDataBytes, compressedDataOffset, packedDataBytes.length - compressedDataOffset)))
        case _ =>
          return SpanBuffer.parseFrom(new ByteArrayInputStream(packedDataBytes, compressedDataOffset, packedDataBytes.length - compressedDataOffset))
      }
    } else {
      parsedDataBytes = packedDataBytes
    }
    SpanBuffer.parseFrom(parsedDataBytes)
  }
}

abstract class Packer[T <: GeneratedMessageV3] {
  val packerType: PackerType

  protected def compressStream(stream: OutputStream): OutputStream

  private def pack(protoObj: T): Array[Byte] = {
    val outStream = new ByteArrayOutputStream
    val compressedStream = compressStream(outStream)
    if (compressedStream != null) {
      IOUtils.copy(new ByteArrayInputStream(protoObj.toByteArray), compressedStream)
      compressedStream.close() // this flushes the data to final outStream
      outStream.toByteArray
    } else {
      protoObj.toByteArray
    }
  }

  def apply(protoObj: T): PackedMessage[T] = {
    PackedMessage(protoObj, pack, PackedMetadata(packerType))
  }
}

class NoopPacker[T <: GeneratedMessageV3] extends Packer[T] {
  override val packerType = PackerType.NONE
  override protected def compressStream(stream: OutputStream): OutputStream = null
}

class SnappyPacker[T <: GeneratedMessageV3] extends Packer[T] {
  override val packerType = PackerType.SNAPPY
  override protected def compressStream(stream: OutputStream): OutputStream = new SnappyOutputStream(stream)
}

class GzipPacker[T <: GeneratedMessageV3] extends Packer[T] {
  override val packerType = PackerType.GZIP
  override protected def compressStream(stream: OutputStream): OutputStream = new GZIPOutputStream(stream)
}

object PackerFactory {
  def spanBufferPacker(`type`: PackerType): Packer[SpanBuffer] = {
    `type` match {
      case PackerType.SNAPPY => new SnappyPacker[SpanBuffer]
      case PackerType.GZIP => new GzipPacker[SpanBuffer]
      case _ => new NoopPacker[SpanBuffer]
    }
  }
}