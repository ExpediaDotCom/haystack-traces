package com.expedia.www.haystack.trace.reader.readers.utils

import com.expedia.open.tracing.Span
import scala.collection.JavaConversions._

object TagExtractors {
  def containsTag(span: Span, tagKey: String): Boolean =
    span.getTagsList.exists(_.getKey == tagKey)

  def extractTagStringValue(span: Span, tagKey: String): String =
    span.getTagsList.find(_.getKey == tagKey) match {
      case Some(t) => t.getVStr
      case _ => ""
    }

  def extractTagLongValue(span: Span, tagKey: String): Long =
    span.getTagsList.find(_.getKey == tagKey) match {
      case Some(t) => t.getVLong
      case _ => -1
    }
}
