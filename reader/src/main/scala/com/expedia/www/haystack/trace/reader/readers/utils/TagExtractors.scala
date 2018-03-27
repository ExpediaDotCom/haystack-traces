package com.expedia.www.haystack.trace.reader.readers.utils

import com.expedia.open.tracing.Span
import scala.collection.JavaConverters._

object TagExtractors {
  def containsTag(span: Span, tagKey: String): Boolean = {
    span.getTagsList.asScala.exists(_.getKey == tagKey)
  }

  def extractTagStringValue(span: Span, tagKey: String): String = {
    span.getTagsList.asScala.find(_.getKey == tagKey) match {
      case Some(t) => t.getVStr
      case _ => ""
    }
  }

  def extractTagLongValue(span: Span, tagKey: String): Long = {
    span.getTagsList.asScala.find(_.getKey == tagKey) match {
      case Some(t) => t.getVLong
      case _ => -1
    }
  }
}
