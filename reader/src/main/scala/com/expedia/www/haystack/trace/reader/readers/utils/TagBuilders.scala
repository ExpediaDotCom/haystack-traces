package com.expedia.www.haystack.trace.reader.readers.utils

import com.expedia.open.tracing.Tag

object TagBuilders {
  def buildStringTag(tagKey: String, tagValue: String): Tag =
    Tag.newBuilder()
      .setKey(tagKey)
      .setType(Tag.TagType.STRING)
      .setVStr(tagValue)
      .build()

  def buildBoolTag(tagKey: String, tagValue: Boolean): Tag =
    Tag.newBuilder()
      .setKey(tagKey)
      .setType(Tag.TagType.BOOL)
      .setVBool(tagValue)
      .build()

  def buildLongTag(tagKey: String, tagValue: Long): Tag =
    Tag.newBuilder()
      .setKey(tagKey)
      .setType(Tag.TagType.LONG)
      .setVLong(tagValue)
      .build()

}
