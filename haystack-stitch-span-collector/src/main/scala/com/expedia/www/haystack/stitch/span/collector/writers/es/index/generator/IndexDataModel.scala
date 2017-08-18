package com.expedia.www.haystack.stitch.span.collector.writers.es.index.generator

import com.expedia.www.haystack.stitch.span.collector.writers.es.index.generator.Document.{TagKey, TagValue}

import scala.collection.mutable

object Document {
  type ServiceName = String
  type OperationName = String
  type TagKey = String
  type TagValue = Any

  type IndexDataModel = mutable.Map[ServiceName, mutable.Map[OperationName, IndexingAttributes]]

  def newIndexDataModel: IndexDataModel = {
    mutable.Map[ServiceName, mutable.Map[OperationName, IndexingAttributes]]()
  }
}

case class Document(id: String, indexJson: String)

case class IndexingAttributes(tags: mutable.Map[TagKey, mutable.Set[TagValue]],
                              var minduration: Long,
                              var maxduration: Long) {

  def merge(from: IndexingAttributes): IndexingAttributes = {
    from.tags.foreach({
      case (k, v) => this.tags.getOrElseUpdate(k, mutable.Set[TagValue]()) ++= v
    })
    if (minduration > from.minduration) minduration = from.minduration
    if (maxduration < from.maxduration) maxduration = from.maxduration
    this
  }

  def updateTags(tagKey: TagKey, value: TagValue): Unit = {
    val tagIndexMap = tags.getOrElseUpdate(tagKey, mutable.Set[TagValue]())
    tagIndexMap += value
  }

  def updateMinMaxDuration(newDuration: Long): Unit = {
    if (newDuration < minduration) {
      minduration = newDuration
    } else if (newDuration > maxduration) {
      maxduration = newDuration
    }
  }
}
