package com.expedia.www.haystack.trace.indexer.writers.es

import java.time.Instant

import com.expedia.open.tracing.Span
import com.expedia.www.haystack.commons.metrics.MetricsSupport
import com.expedia.www.haystack.trace.commons.clients.es.document.ShowValuesDoc
import com.expedia.www.haystack.trace.commons.config.entities.WhitelistIndexFieldConfiguration
import com.expedia.www.haystack.trace.indexer.config.entities.ShowValuesConfiguration
import org.apache.commons.lang3.StringUtils

import scala.collection.mutable

class ShowValuesDocumentGenerator(config: ShowValuesConfiguration, whitelistIndexFieldConfiguration: WhitelistIndexFieldConfiguration) extends MetricsSupport {

  private var showValuesMap = new mutable.HashMap[String, mutable.HashMap[String, mutable.Set[String]]]()
  private var lastFlushInstant = Instant.MIN
  private val PAGE_NAME_KEY = "page-name"
  private var fieldCount = 0

  private def shouldFlush: Boolean = {
    config.flushIntervalInSec == 0 || Instant.now().minusSeconds(config.flushIntervalInSec).isAfter(lastFlushInstant)
  }

  private def areStatementReadyToBeExecuted(): Seq[ShowValuesDoc] = {
    if (showValuesMap.nonEmpty && (shouldFlush || fieldCount > config.flushOnMaxFieldCount)) {
      val statements = showValuesMap.flatMap {
        case (serviceName, fieldValuesMap) =>
          createShowValuesDoc(serviceName, fieldValuesMap)
      }

      lastFlushInstant = Instant.now()
      showValuesMap = new mutable.HashMap[String, mutable.HashMap[String, mutable.Set[String]]]()
      fieldCount = 0
      statements.toSeq
    } else {
      Nil
    }
  }

  /**
    * get the list of unique service metadata documents contained in the list of spans
    *
    * @param spans : list of service metadata
    * @return
    */
  def getAndUpdateShowValues(spans: Iterable[Span]): Seq[ShowValuesDoc] = {
    this.synchronized {
      val showValuesTagList = whitelistIndexFieldConfiguration.whitelistIndexFields.filter(p => p.showValue.equals(true))
      spans.foreach(span => {
        val tagsToSave = span.getTagsList.stream().filter(p => showValuesTagList.contains(p.getKey))
        if (StringUtils.isNotEmpty(span.getServiceName) && tagsToSave.count > 0) {
          val serviceInfo = showValuesMap.getOrElseUpdate(span.getServiceName, mutable.HashMap[String, mutable.Set[String]]())
          tagsToSave.forEach(tag => {
            var tagValues = serviceInfo.getOrElseUpdate(tag.getKey, mutable.Set[String]())
            if (tagValues.add(tag.getVStr)) {
              fieldCount += 1
            }
          })
        }
      })
      areStatementReadyToBeExecuted()
    }
  }

  /**
    * @return index document that can be put in elastic search
    */
  def createShowValuesDoc(serviceName: String, fieldValuesMap: mutable.HashMap[String, mutable.Set[String]]): List[ShowValuesDoc] = {
    fieldValuesMap.flatMap(p => p._2.map(values => ShowValuesDoc(serviceName, p._1, values))).toList
  }

}
