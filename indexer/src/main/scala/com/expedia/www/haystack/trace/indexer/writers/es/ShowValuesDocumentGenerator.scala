package com.expedia.www.haystack.trace.indexer.writers.es

import java.time.Instant

import com.expedia.open.tracing.Span
import com.expedia.www.haystack.commons.metrics.MetricsSupport
import com.expedia.www.haystack.trace.commons.clients.es.document.ShowValuesDoc
import com.expedia.www.haystack.trace.indexer.config.entities.ShowValuesConfiguration
import org.apache.commons.lang3.StringUtils

import scala.collection.mutable

class ShowValuesDocumentGenerator(config: ShowValuesConfiguration) extends MetricsSupport {

  private var showValuesMap = new mutable.HashMap[String, mutable.Set[String]]()
  private var lastFlushInstant = Instant.MIN
  private val PAGE_NAME_KEY = "page-name"
  private var fieldCount = 0

  private def shouldFlush: Boolean = {
    config.flushIntervalInSec == 0 || Instant.now().minusSeconds(config.flushIntervalInSec).isAfter(lastFlushInstant)
  }

  private def areStatementReadyToBeExecuted(): Seq[ShowValuesDoc] = {
    if (showValuesMap.nonEmpty && (shouldFlush || fieldCount > config.flushOnMaxFieldCount)) {
      val statements = showValuesMap.flatMap {
        case (serviceName, fieldList) =>
          createShowValuesDoc(serviceName, fieldList)
      }

      lastFlushInstant = Instant.now()
      showValuesMap = new mutable.HashMap[String, mutable.Set[String]]()
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
      spans.foreach(span => {
        val fieldInfo = span.getTagsList.stream().filter(p => p.getKey.equalsIgnoreCase(PAGE_NAME_KEY))
        if (StringUtils.isNotEmpty(span.getServiceName) && fieldInfo.count() > 0) {
          val pageList = showValuesMap.getOrElseUpdate(span.getServiceName, mutable.Set[String]())
          val fieldValue = fieldInfo.findFirst().get().getVStr
          if (pageList.add(fieldValue)) {
            fieldCount += 1
          }
        }
      })
      areStatementReadyToBeExecuted()
    }
  }

  /**
    * @return index document that can be put in elastic search
    */
  def createShowValuesDoc(serviceName: String, fieldValueList: mutable.Set[String]): List[ShowValuesDoc] = {
    fieldValueList.map(v => ShowValuesDoc(serviceName, PAGE_NAME_KEY, v)).toList
  }

}
