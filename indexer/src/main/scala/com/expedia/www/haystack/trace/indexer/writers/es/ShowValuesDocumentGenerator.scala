package com.expedia.www.haystack.trace.indexer.writers.es

import java.time.Instant
import java.util.stream.Collectors

import com.expedia.open.tracing.{Span, Tag}
import com.expedia.www.haystack.commons.metrics.MetricsSupport
import com.expedia.www.haystack.trace.commons.clients.es.document.ShowValuesDoc
import com.expedia.www.haystack.trace.commons.config.entities.{WhitelistIndexField, WhitelistIndexFieldConfiguration}
import com.expedia.www.haystack.trace.indexer.config.entities.ShowValuesConfiguration
import org.apache.commons.lang3.StringUtils

import scala.collection.mutable
import scala.collection.JavaConverters._

class ShowValuesDocumentGenerator(config: ShowValuesConfiguration, whitelistIndexFieldConfiguration: WhitelistIndexFieldConfiguration) extends MetricsSupport {

  private var showValuesMap = new mutable.HashMap[String, mutable.HashMap[String, mutable.Set[String]]]()
  private var lastFlushInstant = Instant.MIN
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
      val showValuesIndexField: List[WhitelistIndexField] = whitelistIndexFieldConfiguration.whitelistIndexFields.filter(p => p.showValue.equals(true))
      spans.foreach(span => {
        val tagsToSave: List[Tag] = span.getTagsList.stream()
          .filter(t => showValuesIndexField.exists(p => p.name.equalsIgnoreCase(t.getKey)))
          .collect(Collectors.toList[Tag]()).asScala.toList
        if (StringUtils.isNotEmpty(span.getServiceName) && tagsToSave.nonEmpty) {
          val serviceInfo = showValuesMap.getOrElseUpdate(span.getServiceName, mutable.HashMap[String, mutable.Set[String]]())
          tagsToSave.foreach(tag => {
            val tagValues = serviceInfo.getOrElseUpdate(tag.getKey, mutable.Set[String]())
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
