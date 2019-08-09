package com.expedia.www.haystack.trace.indexer.writers.es

import java.util.concurrent.{Semaphore, TimeUnit}

import com.expedia.open.tracing.buffer.SpanBuffer
import com.expedia.www.haystack.commons.metrics.MetricsSupport
import com.expedia.www.haystack.trace.commons.clients.es.document.ShowValuesDoc
import com.expedia.www.haystack.commons.retries.RetryOperation.withRetryBackoff
import com.expedia.www.haystack.trace.commons.config.entities.WhitelistIndexFieldConfiguration
import com.expedia.www.haystack.trace.commons.packer.PackedMessage
import com.expedia.www.haystack.trace.indexer.config.entities.ShowValuesConfiguration
import com.expedia.www.haystack.trace.indexer.metrics.AppMetricNames
import com.expedia.www.haystack.trace.indexer.writers.TraceWriter
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, Index}
import io.searchbox.indices.template.PutTemplate
import io.searchbox.params.Parameters
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.util.Try


object ShowValuesUtils {
  // creates an index name based on current date. following example illustrates the naming convention of
  // elastic search indices for service metadata:
  // service-metadata-2019-02-20
  def indexName(prefix: String): String = {
    val eventTime = new DateTime(DateTimeZone.UTC)
    val dataFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")
    s"$prefix-${dataFormatter.print(eventTime)}"
  }
}

class ShowValuesWriter(config: ShowValuesConfiguration,  whitelistFieldConfig: WhitelistIndexFieldConfiguration) extends TraceWriter with MetricsSupport {
  private val LOGGER: Logger= LoggerFactory.getLogger(ShowValuesConfiguration.getClass)

  // a timer that measures the amount of time it takes to complete one bulk write
  private val writeTimer = metricRegistry.timer(AppMetricNames.METADATA_WRITE_TIME)

  // meter that measures the write failures
  private val failureMeter = metricRegistry.meter(AppMetricNames.METADATA_WRITE_FAILURE)

  // converts a serviceMetadata object into an indexable document
  private val documentGenerator = new ShowValuesDocumentGenerator(config, whitelistFieldConfig)


  // this semaphore controls the parallel writes to service metadata index
  private val inflightRequestsSemaphore = new Semaphore(config.maxInFlightBulkRequests, true)

  // initialize the elastic search client
  private val esClient: JestClient = {
    LOGGER.info("Initializing the http elastic search client with endpoint={}", config.esEndpoint)

    val factory = new JestClientFactory()
    val builder = new HttpClientConfig.Builder(config.esEndpoint)
      .multiThreaded(true)
      .maxConnectionIdleTime(config.flushIntervalInSec + 10, TimeUnit.SECONDS)
      .connTimeout(config.connectionTimeoutMillis)
      .readTimeout(config.readTimeoutMillis)

    if (config.username.isDefined && config.password.isDefined) {
      builder.defaultCredentials(config.username.get, config.password.get)
    }

    factory.setHttpClientConfig(builder.build())
    factory.getObject
  }

  private val bulkBuilder = new ThreadSafeBulkBuilder(config.maxDocsInBulk, config.maxBulkDocSizeInBytes)

  if (config.indexTemplateJson.isDefined) applyTemplate(config.indexTemplateJson.get)

  override def close(): Unit = {
    LOGGER.info("Closing the elastic search client now.")
    Try(esClient.shutdownClient())
  }

  /**
    * converts the spans to an index document and writes to elastic search. Also if the parallel writes
    * exceed the max inflight requests, then we block and this puts backpressure on upstream
    *
    * @param traceId          trace id
    * @param packedSpanBuffer list of spans belonging to this traceId - packed bytes of span buffer
    * @param isLastSpanBuffer tells if this is the last record, so the writer can flush
    * @return
    */
  override def writeAsync(traceId: String, packedSpanBuffer: PackedMessage[SpanBuffer], isLastSpanBuffer: Boolean): Unit = {
    var isSemaphoreAcquired = false
    val idxDocument: Seq[ShowValuesDoc] = documentGenerator.getAndUpdateShowValues(packedSpanBuffer.protoObj.getChildSpansList.asScala)
    idxDocument.foreach(document => {
      try {
        addIndexOperation(traceId, document, ServiceMetadataUtils.indexName(config.indexName)) match {
          case Some(bulkToDispatch) =>
            inflightRequestsSemaphore.acquire()
            isSemaphoreAcquired = true

            // execute the request async with retry
            withRetryBackoff(
              retryCallback => {
                esClient.executeAsync(bulkToDispatch, new ElasticSearchResultHandler(writeTimer.time(), failureMeter, retryCallback))
              },
              config.retryConfig,
              onSuccess = (_ : Any) => inflightRequestsSemaphore.release(),
              onFailure = ex => {
                inflightRequestsSemaphore.release()
                LOGGER.error("Fail to write to ES after {} retry attempts", config.retryConfig.maxRetries, ex)
              })
          case _ =>
        }
      } catch {
        case ex: Exception =>
          if (isSemaphoreAcquired) inflightRequestsSemaphore.release()
          failureMeter.mark()
          LOGGER.error("Failed to write spans to elastic search with exception", ex)
      }
    })
  }

  private def addIndexOperation(traceId: String, document: ShowValuesDoc, indexName: String): Option[Bulk] = { // add all the service field combinations in one bulk
    val action: Index = new Index.Builder(document.json)
      .index(indexName)
      .`type`(config.indexType)
      .setParameter(Parameters.CONSISTENCY, config.consistencyLevel)
      .id(s"${document.servicename}_${document.fieldvalue}")
      .build()

    bulkBuilder.addAction(action, document.json.getBytes("utf-8").length, forceBulkCreate = false)
  }

  private def applyTemplate(templateJson: String) {
    val putTemplateRequest = new PutTemplate.Builder("show-values-template", templateJson).build()
    val result = esClient.execute(putTemplateRequest)
    if (!result.isSucceeded) {
      throw new RuntimeException(s"Fail to apply the following template to elastic search with reason=${result.getErrorMessage}")
    }
  }
}
