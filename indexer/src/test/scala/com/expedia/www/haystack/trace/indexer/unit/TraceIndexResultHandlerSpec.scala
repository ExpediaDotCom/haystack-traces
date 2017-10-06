package com.expedia.www.haystack.trace.indexer.unit

import java.util
import java.util.Collections
import java.util.concurrent.Semaphore

import com.codahale.metrics.Timer
import com.expedia.www.haystack.trace.indexer.writers.es.TraceIndexResultHandler
import com.google.gson.Gson
import io.searchbox.core.BulkResult
import org.scalatest.easymock.EasyMockSugar
import org.scalatest.{FunSpec, Matchers}

class TraceIndexResultHandlerSpec extends FunSpec with Matchers with EasyMockSugar {

  describe("Trace Index Result Handler") {
    it("should complete with success if no failures reported") {
      val inflightSemaphore = mock[Semaphore]
      val timer = mock[Timer.Context]
      val bulkResult = mock[BulkResult]

      expecting {
        inflightSemaphore.release()
        timer.close()
        bulkResult.getFailedItems.andReturn(Collections.emptyList())
      }

      whenExecuting(inflightSemaphore, timer, bulkResult) {
        val handler = new TraceIndexResultHandler(inflightSemaphore, timer)
        handler.completed(bulkResult)
        TraceIndexResultHandler.esWriteFailureMeter.getCount shouldBe 0
      }
    }

    it("should complete with success but mark the failures if happen") {
      val inflightSemaphore = mock[Semaphore]
      val timer = mock[Timer.Context]
      val bulkResult = mock[BulkResult]
      val outer = new BulkResult(new Gson())
      val resultItem = new outer.BulkResultItem("op", "index", "type", "1", 400,
        "error", 1, "errorType", "errorReason")

      expecting {
        inflightSemaphore.release()
        timer.close()
        bulkResult.getFailedItems.andReturn(util.Arrays.asList(resultItem))
      }

      whenExecuting(inflightSemaphore, timer, bulkResult) {
        val handler = new TraceIndexResultHandler(inflightSemaphore, timer)
        val initialFailures = TraceIndexResultHandler.esWriteFailureMeter.getCount
        handler.completed(bulkResult)
        TraceIndexResultHandler.esWriteFailureMeter.getCount - initialFailures shouldBe 1
      }
    }

    it("should report failure and mark the number of failures ") {
      val inflightSemaphore = mock[Semaphore]
      val timer = mock[Timer.Context]
      val bulkResult = mock[BulkResult]

      expecting {
        inflightSemaphore.release()
        timer.close()
      }

      whenExecuting(inflightSemaphore, timer, bulkResult) {
        val handler = new TraceIndexResultHandler(inflightSemaphore, timer)
        val initialFailures = TraceIndexResultHandler.esWriteFailureMeter.getCount
        handler.failed(new RuntimeException)
        TraceIndexResultHandler.esWriteFailureMeter.getCount - initialFailures shouldBe 1
      }
    }
  }
}
