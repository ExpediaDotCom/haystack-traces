package com.expedia.www.haystack.trace.indexer.unit

import com.expedia.www.haystack.trace.indexer.writers.es.ElasticSearchWriterUtils
import org.scalatest.{BeforeAndAfterEach, FunSpec, GivenWhenThen, Matchers}

class ElasticSearchWriterUtilsSpec extends FunSpec with Matchers with GivenWhenThen with BeforeAndAfterEach {
  var timezone = "UCT"

  override def beforeEach() {
    timezone = System.getProperty("user.timezone")
    System.setProperty("user.timezone", "CST")
  }

  override def afterEach(): Unit = {
    System.setProperty("user.timezone", timezone)
  }

  describe("elastic search writer") {
    it("should use UTC when generating ES indexes") {
      Given("the system timezone is not UTC")
      System.setProperty("user.timezone", "CST")

      When("the writer generates the ES indexes")
      val cstName = ElasticSearchWriterUtils.indexName("haystack-traces", 6)
      System.setProperty("user.timezone", "UTC")
      val utcName = ElasticSearchWriterUtils.indexName("haystack-traces", 6)

      Then("it should use UTC to get those indexes")
      cstName shouldBe utcName
    }
  }
}
