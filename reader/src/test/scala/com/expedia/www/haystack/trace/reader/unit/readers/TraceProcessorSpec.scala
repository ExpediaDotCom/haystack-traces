package com.expedia.www.haystack.trace.reader.unit.readers

import com.expedia.www.haystack.trace.reader.readers.TraceProcessor
import com.expedia.www.haystack.trace.reader.readers.transformers._
import com.expedia.www.haystack.trace.reader.readers.validators._
import com.expedia.www.haystack.trace.reader.unit.BaseUnitTestSpec
import com.expedia.www.haystack.trace.reader.unit.readers.builders.{ClockSkewedTraceBuilder, MultiRootTraceBuilder, MultiServerSpanTraceBuilder, ValidTraceBuilder}

class TraceProcessorSpec
  extends BaseUnitTestSpec
    with ValidTraceBuilder
    with MultiServerSpanTraceBuilder
    with MultiRootTraceBuilder
    with ClockSkewedTraceBuilder {

  describe("TraceProcessor for well-formed traces") {
    val traceProcessor = new TraceProcessor(
      Seq(new TraceIdValidator, new RootValidator, new ParentIdValidator),
      Seq(new DeDuplicateSpanTransformer),
      Seq(new PartialSpanTransformer, new ClockSkewTransformer, new SortSpanTransformer))

    it("should successfully process a simple valid trace") {
      Given("a simple liner trace ")
      val trace = buildSimpleLinerTrace()

      When("invoking process")
      val processedTraceOption = traceProcessor.process(trace)

      Then("successfully process trace")
      processedTraceOption.isSuccess should be(true)
      val processedTrace = processedTraceOption.get

      processedTrace.getChildSpansList.size() should be(4)
      getSpan(processedTrace, "a").getStartTime should be(startTimestamp)
      getSpan(processedTrace, "b").getStartTime should be(startTimestamp + 50)
      getSpan(processedTrace, "c").getStartTime should be(startTimestamp + 550)
      getSpan(processedTrace, "d").getStartTime should be(startTimestamp + 750)
    }

    it("should reject a multi-root trace") {
      Given("a multi-root trace ")
      val trace = buildMultiRootTrace()

      When("invoking process")
      val processedTraceOption = traceProcessor.process(trace)

      Then("reject trace")
      processedTraceOption.isSuccess should be(false)
    }

    it("should successfully process a valid multi-service trace without clock skew") {
      Given("a valid multi-service trace without skew")
      val trace = buildMultiServiceWithoutSkewTrace()

      When("invoking process")
      val processedTraceOption = traceProcessor.process(trace)

      Then("successfully process trace")
      processedTraceOption.isSuccess should be(true)
      val processedTrace = processedTraceOption.get

      processedTrace.getChildSpansList.size() should be(5)
      getSpan(processedTrace, "a").getStartTime should be(startTimestamp)
      getSpan(processedTrace, "a").getServiceName should be("x")

      getSpan(processedTrace, "b").getStartTime should be(startTimestamp)
      getSpan(processedTrace, "b").getServiceName should be("y")

      getSpan(processedTrace, "c").getStartTime should be(startTimestamp + 500)
      getSpan(processedTrace, "c").getServiceName should be("x")

      getSpan(processedTrace, "d").getStartTime should be(startTimestamp )
      getSpan(processedTrace, "d").getServiceName should be("y")

      getSpan(processedTrace, "e").getStartTime should be(startTimestamp + 200)
      getSpan(processedTrace, "e").getServiceName should be("y")
    }

    it("should successfully process a valid multi-service trace with positive clock skew") {
      Given("a valid multi-service trace with skew")
      val trace = buildMultiServiceWithPositiveSkewTrace()

      When("invoking process")
      val processedTraceOption = traceProcessor.process(trace)

      Then("successfully process trace")
      processedTraceOption.isSuccess should be(true)
      val processedTrace = processedTraceOption.get

      processedTrace.getChildSpansList.size() should be(5)
      getSpan(processedTrace, "a").getStartTime should be(startTimestamp)
      getSpan(processedTrace, "a").getServiceName should be("x")

      getSpan(processedTrace, "b").getStartTime should be(startTimestamp)
      getSpan(processedTrace, "b").getServiceName should be("y")

      getSpan(processedTrace, "c").getStartTime should be(startTimestamp + 500)
      getSpan(processedTrace, "c").getServiceName should be("x")

      getSpan(processedTrace, "d").getStartTime should be(startTimestamp )
      getSpan(processedTrace, "d").getServiceName should be("y")

      getSpan(processedTrace, "e").getStartTime should be(startTimestamp + 200)
      getSpan(processedTrace, "e").getServiceName should be("y")
    }

    it("should successfully process a valid multi-service trace with negative clock skew") {
      Given("a valid multi-service trace with negative skew")
      val trace = buildMultiServiceWithNegativeSkewTrace()

      When("invoking process")
      val processedTraceOption = traceProcessor.process(trace)

      Then("successfully process trace")
      processedTraceOption.isSuccess should be(true)
      val processedTrace = processedTraceOption.get

      processedTrace.getChildSpansList.size() should be(5)
      getSpan(processedTrace, "a").getStartTime should be(startTimestamp)
      getSpan(processedTrace, "a").getServiceName should be("x")

      getSpan(processedTrace, "b").getStartTime should be(startTimestamp)
      getSpan(processedTrace, "b").getServiceName should be("y")

      getSpan(processedTrace, "c").getStartTime should be(startTimestamp + 500)
      getSpan(processedTrace, "c").getServiceName should be("x")

      getSpan(processedTrace, "d").getStartTime should be(startTimestamp )
      getSpan(processedTrace, "d").getServiceName should be("y")

      getSpan(processedTrace, "e").getStartTime should be(startTimestamp + 200)
      getSpan(processedTrace, "e").getServiceName should be("y")
    }

    it("should successfully process a valid complex multi-service trace") {
      Given("a valid multi-service trace ")
      val trace = buildMultiServiceTrace()

      When("invoking process")
      val processedTraceOption = traceProcessor.process(trace)

      Then("successfully process trace")
      processedTraceOption.isSuccess should be(true)
      val processedTrace = processedTraceOption.get

      processedTrace.getChildSpansList.size() should be(6)
      getSpan(processedTrace, "a").getStartTime should be(startTimestamp)
      getSpan(processedTrace, "a").getServiceName should be("w")

      getSpan(processedTrace, "b").getStartTime should be(startTimestamp + 20)
      getSpan(processedTrace, "b").getServiceName should be("x")

      getSpan(processedTrace, "c").getStartTime should be(startTimestamp + 520)
      getSpan(processedTrace, "c").getServiceName should be("y")

      getSpan(processedTrace, "d").getStartTime should be(startTimestamp + 20)
      getSpan(processedTrace, "d").getServiceName should be("x")

      getSpan(processedTrace, "e").getStartTime should be(startTimestamp + 20)
      getSpan(processedTrace, "e").getServiceName should be("x")

      getSpan(processedTrace, "f").getStartTime should be(startTimestamp + 540)
      getSpan(processedTrace, "f").getServiceName should be("z")
    }
  }

  describe("TraceProcessor for non well-formed traces") {
    val traceProcessor = new TraceProcessor(
      Seq(new TraceIdValidator),
      Seq(new DeDuplicateSpanTransformer),
      Seq(new InvalidParentTransformer, new InvalidRootTransformer, new PartialSpanTransformer, new ClockSkewTransformer, new SortSpanTransformer))

    it("should successfully process a simple valid trace") {
      Given("a simple liner trace ")
      val trace = buildSimpleLinerTrace()

      When("invoking process")
      val processedTraceOption = traceProcessor.process(trace)

      Then("successfully process trace")
      processedTraceOption.isSuccess should be(true)
      val processedTrace = processedTraceOption.get

      processedTrace.getChildSpansList.size() should be(4)
      getSpan(processedTrace, "a").getStartTime should be(startTimestamp)
      getSpan(processedTrace, "b").getStartTime should be(startTimestamp + 50)
      getSpan(processedTrace, "c").getStartTime should be(startTimestamp + 550)
      getSpan(processedTrace, "d").getStartTime should be(startTimestamp + 750)
    }

    it("should successfully process a multi-root trace") {
      Given("a multi-root trace ")
      val trace = buildMultiRootTrace()

      When("invoking process")
      val processedTraceOption = traceProcessor.process(trace)

      Then("successfully process trace")
      processedTraceOption.isSuccess should be(true)
      val processedTrace = processedTraceOption.get

      processedTrace.getChildSpansList.size() should be(4)
      getSpan(processedTrace, "a").getServiceName should be("x")
      getSpan(processedTrace, "b").getParentSpanId should be("a")
      getSpan(processedTrace, "c").getParentSpanId should be("b")
      getSpan(processedTrace, "d").getParentSpanId should be("b")
    }

    it("should successfully process a valid multi-service trace without clock skew") {
      Given("a valid multi-service trace without skew")
      val trace = buildMultiServiceWithoutSkewTrace()

      When("invoking process")
      val processedTraceOption = traceProcessor.process(trace)

      Then("successfully process trace")
      processedTraceOption.isSuccess should be(true)
      val processedTrace = processedTraceOption.get

      processedTrace.getChildSpansList.size() should be(5)
      getSpan(processedTrace, "a").getStartTime should be(startTimestamp)
      getSpan(processedTrace, "a").getServiceName should be("x")

      getSpan(processedTrace, "b").getStartTime should be(startTimestamp)
      getSpan(processedTrace, "b").getServiceName should be("y")

      getSpan(processedTrace, "c").getStartTime should be(startTimestamp + 500)
      getSpan(processedTrace, "c").getServiceName should be("x")

      getSpan(processedTrace, "d").getStartTime should be(startTimestamp )
      getSpan(processedTrace, "d").getServiceName should be("y")

      getSpan(processedTrace, "e").getStartTime should be(startTimestamp + 200)
      getSpan(processedTrace, "e").getServiceName should be("y")
    }

    it("should successfully process a valid multi-service trace with positive clock skew") {
      Given("a valid multi-service trace with skew")
      val trace = buildMultiServiceWithPositiveSkewTrace()

      When("invoking process")
      val processedTraceOption = traceProcessor.process(trace)

      Then("successfully process trace")
      processedTraceOption.isSuccess should be(true)
      val processedTrace = processedTraceOption.get

      processedTrace.getChildSpansList.size() should be(5)
      getSpan(processedTrace, "a").getStartTime should be(startTimestamp)
      getSpan(processedTrace, "a").getServiceName should be("x")

      getSpan(processedTrace, "b").getStartTime should be(startTimestamp)
      getSpan(processedTrace, "b").getServiceName should be("y")

      getSpan(processedTrace, "c").getStartTime should be(startTimestamp + 500)
      getSpan(processedTrace, "c").getServiceName should be("x")

      getSpan(processedTrace, "d").getStartTime should be(startTimestamp )
      getSpan(processedTrace, "d").getServiceName should be("y")

      getSpan(processedTrace, "e").getStartTime should be(startTimestamp + 200)
      getSpan(processedTrace, "e").getServiceName should be("y")
    }

    it("should successfully process a valid multi-service trace with negative clock skew") {
      Given("a valid multi-service trace with negative skew")
      val trace = buildMultiServiceWithNegativeSkewTrace()

      When("invoking process")
      val processedTraceOption = traceProcessor.process(trace)

      Then("successfully process trace")
      processedTraceOption.isSuccess should be(true)
      val processedTrace = processedTraceOption.get

      processedTrace.getChildSpansList.size() should be(5)
      getSpan(processedTrace, "a").getStartTime should be(startTimestamp)
      getSpan(processedTrace, "a").getServiceName should be("x")

      getSpan(processedTrace, "b").getStartTime should be(startTimestamp)
      getSpan(processedTrace, "b").getServiceName should be("y")

      getSpan(processedTrace, "c").getStartTime should be(startTimestamp + 500)
      getSpan(processedTrace, "c").getServiceName should be("x")

      getSpan(processedTrace, "d").getStartTime should be(startTimestamp )
      getSpan(processedTrace, "d").getServiceName should be("y")

      getSpan(processedTrace, "e").getStartTime should be(startTimestamp + 200)
      getSpan(processedTrace, "e").getServiceName should be("y")
    }

    it("should successfully process a valid complex multi-service trace") {
      Given("a valid multi-service trace ")
      val trace = buildMultiServiceTrace()

      When("invoking process")
      val processedTraceOption = traceProcessor.process(trace)

      Then("successfully process trace")
      processedTraceOption.isSuccess should be(true)
      val processedTrace = processedTraceOption.get

      processedTrace.getChildSpansList.size() should be(6)
      getSpan(processedTrace, "a").getStartTime should be(startTimestamp)
      getSpan(processedTrace, "a").getServiceName should be("w")

      getSpan(processedTrace, "b").getStartTime should be(startTimestamp + 20)
      getSpan(processedTrace, "b").getServiceName should be("x")

      getSpan(processedTrace, "c").getStartTime should be(startTimestamp + 520)
      getSpan(processedTrace, "c").getServiceName should be("y")

      getSpan(processedTrace, "d").getStartTime should be(startTimestamp + 20)
      getSpan(processedTrace, "d").getServiceName should be("x")

      getSpan(processedTrace, "e").getStartTime should be(startTimestamp + 20)
      getSpan(processedTrace, "e").getServiceName should be("x")

      getSpan(processedTrace, "f").getStartTime should be(startTimestamp + 540)
      getSpan(processedTrace, "f").getServiceName should be("z")
    }
  }
}
