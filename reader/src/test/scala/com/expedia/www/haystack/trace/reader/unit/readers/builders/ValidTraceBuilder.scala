package com.expedia.www.haystack.trace.reader.unit.readers.builders

import com.expedia.open.tracing.api.Trace
import com.expedia.open.tracing.{Log, Span, Tag}

import scala.collection.JavaConversions._

// helper to create various types of traces for unit testing
trait ValidTraceBuilder extends TraceBuilder {
  /**
    * simple liner trace with a sequence of sequential spans
    *
    * ..................................................... x
    *   a |==================================|
    *     b |-------------------|
    *                         c |------|
    *                                d |---|
    *
    */
  def buildSimpleLinerTrace(): Trace = {
    val aSpan = Span.newBuilder()
      .setSpanId("a")
      .setTraceId(traceId)
      .setServiceName("x")
      .setStartTime(startTimestamp)
      .setDuration(1000)
      .addAllLogs(createServerSpanTags(startTimestamp, startTimestamp + 1000))
      .build()

    val bSpan = Span.newBuilder()
      .setSpanId("b")
      .setParentSpanId("a")
      .setTraceId(traceId)
      .setServiceName("x")
      .setStartTime(startTimestamp + 50)
      .setDuration(500)
      .addAllLogs(createClientSpanTags(startTimestamp + 50, startTimestamp + 50 + 500))
      .build()

    val cSpan = Span.newBuilder()
      .setSpanId("c")
      .setParentSpanId("a")
      .setTraceId(traceId)
      .setServiceName("x")
      .setStartTime(startTimestamp + 550)
      .setDuration(200)
      .addAllLogs(createClientSpanTags(startTimestamp + 550, startTimestamp + 550 + 200))
      .build()

    val dSpan = Span.newBuilder()
      .setSpanId("d")
      .setParentSpanId("a")
      .setTraceId(traceId)
      .setServiceName("x")
      .setStartTime(startTimestamp + 750)
      .setDuration(200)
      .addAllLogs(createClientSpanTags(startTimestamp + 750, startTimestamp + 750 + 200))
      .build()

    toTrace(aSpan, bSpan, cSpan, dSpan)
  }

  /**
    * trace spanning multiple services, assume network delta to be 20ms
    *
    * ...................................................... w
    *   a |============================================|
    *   b |---------------------|
    *                         c |----------------------|
    *
    *  ..................................................... x
    *    b |==================|
    *    d |--------|
    *    e |----------------|
    *
    *  ..................................................... y
    *                          c |====================|
    *                          f |----------|
    *
    * ..................................................... y
    *                           f |========|
    */
  def buildMultiServiceTrace(): Trace = {
    val aSpan = Span.newBuilder()
      .setSpanId("a")
      .setTraceId(traceId)
      .setServiceName("w")
      .setStartTime(startTimestamp)
      .setDuration(1000)
      .addAllLogs(createServerSpanTags(startTimestamp, startTimestamp + 1000))
      .build()

    val bSpan = Span.newBuilder()
      .setSpanId("b")
      .setParentSpanId("a")
      .setTraceId(traceId)
      .setServiceName("w")
      .setStartTime(startTimestamp)
      .setDuration(500)
      .addAllLogs(createClientSpanTags(startTimestamp, startTimestamp + 500))
      .build()

    val cSpan = Span.newBuilder()
      .setSpanId("c")
      .setParentSpanId("a")
      .setTraceId(traceId)
      .setServiceName("w")
      .setStartTime(startTimestamp + 500)
      .setDuration(500)
      .addAllLogs(createClientSpanTags(startTimestamp + 500, startTimestamp + 500 + 500))
      .build()

    val bServerSpan = Span.newBuilder()
      .setSpanId("b")
      .setParentSpanId("a")
      .setTraceId(traceId)
      .setServiceName("x")
      .setStartTime(startTimestamp + 20)
      .setDuration(460)
      .addAllLogs(createServerSpanTags(startTimestamp + 20, startTimestamp + 20 + 460))
      .build()

    val dSpan = Span.newBuilder()
      .setSpanId("d")
      .setParentSpanId("b")
      .setTraceId(traceId)
      .setServiceName("x")
      .setStartTime(startTimestamp + 20)
      .setDuration(200)
      .addAllLogs(createClientSpanTags(startTimestamp + 20, startTimestamp + 20 + 200))
      .build()

    val eSpan = Span.newBuilder()
      .setSpanId("e")
      .setParentSpanId("b")
      .setTraceId(traceId)
      .setServiceName("x")
      .setStartTime(startTimestamp + 20)
      .setDuration(400)
      .addAllLogs(createClientSpanTags(startTimestamp + 20, startTimestamp + 20 + 400))
      .build()

    val cServerSpan = Span.newBuilder()
      .setSpanId("c")
      .setParentSpanId("a")
      .setTraceId(traceId)
      .setServiceName("y")
      .setStartTime(startTimestamp + 520)
      .setDuration(460)
      .addAllLogs(createServerSpanTags(startTimestamp + 520, startTimestamp + 520 + 460))
      .build()

    val fSpan = Span.newBuilder()
      .setSpanId("f")
      .setParentSpanId("c")
      .setTraceId(traceId)
      .setServiceName("y")
      .setStartTime(startTimestamp + 520)
      .setDuration(100)
      .addAllLogs(createClientSpanTags(startTimestamp + 520, startTimestamp + 520 + 100))
      .build()

    val fServerSpan = Span.newBuilder()
      .setSpanId("f")
      .setParentSpanId("c")
      .setTraceId(traceId)
      .setServiceName("z")
      .setStartTime(startTimestamp + 540)
      .setDuration(100)
      .addAllLogs(createServerSpanTags(startTimestamp + 540, startTimestamp + 540 + 50))
      .build()

    toTrace(aSpan, bSpan, cSpan, bServerSpan, dSpan, eSpan, cServerSpan, fSpan, fServerSpan)
  }
}
