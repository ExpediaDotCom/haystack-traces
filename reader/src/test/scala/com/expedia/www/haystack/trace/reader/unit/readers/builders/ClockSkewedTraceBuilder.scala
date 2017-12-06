package com.expedia.www.haystack.trace.reader.unit.readers.builders

import com.expedia.open.tracing.Span
import com.expedia.open.tracing.api.Trace

import scala.collection.JavaConversions._

// helper to create various types of traces for unit testing
trait ClockSkewedTraceBuilder extends TraceBuilder {

  /**
    * trace spanning multiple services without clock skew
    *
    * ...................................................... x
    *     a |============================================|
    *     b |---------------------|
    *                           c |----------------------|
    *
    *  ..................................................... y
    *     b |=====================|
    *     d |---------|
    *               e |-----------|
    *
    */
  def buildMultiServiceWithoutSkewTrace(): Trace = {
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
      .setStartTime(startTimestamp)
      .setDuration(500)
      .addAllLogs(createClientSpanTags(startTimestamp, startTimestamp + 500))
      .build()

    val cSpan = Span.newBuilder()
      .setSpanId("c")
      .setParentSpanId("a")
      .setTraceId(traceId)
      .setServiceName("x")
      .setStartTime(startTimestamp + 500)
      .setDuration(500)
      .addAllLogs(createClientSpanTags(startTimestamp + 500, startTimestamp + 500 + 500))
      .build()

    val bServerSpan = Span.newBuilder()
      .setSpanId("b")
      .setParentSpanId("a")
      .setTraceId(traceId)
      .setServiceName("y")
      .setStartTime(startTimestamp)
      .setDuration(500)
      .addAllLogs(createServerSpanTags(startTimestamp, startTimestamp + 500))
      .build()

    val dSpan = Span.newBuilder()
      .setSpanId("d")
      .setParentSpanId("b")
      .setTraceId(traceId)
      .setServiceName("y")
      .setStartTime(startTimestamp)
      .setDuration(200)
      .addAllLogs(createClientSpanTags(startTimestamp, startTimestamp + 200))
      .build()

    val eSpan = Span.newBuilder()
      .setSpanId("e")
      .setParentSpanId("b")
      .setTraceId(traceId)
      .setServiceName("y")
      .setStartTime(startTimestamp + 200)
      .setDuration(300)
      .addAllLogs(createClientSpanTags(startTimestamp + 200, startTimestamp + 200 + 300))
      .build()

    toTrace(aSpan, bSpan, cSpan, bServerSpan, dSpan, eSpan)
  }

  /**
    * trace spanning multiple services with positive clock skew
    *
    * ...................................................... x
    *     a |============================================|
    *     b |---------------------|
    *                           c |----------------------|
    *
    *  ..................................................... y
    *        b |=====================|
    *        d |--------|
    *                 e |------------|
    *
    */
  def buildMultiServiceWithPositiveSkewTrace(): Trace = {
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
      .setStartTime(startTimestamp)
      .setDuration(500)
      .addAllLogs(createClientSpanTags(startTimestamp, startTimestamp + 500))
      .build()

    val cSpan = Span.newBuilder()
      .setSpanId("c")
      .setParentSpanId("a")
      .setTraceId(traceId)
      .setServiceName("x")
      .setStartTime(startTimestamp + 500)
      .setDuration(500)
      .addAllLogs(createClientSpanTags(startTimestamp + 500, startTimestamp + 500 + 500))
      .build()

    val bServerSpan = Span.newBuilder()
      .setSpanId("b")
      .setParentSpanId("a")
      .setTraceId(traceId)
      .setServiceName("y")
      .setStartTime(startTimestamp + 100)
      .setDuration(500)
      .addAllLogs(createServerSpanTags(startTimestamp + 100, startTimestamp + 100 + 500))
      .build()

    val dSpan = Span.newBuilder()
      .setSpanId("d")
      .setParentSpanId("b")
      .setTraceId(traceId)
      .setServiceName("y")
      .setStartTime(startTimestamp + 100)
      .setDuration(200)
      .addAllLogs(createClientSpanTags(startTimestamp + 100, startTimestamp + 100 + 200))
      .build()

    val eSpan = Span.newBuilder()
      .setSpanId("e")
      .setParentSpanId("b")
      .setTraceId(traceId)
      .setServiceName("y")
      .setStartTime(startTimestamp + 300)
      .setDuration(300)
      .addAllLogs(createClientSpanTags(startTimestamp + 300, startTimestamp + 300 + 300))
      .build()

    toTrace(aSpan, bSpan, cSpan, bServerSpan, dSpan, eSpan)
  }

  /**
    * trace spanning multiple services with negative clock skew
    *
    * ...................................................... x
    *     a |============================================|
    *     b |---------------------|
    *                           c |----------------------|
    *
    *  ..................................................... y
    *  b |===================|
    *  d |--------|
    *           e |----------|
    *
    */
  def buildMultiServiceWithNegativeSkewTrace(): Trace = {
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
      .setStartTime(startTimestamp)
      .setDuration(500)
      .addAllLogs(createClientSpanTags(startTimestamp, startTimestamp + 500))
      .build()

    val cSpan = Span.newBuilder()
      .setSpanId("c")
      .setParentSpanId("a")
      .setTraceId(traceId)
      .setServiceName("x")
      .setStartTime(startTimestamp + 500)
      .setDuration(500)
      .addAllLogs(createClientSpanTags(startTimestamp + 500, startTimestamp + 500 + 500))
      .build()

    val bServerSpan = Span.newBuilder()
      .setSpanId("b")
      .setParentSpanId("a")
      .setTraceId(traceId)
      .setServiceName("y")
      .setStartTime(startTimestamp - 100)
      .setDuration(500)
      .addAllLogs(createServerSpanTags(startTimestamp - 100, startTimestamp - 100 + 500))
      .build()

    val dSpan = Span.newBuilder()
      .setSpanId("d")
      .setParentSpanId("b")
      .setTraceId(traceId)
      .setServiceName("y")
      .setStartTime(startTimestamp - 100)
      .setDuration(200)
      .addAllLogs(createClientSpanTags(startTimestamp - 100, startTimestamp - 100 + 200))
      .build()

    val eSpan = Span.newBuilder()
      .setSpanId("e")
      .setParentSpanId("b")
      .setTraceId(traceId)
      .setServiceName("y")
      .setStartTime(startTimestamp + 100)
      .setDuration(300)
      .addAllLogs(createClientSpanTags(startTimestamp + 100, startTimestamp + 100 + 300))
      .build()

    toTrace(aSpan, bSpan, cSpan, bServerSpan, dSpan, eSpan)
  }

  /**
    * trace spanning multiple services with multi-level clock skew
    *
    * ...................................................... x
    *     a |============================================|
    *     b |--------------------------------------------|
    *
    *  ..................................................... y
    *  b |============================================|
    *  c |--------------------------------------------|
    *
    *  ..................................................... z
    *         c |============================================|
    *         d |--------------------------------------------|
    *
    */
  def buildMultiLevelSkewTrace(): Trace = {
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
      .setStartTime(startTimestamp)
      .setDuration(1000)
      .addAllLogs(createClientSpanTags(startTimestamp, startTimestamp + 1000))
      .build()

    val bServerSpan = Span.newBuilder()
      .setSpanId("b")
      .setParentSpanId("a")
      .setTraceId(traceId)
      .setServiceName("y")
      .setStartTime(startTimestamp - 100)
      .setDuration(1000)
      .addAllLogs(createServerSpanTags(startTimestamp - 100, startTimestamp - 100 + 1000))
      .build()

    val cSpan = Span.newBuilder()
      .setSpanId("c")
      .setParentSpanId("b")
      .setTraceId(traceId)
      .setServiceName("y")
      .setStartTime(startTimestamp - 100)
      .setDuration(1000)
      .addAllLogs(createClientSpanTags(startTimestamp - 100, startTimestamp -100 + 1000))
      .build()

    val cServerSpan = Span.newBuilder()
      .setSpanId("c")
      .setParentSpanId("b")
      .setTraceId(traceId)
      .setServiceName("z")
      .setStartTime(startTimestamp + 500)
      .setDuration(1000)
      .addAllLogs(createServerSpanTags(startTimestamp + 500, startTimestamp + 500 + 1000))
      .build()

    val dSpan = Span.newBuilder()
      .setSpanId("d")
      .setParentSpanId("c")
      .setTraceId(traceId)
      .setServiceName("z")
      .setStartTime(startTimestamp + 500)
      .setDuration(1000)
      .addAllLogs(createClientSpanTags(startTimestamp + 500, startTimestamp + 500 + 1000))
      .build()

    toTrace(aSpan, bSpan, cSpan, bServerSpan, cServerSpan, dSpan)
  }
}
