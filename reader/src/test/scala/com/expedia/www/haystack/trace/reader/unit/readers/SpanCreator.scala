package com.expedia.www.haystack.trace.reader.unit.readers

import com.expedia.open.tracing.api.Trace
import com.expedia.open.tracing.{Log, Span, Tag}

import scala.collection.JavaConversions._

// helper to create various types of traces for unit testing
trait SpanCreator {
  val startTimestamp = 150000000000l
  val traceId = "traceId"

  private def toTrace(spans: Span*): Trace = Trace.newBuilder().setTraceId(traceId).addAllChildSpans(spans).build

  private def createServerSpanTags(start: Long, end: Long) = List(
    Log.newBuilder()
      .setTimestamp(start)
      .addFields(Tag.newBuilder().setKey("event").setVStr("sr").build())
      .build(),
    Log.newBuilder()
      .setTimestamp(end)
      .addFields(Tag.newBuilder().setKey("event").setVStr("ss").build())
      .build()
  )

  private def createClientSpanTags(start: Long, end: Long) = List(
    Log.newBuilder()
      .setTimestamp(start)
      .addFields(Tag.newBuilder().setKey("event").setVStr("cs").build())
      .build(),
    Log.newBuilder()
      .setTimestamp(end)
      .addFields(Tag.newBuilder().setKey("event").setVStr("cr").build())
      .build()
  )

  protected def getSpan(trace: Trace, spanId: String): Span = trace.getChildSpansList.find(_.getSpanId == spanId).get

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
  def createSimpleLinerTrace(): Trace = {
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
    * trace with multiple root spans
    *
    * ..................................................... x
    *   a |=========| b |===================|
    *                 c |-------------------|
    *                         d |------|
    *
    */
  def createMultiRootTrace(): Trace = {
    val aSpan = Span.newBuilder()
      .setSpanId("a")
      .setTraceId(traceId)
      .setServiceName("x")
      .setStartTime(startTimestamp)
      .setDuration(300)
      .addAllLogs(createServerSpanTags(startTimestamp, startTimestamp + 300))
      .build()

    val bSpan = Span.newBuilder()
      .setSpanId("b")
      .setTraceId(traceId)
      .setServiceName("x")
      .setStartTime(startTimestamp + 500)
      .setDuration(500)
      .addAllLogs(createServerSpanTags(startTimestamp + 500, startTimestamp + 500 + 500))
      .build()

    val cSpan = Span.newBuilder()
      .setSpanId("c")
      .setParentSpanId("b")
      .setTraceId(traceId)
      .setServiceName("x")
      .setStartTime(startTimestamp + 500)
      .setDuration(500)
      .addAllLogs(createClientSpanTags(startTimestamp + 500, startTimestamp + 500 + 500))
      .build()

    val dSpan = Span.newBuilder()
      .setSpanId("d")
      .setParentSpanId("b")
      .setTraceId(traceId)
      .setServiceName("x")
      .setStartTime(startTimestamp + 750)
      .setDuration(200)
      .addAllLogs(createClientSpanTags(startTimestamp + 750, startTimestamp + 750 + 200))
      .build()

    toTrace(aSpan, bSpan, cSpan, dSpan)
  }

  /**
    * trace with multiple root spans
    *
    * ..................................................... x
    *   a |============================|
    *   b |----------------------------|
    * ..................................................... y
    *   b |==========| b |========|
    *
    */
  def createMultiServerSpanForAClientSpanTrace(): Trace = {
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

    val bFirstServerSpan = Span.newBuilder()
      .setSpanId("b")
      .setParentSpanId("a")
      .setTraceId(traceId)
      .setServiceName("y")
      .setStartTime(startTimestamp)
      .setDuration(500)
      .addAllLogs(createClientSpanTags(startTimestamp, startTimestamp + 500))
      .build()

    val bSecondServerSpan = Span.newBuilder()
      .setSpanId("b")
      .setParentSpanId("a")
      .setTraceId(traceId)
      .setServiceName("z")
      .setStartTime(startTimestamp + 500)
      .setDuration(500)
      .addAllLogs(createClientSpanTags(startTimestamp + 500, startTimestamp + 500 + 500))
      .build()

    toTrace(aSpan, bSpan, bFirstServerSpan, bSecondServerSpan)
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
  def createMultiServiceTrace(): Trace = {
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
  def createMultiServiceWithoutSkewTrace(): Trace = {
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
  def createMultiServiceWithPositiveSkewTrace(): Trace = {
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
  def createMultiServiceWithNegativeSkewTrace(): Trace = {
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
  def createMultiLevelSkewTrace(): Trace = {
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
