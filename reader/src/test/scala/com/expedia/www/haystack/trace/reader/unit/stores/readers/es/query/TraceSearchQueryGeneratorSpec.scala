/*
 *  Copyright 2017 Expedia, Inc.
 *
 *       Licensed under the Apache License, Version 2.0 (the "License");
 *       you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *       Unless required by applicable law or agreed to in writing, software
 *       distributed under the License is distributed on an "AS IS" BASIS,
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *       See the License for the specific language governing permissions and
 *       limitations under the License.
 */
package com.expedia.www.haystack.trace.reader.unit.stores.readers.es.query

import com.expedia.open.tracing.api.ExpressionTree.Operator
import com.expedia.open.tracing.api.{ExpressionTree, Field, Operand, TracesSearchRequest}
import com.expedia.www.haystack.trace.commons.clients.es.document.TraceIndexDoc
import com.expedia.www.haystack.trace.commons.config.entities.WhitelistIndexFieldConfiguration
import com.expedia.www.haystack.trace.reader.config.entities.ElasticSearchConfiguration
import com.expedia.www.haystack.trace.reader.stores.readers.es.ESUtils._
import com.expedia.www.haystack.trace.reader.stores.readers.es.query.TraceSearchQueryGenerator
import com.expedia.www.haystack.trace.reader.unit.BaseUnitTestSpec
import io.searchbox.core.Search

class TraceSearchQueryGeneratorSpec extends BaseUnitTestSpec {
  private val esConfig = ElasticSearchConfiguration("endpoint", None, None, "haystack-traces", "spans", 5000, 5000, 6, 72, false)

  describe("TraceSearchQueryGenerator") {
    it("should generate valid search queries") {
      Given("a trace search request")
      val serviceName = "svcName"
      val operationName = "opName"
      val request = TracesSearchRequest
        .newBuilder()
        .addFields(Field.newBuilder().setName(TraceIndexDoc.SERVICE_KEY_NAME).setValue(serviceName).build())
        .addFields(Field.newBuilder().setName("operation").setValue(operationName).build())
        .setStartTime(1)
        .setEndTime(System.currentTimeMillis() * 1000)
        .setLimit(10)
        .build()
      val queryGenerator = new TraceSearchQueryGenerator(esConfig, "spans", new WhitelistIndexFieldConfiguration)

      When("generating query")
      val query = queryGenerator.generate(request)

      Then("generate a valid query")
      query.getType should be("spans")
    }

    it("should generate caption independent search queries") {
      Given("a trace search request")
      val fieldKey = "svcName"
      val fieldValue = "opName"
      val request = TracesSearchRequest
        .newBuilder()
        .addFields(Field.newBuilder().setName(fieldKey).setValue(fieldValue).build())
        .setStartTime(1)
        .setEndTime(System.currentTimeMillis() * 1000)
        .setLimit(10)
        .build()
      val queryGenerator = new TraceSearchQueryGenerator(esConfig, "spans", new WhitelistIndexFieldConfiguration)

      When("generating query")
      val query: Search = queryGenerator.generate(request)

      Then("generate a valid query with fields in lowercase")
      query.toJson.contains(fieldKey.toLowerCase()) should be(true)
    }

    it("should generate valid search queries for expression tree based searches") {
      Given("a trace search request")
      val fieldKey = "svcName"
      val fieldValue = "svcValue"
      val expression = ExpressionTree
        .newBuilder()
        .setOperator(Operator.AND)
        .addOperands(Operand.newBuilder().setField(Field.newBuilder().setName(fieldKey).setValue(fieldValue)))
        .addOperands(Operand.newBuilder().setField(Field.newBuilder().setName("1").setValue("1")))
        .addOperands(Operand.newBuilder().setField(Field.newBuilder().setName("2").setValue("2")))
        .addOperands(Operand.newBuilder().setField(Field.newBuilder().setName("3").setValue("3")))
        .build()

      val request = TracesSearchRequest
        .newBuilder()
        .setFilterExpression(expression)
        .setStartTime(1)
        .setEndTime(System.currentTimeMillis() * 1000)
        .setLimit(10)
        .build()

      val queryGenerator = new TraceSearchQueryGenerator(esConfig, "spans", new WhitelistIndexFieldConfiguration)

      When("generating query")
      val query: Search = queryGenerator.generate(request)

      Then("generate a valid query with fields in lowercase")
      query.toJson.contains(fieldKey.toLowerCase()) should be(true)
    }

    it("should generate valid search queries for expression tree based searches with span level searches") {
      Given("a trace search request")
      val fieldKey = "svcName"
      val fieldValue = "svcValue"
      val spanLevelTreeFirst = ExpressionTree
        .newBuilder()
        .setOperator(Operator.AND)
        .setIsSpanLevelExpression(true)
        .addOperands(Operand.newBuilder().setField(Field.newBuilder().setName("1").setValue("1")))
        .addOperands(Operand.newBuilder().setField(Field.newBuilder().setName("2").setValue("2")))
        .addOperands(Operand.newBuilder().setField(Field.newBuilder().setName("3").setValue("3")))
        .build()

      val spanLevelTreeSecond = ExpressionTree
        .newBuilder()
        .setOperator(Operator.AND)
        .setIsSpanLevelExpression(true)
        .addOperands(Operand.newBuilder().setField(Field.newBuilder().setName("4").setValue("4")))
        .addOperands(Operand.newBuilder().setField(Field.newBuilder().setName("5").setValue("5")))
        .build()

      val expression = ExpressionTree
        .newBuilder()
        .setOperator(Operator.AND)
        .setIsSpanLevelExpression(true)
        .addOperands(Operand.newBuilder().setField(Field.newBuilder().setName(fieldKey).setValue(fieldValue)))
        .addOperands(Operand.newBuilder().setField(Field.newBuilder().setName("0").setValue("0")))
        .addOperands(Operand.newBuilder().setExpression(spanLevelTreeFirst))
        .addOperands(Operand.newBuilder().setExpression(spanLevelTreeSecond))
        .build()

      val request = TracesSearchRequest
        .newBuilder()
        .setFilterExpression(expression)
        .setStartTime(1)
        .setEndTime(System.currentTimeMillis() * 1000)
        .setLimit(10)
        .build()

      val queryGenerator = new TraceSearchQueryGenerator(esConfig, "spans", new WhitelistIndexFieldConfiguration)

      When("generating query")
      val query: Search = queryGenerator.generate(request)

      Then("generate a valid query with fields in lowercase")
      query.toJson.contains(fieldKey.toLowerCase()) should be(true)
    }
  }
}
