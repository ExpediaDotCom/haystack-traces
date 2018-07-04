/*
 *  Copyright 2018 Expedia, Inc.
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

package com.expedia.www.haystack.trace.reader.unit.stores.readers.es.query.helper

import com.expedia.open.tracing.api.ExpressionTree.Operator
import com.expedia.open.tracing.api.{ExpressionTree, Field, Operand}

object ExpressionTreeBuilder {

  val fieldKey = "svcName"
  val fieldValue = "svcValue"

  private val spanLevelTreeFirst = ExpressionTree
    .newBuilder()
    .setOperator(Operator.AND)
    .setIsSpanLevelExpression(true)
    .addOperands(Operand.newBuilder().setField(Field.newBuilder().setName("1").setValue("1")))
    .addOperands(Operand.newBuilder().setField(Field.newBuilder().setName("2").setValue("2")))
    .addOperands(Operand.newBuilder().setField(Field.newBuilder().setName("3").setValue("3")))
    .build()

  private val spanLevelTreeSecond = ExpressionTree
    .newBuilder()
    .setOperator(Operator.AND)
    .setIsSpanLevelExpression(true)
    .addOperands(Operand.newBuilder().setField(Field.newBuilder().setName("4").setValue("4")))
    .addOperands(Operand.newBuilder().setField(Field.newBuilder().setName("5").setValue("5")))
    .build()


  val operandLevelExpressionTree = ExpressionTree
    .newBuilder()
    .setOperator(Operator.AND)
    .addOperands(Operand.newBuilder().setField(Field.newBuilder().setName(fieldKey).setValue(fieldValue)))
    .addOperands(Operand.newBuilder().setField(Field.newBuilder().setName("1").setValue("1")))
    .addOperands(Operand.newBuilder().setField(Field.newBuilder().setName("2").setValue("2")))
    .addOperands(Operand.newBuilder().setField(Field.newBuilder().setName("3").setValue("3")))
    .build()

  val spanLevelExpressionTree = ExpressionTree
    .newBuilder()
    .setOperator(Operator.AND)
    .setIsSpanLevelExpression(true)
    .addOperands(Operand.newBuilder().setField(Field.newBuilder().setName(fieldKey).setValue(fieldValue)))
    .addOperands(Operand.newBuilder().setField(Field.newBuilder().setName("0").setValue("0")))
    .addOperands(Operand.newBuilder().setExpression(spanLevelTreeFirst))
    .addOperands(Operand.newBuilder().setExpression(spanLevelTreeSecond))
    .build()


}
