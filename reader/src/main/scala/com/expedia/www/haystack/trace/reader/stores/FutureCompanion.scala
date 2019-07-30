/*
 *  Copyright 2019 Expedia, Inc.
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

package com.expedia.www.haystack.trace.reader.stores

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

trait FutureCompanion {

  def allAsTrys[T](fItems: List[Future[T]])(implicit ec: ExecutionContext): Future[List[Try[T]]] = {
    val listOfFutureTrys: List[Future[Try[T]]] = fItems.map(futureToFutureTry)
    Future.sequence(listOfFutureTrys)
  }

  def futureToFutureTry[T](f: Future[T])(implicit ec: ExecutionContext): Future[Try[T]] = {
    f.map(Success(_)).recover({ case e: Exception => {
      Failure(e)
    }
    })
  }

  def allFailedAsTrys[T](fItems: List[Future[T]])(implicit ec: ExecutionContext): Future[List[Try[T]]] = {
    allAsTrys(fItems).map(_.filter(_.isFailure))
  }

  def allSucceededAsTrys[T](fItems: List[Future[T]])(implicit ec: ExecutionContext): Future[List[Try[T]]] = {
    allAsTrys(fItems).map(_.filter(_.isSuccess))
  }

}
