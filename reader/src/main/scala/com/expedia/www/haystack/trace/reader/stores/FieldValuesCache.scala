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

package com.expedia.www.haystack.trace.reader.stores

import java.util.concurrent.locks.ReentrantLock

import com.expedia.open.tracing.api.FieldValuesRequest
import com.expedia.www.haystack.trace.reader.config.entities.FieldValuesCacheConfiguration
import org.apache.commons.collections4.map.LRUMap

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.Success

case class FieldValuesCacheEntity(value: Seq[String], lastRefreshTimestamp: Long)

class FieldValuesCache(config: FieldValuesCacheConfiguration,
                       refreshOp: (FieldValuesRequest) => Future[Seq[String]])(implicit val executor: ExecutionContextExecutor){

  private val cache = if (config.enabled) new LRUMap[FieldValuesRequest, FieldValuesCacheEntity](config.maxEntries) else null
  private val lock = new ReentrantLock()

  def get(key: FieldValuesRequest): Future[Seq[String]] = {
    if(cache == null) {
      refreshOp(key)
    } else {
      lock.lock()
      try {
        val result = cache.get(key)
        if (result != null && (System.currentTimeMillis() - result.lastRefreshTimestamp >= config.ttlInMillis)) {
          // reset the cache value so that next cache.get() doesn't rerun the refresh operation
          set(key, result.value)

          refreshOp(key) map { newValue =>
            set(key, newValue)
          }
          Future.successful(result.value)
        } else {
          refreshOp(key) andThen {
            case Success(v) => set(key, v)
            case _ =>
          }
        }
      } finally {
        lock.unlock()
      }
    }
  }

  private def set(key: FieldValuesRequest, value: Seq[String]): Unit = {
    lock.lock()
    try {
      cache.put(key, FieldValuesCacheEntity(value, System.currentTimeMillis()))
    } finally {
      lock.unlock()
    }
  }
}
