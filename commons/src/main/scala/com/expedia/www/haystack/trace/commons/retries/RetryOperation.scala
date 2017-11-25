/*
 *
 *     Copyright 2017 Expedia, Inc.
 *
 *      Licensed under the Apache License, Version 2.0 (the "License");
 *      you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *      Unless required by applicable law or agreed to in writing, software
 *      distributed under the License is distributed on an "AS IS" BASIS,
 *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *      See the License for the specific language governing permissions and
 *      limitations under the License.
 *
 */

package com.expedia.www.haystack.trace.commons.retries

import scala.concurrent.duration.FiniteDuration

object RetryOperation {

  trait Callback {
    def onResult(shouldRetry: Boolean): Unit
  }

  def executeAsyncWithRetryBackoff(f: (Callback) => Unit,
                                   maxRetries: Int,
                                   backOffDuration: FiniteDuration,
                                   onSuccess: () => Unit,
                                   onFailure: (Exception) => Unit): Unit = {
    executeAsyncWithRetryBackoff(f, 0, maxRetries, backOffDuration, onSuccess, onFailure)
  }

  private def executeAsyncWithRetryBackoff(f: (Callback) => Unit,
                                           currentRetry: Int,
                                           maxRetries: Int,
                                           backOffDuration: FiniteDuration,
                                           onSuccess: () => Unit,
                                           onFailure: (Exception) => Unit): Unit = {
    try {
      val asyncRetryResult = new Callback {
        override def onResult(shouldRetry: Boolean): Unit = {
          if (shouldRetry) {
            if (currentRetry < maxRetries) {
              Thread.sleep(backOffDuration.toMillis)
              executeAsyncWithRetryBackoff(f, currentRetry + 1, maxRetries, backOffDuration, onSuccess, onFailure)
            } else {
              onFailure(new MaxRetriesAttemptedException(s"max retries=$maxRetries have reached and all attempts have failed!"))
            }
          } else {
            onSuccess()
          }
        }
      }
      f(asyncRetryResult)
    } catch {
      case ex: Exception => onFailure(ex)
    }
  }
}