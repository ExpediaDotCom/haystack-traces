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

object RetryOperation {
  case class Config(maxRetries: Int, initialBackoffInMillis: Long, backoffFactor: Double) {
    def nextBackOffConfig: Config = this.copy(initialBackoffInMillis = Math.ceil(initialBackoffInMillis * backoffFactor).toLong)
  }

  trait Callback {
    def onResult(shouldRetry: Boolean): Unit
  }

  def executeAsyncWithRetryBackoff(f: (Callback) => Unit,
                                   retryConfig: Config,
                                   onSuccess: () => Unit,
                                   onFailure: (Exception) => Unit): Unit = {
    executeAsyncWithRetryBackoff(f, 0, retryConfig, onSuccess, onFailure)
  }

  private def executeAsyncWithRetryBackoff(f: (Callback) => Unit,
                                           currentRetry: Int,
                                           retryConfig: Config,
                                           onSuccess: () => Unit,
                                           onFailure: (Exception) => Unit): Unit = {
    try {
      val asyncRetryResult = new Callback {
        override def onResult(shouldRetry: Boolean): Unit = {
          if (shouldRetry) {
            if (currentRetry < retryConfig.maxRetries) {
              Thread.sleep(retryConfig.initialBackoffInMillis)
              executeAsyncWithRetryBackoff(f, currentRetry + 1, retryConfig.nextBackOffConfig , onSuccess, onFailure)
            } else {
              onFailure(new MaxRetriesAttemptedException(s"max retries=${retryConfig.maxRetries} have reached and all attempts have failed!"))
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