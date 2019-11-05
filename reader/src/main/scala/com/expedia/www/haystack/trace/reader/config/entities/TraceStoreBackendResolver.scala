package com.expedia.www.haystack.trace.reader.config.entities

import com.expedia.www.haystack.trace.commons.config.entities.TraceStoreBackends

trait TraceStoreBackendResolver {
  def get: TraceStoreBackends
}
