package com.expedia.www.haystack.trace.reader.config.entities

import com.expedia.www.haystack.trace.commons.config.entities.TraceStoreBackends

case class StaticTraceStoreBackedResolver (storeBackends: TraceStoreBackends) extends TraceStoreBackendResolver {

  override def get(): TraceStoreBackends = {
    storeBackends
  }
}
