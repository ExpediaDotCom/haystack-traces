package com.expedia.www.haystack.trace.reader.stores.readers.es

trait ElasticSearchResultListener {
  protected val INDEX_NOT_FOUND_EXCEPTION = "index_not_found_exception"
  protected def is2xx(code: Int): Boolean = (code / 100) == 2
}
