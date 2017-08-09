package com.expedia.www.haystack.span.stitcher.store.traits

/**
  * this is an observer that is called whenever maxSize of the cache is changed. This happens when kafka partitions
  * are assigned or revoked resulting in a change of total number of state stores
  */
trait CacheSizeObserver {
  def onCacheSizeChange(maxEntries: Int): Unit
}
