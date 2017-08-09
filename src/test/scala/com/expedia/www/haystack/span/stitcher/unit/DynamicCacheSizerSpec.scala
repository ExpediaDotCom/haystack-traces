package com.expedia.www.haystack.span.stitcher.unit

import com.expedia.www.haystack.span.stitcher.store.DynamicCacheSizer
import com.expedia.www.haystack.span.stitcher.store.traits.CacheSizeObserver
import org.scalatest.easymock.EasyMockSugar
import org.scalatest.{FunSpec, Matchers}

class DynamicCacheSizerSpec extends FunSpec with Matchers with EasyMockSugar {
  private val MAX_CACHE_ENTRIES = 500
  private val INITIAL_CAPACITY = 100

  describe("dynamic cache sizer") {
    it("should notify the cache observer with new cache size") {
      val sizer = new DynamicCacheSizer(INITIAL_CAPACITY, MAX_CACHE_ENTRIES)
      val observer = mock[CacheSizeObserver]
      expecting {
        observer.onCacheSizeChange(MAX_CACHE_ENTRIES)
      }
      whenExecuting(observer) {
        sizer.addCacheObserver(observer)
      }
    }

    it("should notify multiple cache observers with new cache size") {
      val sizer = new DynamicCacheSizer(INITIAL_CAPACITY, MAX_CACHE_ENTRIES)
      val observer_1 = mock[CacheSizeObserver]
      val observer_2 = mock[CacheSizeObserver]

      expecting {
        observer_1.onCacheSizeChange(MAX_CACHE_ENTRIES)
        observer_1.onCacheSizeChange(MAX_CACHE_ENTRIES / 2)
        observer_2.onCacheSizeChange(MAX_CACHE_ENTRIES / 2)
      }
      whenExecuting(observer_1, observer_2) {
        sizer.addCacheObserver(observer_1)
        sizer.addCacheObserver(observer_2)
      }
    }

    it("should notify existing cache observers when an existing observer is removed with new cache size") {
      val sizer = new DynamicCacheSizer(INITIAL_CAPACITY, MAX_CACHE_ENTRIES)
      val observer_1 = mock[CacheSizeObserver]
      val observer_2 = mock[CacheSizeObserver]

      expecting {
        observer_1.onCacheSizeChange(MAX_CACHE_ENTRIES)
        observer_1.onCacheSizeChange(MAX_CACHE_ENTRIES / 2)
        observer_2.onCacheSizeChange(MAX_CACHE_ENTRIES / 2)
        observer_2.onCacheSizeChange(MAX_CACHE_ENTRIES)
      }
      whenExecuting(observer_1, observer_2) {
        sizer.addCacheObserver(observer_1)
        sizer.addCacheObserver(observer_2)
        sizer.removeCacheObserver(observer_1)
      }
    }
  }
}
