package com.expedia.www.haystack.span.stitcher.store

import java.util.concurrent.atomic.AtomicReference
import java.util.function.UnaryOperator

import com.expedia.www.haystack.span.stitcher.store.traits.CacheSizeObserver

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class DynamicCacheSizer(val defaultSizePerCache: Int, maxEntriesAcrossCaches: Int) {

  private val cacheObservers = new AtomicReference[mutable.ListBuffer[CacheSizeObserver]](mutable.ListBuffer[CacheSizeObserver]())

  /**
    * adds cache observer
    * @param observer state store acts as an observer
    */
  def addCacheObserver(observer: CacheSizeObserver): Unit = {
    val updatedObservers = cacheObservers.updateAndGet(new UnaryOperator[ListBuffer[CacheSizeObserver]] {
      override def apply(observers: ListBuffer[CacheSizeObserver]) = observers += observer
    })
    evaluateCacheSizing(updatedObservers)
  }

  /**
    * removes cache observer
    * @param observer state store acts as an observer
    */
  def removeCacheObserver(observer: CacheSizeObserver): Unit = {
    val updatedObservers = cacheObservers.updateAndGet(new UnaryOperator[ListBuffer[CacheSizeObserver]] {
      override def apply(observers: ListBuffer[CacheSizeObserver]) = observers -= observer
    })
    evaluateCacheSizing(updatedObservers)
  }

  /**
    * notify the observers with a change in their cache size
    * @param observers list of observers
    * @param newMaxEntriesPerCache new cache size
    */
  private def notifyObservers(observers: mutable.ListBuffer[CacheSizeObserver], newMaxEntriesPerCache: Int): Unit = {
    observers.foreach(obs => obs.onCacheSizeChange(newMaxEntriesPerCache))
  }

  /**
    * Cache sizing strategy is simple, distribute the maxEntriesAcrossCaches across all observers
    * @param observers list of changed observers
    */
  private def evaluateCacheSizing(observers: mutable.ListBuffer[CacheSizeObserver]): Unit = {
    if(observers.nonEmpty) {
      val newMaxEntriesPerCache = Math.floor(maxEntriesAcrossCaches / observers.size).toInt
      notifyObservers(observers, newMaxEntriesPerCache)
    }
  }
}