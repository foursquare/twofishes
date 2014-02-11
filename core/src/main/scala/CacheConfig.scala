// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.

// This is a work-around for ultilizing org.apache.hadoop.hbase.io.hfile.CacheConfig and calling one of its protected
// constructor to optionally specify a BlockCache implementation other than the default LruBlockCache. The LruBlockCache
// implementation is a bit too complex for our purpose. We do not need the cache layer to monitor and react to memory
// consumption.
// NOTE: The following package namespace violates our guidelines around placing source code in files where their paths
// match the namespace.
package org.apache.hadoop.hbase.io.hfile

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import org.apache.hadoop.conf.Configuration
import scalaj.collection.Implicits._


// The following is an in-memory BlockCache implementation where all blocks are stored in memory and never evicted.
private class TwofishesInMemoryBlockCache extends BlockCache {
  private val cache = new ConcurrentHashMap[BlockCacheKey, Cacheable]

  private val stats = new CacheStats

  private val sizeInBytes = new AtomicInteger

  def cacheBlock(cacheKey: BlockCacheKey, buf: Cacheable, inMemory: Boolean) = if (inMemory) {
    cache.put(cacheKey, buf)
    sizeInBytes.addAndGet(buf.getSerializedLength)
  } else throw new Exception("Using InMemoryBlockCache with inMemory disabled")

  def cacheBlock(cacheKey: BlockCacheKey, buf: Cacheable) = cacheBlock(cacheKey, buf, true)

  def evictBlock(cacheKey: BlockCacheKey) = throw new Exception("evictBlock not supported")

  def evictBlocksByHfileName(hfileName: String) = throw new Exception("evictBlocksByHfileName ot supported")

  def getBlock(cacheKey: BlockCacheKey, caching: Boolean, isRepeatRequest: Boolean) = cache.get(cacheKey)

  def getBlockCacheColumnFamilySummaries(conf: Configuration) = List.empty[BlockCacheColumnFamilySummary].asJava

  def getBlockCount = cache.size

  def getCurrentSize = sizeInBytes.get

  def getEvictedCount = 0

  def getFreeSize = 0

  def getStats = stats

  def shutdown = cache.clear

  def size = sizeInBytes.get
}

// The following is a simple wrapper around CacheConfig to force it to use the supplied BlockCache implementation.
class TwofishesFoursquareCacheConfig(config: Configuration, cacheLimit: Option[Int] = None) extends
    CacheConfig(cacheLimit.map(new LruBlockCache(_, 4*1024, config)).getOrElse(new TwofishesInMemoryBlockCache()),
                true, cacheLimit.isEmpty, true, true, true, true, false) {
      // TODO (norberthu): Not sure if inMemory arg should use cacheLimit.isEmpty or hardcod to true. Still need to
      // investigate how LruBlockCache.cacheBlock treats this flag. The documentation for CacheConfig is not apparent
      // what the contract for this flag indicates.
}
