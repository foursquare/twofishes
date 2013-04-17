package com.foursquare.twofishes

// Copyright 2012 Foursquare Labs Inc. All Rights Reserved.

import com.twitter.ostrich.stats.Stats
import java.util.concurrent.{BlockingQueue, ExecutorService, LinkedBlockingQueue, ThreadFactory, ThreadPoolExecutor,
    TimeUnit, atomic}

case class NamedThreadFactory(name: String) extends ThreadFactory {
  val threadNumber = new atomic.AtomicInteger(1)
  val namePrefix = "pool-" + name + "-thread-"

  def newThread(r: Runnable): Thread =  {
    val t = new Thread(r, namePrefix + threadNumber.getAndIncrement())
    t.setDaemon(true)
    t
  }
}


object StatsWrappedExecutors {
  
  def create(coreSize: Int, maxSize: Int, name: String, 
      queue: BlockingQueue[Runnable] = new LinkedBlockingQueue[Runnable]()): ExecutorService = {
    Stats.addGauge("ThreadPool-" + name + "-Queue") {
      queue.size().toDouble
    }

    new ThreadPoolExecutor(
      coreSize, maxSize,
      0L, TimeUnit.MILLISECONDS,
      queue,
      NamedThreadFactory(name))
  }
}