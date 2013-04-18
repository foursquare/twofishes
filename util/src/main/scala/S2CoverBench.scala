// Copyright 2013 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.twofishes

import com.foursquare.twofishes.util.GeometryUtils
import com.foursquare.twofishes.util.Lists.Implicits._
import com.google.caliper.{Runner, SimpleBenchmark}
import com.vividsolutions.jts.geom.Coordinate
import com.vividsolutions.jts.util.GeometricShapeFactory
//import org.bson.types.ObjectId
import scala.util.Random

class S2CoverBenchmark extends SimpleBenchmark {
  val rand = new Random(7)
  val points = (1 to 1000).map(_ => new Coordinate((rand.nextDouble*360) - 180, (rand.nextDouble*360) - 180))

/*
  for use in console:

  import com.vividsolutions.jts.geom.Coordinate
  def geocode2(points: Seq[Coordinate], radius: Int, levelMod: Int) = points.map(p => {
    println(p)
    import com.foursquare.twofishes.util.GeometryUtils
    import com.vividsolutions.jts.util.GeometricShapeFactory
    val sizeDegrees = radius / 111319.9
    val gsf = new GeometricShapeFactory()
    gsf.setSize(sizeDegrees)
    gsf.setNumPoints(100)
    gsf.setCentre(p) //new Coordinate(2.1684228875988367, 156.2735779249531))
    val geom = gsf.createCircle()
    GeometryUtils.coverAtAllLevels(
      geom,
      8, //store.getMinS2Level,
      12, //store.getMaxS2Level,
      Some(levelMod) //Some(store.getLevelMod)
    ).map(_.id())
  })
  */

  def geocode(radius: Int): Unit = {
    val sizeDegrees = radius / 111319.9 // req.radius / ...
    points.foreach(c => {
//      println(c)
      val gsf = new GeometricShapeFactory()
      gsf.setSize(sizeDegrees)
      gsf.setNumPoints(100)
      gsf.setCentre(c) //new Coordinate(40.74, -74.0))//req.ll.lng, req.ll.lat))
      val geom = gsf.createCircle()
      GeometryUtils.coverAtAllLevels(
        geom,
        8, //store.getMinS2Level,
        12, //store.getMaxS2Level,
        Some(2) //Some(store.getLevelMod)
      ).map(_.id())
     })
  }


  def helpTime[T](f: Unit => T)(reps: Int): Int = {
    var i = 0
    var sum = 0
    while (i < reps) {
      i += 1
      f()
      sum += i*i
    }
    sum
  }

  def time100(reps: Int): Unit = helpTime(Unit => geocode(100))(reps)
  def time1k(reps: Int): Unit = helpTime(Unit => geocode(1000))(reps)
  def time2k(reps: Int): Unit = helpTime(Unit => geocode(2*1000))(reps)
  def time5k(reps: Int): Unit = helpTime(Unit => geocode(5*1000))(reps)
  def time10k(reps: Int): Unit = helpTime(Unit => geocode(10*1000))(reps)
}

/** To run this benchmark:

mkdir benchmark-code
cd benchmark-code

after you change code:

util/assembly
cd .. && rm -rf benchmark-code && mkdir benchmark-code && cd benchmark-code && jar xvf ../util/target/util-assembly-0.74.7.jar
java -Xmx5G -Xms5G com.foursquare.twofishes.S2CoverBenchmark

This is sample output:

 0% Scenario{vm=java, trial=0, benchmark=100} 402026404.00 ns; σ=21686238.22 ns @ 10 trials
10% Scenario{vm=java, trial=0, benchmark=100_Naive} 1260211459.00 ns; σ=39077099.77 ns @ 10 trials
20% Scenario{vm=java, trial=0, benchmark=1k} 416720337.50 ns; σ=22510071.89 ns @ 10 trials
30% Scenario{vm=java, trial=0, benchmark=1k_Naive} 1236666454.00 ns; σ=41907758.68 ns @ 10 trials
40% Scenario{vm=java, trial=0, benchmark=2k} 503441022.00 ns; σ=2256590.83 ns @ 3 trials
50% Scenario{vm=java, trial=0, benchmark=2k_Naive} 1275882982.00 ns; σ=40804666.32 ns @ 10 trials
60% Scenario{vm=java, trial=0, benchmark=5k} 527209018.50 ns; σ=23747696.18 ns @ 10 trials
70% Scenario{vm=java, trial=0, benchmark=5k_Naive} 1348273375.50 ns; σ=21625954.85 ns @ 10 trials
80% Scenario{vm=java, trial=0, benchmark=10k} 689548497.50 ns; σ=21594835.09 ns @ 10 trials
90% Scenario{vm=java, trial=0, benchmark=10k_Naive} 1549261649.00 ns; σ=44358920.17 ns @ 10 trials

benchmark   ms linear runtime
      100  402 =======
100_Naive 1260 ========================
       1k  417 ========
 1k_Naive 1237 =======================
       2k  503 =========
 2k_Naive 1276 ========================
       5k  527 ==========
 5k_Naive 1348 ==========================
      10k  690 =============
10k_Naive 1549 ==============================


  */
object S2CoverBenchmark {
  def main(args: Array[String]): Unit = {
    Runner.main((classOf[S2CoverBenchmark].getName :: args.toList).toArray)
  }
}
