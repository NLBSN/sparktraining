package org.training.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Description: 看过“Lord of the Rings, The (1978)”用户和年龄性别分布      将三个数据文件进行了一起处理，不再是人工提出movieID
  * UserID::MovieID::Rating::Timestamp            ratings.dat
  * UserID::Gender::Age::Occupation::Zip-code     users.dat
  * MovieID::Title::Genres                        movies.dat
  * @author: fan
  * @Date: Created in 2019/5/25 14:08
  * @Modified By: fan         --下面代码的()和{}没有进行严格的区分，主要是为了做个小测试。{}传入单一参数，()多参数的传递-------待验证这句话
  */
object MovieUserThreeAnalyzer {
  def main(args: Array[String]): Unit = {
    var dataPath = "data/ml-1m"
    val conf = new SparkConf().setAppName("PopularMovieAnalyzer")
    if (args.length > 0) {
      dataPath = args(0)
    } else {
      conf.setMaster("local[1]")
    }

    val sc = new SparkContext(conf)

    /**
      * @description 创建rdds
      */
    val DATA_PATH = dataPath
    val MOVIE_TITLE = "Lord of the Rings, The (1978)"

    val usersRdd = sc.textFile(DATA_PATH + "/users.dat")
    val ratingsRdd = sc.textFile(DATA_PATH + "/ratings.dat")
    val moviesRdd = sc.textFile(DATA_PATH + "/movies.dat")

    /**
      * @description 从rdds中提取所需要的列
      */
    // 从movies中根据电影名提取电影的id号
    // movies: RDD[(movieID,Title)]
    val movies = moviesRdd.map(_.split("::")).map { x => (x(0), x(1)) }.filter(_._2.equals(MOVIE_TITLE)).map(_._1)
    // 取到rdd的第一个元素
    val moviesID = movies.first()
    // rating: RDD[Array(userID, movieID, ratings, timestamp)]
    val rating = ratingsRdd.map(_.split("::")).map { x => (x(0), x(1)) }.filter(_._2.equals(moviesID))
    // users: RDD[(userID, (gender, age))]
    val users = usersRdd.map(_.split("::")).map(x => (x(0), (x(1), x(2))))
    // join RDDs       == RDD[(userID, (movieID, (gender, age))]
    // reduceByKey RDDs       == RDD[(movieID, (movieTile, (gender, age))]
    val userRating = rating.join(users).map(x => (x._2._2, 1)).reduceByKey(_ + _)
    userRating.collect().foreach(println)
    sc.stop()
  }
}
