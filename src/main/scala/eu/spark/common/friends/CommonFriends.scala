package eu.spark.common.friends

import org.apache.spark.sql.{Row, SparkSession}


object CommonFriends {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("CommonFriends").master("local").getOrCreate()

    val rdd = spark.sparkContext.textFile("hdfs://10.0.0.2:9000/data/users_and_friends.txt")
    rdd.take(10).foreach(println(_))
    var userToFriendsRdd = rdd
      .map(line => line.split(","))
      .map(tokens => Row(tokens(0).toInt,
        tokens(1).split(" ").filter(!_.isEmpty).map(_.toInt).toList))


    println("Users and their corresponding friends")
    userToFriendsRdd.take(10).foreach(println(_))


    val useruserToFriendsRdd = userToFriendsRdd.flatMap { case row: Row =>
      val person = row(0).asInstanceOf[Int]
      val friends = row(1).asInstanceOf[Seq[Int]]

      friends.indices
        .map(i => (buildSortedTuple(person, friends(i)),
          friends.slice(0, i).union(friends.slice(i+1, friends.length))))
    }

    println("User-User tuples and their corresponding friends")
    useruserToFriendsRdd.take(10).foreach(println(_))

    val useruserToFriendsListRdd = useruserToFriendsRdd.groupByKey()

    println("User-User tuples and their corresponding friends lists")
    useruserToFriendsListRdd.take(10).foreach(println(_))

    val commonFriendsRdd = useruserToFriendsListRdd.mapValues(friendsLists =>{

      val listsCount = friendsLists.foldLeft(0)((sum,_) => sum + 1)

      val commonFriends  = friendsLists.flatten
        .groupBy(identity)
        .mapValues(f => f.foldLeft(0)((sum,_) => sum + 1))
        .filter(e => e._2 == listsCount)
        .keys
        .toList
        .sorted

      commonFriends
    }).filter(row => row._2.nonEmpty)
    .sortBy(row => (row._1._1, row._1._2))


    println("Common friends")
    commonFriendsRdd.take(10).foreach(println(_))

    spark.stop()
  }


  def buildSortedTuple(a: Long, b: Long): (Long, Long) = {
    if (a < b) new Tuple2[Long, Long](a, b)
    else new Tuple2[Long, Long](b, a)
  }


  def combine(in: List[Char]): Seq[String] =
    for {
      len <- 1 to in.length
      combinations <- in combinations len
    } yield combinations.mkString
}
