package com.practice.spark.eda

import com.practice.spark.utils.SqlConnector
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Eda {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("HelloWorld")
      .master("local[*]")
      .config(new SparkConf().setAppName("name").setMaster("local")
        .set("spark.testing.memory", "2147480000"))
      .getOrCreate()

    import spark.implicits._
    val sqlConnector = new SqlConnector();

    val dfSongPlayFact = sqlConnector.fetch(spark, "songplay_fact")
    val dfUserDim = sqlConnector.fetch(spark, "user_dim")
    val dfSongDim = sqlConnector.fetch(spark, "song_dim")
    val dfArtistDim = sqlConnector.fetch(spark, "artist_dim")

    // join all, just take a look
    val dfSongPlayDataJoined = dfSongPlayFact.as("dfSongPlayFact")
      .join(dfUserDim.as("dfUserDim"), $"dfSongPlayFact.user_pk" === $"dfUserDim.user_pk")
      .join(dfSongDim.as("dfSongDim"), $"dfSongPlayFact.song_pk" === $"dfSongDim.song_pk")
      .join(dfArtistDim.as("dfArtistDim"), $"dfSongPlayFact.artist_pk" === $"dfArtistDim.artist_pk")
      .show()

    // count played per song (never played = 0)
    // using simple group by
    val dfDurationAndCountPerSong = dfSongPlayFact.as("dfSongPlayFact")
      .join(dfSongDim.as("dfSongDim"), $"dfSongPlayFact.song_pk" === $"dfSongDim.song_pk", "right")
      .groupBy($"song_id", $"title".as("song_title"))
      .agg(
        count($"dfSongPlayFact.song_pk").as("count_played"))
      .select($"song_id", $"song_title", $"count_played")
      .show()

    // for each song rank user by how many times that user played that song
    // using window function
    val dfUserRankPerSong = dfSongPlayFact.as("dfSongPlayFact")
      .join(dfUserDim.as("dfUserDim"), $"dfSongPlayFact.user_pk" === $"dfUserDim.user_pk")
      .join(dfSongDim.as("dfSongDim"), $"dfSongPlayFact.song_pk" === $"dfSongDim.song_pk")
      .groupBy($"song_id", $"user_id").agg(count($"*").as("count_played"))
      .withColumn("dense_rank_this_song",
        dense_rank().over(Window.partitionBy("song_id").orderBy(desc("count_played"))))
      .select("user_id", "song_id", "count_played", "dense_rank_this_song")
      .show()
  }
}

