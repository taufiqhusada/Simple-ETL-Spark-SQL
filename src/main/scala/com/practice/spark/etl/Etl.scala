package com.practice.spark.etl

import com.practice.spark.utils.SqlConnector
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Etl {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("HelloWorld")
      .master("local[*]")
      .config(new SparkConf().setAppName("name").setMaster("local")
        .set("spark.testing.memory", "2147480000"))
      .getOrCreate()

    val dfSongData = spark.read.option("multiline", "true").json("data/song_data.json")
    dfSongData.show()

    val dfLogData = spark.read.option("multiline", "true").json("data/log_data.json")
    dfLogData.show()

    // insert to user table
    val sqlConnector = new SqlConnector();

    import spark.implicits._
    val dfUserDim = dfLogData.select(
      $"userId".as("user_id").cast("int"),
      $"firstName".as("first_name"),
      $"lastName".as("last_name"),
      $"gender",
      $"level").distinct()  // distinct in here is important because the same user can appear twice in the log
    sqlConnector.write(dfUserDim, "user_dim")

    // insert to artist dim
    val dfArtistDim = dfSongData.select(
      $"artist_id",
      $"artist_name".alias("name"),
      $"artist_location".alias("location"),
      $"artist_latitude".alias("latitude").cast("float"),
      $"artist_longitude".alias("longitude").cast("float")
    )
    sqlConnector.write(dfArtistDim, "artist_dim")

    // insert into song dim
    val dfArtistPkandId = sqlConnector.fetch(spark, "artist_dim").select("artist_pk", "artist_id")
    val dfSongDim = dfSongData.as("dfSongData")
      .join(dfArtistPkandId.as("dfArtistPkandId"), $"dfSongData.artist_id" === $"dfArtistPkandId.artist_id")
      .select("song_id", "title", "artist_pk", "year", "duration")
    sqlConnector.write(dfSongDim, "song_dim")

    // insert time dim
    val dfTime = dfLogData.select($"ts".alias("start_time"))
      .withColumn("start_time", ($"start_time" / 1000).cast("timestamp"))
      .withColumn("hour", hour($"start_time"))
      .withColumn("day", dayofweek($"start_time"))
      .withColumn("week", weekofyear($"start_time"))
      .withColumn("month", month($"start_time"))
      .withColumn("year", year($"start_time"))
      .withColumn("weekday", when($"day" <= 5, 'Y').otherwise('N'))
    sqlConnector.write(dfTime, "time_dim")

    // insert to songplay_fact
    val dfLogDataNextSong = dfLogData.select($"ts".alias("start_time"), $"userId", $"level", $"song",
      $"location", $"userAgent".alias("user_agent"), $"sessionId".alias("session_id"))
      .where($"page" === "NextSong")
    dfLogDataNextSong.show()

    val dfUserIdAndPk = sqlConnector.fetch(spark, "user_dim").select("user_pk", "user_id")
    val dfSongIdAndPk = sqlConnector.fetch(spark, "song_dim").select("song_pk", "song_id", "artist_pk")

    val dfSongPlayFact = dfLogDataNextSong.as("dfLogDataNextSong")
      .join(dfUserIdAndPk.as("dfUserIdAndPk"), $"dfLogDataNextSong.userId" === $"dfUserIdAndPk.user_id", "inner")
      .join(dfSongIdAndPk.as("dfSongIdAndPk"), $"dfLogDataNextSong.song" === $"dfSongIdAndPk.song_id", "inner")
      .select(($"start_time" / 1000).cast("timestamp").alias("start_time"),
        $"user_pk", $"level", $"song_pk", $"artist_pk", $"session_id", $"location", $"user_agent")

    dfSongPlayFact.show()
    sqlConnector.write(dfSongPlayFact, "songplay_fact")
  }
}