package com.practice.spark.utils

import org.apache.spark.sql.{DataFrame, SparkSession}

class SqlConnector {
  def write(df: DataFrame, dbTable: String) = {
    df.write.format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/ddm_spark")
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", dbTable)
      .option("user", "db_user")
      .option("password", "db_user")
      .mode("append")
      .save()
  }

  def fetch(sparkSession: SparkSession, dbTable: String) : DataFrame = {
    val df = sparkSession.read.format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/ddm_spark")
      .option("driver", "org.postgresql.Driver")
      .option("dbtable",dbTable)
      .option("user", "db_user")
      .option("password", "db_user")
      .load()

    df
  }
}
