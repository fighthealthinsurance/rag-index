package com.fighthealthinsurance.rag
import org.apache.spark.sql.{Dataset, DataFrame, SparkSession}

class BaseLoader {
  val urlRegex = """(https?://[^\s]+|www\.[^\s]+)"""
  val doiRegex = """10\.\d{4,9}/[-._;()/:A-Z0-9]+"""
  val relevantDocumentRegex = """(nih.gov|Category:Nutrition|modernmedicine|PLOS Medicine|veterinaryevidence|Portal bar \|Medicine|World Health Organization|cihr-irsc.gc.ca|nihr.ac.uk|nhs.uk)"""

  def loadBase(spark: SparkSession): DataFrame

  def extractAndAnotate(input: DataFrame)
}
