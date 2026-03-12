from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="hackathon.market_agents.gold_market_summary",
    comment="Gold layer: aggregated oil and gas market summary by commodity and date"
)
def gold_market_summary():
    silver = spark.read.table("hackathon.market_agents.silver_market_data")

    agg = silver.groupBy(
        F.to_date("creation_date").alias("market_date"),
        F.col("commodity"),
    ).agg(
        F.count("*").alias("article_count"),
        F.sum(F.when(F.col("sentiment") == "positive", 1).otherwise(0)).alias("positive_count"),
        F.sum(F.when(F.col("sentiment") == "negative", 1).otherwise(0)).alias("negative_count"),
        F.sum(F.when(F.col("sentiment") == "neutral", 1).otherwise(0)).alias("neutral_count"),
        F.collect_list("headline").alias("headlines"),
        F.collect_list("recommendation").alias("article_recommendations"),
        F.max("creation_date").alias("latest_entry"),
        F.min("reported_date").alias("earliest_reported_date"),
        F.max("reported_date").alias("latest_reported_date"),
    )

    # Add overall recommendation per commodity/date using AI
    return agg.withColumn(
        "overall_recommendation",
        F.expr("""
            ai_query(
                'databricks-meta-llama-3-3-70b-instruct',
                'You are an oil and gas trading advisor. Based on the following market data for ' || commodity || ' on ' || CAST(market_date AS STRING) || ': '
                || CAST(article_count AS STRING) || ' articles, '
                || CAST(positive_count AS STRING) || ' positive, '
                || CAST(negative_count AS STRING) || ' negative, '
                || CAST(neutral_count AS STRING) || ' neutral sentiment. '
                || 'Headlines: ' || CAST(headlines AS STRING) || '. '
                || 'Provide a 1-2 sentence overall trading recommendation: should traders buy, sell, or hold this commodity? Include reasoning.'
            )
        """)
    )
