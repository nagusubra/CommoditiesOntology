from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="hackathon.market_agents.silver_market_data",
    comment="Silver layer: structured oil and gas market data parsed from raw text using AI"
)
def silver_market_data():
    bronze = spark.read.table("hackathon.market_agents.bronze_market_data")

    parsed = bronze.select(
        F.col("id"),
        F.col("creation_date"),
        F.col("reported_date"),
        F.col("source_url"),
        F.expr("""
            ai_extract(
                text_data,
                array(
                    'headline',
                    'commodity',
                    'price',
                    'currency',
                    'price_direction',
                    'sentiment',
                    'summary'
                )
            )
        """).alias("parsed"),
        F.col("text_data"),
    ).select(
        "id",
        "creation_date",
        "reported_date",
        "source_url",
        F.col("parsed.headline").alias("headline"),
        F.col("parsed.commodity").alias("commodity"),
        F.col("parsed.price").alias("price"),
        F.col("parsed.currency").alias("currency"),
        F.col("parsed.price_direction").alias("price_direction"),
        F.col("parsed.sentiment").alias("sentiment"),
        F.col("parsed.summary").alias("summary"),
        "text_data",
    )

    # Filter to only oil and gas related articles
    oil_gas_keywords = "(?i)(oil|gas|crude|petroleum|brent|wti|lng|energy|barrel|opec|fuel|diesel|gasoline|hydrocarbon)"
    filtered = parsed.filter(
        F.col("commodity").rlike(oil_gas_keywords)
        | F.col("headline").rlike(oil_gas_keywords)
    )

    # Add per-article trade recommendation using AI
    return filtered.withColumn(
        "recommendation",
        F.expr("""
            ai_query(
                'databricks-meta-llama-3-3-70b-instruct',
                'Based on this oil/gas market article, provide a 1-2 sentence trading recommendation. Should traders buy, sell, or hold? Be specific about the commodity. Article: ' || summary || ' Sentiment: ' || sentiment || ' Price direction: ' || price_direction
            )
        """)
    )
