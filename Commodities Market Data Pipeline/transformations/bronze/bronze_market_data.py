from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql import functions as F
import json
import re
import uuid
from datetime import datetime


def _parse_articles(text):
    """Parse MCP response text into individual articles with URLs and published dates."""
    articles = []
    # Split on "Title:" to get individual article blocks
    blocks = re.split(r'\nTitle:\s*', text)
    for block in blocks[1:]:  # Skip the header before first Title
        title_match = re.match(r'(.+?)(?:\n|$)', block)
        url_match = re.search(r'URL:\s*(https?://\S+)', block)
        published_match = re.search(r'Published:\s*(\d{4}-\d{2}-\d{2}T[\d:]+)', block)
        desc_match = re.search(r'Description:\s*(.+?)(?:\nSnippets:|\nTitle:|\nURL:|\nPublished:|$)', block, re.DOTALL)
        snippets_match = re.search(r'Snippets:\s*\n((?:- .+?\n?)+)', block, re.DOTALL)

        title = title_match.group(1).strip() if title_match else ""
        url = url_match.group(1).strip() if url_match else ""
        description = desc_match.group(1).strip() if desc_match else ""
        snippets = snippets_match.group(1).strip() if snippets_match else ""

        reported_date = None
        if published_match:
            try:
                reported_date = datetime.fromisoformat(published_match.group(1))
            except (ValueError, TypeError):
                pass

        # Combine title + description + snippets into text_data
        parts = [p for p in [title, description, snippets] if p]
        text_data = "\n\n".join(parts)
        if text_data:
            articles.append((text_data, url, reported_date))
    return articles


def _generate_historical_queries():
    """Generate month-specific search queries from January 2025 to March 2026."""
    topics = [
        # Core oil & gas market topics
        "oil prices market news",
        "natural gas energy prices",
        "crude oil Brent WTI futures",
        "OPEC oil production supply",
        "LNG liquefied natural gas market",
        "oil and gas drilling rig count",
        "petroleum refinery gasoline diesel",
        "shale oil fracking production",
        "energy sector oil gas stocks",
        "oil and gas industry outlook",
        # Geopolitical and war topics affecting energy
        "Russia Ukraine war oil gas energy impact",
        "Middle East conflict oil supply disruption",
        "Iran sanctions oil exports energy",
        "Red Sea Houthi shipping oil tanker disruption",
        "China Taiwan geopolitical tension energy trade",
        "OPEC sanctions geopolitical oil production cuts",
        "Venezuela oil sanctions energy policy",
        "Libya oil production shutdown conflict",
        "Israel Hamas war energy market impact",
        "NATO energy security oil gas policy",
        # Macro and trade policy topics
        "US tariffs trade war energy commodities impact",
        "global recession risk oil demand outlook",
        "energy transition renewable vs fossil fuels policy",
        "carbon tax emissions oil gas regulation",
    ]

    months = []
    # January 2025 through March 2026
    for year in [2025, 2026]:
        end_month = 12 if year == 2025 else 3
        for month in range(1, end_month + 1):
            month_name = datetime(year, month, 1).strftime("%B")
            months.append((month_name, year))

    queries = []
    for month_name, year in months:
        for topic in topics:
            queries.append(f"{topic} {month_name} {year}")
    return queries


def _fetch_oil_gas_news():
    """Call You.com MCP proxy to search Yahoo Finance for oil & gas news."""
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()

    # Databricks MCP proxy endpoint
    mcp_path = "/api/2.0/mcp/external/web-search-you-com"
    extra_headers = {"Accept": "application/json, text/event-stream"}

    # Discover available tools via MCP tools/list
    tool_name = None
    try:
        list_payload = {"jsonrpc": "2.0", "method": "tools/list", "params": {}, "id": 0}
        list_resp = w.api_client.do("POST", mcp_path, body=list_payload, headers=extra_headers)
        list_data = list_resp if isinstance(list_resp, dict) else json.loads(list_resp)
        tools = list_data.get("result", {}).get("tools", [])
        if tools:
            tool_name = tools[0].get("name")
    except Exception:
        pass

    tool_names = [tool_name] if tool_name else ["you_search", "web_search", "you-search", "search"]

    # Generate historical monthly queries from Jan 2025 to Mar 2026
    queries = _generate_historical_queries()

    rows = []
    for q in queries:
        for tool in tool_names:
            payload = {
                "jsonrpc": "2.0",
                "method": "tools/call",
                "params": {
                    "name": tool,
                    "arguments": {"query": q, "num_web_results": 10},
                },
                "id": 1,
            }
            try:
                resp = w.api_client.do("POST", mcp_path, body=payload, headers=extra_headers)
                data = resp if isinstance(resp, dict) else json.loads(resp)

                if "error" in data:
                    continue

                result = data.get("result", data)
                content_list = result.get("content", [])
                if not content_list:
                    continue

                first_text = content_list[0].get("text", "")
                if "not found" in first_text.lower() or "error" in first_text.lower()[:20]:
                    continue

                for content_item in content_list:
                    text = content_item.get("text", "")
                    if not text:
                        continue
                    # Try parsing as structured JSON first
                    try:
                        parsed = json.loads(text)
                    except (json.JSONDecodeError, TypeError):
                        parsed = None

                    if isinstance(parsed, dict):
                        hits = parsed.get("hits", parsed.get("results", []))
                        for hit in hits if isinstance(hits, list) else []:
                            description = hit.get("description", hit.get("snippet", ""))
                            if not description:
                                snippets = hit.get("snippets", [])
                                description = " ".join(snippets) if snippets else ""
                            # Try to parse published date from JSON hits
                            pub_date = None
                            for date_key in ("published", "publishedDate", "date", "datePublished"):
                                if hit.get(date_key):
                                    try:
                                        pub_date = datetime.fromisoformat(hit[date_key].replace("Z", "+00:00"))
                                    except (ValueError, TypeError):
                                        pass
                                    break
                            rows.append((
                                str(uuid.uuid4()), datetime.utcnow(),
                                description, hit.get("url", ""), pub_date,
                            ))
                    elif isinstance(parsed, list):
                        for hit in parsed:
                            rows.append((
                                str(uuid.uuid4()), datetime.utcnow(),
                                hit.get("description", ""), hit.get("url", ""), None,
                            ))
                    else:
                        # Parse formatted text into individual articles
                        for text_data, url, reported_date in _parse_articles(text):
                            rows.append((
                                str(uuid.uuid4()), datetime.utcnow(),
                                text_data, url, reported_date,
                            ))
                break  # Tool worked
            except Exception:
                pass
    return rows


@dp.materialized_view(
    name="hackathon.market_agents.bronze_market_data",
    comment="Bronze layer: oil and gas market data from Yahoo Finance via You.com MCP"
)
def bronze_market_data():
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("creation_date", TimestampType(), True),
        StructField("text_data", StringType(), True),
        StructField("source_url", StringType(), False),
        StructField("reported_date", TimestampType(), True),
    ])

    fetched = _fetch_oil_gas_news()
    fetched_df = spark.createDataFrame(fetched, schema)

    source_df = spark.read.table("hackathon.market_agents.market_scrapper_agent_data")

    # Add reported_date column to source if missing, then union
    if "reported_date" not in source_df.columns:
        source_df = source_df.withColumn("reported_date", F.lit(None).cast(TimestampType()))

    return source_df.unionByName(fetched_df)
