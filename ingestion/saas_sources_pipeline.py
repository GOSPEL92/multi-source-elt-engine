import dlt
import requests
from datetime import datetime, timezone

# 1. Billing / Financial Feed (CoinGecko Public API - Append Mode)
@dlt.resource(name="raw_market_rates", write_disposition="append")
def fetch_market_rates():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "ids": "bitcoin,ethereum,solana,cardano,ripple",
        "order": "market_cap_desc",
        "sparkline": "false",
    }
    headers = {"User-Agent": "multi-source-saas-elt/1.0"}
    response = requests.get(url, params=params, headers=headers, timeout=30)
    response.raise_for_status()
    
    extracted_at = datetime.now(timezone.utc).isoformat()
    for record in response.json():
        record["_extracted_at"] = extracted_at
        yield record


# 2. CRM / Operations Support & Community Tickets (Hacker News Algolia API - Incremental Merge Mode)
@dlt.resource(name="raw_support_tickets", write_disposition="merge", primary_key="id")
def fetch_support_tickets(
    updated_at=dlt.sources.incremental("updated_at", initial_value="2026-11-01T00:00:00Z")
):
    url = "https://hn.algolia.com/api/v1/search_by_date"
    params = {
        "query": "saas",
        "tags": "story",
        "hitsPerPage": 50,
    }
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    for hit in response.json().get("hits", []):
        comments = hit.get("num_comments") or 0
        yield {
            "id": str(hit["objectID"]),
            "ticket_number": int(hit["objectID"]),
            "title": hit.get("title") or "Untitled Ticket",
            "state": "closed" if comments >= 5 else "open",
            "created_at": hit["created_at"],
            "updated_at": hit.get("updated_at") or hit["created_at"],
            "closed_at": hit.get("updated_at") if comments >= 5 else None,
            "comments_count": comments,
            "author_login": hit.get("author"),
        }


def run_ingestion(destination: str = "snowflake"):
    pipeline = dlt.pipeline(
        pipeline_name="multi_source_saas_elt",
        destination=destination,
        dataset_name="raw_saas_data",
    )
    load_info = pipeline.run([fetch_market_rates(), fetch_support_tickets()])
    print(load_info)
    return load_info


if __name__ == "__main__":
    run_ingestion(destination="snowflake")