import hashlib
from datetime import datetime, timezone
import dlt
import requests

SAAS_ACCOUNTS = [
    {
        "account_id": "ACC-101",
        "company_name": "Northstar Logistics",
        "plan_tier": "Enterprise",
        "mrr_usd": 4800.0,
        "contract_renewal_date": "2027-03-31",
        "updated_at": "2026-11-18T10:00:00Z",
    },
    {
        "account_id": "ACC-102",
        "company_name": "Vanguard Cloud Ops",
        "plan_tier": "Enterprise",
        "mrr_usd": 6200.0,
        "contract_renewal_date": "2027-01-15",
        "updated_at": "2026-11-19T08:30:00Z",
    },
    {
        "account_id": "ACC-103",
        "company_name": "Harborview Retail",
        "plan_tier": "Growth",
        "mrr_usd": 1450.0,
        "contract_renewal_date": "2027-02-28",
        "updated_at": "2026-11-19T14:30:00Z",
    },
    {
        "account_id": "ACC-104",
        "company_name": "Meridian Health Tech",
        "plan_tier": "Growth",
        "mrr_usd": 1850.0,
        "contract_renewal_date": "2027-04-30",
        "updated_at": "2026-11-20T11:45:00Z",
    },
    {
        "account_id": "ACC-105",
        "company_name": "Brightleaf Studio",
        "plan_tier": "Starter",
        "mrr_usd": 290.0,
        "contract_renewal_date": "2027-01-31",
        "updated_at": "2026-11-20T09:15:00Z",
    },
    {
        "account_id": "ACC-106",
        "company_name": "Kestrel Analytics",
        "plan_tier": "Starter",
        "mrr_usd": 350.0,
        "contract_renewal_date": "2027-05-15",
        "updated_at": "2026-11-21T16:00:00Z",
    },
]


def _assign_account_id(ticket_id: str) -> str:
    """Deterministically map any ticket ID to a SaaS customer account."""
    digest = int(hashlib.md5(str(ticket_id).encode("utf-8")).hexdigest(), 16)
    return SAAS_ACCOUNTS[digest % len(SAAS_ACCOUNTS)]["account_id"]


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


# 2. CRM / Customer Accounts & Subscription Tiers (Incremental Merge Mode)
@dlt.resource(name="raw_saas_accounts", write_disposition="merge", primary_key="account_id")
def fetch_saas_accounts(
    updated_at=dlt.sources.incremental("updated_at", initial_value="2026-11-01T00:00:00Z")
):
    for account in SAAS_ACCOUNTS:
        yield dict(account)


# 3. CRM / Operations Support & Community Tickets (Hacker News Algolia API - Incremental Merge Mode)
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
        ticket_id = str(hit["objectID"])
        comments = hit.get("num_comments") or 0
        yield {
            "id": ticket_id,
            "account_id": _assign_account_id(ticket_id),
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
    load_info = pipeline.run(
        [fetch_market_rates(), fetch_saas_accounts(), fetch_support_tickets()]
    )
    print(load_info)
    return load_info


if __name__ == "__main__":
    run_ingestion(destination="snowflake")