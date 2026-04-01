"""
Pricing API client for the ScyllaDB VSS Sizing Calculator.

Fetches on-demand and yearly instance prices from the internal pricing
API and builds ``InstanceType`` objects that the sizing algorithm needs.
"""

from __future__ import annotations

import logging

import requests

import vss_sizing as vs

log = logging.getLogger(__name__)

_PRICING_API_URL = "https://api-v2.lab.dbaas.scyop.net/pricing"
_PRICE_MULTIPLIER = 2  # Vector Search price = 2 × base instance price.

# Map cloud provider → (API provider string, API region string).
_PROVIDER_PARAMS: dict[vs.CloudProvider, tuple[str, str]] = {
    vs.CloudProvider.AWS: ("AWS", "us-east-1"),
    vs.CloudProvider.GCP: ("GCP", "us-east1"),
}


def _fetch_prices(
    provider: str,
    region: str,
    contract_type: str,
    *,
    contract_term: str | None = None,
    subscription_tier: str | None = None,
    payment_option: str | None = None,
) -> dict[str, float]:
    """Fetch hourly prices from the pricing API.

    Returns a mapping of ``instance_type_name → hourly_price * 2``.
    """
    commercial: dict[str, str] = {"contractType": contract_type}
    if contract_term:
        commercial["contractTerm"] = contract_term
    if subscription_tier:
        commercial["subscriptionTier"] = subscription_tier
    if payment_option:
        commercial["paymentOption"] = payment_option

    payload = {
        "infrastructure": {
            "provider": provider,
            "region": region,
            "deployment": "Cloud",
        },
        "product": {"productTypes": ["VECTOR_SEARCH"]},
        "commercial": commercial,
        "pagination": {"page": 1, "limit": 50},
    }

    resp = requests.post(_PRICING_API_URL, json=payload, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    prices: dict[str, float] = {}
    for item in data.get("items", []):
        name = item.get("infrastructure", {}).get("instanceType", "")
        for est in item.get("pricing", {}).get("estimates", []):
            if est.get("intervalCode") == "PT1H" and name:
                amount = est.get("totalAmount")
                if amount is not None:
                    prices[name] = round(float(amount) * _PRICE_MULTIPLIER, 6)
    return prices


def _build_instances(
    cloud_provider: vs.CloudProvider,
    on_demand_prices: dict[str, float],
    yearly_prices: dict[str, float],
) -> list[vs.InstanceType]:
    """Build InstanceType objects by merging API prices with hardware specs.

    Instances with both on-demand and yearly prices use their respective
    values.  Instances with only on-demand pricing (e.g. GCP ``e2-medium``)
    are still included with ``on_demand_only=True`` and the on-demand price
    used for the yearly field.  Instances without any on-demand pricing data
    are skipped.
    """
    specs = vs.get_instance_specs(cloud_provider)
    result: list[vs.InstanceType] = []
    for spec in specs:
        od = on_demand_prices.get(spec.name)
        if od is None:
            continue
        yr = yearly_prices.get(spec.name)
        result.append(vs.InstanceType(
            name=spec.name,
            vcpus=spec.vcpus,
            ram_gb=spec.ram_gb,
            cost_per_hour=od,
            cost_per_hour_yearly=yr if yr is not None else od,
            on_demand_only=yr is None,
        ))
    return result


def fetch_all_prices() -> tuple[
    dict[vs.CloudProvider, list[vs.InstanceType]],
    list[str],
]:
    """Fetch prices for all providers and return instance lists.

    Returns ``(instances_dict, errors)`` where *errors* is a list of
    human-readable messages for providers that failed.
    """
    instances: dict[vs.CloudProvider, list[vs.InstanceType]] = {}
    errors: list[str] = []
    for cp, (prov, region) in _PROVIDER_PARAMS.items():
        try:
            od = _fetch_prices(prov, region, "ON_DEMAND")
            yr = _fetch_prices(
                prov, region, "SUBSCRIPTION",
                contract_term="1_YEAR",
                subscription_tier="STANDARD",
                payment_option="MONTHLY",
            )
            built = _build_instances(cp, od, yr)
            if not built:
                msg = f"No pricing data returned for {cp.value.upper()}."
                log.error(msg)
                errors.append(msg)
            else:
                instances[cp] = built
                log.info("Fetched %d instance prices for %s", len(built), cp.value)
        except Exception as exc:
            msg = f"Failed to fetch prices for {cp.value.upper()}: {exc}"
            log.error(msg, exc_info=True)
            errors.append(msg)
    return instances, errors
