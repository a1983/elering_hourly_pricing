"""Constant values for elering_hourly_pricing."""

import sys

if sys.version_info[:2] >= (3, 9):  # pragma: no cover
    import zoneinfo  # pylint: disable=import-error
else:  # pragma: no cover
    from backports import zoneinfo

UTC_TZ = zoneinfo.ZoneInfo("UTC")

DOMAIN = "elering_hourly_pricing"
PLATFORMS = ["sensor"]
DEFAULT_NAME = "Elering Pricing"

ELERING_UNIQUE_ID = "EleringHourlyPricingUniqueId"

DEFAULT_TIMEOUT = 5
PRICE_PRECISION = 5
URL_EE_RESOURCE = "https://dashboard.elering.ee/api/nps/price?start={:%Y-%m-%dT%H}%3A00%3A00.000Z&end={:%Y-%m-%dT%H}%3A59%3A59.999Z"
