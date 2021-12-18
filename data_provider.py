"""
Simple aio library to download Elering electricity hourly prices.

Externalization of download and parsing logic for the `elering_hourly_pricing`
HomeAssistant integration,
https://www.home-assistant.io/integrations/elering_hourly_pricing/
"""
import asyncio
from collections import deque
from datetime import date, datetime, time, timedelta
import logging
from random import random
from time import monotonic
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import aiohttp
import async_timeout

from .const import DEFAULT_TIMEOUT, URL_EE_RESOURCE, UTC_TZ, zoneinfo
from .prices import make_price_sensor_attributes

# Use randomized standard User-Agent info to avoid server banning
_STANDARD_USER_AGENTS = [
    (
        "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) "
        "Gecko/20100101 Firefox/47.3"
    ),
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X x.y; rv:42.0) "
        "Gecko/20100101 Firefox/43.4"
    ),
    (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 11_3_1 like Mac OS X) "
        "AppleWebKit/603.1.30 (KHTML, like Gecko)"
    ),
    "Version/10.0 Mobile/14E304 Safari/602.1",
]

_LOGGER = logging.getLogger(__name__)


def ensure_utc_time(ts: datetime) -> datetime:
    """Return tz-aware datetime in UTC from any datetime."""
    if ts.tzinfo is None:
        return datetime(*ts.timetuple()[:6], tzinfo=UTC_TZ)
    elif str(ts.tzinfo) != str(UTC_TZ):
        return ts.astimezone(UTC_TZ)
    return ts


def extract_data(
    data: Dict[str, Any],
    tz: zoneinfo.ZoneInfo = UTC_TZ,
) -> Dict[datetime, float]:
    """Parse the contents of a daily json file."""
    result: Dict[datetime, float] = {}
    for day_price in data["data"]["ee"]:
        timestamp = day_price["timestamp"]
        date_time = datetime.fromtimestamp(timestamp).astimezone(tz)
        price = round(day_price["price"] / 1000, 3)
        result[date_time] = price

    return result


class EEData:
    """
    Data handler for elering hourly prices.

    * Async download of prices for each day
    * Generate state attributes for HA integration.
    * Async download of prices for a range of days

    - Prices are returned in a `Dict[datetime, float]`,
    with timestamps in UTC and prices in €/kWh.
    """

    def __init__(
        self,
        websession: Optional[aiohttp.ClientSession] = None,
        local_timezone: Union[str, zoneinfo.ZoneInfo] = UTC_TZ,
        timeout: float = DEFAULT_TIMEOUT,
    ):
        """Elering EEData init."""
        self.source_available = True
        self.state: Optional[float] = None
        self.state_available = False
        self.attributes: Dict[str, Any] = {}

        self.timeout = timeout
        self._session = websession
        self._user_agents = deque(sorted(_STANDARD_USER_AGENTS, key=lambda x: random()))
        self._headers = {
            "User-Agent": self._user_agents[0],
            "Accept": "application/json",
        }

        self._with_initial_session = websession is not None
        self._local_timezone = zoneinfo.ZoneInfo(str(local_timezone))

        self._current_prices: Dict[datetime, float] = {}

    async def _api_get_prices(self, url: str) -> Dict[datetime, Any]:
        assert self._session is not None

        resp = await self._session.get(url, headers=self._headers)
        if resp.status < 400:
            data = await resp.json()
            return extract_data(data, tz=self._local_timezone)
        elif resp.status == 403:  # pragma: no cover
            _LOGGER.warning(
                "Forbidden error with '%s' -> Headers: %s", url, resp.headers
            )
            # loop user-agent
            self._user_agents.rotate()
            self._headers["User-Agent"] = self._user_agents[0]
            # and retry
            resp = await self._session.get(url, headers=self._headers)
            if resp.status < 400:
                data = await resp.json()
                return extract_data(data, tz=self._local_timezone)
            elif resp.status == 403:
                _LOGGER.error(
                    "Repeaed forbidden error with '%s' -> Headers: %s",
                    url,
                    resp.headers,
                )
                self._headers.pop("User-Agent")
        return {}

    async def _download_prices(self, day: date) -> Dict[datetime, Any]:
        """
        Elering data extractor.

        Prices are referenced with datetimes in UTC.
        """
        local_start = datetime.combine(day, time(0, 0, 0, tzinfo=self._local_timezone))
        utc_start = local_start.astimezone(UTC_TZ)
        utc_end = utc_start + timedelta(hours=23)
        url = URL_EE_RESOURCE.format(utc_start, utc_end)

        try:
            async with async_timeout.timeout(2 * self.timeout):
                return await self._api_get_prices(url)
        except KeyError:
            _LOGGER.debug("Bad try on getting prices for %s", day)
        except asyncio.TimeoutError:
            if self.source_available:
                _LOGGER.warning("Timeout error requesting data from '%s'", url)
        except aiohttp.ClientError:
            if self.source_available:
                _LOGGER.warning("Client error in '%s'", url)
        return {}

    async def async_update_prices(self, now: datetime) -> Dict[datetime, float]:
        """
        Update electricity prices from the ESIOS API.

        Input `now: datetime` is assumed tz-aware in UTC.
        If not, it is converted to UTC from the original timezone,
        or set as UTC-time if it is a naive datetime.
        """
        utc_now = ensure_utc_time(now)
        local_ref_now = utc_now.astimezone(self._local_timezone)
        current_num_prices = len(self._current_prices)
        if local_ref_now.hour >= 20 and current_num_prices > 30:
            # already have today+tomorrow prices, avoid requests
            _LOGGER.debug(
                "Evening download avoided, now with %d prices from %s UTC",
                current_num_prices,
                list(self._current_prices)[0].strftime("%Y-%m-%d %Hh"),
            )
            return self._current_prices
        elif (
            local_ref_now.hour < 20
            and current_num_prices > 20
            and (
                list(self._current_prices)[-12].astimezone(self._local_timezone).date()
                == local_ref_now.date()
            )
        ):
            # already have today prices, avoid request
            _LOGGER.debug(
                "Download avoided, now with %d prices up to %s UTC",
                current_num_prices,
                list(self._current_prices)[-1].strftime("%Y-%m-%d %Hh"),
            )
            return self._current_prices

        if current_num_prices and (
            list(self._current_prices)[0].astimezone(self._local_timezone).date()
            == local_ref_now.date()
        ):
            # avoid download of today prices
            prices = self._current_prices.copy()
            _LOGGER.debug(
                "Avoided: %s, with %d prices -> last: %s, download-day: %s",
                local_ref_now,
                current_num_prices,
                list(self._current_prices)[0].astimezone(self._local_timezone).date(),
                local_ref_now.date(),
            )
        else:
            txt_last = "--"
            if current_num_prices:
                txt_last = str(
                    list(self._current_prices)[-1].astimezone(self._local_timezone)
                )
            # make API call to download today prices
            _LOGGER.debug(
                "UN-Avoided: %s, with %d prices ->; last:%s, download-day: %s",
                local_ref_now,
                current_num_prices,
                txt_last,
                local_ref_now.date(),
            )
            prices = await self._download_prices(local_ref_now.date())
            if not prices:
                return prices

        # At evening, it is possible to retrieve next day prices
        if local_ref_now.hour >= 18:
            next_day = (local_ref_now + timedelta(days=1)).date()
            prices_fut = await self._download_prices(next_day)
            if prices_fut:
                prices.update(prices_fut)

        self._current_prices.update(prices)
        _LOGGER.debug(
            "Download done, now with %d prices from %s UTC",
            len(self._current_prices),
            list(self._current_prices)[0].strftime("%Y-%m-%d %Hh"),
        )

        return prices

    def process_state_and_attributes(self, utc_now: datetime) -> bool:
        """
        Generate the current state and sensor attributes.

        The data source provides prices in 0 to 24h sets, with correspondence
        with the main timezone in Spain. They are stored with UTC datetimes.

        Input `now: datetime` is assumed tz-aware in UTC.
        If not, it is converted to UTC from the original timezone,
        or set as UTC-time if it is a naive datetime.
        """
        attributes: Dict[str, Any] = {}

        utc_time = ensure_utc_time(utc_now.replace(minute=0, second=0, microsecond=0))
        local_time = utc_time.astimezone(self._local_timezone)

        if len(self._current_prices) > 25 and local_time.hour < 20:
            # there are 'today' and 'next day' prices, but 'today' has expired
            max_age = local_time.replace(hour=0).astimezone(UTC_TZ)
            self._current_prices = {
                key_ts: price
                for key_ts, price in self._current_prices.items()
                if key_ts >= max_age
            }

        # set current price
        try:
            self.state = self._current_prices[local_time]
            self.state_available = True
        except KeyError:
            self.state_available = False
            return False

        price_attrs = make_price_sensor_attributes(
            self._current_prices, utc_time, self._local_timezone
        )

        self.attributes = {**attributes, **price_attrs}

        return True

    async def _download_worker(self, wk_name: str, queue: asyncio.Queue):
        downloaded_prices = []
        try:
            while True:
                day: date = await queue.get()
                tic = monotonic()
                prices = await self._download_prices(day)
                took = monotonic() - tic
                queue.task_done()
                if not prices:
                    _LOGGER.warning(
                        "[%s]: Bad download for day: %s in %.3f s", wk_name, day, took
                    )
                    continue

                downloaded_prices.append((day, prices))
                _LOGGER.debug(
                    "[%s]: Task done for day: %s in %.3f s", wk_name, day, took
                )
        except asyncio.CancelledError:
            return downloaded_prices

    async def _multi_download(
        self, days_to_download: List[date], max_calls: int
    ) -> Iterable[Tuple[date, Dict[datetime, Any]]]:
        """Multiple requests using an asyncio.Queue for concurrency."""
        queue: asyncio.Queue = asyncio.Queue()
        # setup `max_calls` queue workers
        worker_tasks = [
            asyncio.create_task(self._download_worker(f"worker-{i+1}", queue))
            for i in range(max_calls)
        ]
        # fill queue
        for day in days_to_download:
            queue.put_nowait(day)

        # wait for the queue to process all
        await queue.join()

        for task in worker_tasks:
            task.cancel()
        # Wait until all worker tasks are cancelled.
        wk_tasks_results = await asyncio.gather(*worker_tasks, return_exceptions=True)

        return sorted(
            (day_data for wk_results in wk_tasks_results for day_data in wk_results),
            key=lambda x: x[0],
        )

    async def _ensure_session(self):
        if self._session is None:
            assert not self._with_initial_session
            self._session = aiohttp.ClientSession()

    async def _close_temporal_session(self):
        if not self._with_initial_session and self._session is not None:
            await self._session.close()
            self._session = None

    async def async_download_prices_for_range(
        self, start: datetime, end: datetime, concurrency_calls: int = 20
    ) -> Dict[datetime, Any]:
        """Download a time range burst of electricity prices from the ESIOS API."""

        def _adjust_dates(ts: datetime) -> Tuple[datetime, datetime]:
            # adjust dates and tz from inputs to retrieve prices as it was in
            #  Spain mainland, so tz-independent!!
            ts_ref = datetime(*ts.timetuple()[:6], tzinfo=self._local_timezone)
            ts_utc = ts_ref.astimezone(UTC_TZ)
            return ts_utc, ts_ref

        start_utc, start_local = _adjust_dates(start)
        end_utc, end_local = _adjust_dates(end)
        delta: timedelta = end_local.date() - start_local.date()
        days_to_download = [
            start_local.date() + timedelta(days=i) for i in range(delta.days + 1)
        ]

        tic = monotonic()
        max_calls = concurrency_calls
        await self._ensure_session()
        data_days = await self._multi_download(days_to_download, max_calls)
        await self._close_temporal_session()

        prices = {
            hour: hourly_data[hour]
            for (day, hourly_data) in data_days
            for hour in hourly_data
            if start_utc <= hour <= end_utc
        }
        if prices:
            _LOGGER.warning(
                "Download of %d prices from %s to %s in %.2f sec",
                len(prices),
                min(prices),
                max(prices),
                monotonic() - tic,
            )
        else:
            _LOGGER.error(
                "BAD Download of Elering prices from %s to %s in %.2f sec",
                start,
                end,
                monotonic() - tic,
            )

        return prices

    def download_prices_for_range(
        self, start: datetime, end: datetime, concurrency_calls: int = 20
    ) -> Dict[datetime, Any]:
        """Blocking method to download a time range burst of elec prices."""
        return asyncio.run(
            self.async_download_prices_for_range(start, end, concurrency_calls)
        )
