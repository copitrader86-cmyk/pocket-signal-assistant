import io
import math
import re
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from pathlib import PurePosixPath

import httpx

from app.dukascopy import INSTRUMENTS
from app.market_data import Candle, MarketDataError


PROVIDER_NAME = "HistData"
BASE_URL = "https://www.histdata.com"
MAX_ARCHIVE_BYTES = 50 * 1024 * 1024
MAX_CSV_BYTES = 250 * 1024 * 1024
SOURCE_RESOLUTION_MS = 60_000
EST_WITHOUT_DST = timezone(timedelta(hours=-5))


@dataclass(frozen=True)
class HistDataBatch:
    symbol: str
    year: int
    rows_read: int
    invalid_rows: int
    duplicate_rows: int
    missing_intervals: int
    candles: tuple[Candle, ...]


class _FormParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.form_id: str | None = None
        self.download_form_found = False
        self.values: dict[str, str] = {}
        self.links: list[tuple[str, str]] = []
        self._anchor_href: str | None = None
        self._anchor_text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = dict(attrs)
        if tag == "form" and values.get("id") == "file_down":
            self.form_id = "file_down"
            self.download_form_found = True
        elif tag == "input" and self.form_id == "file_down":
            name, value = values.get("name"), values.get("value")
            if name and value is not None:
                self.values[name] = value
        elif tag == "a":
            self._anchor_href = values.get("href")
            self._anchor_text = []

    def handle_data(self, data: str) -> None:
        if self._anchor_href is not None:
            self._anchor_text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "form":
            self.form_id = None
        elif tag == "a" and self._anchor_href is not None:
            self.links.append((self._anchor_href, "".join(self._anchor_text)))
            self._anchor_href = None
            self._anchor_text = []


def parse_m1_csv(text: str, *, symbol: str, year: int) -> HistDataBatch:
    if symbol not in INSTRUMENTS:
        raise ValueError("Unsupported HistData instrument")
    if not 2000 <= year <= 2100:
        raise ValueError("HistData archive year is out of range")

    candles_by_timestamp: dict[int, Candle] = {}
    rows_read = 0
    invalid_rows = 0
    duplicate_rows = 0
    for line in io.StringIO(text.lstrip("\ufeff")):
        if not line.strip():
            continue
        rows_read += 1
        try:
            fields = line.rstrip("\r\n").split(";")
            if len(fields) != 6:
                raise ValueError("invalid column count")
            stamp = datetime.strptime(fields[0], "%Y%m%d %H%M%S")
            if stamp.year != year or stamp.second != 0:
                raise ValueError("timestamp does not match the archive resolution")
            prices = tuple(float(value) for value in fields[1:5])
            volume = float(fields[5])
            open_price, high, low, close = prices
            if not all(math.isfinite(value) and value > 0 for value in prices):
                raise ValueError("invalid price")
            if high < max(open_price, close) or low > min(open_price, close) or high < low:
                raise ValueError("invalid OHLC values")
            if not math.isfinite(volume) or volume < 0:
                raise ValueError("invalid volume")
            timestamp_ms = int(
                stamp.replace(tzinfo=EST_WITHOUT_DST).astimezone(timezone.utc).timestamp()
                * 1000
            )
        except (OverflowError, ValueError):
            invalid_rows += 1
            continue

        if timestamp_ms in candles_by_timestamp:
            duplicate_rows += 1
            continue
        candles_by_timestamp[timestamp_ms] = Candle(
            timestamp_ms=timestamp_ms,
            open=open_price,
            high=high,
            low=low,
            close=close,
            volume=volume,
        )

    candles = tuple(
        sorted(candles_by_timestamp.values(), key=lambda candle: candle.timestamp_ms)
    )
    interval_ms = SOURCE_RESOLUTION_MS
    missing_intervals = sum(
        max(0, (right.timestamp_ms - left.timestamp_ms) // interval_ms - 1)
        for left, right in zip(candles, candles[1:])
    )
    if rows_read and not candles:
        raise MarketDataError("HistData M1 archive contained no valid candles")
    return HistDataBatch(
        symbol=symbol,
        year=year,
        rows_read=rows_read,
        invalid_rows=invalid_rows,
        duplicate_rows=duplicate_rows,
        missing_intervals=missing_intervals,
        candles=candles,
    )


class HistDataClient:
    def __init__(self, *, client: httpx.AsyncClient | None = None) -> None:
        self._client = client
        self._owns_client = client is None

    async def __aenter__(self) -> "HistDataClient":
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(90, connect=10),
                follow_redirects=True,
                headers={"User-Agent": "PocketSignalAssistant/1.0 (historical research)"},
            )
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    def _client_or_raise(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("HistDataClient must be used as an async context manager")
        return self._client

    @staticmethod
    def _pair_code(symbol: str) -> str:
        try:
            return INSTRUMENTS[symbol].lower()
        except KeyError as error:
            raise ValueError("Unsupported HistData instrument") from error

    async def available_years(self, symbol: str) -> tuple[int, ...]:
        pair = self._pair_code(symbol).upper()
        url = f"{BASE_URL}/download-free-forex-data/?/ascii/1-minute-bar-quotes/{pair}"
        try:
            response = await self._client_or_raise().get(url)
            response.raise_for_status()
        except httpx.HTTPError as error:
            raise MarketDataError("HistData archive listing request failed") from error

        parser = _FormParser()
        parser.feed(response.text)
        years = {
            int(match.group(1))
            for href, label in parser.links
            if (match := re.search(rf"/{pair.lower()}/(\d{{4}})$", href))
            and re.search(rf"\b{match.group(1)}\s*-\s*\.Zip with Full", label, re.I)
        }
        if not years:
            raise MarketDataError("HistData listed no complete annual M1 archives")
        return tuple(sorted(years))

    async def fetch_year(self, symbol: str, year: int) -> HistDataBatch:
        pair = self._pair_code(symbol).upper()
        if not 2000 <= year <= 2100:
            raise ValueError("HistData archive year is out of range")
        client = self._client_or_raise()
        page_url = (
            f"{BASE_URL}/download-free-forex-data/"
            f"?/ascii/1-minute-bar-quotes/{pair}/{year}"
        )
        try:
            page = await client.get(page_url)
            page.raise_for_status()
        except httpx.HTTPError as error:
            raise MarketDataError("HistData annual archive page request failed") from error

        parser = _FormParser()
        parser.feed(page.text)
        required_fields = {"tk", "date", "datemonth", "platform", "timeframe", "fxpair"}
        if not parser.download_form_found or not required_fields.issubset(parser.values):
            raise MarketDataError("HistData page did not provide an annual download form")
        if (
            parser.values["date"] != str(year)
            or parser.values["datemonth"] != str(year)
            or parser.values["platform"] != "ASCII"
            or parser.values["timeframe"] != "M1"
            or parser.values["fxpair"].upper() != pair
            or not parser.values["tk"]
        ):
            raise MarketDataError("HistData annual download form is incomplete or mismatched")

        try:
            response = await client.post(
                f"{BASE_URL}/get.php",
                data=parser.values,
                headers={"Referer": page_url},
            )
            response.raise_for_status()
        except httpx.HTTPError as error:
            raise MarketDataError("HistData archive download request failed") from error
        if not response.content or len(response.content) > MAX_ARCHIVE_BYTES:
            raise MarketDataError("HistData archive is empty or exceeds the size limit")
        if not response.content.startswith(b"PK\x03\x04"):
            raise MarketDataError("HistData download was not a ZIP archive")

        expected_name = f"DAT_ASCII_{pair}_M1_{year}.csv"
        try:
            with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
                members = [
                    item
                    for item in archive.infolist()
                    if PurePosixPath(item.filename).name == expected_name
                    and not item.is_dir()
                ]
                if len(members) != 1 or members[0].file_size > MAX_CSV_BYTES:
                    raise MarketDataError("HistData ZIP has no unique, bounded M1 data file")
                if members[0].filename != PurePosixPath(members[0].filename).name:
                    raise MarketDataError("HistData ZIP contains an unsafe data-file path")
                with archive.open(members[0]) as data_file:
                    csv_text = data_file.read(MAX_CSV_BYTES + 1).decode("ascii")
        except (OSError, UnicodeDecodeError, zipfile.BadZipFile, RuntimeError) as error:
            raise MarketDataError("HistData ZIP is invalid or its CSV is unreadable") from error
        if len(csv_text.encode("ascii")) > MAX_CSV_BYTES:
            raise MarketDataError("HistData CSV exceeds the decompressed size limit")
        return parse_m1_csv(csv_text, symbol=symbol, year=year)
