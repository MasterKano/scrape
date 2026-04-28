from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
import threading
import time
from collections import Counter, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import requests

API_BASE_URL = "https://api.eex-group.com/pub/market-data/table-data"
ICE_HISTORICAL_URL = "https://www.ice.com/marketdata/api/productguide/charting/data/historical"
ACCEPT_HEADER = "application/json, text/javascript, */*; q=0.01"
RETRYABLE_HTTP_STATUSES = {429, 500, 502, 503, 504}
FULL_HISTORY_START_DATE = "2000-01-01"

MAIN_CSV_PREFIX = "CSV"
FAILED_CSV_PREFIX = "FCSV"
SUMMARY_JSON_PREFIX = "JSON"
LOG_PREFIX = "LOG"

REQUEST_HEADERS = {
    "accept": ACCEPT_HEADER,
    "accept-language": "en-GB,en;q=0.9",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/134.0.0.0 Safari/537.36"
    ),
    "referer": "https://www.eex.com/",
    "origin": "https://www.eex.com",
}

ICE_REQUEST_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "en-GB,en;q=0.9",
    "user-agent": REQUEST_HEADERS["user-agent"],
    "referer": "https://www.ice.com/",
    "origin": "https://www.ice.com",
}

POWER_AREAS = {
    "FI": {"codes": {"Month": "FNBM", "Quarter": "FNBQ", "Year": "FNBY"}, "assumed_types": []},
    "SE1": {"codes": {"Month": "1SBM", "Quarter": "1SBQ", "Year": "1SBY"}, "assumed_types": []},
    "SE2": {"codes": {"Month": "2SBM", "Quarter": "2SBQ", "Year": "2SBY"}, "assumed_types": []},
    "SE3": {"codes": {"Month": "3SBM", "Quarter": "3SBQ", "Year": "3SBY"}, "assumed_types": []},
    "SE4": {"codes": {"Month": "4SBM", "Quarter": "4SBQ", "Year": "4SBY"}, "assumed_types": []},
    "DE": {"codes": {"Month": "DEBM", "Quarter": "DEBQ", "Year": "DEBY"}, "assumed_types": []},
    "GB": {"codes": {"Month": "FUBM", "Quarter": "FUBQ", "Year": "FUBY"}, "assumed_types": []},
    "ES": {"codes": {"Month": "FEBM", "Quarter": "FEBQ", "Year": "FEBY"}, "assumed_types": []},
    "IT": {"codes": {"Month": "FDBM", "Quarter": "FDBQ", "Year": "FDBY"}, "assumed_types": []},
    "NL": {"codes": {"Month": "Q0BM", "Quarter": "Q0BQ", "Year": "Q0BY"}, "assumed_types": []},
    "Nordic": {"codes": {"Month": "FBBM", "Quarter": "FBBQ", "Year": "FBBY"}, "assumed_types": []},
    "FR": {"codes": {"Month": "F7BM", "Quarter": "F7BQ", "Year": "F7BY"}, "assumed_types": []},
    "NO1": {"codes": {"Month": "1NBM", "Quarter": "1NBQ", "Year": "1NBY"}, "assumed_types": []},
    "NO2": {"codes": {"Month": "2NBM", "Quarter": "2NBQ", "Year": "2NBY"}, "assumed_types": ["Month", "Quarter", "Year"]},
    "NO3": {"codes": {"Month": "3NBM", "Quarter": "3NBQ", "Year": "3NBY"}, "assumed_types": ["Month", "Quarter", "Year"]},
    "NO4": {"codes": {"Month": "4NBM", "Quarter": "4NBQ", "Year": "4NBY"}, "assumed_types": ["Month", "Quarter", "Year"]},
    "NO5": {"codes": {"Month": "5NBM", "Quarter": "5NBQ", "Year": "5NBY"}, "assumed_types": ["Month", "Quarter"]},
}

GAS_FUTURES_TTF = {
    "TTF": {
        "commodity": "NATGAS",
        "pricing": "F",
        "product": "Physical",
        "codes": {
            "Month": "G3BM",
            "Quarter": "G3BQ",
            "Season": "G3BS",
            "Year": "G3BY",
        },
        "assumed_types": [],
    }
}

GAS_SPOT_CONFIG = {
    "TTF_DA": {
        "area": "TTF",
        "commodity": "NATGAS",
        "pricing": "S",
        "product": "DA",
        "shortCode": "TTFDA",
        "maturityType": None,
        "maturity": None,
        "delivery": "TTF Day-Ahead",
        "assumedCode": False,
    }
}

GOO_FUTURES_CONFIG = {
    "EU_WIND": {
        "area": "EU",
        "commodity": "GO",
        "pricing": "F",
        "product": "Wind",
        "shortCode": "EGOW",
        "maturityType": "Year",
        "assumedCode": False,
    }
}

EUA_FUTURES_CONFIG = {
    "EUA": {
        "area": "EU",
        "commodity": "ENVIRONMENTALS",
        "pricing": "F",
        "product": "EUA",
        "shortCode": "FEUA",
        "maturityType": "Month",
        "assumedCode": False,
    }
}

EUA_DISCOVERY_MONTHS = 84

ICE_COAL_CONTRACTS = [
    {
        "marketGroup": "ICE_COAL_FUTURES",
        "area": "ICE",
        "commodity": "COAL",
        "pricing": "F",
        "product": "API2 Rotterdam",
        "shortCode": "ICE_API2",
        "maturityType": "Month",
        "maturity": "202612",
        "delivery": "Coal Dec-26",
        "assumedCode": False,
        "iceMarketId": 6265637,
    },
    {
        "marketGroup": "ICE_COAL_FUTURES",
        "area": "ICE",
        "commodity": "COAL",
        "pricing": "F",
        "product": "API2 Rotterdam",
        "shortCode": "ICE_API2",
        "maturityType": "Month",
        "maturity": "202712",
        "delivery": "Coal Dec-27",
        "assumedCode": False,
        "iceMarketId": 6890770,
    },
]

MONTH_NAMES = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
SORT_ORDER = {"Month": 1, "Quarter": 2, "Season": 3, "Year": 4, None: 5}

FIELDNAMES = [
    "mode",
    "contractAnchorDate",
    "marketGroup",
    "area",
    "commodity",
    "pricing",
    "product",
    "maturityType",
    "delivery",
    "requestedTradeDate",
    "windowStartDate",
    "windowEndDate",
    "tradeDate",
    "status",
    "settlementPrice",
    "volume",
    "grossOpenInterest",
    "shortCode",
    "maturity",
    "assumedCode",
    "httpStatus",
    "errorMessage",
]

MASTER_KEY_FIELDS = [
    "marketGroup",
    "area",
    "commodity",
    "pricing",
    "product",
    "shortCode",
    "maturityType",
    "maturity",
    "tradeDate",
]

_thread_local = threading.local()


@dataclass
class InferredRunConfig:
    mode: str | None = None
    contract_anchor_date: str | None = None
    requested_trade_date: str | None = None
    window_start_date: str | None = None
    window_end_date: str | None = None


@dataclass
class ResolvedRunConfig:
    mode: str
    contract_anchor_date: str
    requested_trade_date: str | None
    window_start_date: str
    window_end_date: str
    allow_fallback: bool


class AdaptiveRateController:
    def __init__(
        self,
        base_interval_seconds: float,
        max_interval_seconds: float,
        cooldown_threshold: int,
        cooldown_window_seconds: float,
        cooldown_seconds: float,
        relax_after_seconds: float,
        relax_factor: float,
    ) -> None:
        self.base_interval_seconds = max(0.0, base_interval_seconds)
        self.max_interval_seconds = max(self.base_interval_seconds, max_interval_seconds)
        self.current_interval_seconds = self.base_interval_seconds

        self.cooldown_threshold = max(1, cooldown_threshold)
        self.cooldown_window_seconds = max(0.1, cooldown_window_seconds)
        self.cooldown_seconds = max(0.0, cooldown_seconds)
        self.relax_after_seconds = max(1.0, relax_after_seconds)
        self.relax_factor = min(max(relax_factor, 0.1), 0.99)

        self._lock = threading.Lock()
        self._next_allowed_time = 0.0
        self._cooldown_until = 0.0
        self._recent_429s: deque[float] = deque()
        self._last_pressure_time: float | None = None

    def _maybe_relax_locked(self, now: float) -> None:
        if self.current_interval_seconds <= self.base_interval_seconds:
            return
        if self._last_pressure_time is None:
            return
        if now - self._last_pressure_time < self.relax_after_seconds:
            return

        new_interval = max(
            self.base_interval_seconds,
            self.current_interval_seconds * self.relax_factor,
        )
        if abs(new_interval - self.current_interval_seconds) > 1e-9:
            self.current_interval_seconds = new_interval
            self._last_pressure_time = now

    def wait_for_slot(self) -> float:
        while True:
            with self._lock:
                now = time.monotonic()
                self._maybe_relax_locked(now)

                available_time = max(self._next_allowed_time, self._cooldown_until)
                if now >= available_time:
                    self._next_allowed_time = now + self.current_interval_seconds
                    return self.current_interval_seconds

                sleep_for = available_time - now

            if sleep_for > 0:
                time.sleep(min(sleep_for, 1.0))

    def record_429(self) -> tuple[float, float, float]:
        with self._lock:
            now = time.monotonic()
            old_interval = self.current_interval_seconds
            self.current_interval_seconds = min(
                self.max_interval_seconds,
                max(self.current_interval_seconds * 1.5, self.base_interval_seconds + 0.25),
            )
            self._last_pressure_time = now

            self._recent_429s.append(now)
            while self._recent_429s and now - self._recent_429s[0] > self.cooldown_window_seconds:
                self._recent_429s.popleft()

            previous_cooldown_until = self._cooldown_until
            if len(self._recent_429s) >= self.cooldown_threshold:
                self._cooldown_until = max(self._cooldown_until, now + self.cooldown_seconds)
                self._recent_429s.clear()

            added_cooldown = max(0.0, self._cooldown_until - max(now, previous_cooldown_until))
            return old_interval, self.current_interval_seconds, added_cooldown

    def current_interval(self) -> float:
        with self._lock:
            return self.current_interval_seconds


def get_thread_session(kind: str = "eex") -> requests.Session:
    attr = f"session_{kind}"
    session = getattr(_thread_local, attr, None)
    if session is None:
        session = requests.Session()
        if kind == "ice":
            session.headers.update(ICE_REQUEST_HEADERS)
        else:
            session.headers.update(REQUEST_HEADERS)
        setattr(_thread_local, attr, session)
    return session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Unified market scraper: EEX + ICE coal, with selected date, explicit range, full history, or auto-update from master."
    )
    parser.add_argument(
        "--mode",
        choices=["selected", "range", "full-history", "auto-update"],
        default=None,
        help="selected = one requested date per contract; range = all dates in a given window; full-history = all dates from a configured lower bound to an end date; auto-update = derive start date from master file overlap.",
    )
    parser.add_argument("--trade-date", help="Requested trade date in YYYY-MM-DD for selected mode.")
    parser.add_argument("--start-date", help="Start date in YYYY-MM-DD for range mode.")
    parser.add_argument("--end-date", help="End date in YYYY-MM-DD for range, full-history, or auto-update mode.")
    parser.add_argument(
        "--contract-anchor-date",
        help="Date in YYYY-MM-DD used to build the contract set. Defaults to the selected/end date.",
    )
    parser.add_argument(
        "--full-history-start-date",
        default=FULL_HISTORY_START_DATE,
        help=f"Lower bound for full-history mode. Default: {FULL_HISTORY_START_DATE}",
    )
    parser.add_argument("--areas", help="Comma-separated areas, e.g. DE,GB,FR,EU,ICE. Default is all defined areas.")
    parser.add_argument("--max-contracts", type=int, default=None, help="Optional cap for testing.")
    parser.add_argument("--output-dir", default="Output", help="Folder for output files. Default: Output")
    parser.add_argument("--master-file", help="Optional path to a master CSV file to merge the current run into.")
    parser.add_argument(
        "--auto-update-overlap-days",
        type=int,
        default=10,
        help="Used only in auto-update mode. Re-fetch this many calendar days before the latest tradeDate in the master. Default: 10",
    )
    parser.add_argument(
        "--rerun-failed-from",
        help="Path to a previous main CSV or failed CSV. Re-runs only non-ok contracts.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=7,
        help="Used only in selected mode. Query window is trade-date minus lookback-days to trade-date. Default: 7",
    )
    parser.add_argument(
        "--request-gap-seconds",
        type=float,
        default=1.0,
        help="Base minimum gap between request starts across all workers. Default: 1.0",
    )
    parser.add_argument(
        "--max-request-gap-seconds",
        type=float,
        default=3.0,
        help="Upper bound for adaptive pacing after 429s. Default: 3.0",
    )
    parser.add_argument("--max-retries", type=int, default=2, help="Retry count for request failures. Default: 2")
    parser.add_argument(
        "--retry-delay-seconds",
        type=float,
        default=5.0,
        help="Base retry delay. Default: 5.0",
    )
    parser.add_argument(
        "--request-timeout-seconds",
        type=float,
        default=20.0,
        help="Per-request timeout. Default: 20",
    )
    parser.add_argument(
        "--no-fallback",
        action="store_true",
        help="Selected mode only. Disable fallback to the latest available row before the requested trade date.",
    )
    parser.add_argument("--max-workers", type=int, default=1, help="Number of parallel workers. Default: 1")
    parser.add_argument(
        "--cooldown-threshold",
        type=int,
        default=3,
        help="Trigger a global cooldown after this many 429s within the cooldown window. Default: 3",
    )
    parser.add_argument(
        "--cooldown-window-seconds",
        type=float,
        default=15.0,
        help="Window for counting clustered 429s. Default: 15.0",
    )
    parser.add_argument(
        "--cooldown-seconds",
        type=float,
        default=30.0,
        help="Global cooldown length after clustered 429s. Default: 30.0",
    )
    parser.add_argument(
        "--relax-after-seconds",
        type=float,
        default=60.0,
        help="Quiet period before the adaptive gap starts relaxing downward. Default: 60.0",
    )
    parser.add_argument(
        "--relax-factor",
        type=float,
        default=0.9,
        help="Adaptive gap relaxation factor after a quiet period. Default: 0.9",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=25,
        help="Progress log interval for queued/completed contracts. Default: 25",
    )
    return parser.parse_args()


def parse_trade_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError("Bad date format. Use YYYY-MM-DD.") from exc


def parse_ice_bar_date(value: str) -> date:
    return datetime.strptime(value, "%a %b %d %H:%M:%S %Y").date()


def fmt_date(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def fmt_maturity(year: int, month: int) -> str:
    return f"{year}{month:02d}"


def add_months(year: int, month: int, offset: int) -> tuple[int, int]:
    total = year * 12 + (month - 1) + offset
    new_year = total // 12
    new_month = total % 12 + 1
    return new_year, new_month


def first_defined(obj: dict[str, Any], keys: list[str]) -> Any:
    for key in keys:
        if key in obj and obj[key] not in (None, ""):
            return obj[key]
    return None


def normalize_number(value: Any) -> Any:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return value

    s = str(value).strip()
    if not s:
        return None

    s = s.replace(" ", "")

    if "," in s and "." in s:
        s = s.replace(",", "")
    elif "," in s and "." not in s:
        s = s.replace(",", ".")

    try:
        return float(s)
    except ValueError:
        return value


def parse_bool(value: Any) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def parse_area_filter(raw: str | None) -> list[str] | None:
    if not raw:
        return None
    areas = [item.strip() for item in raw.split(",") if item.strip()]
    return areas or None


def build_contracts(
    anchor_date: date,
    area_filter: list[str] | None,
    max_contracts: int | None,
) -> list[dict[str, Any]]:
    contracts: list[dict[str, Any]] = []

    target_year = anchor_date.year
    target_month = anchor_date.month
    current_quarter_start_month = ((target_month - 1) // 3) * 3 + 1

    for area, area_cfg in POWER_AREAS.items():
        if area_filter and area not in area_filter:
            continue

        codes = area_cfg["codes"]
        assumed_types = set(area_cfg["assumed_types"])

        if codes.get("Month"):
            for i in range(7):
                year, month = add_months(target_year, target_month, i)
                contracts.append({
                    "marketGroup": "POWER",
                    "area": area,
                    "commodity": "POWER",
                    "pricing": "F",
                    "product": "Base",
                    "shortCode": codes["Month"],
                    "maturityType": "Month",
                    "maturity": fmt_maturity(year, month),
                    "delivery": f"{MONTH_NAMES[month - 1]}-{str(year)[-2:]}",
                    "assumedCode": "Month" in assumed_types,
                })

        if codes.get("Quarter"):
            for i in range(1, 8):
                year, month = add_months(target_year, current_quarter_start_month, i * 3)
                quarter = ((month - 1) // 3) + 1
                contracts.append({
                    "marketGroup": "POWER",
                    "area": area,
                    "commodity": "POWER",
                    "pricing": "F",
                    "product": "Base",
                    "shortCode": codes["Quarter"],
                    "maturityType": "Quarter",
                    "maturity": fmt_maturity(year, month),
                    "delivery": f"Q{quarter}-{str(year)[-2:]}",
                    "assumedCode": "Quarter" in assumed_types,
                })

        if codes.get("Year"):
            for i in range(1, 7):
                year = target_year + i
                contracts.append({
                    "marketGroup": "POWER",
                    "area": area,
                    "commodity": "POWER",
                    "pricing": "F",
                    "product": "Base",
                    "shortCode": codes["Year"],
                    "maturityType": "Year",
                    "maturity": f"{year}01",
                    "delivery": f"Cal-{str(year)[-2:]}",
                    "assumedCode": "Year" in assumed_types,
                })

    for area, area_cfg in GAS_FUTURES_TTF.items():
        if area_filter and area not in area_filter:
            continue

        codes = area_cfg["codes"]
        assumed_types = set(area_cfg["assumed_types"])

        if codes.get("Month"):
            for i in range(7):
                year, month = add_months(target_year, target_month, i)
                contracts.append({
                    "marketGroup": "GAS_FUTURES",
                    "area": area,
                    "commodity": area_cfg["commodity"],
                    "pricing": area_cfg["pricing"],
                    "product": area_cfg["product"],
                    "shortCode": codes["Month"],
                    "maturityType": "Month",
                    "maturity": fmt_maturity(year, month),
                    "delivery": f"Gas Month {MONTH_NAMES[month - 1]}-{str(year)[-2:]}",
                    "assumedCode": "Month" in assumed_types,
                })

        if codes.get("Quarter"):
            for i in range(1, 8):
                year, month = add_months(target_year, current_quarter_start_month, i * 3)
                quarter = ((month - 1) // 3) + 1
                contracts.append({
                    "marketGroup": "GAS_FUTURES",
                    "area": area,
                    "commodity": area_cfg["commodity"],
                    "pricing": area_cfg["pricing"],
                    "product": area_cfg["product"],
                    "shortCode": codes["Quarter"],
                    "maturityType": "Quarter",
                    "maturity": fmt_maturity(year, month),
                    "delivery": f"Gas Q{quarter}-{str(year)[-2:]}",
                    "assumedCode": "Quarter" in assumed_types,
                })

        if codes.get("Season"):
            season_start_year = target_year if target_month < 10 else target_year + 1
            for i in range(6):
                year = season_start_year + i
                contracts.append({
                    "marketGroup": "GAS_FUTURES",
                    "area": area,
                    "commodity": area_cfg["commodity"],
                    "pricing": area_cfg["pricing"],
                    "product": area_cfg["product"],
                    "shortCode": codes["Season"],
                    "maturityType": "Season",
                    "maturity": f"{year}10",
                    "delivery": f"Gas Winter-{str(year)[-2:]}",
                    "assumedCode": "Season" in assumed_types,
                })

        if codes.get("Year"):
            for i in range(1, 7):
                year = target_year + i
                contracts.append({
                    "marketGroup": "GAS_FUTURES",
                    "area": area,
                    "commodity": area_cfg["commodity"],
                    "pricing": area_cfg["pricing"],
                    "product": area_cfg["product"],
                    "shortCode": codes["Year"],
                    "maturityType": "Year",
                    "maturity": f"{year}01",
                    "delivery": f"Gas Cal-{str(year)[-2:]}",
                    "assumedCode": "Year" in assumed_types,
                })

    for _, cfg in GAS_SPOT_CONFIG.items():
        if area_filter and cfg["area"] not in area_filter:
            continue

        contracts.append({
            "marketGroup": "GAS_SPOT",
            "area": cfg["area"],
            "commodity": cfg["commodity"],
            "pricing": cfg["pricing"],
            "product": cfg["product"],
            "shortCode": cfg["shortCode"],
            "maturityType": cfg["maturityType"],
            "maturity": cfg["maturity"],
            "delivery": cfg["delivery"],
            "assumedCode": cfg["assumedCode"],
        })

    for _, cfg in GOO_FUTURES_CONFIG.items():
        if area_filter and cfg["area"] not in area_filter:
            continue

        for i in range(1, 5):
            year = target_year + i
            contracts.append({
                "marketGroup": "GO_FUTURES",
                "area": cfg["area"],
                "commodity": cfg["commodity"],
                "pricing": cfg["pricing"],
                "product": cfg["product"],
                "shortCode": cfg["shortCode"],
                "maturityType": cfg["maturityType"],
                "maturity": f"{year}01",
                "delivery": f"GO Wind Cal-{str(year)[-2:]}",
                "assumedCode": cfg["assumedCode"],
            })

    for _, cfg in EUA_FUTURES_CONFIG.items():
        if area_filter and cfg["area"] not in area_filter:
            continue

        for i in range(EUA_DISCOVERY_MONTHS):
            year, month = add_months(target_year, target_month, i)
            contracts.append({
                "marketGroup": "EUA_FUTURES",
                "area": cfg["area"],
                "commodity": cfg["commodity"],
                "pricing": cfg["pricing"],
                "product": cfg["product"],
                "shortCode": cfg["shortCode"],
                "maturityType": cfg["maturityType"],
                "maturity": fmt_maturity(year, month),
                "delivery": f"EUA {MONTH_NAMES[month - 1]}-{str(year)[-2:]}",
                "assumedCode": cfg["assumedCode"],
                "suppressNoRow": True,
            })

    for contract in ICE_COAL_CONTRACTS:
        if area_filter and contract["area"] not in area_filter:
            continue
        contracts.append(contract.copy())

    if max_contracts is not None:
        contracts = contracts[:max_contracts]

    return contracts


def build_request_params(contract: dict[str, Any], start_date_str: str, end_date_str: str) -> dict[str, str]:
    maturity = contract["maturity"] if contract["maturity"] is not None else "null"
    maturity_type = contract["maturityType"] if contract["maturityType"] is not None else "null"

    return {
        "shortCode": contract["shortCode"],
        "commodity": contract["commodity"],
        "pricing": contract["pricing"],
        "area": contract["area"],
        "product": contract["product"],
        "maturity": maturity,
        "startDate": start_date_str,
        "endDate": end_date_str,
        "maturityType": maturity_type,
        "isRolling": "true",
    }


def parse_payload_to_objects(payload: dict[str, Any]) -> list[dict[str, Any]]:
    headers_row = payload.get("header", [])
    data_rows = payload.get("data", [])

    objects: list[dict[str, Any]] = []
    for row in data_rows:
        if isinstance(row, list):
            objects.append(dict(zip(headers_row, row)))
        elif isinstance(row, dict):
            objects.append(row)
    return objects


def select_best_row(
    rows: list[dict[str, Any]],
    requested_trade_date: str,
    allow_fallback: bool,
) -> tuple[dict[str, Any] | None, str]:
    exact = next((row for row in rows if row.get("tradeDate") == requested_trade_date), None)
    if exact:
        return exact, "ok"

    if not allow_fallback:
        return None, "no_row"

    previous_rows = [
        row for row in rows
        if isinstance(row.get("tradeDate"), str) and row["tradeDate"] <= requested_trade_date
    ]
    previous_rows.sort(key=lambda row: row["tradeDate"], reverse=True)

    if previous_rows:
        return previous_rows[0], "fallback_previous_trade_day"

    return None, "no_row"


def build_error_row(
    mode: str,
    contract_anchor_date: str,
    contract: dict[str, Any],
    requested_trade_date: str | None,
    window_start_date: str,
    window_end_date: str,
    status: str,
    error_message: str | None = None,
    http_status: int | None = None,
) -> dict[str, Any]:
    return {
        "mode": mode,
        "contractAnchorDate": contract_anchor_date,
        "marketGroup": contract["marketGroup"],
        "area": contract["area"],
        "commodity": contract["commodity"],
        "pricing": contract["pricing"],
        "product": contract["product"],
        "maturityType": contract["maturityType"],
        "delivery": contract["delivery"],
        "requestedTradeDate": requested_trade_date,
        "windowStartDate": window_start_date,
        "windowEndDate": window_end_date,
        "tradeDate": None,
        "status": status,
        "settlementPrice": None,
        "volume": None,
        "grossOpenInterest": None,
        "shortCode": contract["shortCode"],
        "maturity": contract["maturity"],
        "assumedCode": contract["assumedCode"],
        "httpStatus": http_status,
        "errorMessage": error_message,
    }


def build_data_row(
    mode: str,
    contract_anchor_date: str,
    contract: dict[str, Any],
    requested_trade_date: str | None,
    window_start_date: str,
    window_end_date: str,
    row: dict[str, Any],
    status: str,
    http_status: int | None,
) -> dict[str, Any]:
    return {
        "mode": mode,
        "contractAnchorDate": contract_anchor_date,
        "marketGroup": contract["marketGroup"],
        "area": contract["area"],
        "commodity": contract["commodity"],
        "pricing": contract["pricing"],
        "product": contract["product"],
        "maturityType": contract["maturityType"],
        "delivery": contract["delivery"],
        "requestedTradeDate": requested_trade_date,
        "windowStartDate": window_start_date,
        "windowEndDate": window_end_date,
        "tradeDate": row.get("tradeDate"),
        "status": status,
        "settlementPrice": normalize_number(first_defined(row, ["settlPx", "settlementPrice", "settlPrice"])),
        "volume": normalize_number(first_defined(row, ["totVolTrdd", "totVolTrd", "volume", "totalVolume"])),
        "grossOpenInterest": normalize_number(first_defined(row, ["grossOpenInt", "grossOpenInterest"])),
        "shortCode": contract["shortCode"],
        "maturity": contract["maturity"],
        "assumedCode": contract["assumedCode"],
        "httpStatus": http_status,
        "errorMessage": None,
    }


def build_ice_data_row(
    mode: str,
    contract_anchor_date: str,
    contract: dict[str, Any],
    window_start_date: str,
    window_end_date: str,
    trade_date: str,
    settlement_price: float | None,
    http_status: int | None,
) -> dict[str, Any]:
    return {
        "mode": mode,
        "contractAnchorDate": contract_anchor_date,
        "marketGroup": contract["marketGroup"],
        "area": contract["area"],
        "commodity": contract["commodity"],
        "pricing": contract["pricing"],
        "product": contract["product"],
        "maturityType": contract["maturityType"],
        "delivery": contract["delivery"],
        "requestedTradeDate": None,
        "windowStartDate": window_start_date,
        "windowEndDate": window_end_date,
        "tradeDate": trade_date,
        "status": "ok",
        "settlementPrice": settlement_price,
        "volume": None,
        "grossOpenInterest": None,
        "shortCode": contract["shortCode"],
        "maturity": contract["maturity"],
        "assumedCode": contract["assumedCode"],
        "httpStatus": http_status,
        "errorMessage": None,
    }


def dedupe_contracts(contracts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str, str, Any]] = set()
    unique_contracts: list[dict[str, Any]] = []

    for contract in contracts:
        key = (
            contract["area"],
            contract["shortCode"],
            str(contract["maturityType"]),
            contract["maturity"],
        )
        if key in seen:
            continue
        seen.add(key)
        unique_contracts.append(contract)

    return unique_contracts


def load_contracts_from_previous_csv(csv_path: Path) -> tuple[list[dict[str, Any]], InferredRunConfig]:
    contracts: list[dict[str, Any]] = []
    inferred = InferredRunConfig()

    with csv_path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)

    for row in rows:
        status = (row.get("status") or "").strip()
        should_include = status != "ok" if status else True
        if not should_include:
            continue

        if inferred.mode is None:
            inferred.mode = (row.get("mode") or "").strip() or None
        if inferred.contract_anchor_date is None:
            inferred.contract_anchor_date = (row.get("contractAnchorDate") or "").strip() or None
        if inferred.requested_trade_date is None:
            inferred.requested_trade_date = (row.get("requestedTradeDate") or "").strip() or None
        if inferred.window_start_date is None:
            inferred.window_start_date = (row.get("windowStartDate") or "").strip() or None
        if inferred.window_end_date is None:
            inferred.window_end_date = (row.get("windowEndDate") or "").strip() or None

        maturity_type_raw = row.get("maturityType")
        maturity_raw = row.get("maturity")

        contracts.append({
            "marketGroup": row.get("marketGroup") or "UNKNOWN",
            "area": row["area"],
            "commodity": row.get("commodity"),
            "pricing": row.get("pricing"),
            "product": row.get("product"),
            "shortCode": row["shortCode"],
            "maturityType": None if maturity_type_raw in ("", "None", None, "null") else maturity_type_raw,
            "maturity": None if maturity_raw in ("", "None", None, "null") else maturity_raw,
            "delivery": row["delivery"],
            "assumedCode": parse_bool(row.get("assumedCode")),
        })

    if inferred.mode is None:
        if inferred.requested_trade_date:
            inferred.mode = "selected"
        elif inferred.window_start_date and inferred.window_end_date:
            inferred.mode = "range"

    return dedupe_contracts(contracts), inferred


def get_latest_trade_date_from_master(master_file: Path) -> date:
    if not master_file.exists():
        raise FileNotFoundError(f"Master file not found: {master_file}")

    latest: date | None = None

    with master_file.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            trade_date_raw = (row.get("tradeDate") or "").strip()
            if not trade_date_raw:
                continue
            try:
                trade_date = parse_trade_date(trade_date_raw)
            except ValueError:
                continue
            if latest is None or trade_date > latest:
                latest = trade_date

    if latest is None:
        raise ValueError(f"No valid tradeDate values found in master file: {master_file}")

    return latest


def resolve_run_config(
    args: argparse.Namespace,
    inferred: InferredRunConfig,
    master_file_path: Path | None = None,
) -> ResolvedRunConfig:
    mode = args.mode or inferred.mode or "selected"

    if mode == "selected":
        requested_trade_date = args.trade_date or inferred.requested_trade_date or inferred.window_end_date
        if not requested_trade_date:
            raise ValueError("Selected mode requires --trade-date, or a rerun file containing requestedTradeDate.")
        contract_anchor_date = args.contract_anchor_date or inferred.contract_anchor_date or requested_trade_date
        end_date = requested_trade_date
        start_date = fmt_date(parse_trade_date(requested_trade_date) - timedelta(days=args.lookback_days))
        return ResolvedRunConfig(
            mode=mode,
            contract_anchor_date=contract_anchor_date,
            requested_trade_date=requested_trade_date,
            window_start_date=start_date,
            window_end_date=end_date,
            allow_fallback=not args.no_fallback,
        )

    if mode == "range":
        start_date = args.start_date or inferred.window_start_date
        end_date = args.end_date or inferred.window_end_date
        if not start_date or not end_date:
            raise ValueError("Range mode requires --start-date and --end-date, or a rerun file containing windowStartDate/windowEndDate.")
        contract_anchor_date = args.contract_anchor_date or inferred.contract_anchor_date or end_date
        return ResolvedRunConfig(
            mode=mode,
            contract_anchor_date=contract_anchor_date,
            requested_trade_date=None,
            window_start_date=start_date,
            window_end_date=end_date,
            allow_fallback=False,
        )

    if mode == "full-history":
        end_date = args.end_date or inferred.window_end_date or args.trade_date or inferred.requested_trade_date
        if not end_date:
            raise ValueError("Full-history mode requires --end-date, --trade-date, or a rerun file containing an end date.")
        start_date = args.full_history_start_date
        contract_anchor_date = args.contract_anchor_date or inferred.contract_anchor_date or end_date
        return ResolvedRunConfig(
            mode=mode,
            contract_anchor_date=contract_anchor_date,
            requested_trade_date=None,
            window_start_date=start_date,
            window_end_date=end_date,
            allow_fallback=False,
        )

    if mode == "auto-update":
        if master_file_path is None:
            raise ValueError("Auto-update mode requires --master-file.")
        latest_trade_date = get_latest_trade_date_from_master(master_file_path)
        end_date = args.end_date or fmt_date(latest_trade_date)
        start_date = fmt_date(latest_trade_date - timedelta(days=args.auto_update_overlap_days))
        contract_anchor_date = args.contract_anchor_date or end_date
        return ResolvedRunConfig(
            mode=mode,
            contract_anchor_date=contract_anchor_date,
            requested_trade_date=None,
            window_start_date=start_date,
            window_end_date=end_date,
            allow_fallback=False,
        )

    raise ValueError(f"Unsupported mode: {mode}")


def get_retry_wait_seconds(
    response: requests.Response | None,
    attempt: int,
    retry_delay_seconds: float,
) -> float:
    wait_seconds = retry_delay_seconds * (2 ** attempt)

    if response is not None and response.status_code == 429:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                wait_seconds = max(wait_seconds, float(retry_after))
            except ValueError:
                pass

    return wait_seconds


def row_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return tuple(row.get(field) for field in MASTER_KEY_FIELDS)


def row_quality_score(row: dict[str, Any]) -> tuple[int, int, str]:
    populated = sum(
        1
        for field in ("settlementPrice", "volume", "grossOpenInterest")
        if row.get(field) not in (None, "", "null")
    )
    status_rank = 1 if row.get("status") == "ok" else 0
    http_rank = str(row.get("httpStatus") or "")
    return (status_rank, populated, http_rank)


def merge_into_master(master_file: Path, new_rows: list[dict[str, Any]]) -> tuple[int, int]:
    merged: dict[tuple[Any, ...], dict[str, Any]] = {}
    existing_count = 0

    if master_file.exists():
        with master_file.open("r", newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                normalized = {field: row.get(field, "") for field in FIELDNAMES}
                key = row_key(normalized)
                merged[key] = normalized
                existing_count += 1

    added_or_updated = 0
    for row in new_rows:
        normalized = {field: row.get(field, "") if row.get(field) is not None else "" for field in FIELDNAMES}
        key = row_key(normalized)
        existing = merged.get(key)

        if existing is None:
            merged[key] = normalized
            added_or_updated += 1
            continue

        if row_quality_score(normalized) >= row_quality_score(existing):
            merged[key] = normalized
            added_or_updated += 1

    master_file.parent.mkdir(parents=True, exist_ok=True)

    sorted_rows = sorted(
        merged.values(),
        key=lambda r: (
            r.get("marketGroup", ""),
            r.get("area", ""),
            SORT_ORDER.get(r.get("maturityType"), 99),
            r.get("maturity", "") or "",
            r.get("tradeDate", "") or "",
        ),
    )

    with master_file.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(sorted_rows)

    return existing_count, len(sorted_rows)


def fetch_contract_rows_eex(
    logger: logging.Logger,
    rate_controller: AdaptiveRateController,
    contract: dict[str, Any],
    run_cfg: ResolvedRunConfig,
    max_retries: int,
    retry_delay_seconds: float,
    request_timeout_seconds: float,
) -> list[dict[str, Any]]:
    session = get_thread_session("eex")
    params = build_request_params(contract, run_cfg.window_start_date, run_cfg.window_end_date)
    last_http_status: int | None = None

    for attempt in range(max_retries + 1):
        rate_controller.wait_for_slot()

        try:
            response = session.get(
                API_BASE_URL,
                params=params,
                timeout=request_timeout_seconds,
            )
        except requests.RequestException as exc:
            if attempt < max_retries:
                wait_seconds = get_retry_wait_seconds(None, attempt, retry_delay_seconds)
                logger.warning(
                    "Request error for %s %s %s: %s. Retrying in %.1fs.",
                    contract["area"],
                    str(contract["maturityType"]),
                    contract["delivery"],
                    exc,
                    wait_seconds,
                )
                time.sleep(wait_seconds)
                continue

            return [
                build_error_row(
                    mode=run_cfg.mode,
                    contract_anchor_date=run_cfg.contract_anchor_date,
                    contract=contract,
                    requested_trade_date=run_cfg.requested_trade_date,
                    window_start_date=run_cfg.window_start_date,
                    window_end_date=run_cfg.window_end_date,
                    status="fetch_error",
                    error_message=str(exc),
                )
            ]

        last_http_status = response.status_code

        if response.status_code == 429:
            old_gap, new_gap, added_cooldown = rate_controller.record_429()
            logger.warning(
                "HTTP 429 for %s %s %s. Adaptive gap %.2fs -> %.2fs.",
                contract["area"],
                str(contract["maturityType"]),
                contract["delivery"],
                old_gap,
                new_gap,
            )
            if added_cooldown > 0:
                logger.warning(
                    "Global cooldown triggered after clustered 429s. Pausing new requests for %.1fs.",
                    added_cooldown,
                )

        if response.status_code in RETRYABLE_HTTP_STATUSES and attempt < max_retries:
            wait_seconds = get_retry_wait_seconds(response, attempt, retry_delay_seconds)
            logger.warning(
                "Retryable HTTP status for %s %s %s: %s. Retrying in %.1fs.",
                contract["area"],
                str(contract["maturityType"]),
                contract["delivery"],
                response.status_code,
                wait_seconds,
            )
            time.sleep(wait_seconds)
            continue

        if response.status_code == 429:
            return [
                build_error_row(
                    mode=run_cfg.mode,
                    contract_anchor_date=run_cfg.contract_anchor_date,
                    contract=contract,
                    requested_trade_date=run_cfg.requested_trade_date,
                    window_start_date=run_cfg.window_start_date,
                    window_end_date=run_cfg.window_end_date,
                    status="rate_limited",
                    error_message="HTTP 429 rate limited after retries",
                    http_status=response.status_code,
                )
            ]

        if not response.ok:
            return [
                build_error_row(
                    mode=run_cfg.mode,
                    contract_anchor_date=run_cfg.contract_anchor_date,
                    contract=contract,
                    requested_trade_date=run_cfg.requested_trade_date,
                    window_start_date=run_cfg.window_start_date,
                    window_end_date=run_cfg.window_end_date,
                    status="http_error",
                    error_message=f"HTTP {response.status_code}",
                    http_status=response.status_code,
                )
            ]

        try:
            payload = response.json()
        except ValueError as exc:
            return [
                build_error_row(
                    mode=run_cfg.mode,
                    contract_anchor_date=run_cfg.contract_anchor_date,
                    contract=contract,
                    requested_trade_date=run_cfg.requested_trade_date,
                    window_start_date=run_cfg.window_start_date,
                    window_end_date=run_cfg.window_end_date,
                    status="json_error",
                    error_message=f"JSON parse failed: {exc}",
                    http_status=last_http_status,
                )
            ]

        objects = parse_payload_to_objects(payload)

        if run_cfg.mode == "selected":
            selected_row, status = select_best_row(
                objects,
                run_cfg.requested_trade_date or "",
                run_cfg.allow_fallback,
            )
            if selected_row is None:
                if contract.get("suppressNoRow"):
                    return []
                return [
                    build_error_row(
                        mode=run_cfg.mode,
                        contract_anchor_date=run_cfg.contract_anchor_date,
                        contract=contract,
                        requested_trade_date=run_cfg.requested_trade_date,
                        window_start_date=run_cfg.window_start_date,
                        window_end_date=run_cfg.window_end_date,
                        status="no_row",
                        error_message=f"No row found for {run_cfg.requested_trade_date}",
                        http_status=last_http_status,
                    )
                ]

            return [
                build_data_row(
                    mode=run_cfg.mode,
                    contract_anchor_date=run_cfg.contract_anchor_date,
                    contract=contract,
                    requested_trade_date=run_cfg.requested_trade_date,
                    window_start_date=run_cfg.window_start_date,
                    window_end_date=run_cfg.window_end_date,
                    row=selected_row,
                    status=status,
                    http_status=last_http_status,
                )
            ]

        if not objects:
            if contract.get("suppressNoRow"):
                return []
            return [
                build_error_row(
                    mode=run_cfg.mode,
                    contract_anchor_date=run_cfg.contract_anchor_date,
                    contract=contract,
                    requested_trade_date=None,
                    window_start_date=run_cfg.window_start_date,
                    window_end_date=run_cfg.window_end_date,
                    status="no_row",
                    error_message=f"No rows returned for {run_cfg.window_start_date} -> {run_cfg.window_end_date}",
                    http_status=last_http_status,
                )
            ]

        result_rows = [
            build_data_row(
                mode=run_cfg.mode,
                contract_anchor_date=run_cfg.contract_anchor_date,
                contract=contract,
                requested_trade_date=None,
                window_start_date=run_cfg.window_start_date,
                window_end_date=run_cfg.window_end_date,
                row=row,
                status="ok",
                http_status=last_http_status,
            )
            for row in objects
        ]

        result_rows.sort(key=lambda r: r["tradeDate"] or "", reverse=True)
        return result_rows

    return [
        build_error_row(
            mode=run_cfg.mode,
            contract_anchor_date=run_cfg.contract_anchor_date,
            contract=contract,
            requested_trade_date=run_cfg.requested_trade_date,
            window_start_date=run_cfg.window_start_date,
            window_end_date=run_cfg.window_end_date,
            status="rate_limited" if last_http_status == 429 else "fetch_error",
            error_message="HTTP 429 rate limited after retries" if last_http_status == 429 else "Unexpected retry loop exit",
            http_status=last_http_status,
        )
    ]


def fetch_contract_rows_ice(
    logger: logging.Logger,
    rate_controller: AdaptiveRateController,
    contract: dict[str, Any],
    run_cfg: ResolvedRunConfig,
    max_retries: int,
    retry_delay_seconds: float,
    request_timeout_seconds: float,
) -> list[dict[str, Any]]:
    session = get_thread_session("ice")
    params = {
        "marketId": str(contract["iceMarketId"]),
        "historicalSpan": "2",
    }
    last_http_status: int | None = None

    for attempt in range(max_retries + 1):
        rate_controller.wait_for_slot()

        try:
            response = session.get(
                ICE_HISTORICAL_URL,
                params=params,
                timeout=request_timeout_seconds,
            )
        except requests.RequestException as exc:
            if attempt < max_retries:
                wait_seconds = get_retry_wait_seconds(None, attempt, retry_delay_seconds)
                logger.warning(
                    "ICE request error for %s %s: %s. Retrying in %.1fs.",
                    contract["delivery"],
                    contract["iceMarketId"],
                    exc,
                    wait_seconds,
                )
                time.sleep(wait_seconds)
                continue

            return [
                build_error_row(
                    mode=run_cfg.mode,
                    contract_anchor_date=run_cfg.contract_anchor_date,
                    contract=contract,
                    requested_trade_date=None,
                    window_start_date=run_cfg.window_start_date,
                    window_end_date=run_cfg.window_end_date,
                    status="fetch_error",
                    error_message=str(exc),
                )
            ]

        last_http_status = response.status_code

        if response.status_code == 429:
            old_gap, new_gap, added_cooldown = rate_controller.record_429()
            logger.warning(
                "ICE HTTP 429 for %s. Adaptive gap %.2fs -> %.2fs.",
                contract["delivery"],
                old_gap,
                new_gap,
            )
            if added_cooldown > 0:
                logger.warning(
                    "Global cooldown triggered after clustered 429s. Pausing new requests for %.1fs.",
                    added_cooldown,
                )

        if response.status_code in RETRYABLE_HTTP_STATUSES and attempt < max_retries:
            wait_seconds = get_retry_wait_seconds(response, attempt, retry_delay_seconds)
            logger.warning(
                "Retryable ICE HTTP status for %s: %s. Retrying in %.1fs.",
                contract["delivery"],
                response.status_code,
                wait_seconds,
            )
            time.sleep(wait_seconds)
            continue

        if response.status_code == 429:
            return [
                build_error_row(
                    mode=run_cfg.mode,
                    contract_anchor_date=run_cfg.contract_anchor_date,
                    contract=contract,
                    requested_trade_date=None,
                    window_start_date=run_cfg.window_start_date,
                    window_end_date=run_cfg.window_end_date,
                    status="rate_limited",
                    error_message="HTTP 429 rate limited after retries",
                    http_status=response.status_code,
                )
            ]

        if not response.ok:
            return [
                build_error_row(
                    mode=run_cfg.mode,
                    contract_anchor_date=run_cfg.contract_anchor_date,
                    contract=contract,
                    requested_trade_date=None,
                    window_start_date=run_cfg.window_start_date,
                    window_end_date=run_cfg.window_end_date,
                    status="http_error",
                    error_message=f"HTTP {response.status_code}",
                    http_status=response.status_code,
                )
            ]

        try:
            payload = response.json()
        except ValueError as exc:
            return [
                build_error_row(
                    mode=run_cfg.mode,
                    contract_anchor_date=run_cfg.contract_anchor_date,
                    contract=contract,
                    requested_trade_date=None,
                    window_start_date=run_cfg.window_start_date,
                    window_end_date=run_cfg.window_end_date,
                    status="json_error",
                    error_message=f"JSON parse failed: {exc}",
                    http_status=last_http_status,
                )
            ]

        bars = payload.get("bars", [])
        if not isinstance(bars, list):
            return [
                build_error_row(
                    mode=run_cfg.mode,
                    contract_anchor_date=run_cfg.contract_anchor_date,
                    contract=contract,
                    requested_trade_date=None,
                    window_start_date=run_cfg.window_start_date,
                    window_end_date=run_cfg.window_end_date,
                    status="json_error",
                    error_message="ICE payload missing bars list",
                    http_status=last_http_status,
                )
            ]

        start_dt = parse_trade_date(run_cfg.window_start_date)
        end_dt = parse_trade_date(run_cfg.window_end_date)

        rows: list[dict[str, Any]] = []
        for bar in bars:
            if not isinstance(bar, list) or len(bar) < 2:
                continue
            try:
                trade_dt = parse_ice_bar_date(str(bar[0]))
            except ValueError:
                continue

            if trade_dt < start_dt or trade_dt > end_dt:
                continue

            rows.append(
                build_ice_data_row(
                    mode=run_cfg.mode,
                    contract_anchor_date=run_cfg.contract_anchor_date,
                    contract=contract,
                    window_start_date=run_cfg.window_start_date,
                    window_end_date=run_cfg.window_end_date,
                    trade_date=fmt_date(trade_dt),
                    settlement_price=normalize_number(bar[1]),
                    http_status=last_http_status,
                )
            )

        if run_cfg.mode == "selected":
            selected_row, status = select_best_row(
                rows,
                run_cfg.requested_trade_date or "",
                run_cfg.allow_fallback,
            )
            if selected_row is None:
                return [
                    build_error_row(
                        mode=run_cfg.mode,
                        contract_anchor_date=run_cfg.contract_anchor_date,
                        contract=contract,
                        requested_trade_date=run_cfg.requested_trade_date,
                        window_start_date=run_cfg.window_start_date,
                        window_end_date=run_cfg.window_end_date,
                        status="no_row",
                        error_message=f"No row found for {run_cfg.requested_trade_date}",
                        http_status=last_http_status,
                    )
                ]
            selected_row["status"] = status
            return [selected_row]

        if not rows:
            return []

        rows.sort(key=lambda r: r["tradeDate"] or "", reverse=True)
        return rows

    return [
        build_error_row(
            mode=run_cfg.mode,
            contract_anchor_date=run_cfg.contract_anchor_date,
            contract=contract,
            requested_trade_date=None,
            window_start_date=run_cfg.window_start_date,
            window_end_date=run_cfg.window_end_date,
            status="fetch_error",
            error_message="Unexpected retry loop exit",
            http_status=last_http_status,
        )
    ]


def fetch_contract_rows(
    logger: logging.Logger,
    rate_controller: AdaptiveRateController,
    contract: dict[str, Any],
    run_cfg: ResolvedRunConfig,
    max_retries: int,
    retry_delay_seconds: float,
    request_timeout_seconds: float,
) -> list[dict[str, Any]]:
    if contract["marketGroup"] == "ICE_COAL_FUTURES":
        return fetch_contract_rows_ice(
            logger,
            rate_controller,
            contract,
            run_cfg,
            max_retries,
            retry_delay_seconds,
            request_timeout_seconds,
        )

    return fetch_contract_rows_eex(
        logger,
        rate_controller,
        contract,
        run_cfg,
        max_retries,
        retry_delay_seconds,
        request_timeout_seconds,
    )


def setup_logging(log_path: Path) -> logging.Logger:
    logger = logging.getLogger("market_scraper_unified")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    return logger


def should_log_progress(index: int, total: int, progress_every: int) -> bool:
    if index == 1 or index == total:
        return True
    if progress_every <= 0:
        return False
    return index % progress_every == 0


def init_csv_writer(file_path: Path) -> tuple[Any, csv.DictWriter]:
    handle = file_path.open("w", newline="", encoding="utf-8")
    writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
    writer.writeheader()
    return handle, writer


def main() -> None:
    args = parse_args()

    script_dir = Path(__file__).resolve().parent
    output_dir = Path(args.output_dir)
    if not output_dir.is_absolute():
        output_dir = script_dir / output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    master_file_path = None
    if args.master_file:
        master_file_path = Path(args.master_file)
        if not master_file_path.is_absolute():
            master_file_path = script_dir / master_file_path

    rerun_contracts: list[dict[str, Any]] | None = None
    inferred = InferredRunConfig()

    if args.rerun_failed_from:
        rerun_csv_path = Path(args.rerun_failed_from)
        if not rerun_csv_path.is_absolute():
            rerun_csv_path = script_dir / rerun_csv_path
        if not rerun_csv_path.exists():
            raise FileNotFoundError(f"Rerun file not found: {rerun_csv_path}")

        rerun_contracts, inferred = load_contracts_from_previous_csv(rerun_csv_path)
        if not rerun_contracts:
            raise ValueError("No failed rows found in the rerun file.")

    run_cfg = resolve_run_config(args, inferred, master_file_path)
    anchor_date = parse_trade_date(run_cfg.contract_anchor_date)

    timestamp_suffix = datetime.now().strftime("%d-%m-%y_%H%M")
    main_csv_path = output_dir / f"{MAIN_CSV_PREFIX}_[{timestamp_suffix}].csv"
    failed_csv_path = output_dir / f"{FAILED_CSV_PREFIX}_[{timestamp_suffix}].csv"
    summary_json_path = output_dir / f"{SUMMARY_JSON_PREFIX}_[{timestamp_suffix}].json"
    log_path = output_dir / f"{LOG_PREFIX}_[{timestamp_suffix}].log"

    logger = setup_logging(log_path)
    rate_controller = AdaptiveRateController(
        base_interval_seconds=args.request_gap_seconds,
        max_interval_seconds=args.max_request_gap_seconds,
        cooldown_threshold=args.cooldown_threshold,
        cooldown_window_seconds=args.cooldown_window_seconds,
        cooldown_seconds=args.cooldown_seconds,
        relax_after_seconds=args.relax_after_seconds,
        relax_factor=args.relax_factor,
    )

    area_filter = parse_area_filter(args.areas)

    if rerun_contracts is not None:
        contracts = rerun_contracts
        if args.max_contracts is not None:
            contracts = contracts[:args.max_contracts]
        run_mode = "rerun_failed"
    else:
        contracts = build_contracts(
            anchor_date=anchor_date,
            area_filter=area_filter,
            max_contracts=args.max_contracts,
        )
        run_mode = "full"

    logger.info("Run mode: %s", run_mode)
    logger.info("Scrape mode: %s", run_cfg.mode)
    logger.info("Built %s contracts.", len(contracts))
    logger.info("Contract anchor date: %s", run_cfg.contract_anchor_date)
    logger.info("Window: %s -> %s", run_cfg.window_start_date, run_cfg.window_end_date)
    if run_cfg.requested_trade_date:
        logger.info("Requested trade date: %s", run_cfg.requested_trade_date)
    logger.info(
        "Adaptive pacing: base %.2fs, max %.2fs, workers %s.",
        args.request_gap_seconds,
        args.max_request_gap_seconds,
        args.max_workers,
    )
    logger.info(
        "429 cooldown rule: %s hit(s) within %.1fs triggers a %.1fs global pause.",
        args.cooldown_threshold,
        args.cooldown_window_seconds,
        args.cooldown_seconds,
    )
    if master_file_path is not None:
        logger.info("Master merge target: %s", master_file_path)

    main_handle, main_writer = init_csv_writer(main_csv_path)
    failed_handle = None
    failed_writer = None

    summary = Counter()
    total_output_rows = 0
    total_ok_rows = 0
    unique_areas: set[str] = set()
    all_rows_for_master: list[dict[str, Any]] = []

    start_ts = time.time()
    completed_contracts = 0
    future_to_meta: dict[Any, tuple[int, dict[str, Any]]] = {}

    try:
        with ThreadPoolExecutor(max_workers=args.max_workers, thread_name_prefix="market") as executor:
            for index, contract in enumerate(contracts, start=1):
                if should_log_progress(index, len(contracts), args.progress_every):
                    logger.info(
                        "[queued %s/%s] %s %s %s (%s, %s)",
                        index,
                        len(contracts),
                        contract["area"],
                        str(contract["maturityType"]),
                        contract["delivery"],
                        contract["shortCode"],
                        contract["maturity"],
                    )

                future = executor.submit(
                    fetch_contract_rows,
                    logger,
                    rate_controller,
                    contract,
                    run_cfg,
                    args.max_retries,
                    args.retry_delay_seconds,
                    args.request_timeout_seconds,
                )
                future_to_meta[future] = (index, contract)

            for future in as_completed(future_to_meta):
                index, contract = future_to_meta[future]

                try:
                    contract_rows = future.result()
                except Exception as exc:
                    logger.exception(
                        "Unhandled error for [%s/%s] %s %s %s (%s, %s): %s",
                        index,
                        len(contracts),
                        contract["area"],
                        str(contract["maturityType"]),
                        contract["delivery"],
                        contract["shortCode"],
                        contract["maturity"],
                        exc,
                    )
                    contract_rows = [
                        build_error_row(
                            mode=run_cfg.mode,
                            contract_anchor_date=run_cfg.contract_anchor_date,
                            contract=contract,
                            requested_trade_date=run_cfg.requested_trade_date,
                            window_start_date=run_cfg.window_start_date,
                            window_end_date=run_cfg.window_end_date,
                            status="fetch_error",
                            error_message=f"Unhandled worker error: {exc}",
                        )
                    ]

                if not contract_rows:
                    completed_contracts += 1

                    if should_log_progress(completed_contracts, len(contracts), args.progress_every):
                        logger.info(
                            "[done %s/%s] %s %s %s -> skipped_no_rows (0 row(s))",
                            completed_contracts,
                            len(contracts),
                            contract["area"],
                            str(contract["maturityType"]),
                            contract["delivery"],
                        )
                    continue

                for row in contract_rows:
                    main_writer.writerow(row)
                    if row.get("status") == "ok" and row.get("tradeDate"):
                        all_rows_for_master.append(row)
                    total_output_rows += 1
                    unique_areas.add(row["area"])
                    summary[row["status"]] += 1
                    if row["status"] == "ok":
                        total_ok_rows += 1
                    else:
                        if failed_writer is None:
                            failed_handle, failed_writer = init_csv_writer(failed_csv_path)
                        failed_writer.writerow(row)

                completed_contracts += 1

                ok_rows = sum(1 for row in contract_rows if row["status"] == "ok")
                contract_status = "ok" if ok_rows == len(contract_rows) else contract_rows[0]["status"]

                if (
                    contract_status != "ok"
                    or should_log_progress(completed_contracts, len(contracts), args.progress_every)
                ):
                    logger.info(
                        "[done %s/%s] %s %s %s -> %s (%s row(s))",
                        completed_contracts,
                        len(contracts),
                        contract["area"],
                        str(contract["maturityType"]),
                        contract["delivery"],
                        contract_status,
                        len(contract_rows),
                    )
    finally:
        main_handle.close()
        if failed_handle is not None:
            failed_handle.close()

    master_existing_rows = None
    master_final_rows = None
    if master_file_path is not None:
        master_existing_rows, master_final_rows = merge_into_master(master_file_path, all_rows_for_master)

    duration_seconds = round(time.time() - start_ts, 2)
    contracts_per_minute = round((len(contracts) / duration_seconds) * 60, 2) if duration_seconds > 0 else 0.0
    avg_seconds_per_contract = round(duration_seconds / len(contracts), 2) if contracts else 0.0

    logger.info("Status summary:")
    for status, count in sorted(summary.items()):
        logger.info("  %s: %s", status, count)

    logger.info("Saved main CSV: %s", main_csv_path)
    if failed_writer is not None:
        logger.info("Saved failed CSV: %s", failed_csv_path)
    else:
        logger.info("No failed rows. Failed CSV not created.")

    if master_file_path is not None:
        logger.info("Updated master CSV: %s", master_file_path)
        logger.info("Master rows before merge: %s", master_existing_rows)
        logger.info("Master rows after merge: %s", master_final_rows)

    logger.info("Total output rows: %s", total_output_rows)
    logger.info("Total ok rows: %s", total_ok_rows)
    logger.info("Duration seconds: %s", duration_seconds)
    logger.info("Throughput contracts/minute: %s", contracts_per_minute)
    logger.info("Average seconds/contract: %s", avg_seconds_per_contract)
    logger.info("Final adaptive request gap: %.2fs", rate_controller.current_interval())

    summary_payload = {
        "runMode": run_mode,
        "scrapeMode": run_cfg.mode,
        "contractAnchorDate": run_cfg.contract_anchor_date,
        "requestedTradeDate": run_cfg.requested_trade_date,
        "startDate": run_cfg.window_start_date,
        "endDate": run_cfg.window_end_date,
        "requestGapSeconds": args.request_gap_seconds,
        "maxRequestGapSeconds": args.max_request_gap_seconds,
        "maxRetries": args.max_retries,
        "retryDelaySeconds": args.retry_delay_seconds,
        "requestTimeoutSeconds": args.request_timeout_seconds,
        "allowPreviousTradeDayFallback": run_cfg.allow_fallback,
        "requestedAreas": area_filter if area_filter is not None else "ALL",
        "areasInRun": sorted(unique_areas),
        "totalContracts": len(contracts),
        "totalOutputRows": total_output_rows,
        "totalOkRows": total_ok_rows,
        "failedRows": total_output_rows - total_ok_rows,
        "statusSummary": dict(summary),
        "durationSeconds": duration_seconds,
        "throughputContractsPerMinute": contracts_per_minute,
        "averageSecondsPerContract": avg_seconds_per_contract,
        "cooldownThreshold": args.cooldown_threshold,
        "cooldownWindowSeconds": args.cooldown_window_seconds,
        "cooldownSeconds": args.cooldown_seconds,
        "relaxAfterSeconds": args.relax_after_seconds,
        "relaxFactor": args.relax_factor,
        "finalAdaptiveRequestGapSeconds": rate_controller.current_interval(),
        "files": {
            "mainCsv": str(main_csv_path),
            "failedCsv": str(failed_csv_path) if failed_writer is not None else None,
            "summaryJson": str(summary_json_path),
            "logFile": str(log_path),
            "masterCsv": str(master_file_path) if master_file_path is not None else None,
        },
        "masterMerge": {
            "enabled": master_file_path is not None,
            "rowsBefore": master_existing_rows,
            "rowsAfter": master_final_rows,
        },
        "generatedAt": datetime.now().isoformat(timespec="seconds"),
    }

    with summary_json_path.open("w", encoding="utf-8") as handle:
        json.dump(summary_payload, handle, indent=2)

    logger.info("Saved summary JSON: %s", summary_json_path)

    if failed_writer is not None:
        sys.exit(1)


if __name__ == "__main__":
    main()