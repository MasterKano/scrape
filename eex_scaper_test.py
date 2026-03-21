from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import requests

FILE_NAME_PREFIX = "eex_power_futures_strip"
API_BASE_URL = "https://api.eex-group.com/pub/market-data/table-data"
ACCEPT_HEADER = "application/json, text/javascript, */*; q=0.01"

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

AREAS = {
    "FI":     {"codes": {"Month": "FNBM", "Quarter": "FNBQ", "Year": "FNBY"}, "assumed_types": []},
    "SE1":    {"codes": {"Month": "1SBM", "Quarter": "1SBQ", "Year": "1SBY"}, "assumed_types": []},
    "SE2":    {"codes": {"Month": "2SBM", "Quarter": "2SBQ", "Year": "2SBY"}, "assumed_types": []},
    "SE3":    {"codes": {"Month": "3SBM", "Quarter": "3SBQ", "Year": "3SBY"}, "assumed_types": []},
    "SE4":    {"codes": {"Month": "4SBM", "Quarter": "4SBQ", "Year": "4SBY"}, "assumed_types": []},
    "DE":     {"codes": {"Month": "DEBM", "Quarter": "DEBQ", "Year": "DEBY"}, "assumed_types": []},
    "GB":     {"codes": {"Month": "FUBM", "Quarter": "FUBQ", "Year": "FUBY"}, "assumed_types": []},
    "ES":     {"codes": {"Month": "FEBM", "Quarter": "FEBQ", "Year": "FEBY"}, "assumed_types": []},
    "IT":     {"codes": {"Month": "FDBM", "Quarter": "FDBQ", "Year": "FDBY"}, "assumed_types": []},
    "NL":     {"codes": {"Month": "Q0BM", "Quarter": "Q0BQ", "Year": "Q0BY"}, "assumed_types": []},
    "Nordic": {"codes": {"Month": "FBBM", "Quarter": "FBBQ", "Year": "FBBY"}, "assumed_types": []},
    "FR":     {"codes": {"Month": "F7BM", "Quarter": "F7BQ", "Year": "F7BY"}, "assumed_types": []},
    "NO1":    {"codes": {"Month": "1NBM", "Quarter": "1NBQ", "Year": "1NBY"}, "assumed_types": []},
    "NO2":    {"codes": {"Month": "2NBM", "Quarter": "2NBQ", "Year": "2NBY"}, "assumed_types": ["Month", "Quarter", "Year"]},
    "NO3":    {"codes": {"Month": "3NBM", "Quarter": "3NBQ", "Year": "3NBY"}, "assumed_types": ["Month", "Quarter", "Year"]},
    "NO4":    {"codes": {"Month": "4NBM", "Quarter": "4NBQ", "Year": "4NBY"}, "assumed_types": ["Month", "Quarter", "Year"]},
    "NO5":    {"codes": {"Month": "5NBM", "Quarter": "5NBQ", "Year": "5NBY"}, "assumed_types": ["Month", "Quarter"]},
}

MONTH_NAMES = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
SORT_ORDER = {"Month": 1, "Quarter": 2, "Year": 3}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download EEX power futures data and save CSV outputs."
    )
    parser.add_argument(
        "--trade-date",
        help="Trade date in YYYY-MM-DD format. Required unless using --rerun-failed-from.",
    )
    parser.add_argument(
        "--areas",
        help="Comma-separated areas, e.g. DE,GB,FR. Default is all defined areas.",
    )
    parser.add_argument(
        "--max-contracts",
        type=int,
        default=None,
        help="Optional cap for testing.",
    )
    parser.add_argument(
        "--output-dir",
        default="Output",
        help="Folder for output files. Default: Output",
    )
    parser.add_argument(
        "--rerun-failed-from",
        help="Path to a previous full-results CSV or failed-results CSV. Re-runs only non-ok rows.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=7,
        help="Number of days to look back for rows. Default: 7",
    )
    parser.add_argument(
        "--request-gap-seconds",
        type=float,
        default=1.5,
        help="Delay between requests. Default: 1.5",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=1,
        help="Retry count for request failures. Default: 1",
    )
    parser.add_argument(
        "--retry-delay-seconds",
        type=float,
        default=3.0,
        help="Base retry delay. Default: 3.0",
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
        help="Disable fallback to the latest available row before the trade date.",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=1,
        help="Number of parallel workers. Default: 1",
    )
    return parser.parse_args()


def parse_trade_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError("Bad trade date format. Use YYYY-MM-DD.") from exc


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


def build_result(contract: dict[str, Any], trade_date_str: str, **overrides: Any) -> dict[str, Any]:
    result = {
        "area": contract["area"],
        "maturityType": contract["maturityType"],
        "delivery": contract["delivery"],
        "requestedTradeDate": trade_date_str,
        "tradeDate": None,
        "status": "unknown",
        "settlementPrice": None,
        "volume": None,
        "grossOpenInterest": None,
        "shortCode": contract["shortCode"],
        "maturity": contract["maturity"],
        "assumedCode": contract["assumedCode"],
        "httpStatus": None,
        "errorMessage": None,
    }
    result.update(overrides)
    return result


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


def build_contracts(
    trade_date: date,
    area_filter: list[str] | None,
    max_contracts: int | None,
) -> list[dict[str, Any]]:
    contracts: list[dict[str, Any]] = []

    target_year = trade_date.year
    target_month = trade_date.month
    current_quarter_start_month = ((target_month - 1) // 3) * 3 + 1

    for area, area_cfg in AREAS.items():
        if area_filter and area not in area_filter:
            continue

        codes = area_cfg["codes"]
        assumed_types = set(area_cfg["assumed_types"])

        if codes.get("Month"):
            for i in range(7):
                year, month = add_months(target_year, target_month, i)
                contracts.append({
                    "area": area,
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
                    "area": area,
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
                    "area": area,
                    "shortCode": codes["Year"],
                    "maturityType": "Year",
                    "maturity": f"{year}01",
                    "delivery": f"Cal-{str(year)[-2:]}",
                    "assumedCode": "Year" in assumed_types,
                })

    if max_contracts is not None:
        contracts = contracts[:max_contracts]

    return contracts


def build_request_params(contract: dict[str, Any], start_date_str: str, end_date_str: str) -> dict[str, str]:
    return {
        "shortCode": contract["shortCode"],
        "commodity": "POWER",
        "pricing": "F",
        "area": contract["area"],
        "product": "Base",
        "maturity": contract["maturity"],
        "startDate": start_date_str,
        "endDate": end_date_str,
        "maturityType": contract["maturityType"],
        "isRolling": "true",
    }


def fetch_one(
    logger: logging.Logger,
    session: requests.Session,
    contract: dict[str, Any],
    trade_date_str: str,
    start_date_str: str,
    end_date_str: str,
    request_gap_seconds: float,
    max_retries: int,
    retry_delay_seconds: float,
    request_timeout_seconds: float,
    allow_fallback: bool,
) -> dict[str, Any]:
    params = build_request_params(contract, start_date_str, end_date_str)

    for attempt in range(max_retries + 1):
        if request_gap_seconds > 0:
            time.sleep(request_gap_seconds)

        try:
            response = session.get(
                API_BASE_URL,
                params=params,
                timeout=request_timeout_seconds,
            )

            if response.status_code == 429 and attempt < max_retries:
                retry_after = response.headers.get("Retry-After")
                wait_seconds = retry_delay_seconds * (2 ** attempt)

                if retry_after:
                    try:
                        wait_seconds = max(wait_seconds, float(retry_after))
                    except ValueError:
                        pass

                logger.warning(
                    "Rate limited for %s %s %s. Retrying in %.1fs.",
                    contract["area"],
                    contract["maturityType"],
                    contract["delivery"],
                    wait_seconds,
                )
                time.sleep(wait_seconds)
                continue

            if not response.ok:
                return build_result(
                    contract,
                    trade_date_str,
                    status="http_error",
                    httpStatus=response.status_code,
                    errorMessage=f"HTTP {response.status_code}",
                )

            try:
                payload = response.json()
            except ValueError as exc:
                return build_result(
                    contract,
                    trade_date_str,
                    status="json_error",
                    errorMessage=f"JSON parse failed: {exc}",
                )

            headers_row = payload.get("header", [])
            data_rows = payload.get("data", [])

            objects: list[dict[str, Any]] = []
            for row in data_rows:
                if isinstance(row, list):
                    objects.append(dict(zip(headers_row, row)))
                elif isinstance(row, dict):
                    objects.append(row)

            match, status = select_best_row(objects, trade_date_str, allow_fallback)

            if not match:
                return build_result(
                    contract,
                    trade_date_str,
                    status="no_row",
                    errorMessage=f"No row found for {trade_date_str}",
                )

            return build_result(
                contract,
                trade_date_str,
                tradeDate=match.get("tradeDate"),
                status=status,
                settlementPrice=normalize_number(first_defined(match, ["settlPx", "settlementPrice", "settlPrice"])),
                volume=normalize_number(first_defined(match, ["totVolTrd", "volume", "totalVolume"])),
                grossOpenInterest=normalize_number(first_defined(match, ["grossOpenInt", "grossOpenInterest"])),
            )

        except requests.RequestException as exc:
            if attempt < max_retries:
                wait_seconds = retry_delay_seconds * (2 ** attempt)
                logger.warning(
                    "Request error for %s %s %s: %s. Retrying in %.1fs.",
                    contract["area"],
                    contract["maturityType"],
                    contract["delivery"],
                    exc,
                    wait_seconds,
                )
                time.sleep(wait_seconds)
                continue

            return build_result(
                contract,
                trade_date_str,
                status="fetch_error",
                errorMessage=str(exc),
            )

    return build_result(
        contract,
        trade_date_str,
        status="fetch_error",
        errorMessage="Unexpected retry loop exit",
    )


def write_csv(rows: list[dict[str, Any]], file_path: Path) -> None:
    fieldnames = [
        "area",
        "maturityType",
        "delivery",
        "requestedTradeDate",
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

    with file_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def load_contracts_from_previous_csv(csv_path: Path) -> tuple[list[dict[str, Any]], str | None]:
    contracts: list[dict[str, Any]] = []
    inferred_trade_date: str | None = None

    with csv_path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)

    for row in rows:
        status = (row.get("status") or "").strip()
        should_include = status != "ok" if status else True
        if not should_include:
            continue

        requested_trade_date = (row.get("requestedTradeDate") or "").strip()
        if requested_trade_date and inferred_trade_date is None:
            inferred_trade_date = requested_trade_date

        contracts.append({
            "area": row["area"],
            "shortCode": row["shortCode"],
            "maturityType": row["maturityType"],
            "maturity": row["maturity"],
            "delivery": row["delivery"],
            "assumedCode": parse_bool(row.get("assumedCode")),
        })

    return contracts, inferred_trade_date


def setup_logging(log_path: Path) -> logging.Logger:
    logger = logging.getLogger("eex_scraper")
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


def process_contract(
    logger: logging.Logger,
    contract: dict[str, Any],
    trade_date_str: str,
    start_date_str: str,
    end_date_str: str,
    request_gap_seconds: float,
    max_retries: int,
    retry_delay_seconds: float,
    request_timeout_seconds: float,
    allow_fallback: bool,
) -> dict[str, Any]:
    with requests.Session() as session:
        session.headers.update(REQUEST_HEADERS)
        return fetch_one(
            logger=logger,
            session=session,
            contract=contract,
            trade_date_str=trade_date_str,
            start_date_str=start_date_str,
            end_date_str=end_date_str,
            request_gap_seconds=request_gap_seconds,
            max_retries=max_retries,
            retry_delay_seconds=retry_delay_seconds,
            request_timeout_seconds=request_timeout_seconds,
            allow_fallback=allow_fallback,
        )


def main() -> None:
    args = parse_args()

    script_dir = Path(__file__).resolve().parent
    output_dir = Path(args.output_dir)
    if not output_dir.is_absolute():
        output_dir = script_dir / output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    rerun_contracts: list[dict[str, Any]] | None = None
    inferred_trade_date: str | None = None

    if args.rerun_failed_from:
        rerun_csv_path = Path(args.rerun_failed_from)
        if not rerun_csv_path.is_absolute():
            rerun_csv_path = script_dir / rerun_csv_path
        if not rerun_csv_path.exists():
            raise FileNotFoundError(f"Rerun file not found: {rerun_csv_path}")

        rerun_contracts, inferred_trade_date = load_contracts_from_previous_csv(rerun_csv_path)
        if not rerun_contracts:
            raise ValueError("No failed rows found in the rerun file.")

    trade_date_str = args.trade_date or inferred_trade_date
    if not trade_date_str:
        raise ValueError("Provide --trade-date, or use --rerun-failed-from with a CSV that contains requestedTradeDate.")

    requested_trade_date = parse_trade_date(trade_date_str)
    start_date = requested_trade_date - timedelta(days=args.lookback_days)
    start_date_str = fmt_date(start_date)
    end_date_str = fmt_date(requested_trade_date)

    timestamp_suffix = datetime.now().strftime("%H%M")

    all_csv_path = output_dir / f"{FILE_NAME_PREFIX}_{trade_date_str}_{timestamp_suffix}.csv"
    failed_csv_path = output_dir / f"{FILE_NAME_PREFIX}_failed_{trade_date_str}_{timestamp_suffix}.csv"
    summary_json_path = output_dir / f"{FILE_NAME_PREFIX}_summary_{trade_date_str}_{timestamp_suffix}.json"
    log_path = output_dir / f"{FILE_NAME_PREFIX}_log_{trade_date_str}_{timestamp_suffix}.log"

    logger = setup_logging(log_path)

    area_filter = parse_area_filter(args.areas)

    if rerun_contracts is not None:
        contracts = rerun_contracts
        if args.max_contracts is not None:
            contracts = contracts[:args.max_contracts]
        run_mode = "rerun_failed"
    else:
        contracts = build_contracts(
            trade_date=requested_trade_date,
            area_filter=area_filter,
            max_contracts=args.max_contracts,
        )
        run_mode = "full"

    logger.info("Run mode: %s", run_mode)
    logger.info("Built %s contracts.", len(contracts))
    logger.info("Date window: %s -> %s", start_date_str, end_date_str)

    results: list[dict[str, Any]] = []
    start_ts = time.time()
    future_to_meta: dict[Any, tuple[int, dict[str, Any]]] = {}

    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        for index, contract in enumerate(contracts, start=1):
            logger.info(
                "[queued %s/%s] %s %s %s (%s, %s)",
                index,
                len(contracts),
                contract["area"],
                contract["maturityType"],
                contract["delivery"],
                contract["shortCode"],
                contract["maturity"],
            )

            future = executor.submit(
                process_contract,
                logger,
                contract,
                trade_date_str,
                start_date_str,
                end_date_str,
                args.request_gap_seconds,
                args.max_retries,
                args.retry_delay_seconds,
                args.request_timeout_seconds,
                not args.no_fallback,
            )
            future_to_meta[future] = (index, contract)

        for future in as_completed(future_to_meta):
            index, contract = future_to_meta[future]
            try:
                result = future.result()
            except Exception as exc:
                logger.exception(
                    "Unhandled error for [%s/%s] %s %s %s (%s, %s): %s",
                    index,
                    len(contracts),
                    contract["area"],
                    contract["maturityType"],
                    contract["delivery"],
                    contract["shortCode"],
                    contract["maturity"],
                    exc,
                )
                result = build_result(
                    contract,
                    trade_date_str,
                    status="fetch_error",
                    errorMessage=f"Unhandled worker error: {exc}",
                )

            results.append(result)
            logger.info(
                "[done %s/%s] %s %s %s -> %s",
                index,
                len(contracts),
                contract["area"],
                contract["maturityType"],
                contract["delivery"],
                result["status"],
            )

    duration_seconds = round(time.time() - start_ts, 2)

    results.sort(
        key=lambda row: (
            row["area"],
            SORT_ORDER[row["maturityType"]],
            row["maturity"],
        )
    )

    write_csv(results, all_csv_path)

    failed_rows = [row for row in results if row["status"] != "ok"]
    if failed_rows:
        write_csv(failed_rows, failed_csv_path)

    summary = Counter(row["status"] for row in results)
    unique_areas = sorted({row["area"] for row in results})

    logger.info("Status summary:")
    for status, count in sorted(summary.items()):
        logger.info("  %s: %s", status, count)

    logger.info("Saved main CSV: %s", all_csv_path)
    if failed_rows:
        logger.info("Saved failed CSV: %s", failed_csv_path)
    else:
        logger.info("No failed rows. Failed CSV not created.")

    summary_payload = {
        "runMode": run_mode,
        "tradeDate": trade_date_str,
        "lookbackDays": args.lookback_days,
        "startDate": start_date_str,
        "endDate": end_date_str,
        "requestGapSeconds": args.request_gap_seconds,
        "maxRetries": args.max_retries,
        "retryDelaySeconds": args.retry_delay_seconds,
        "requestTimeoutSeconds": args.request_timeout_seconds,
        "allowPreviousTradeDayFallback": not args.no_fallback,
        "requestedAreas": area_filter if area_filter is not None else "ALL",
        "areasInRun": unique_areas,
        "totalContracts": len(results),
        "failedContracts": len(failed_rows),
        "statusSummary": dict(summary),
        "durationSeconds": duration_seconds,
        "files": {
            "mainCsv": str(all_csv_path),
            "failedCsv": str(failed_csv_path) if failed_rows else None,
            "summaryJson": str(summary_json_path),
            "logFile": str(log_path),
        },
        "generatedAt": datetime.now().isoformat(timespec="seconds"),
    }

    with summary_json_path.open("w", encoding="utf-8") as handle:
        json.dump(summary_payload, handle, indent=2)

    logger.info("Saved summary JSON: %s", summary_json_path)

    if failed_rows:
        sys.exit(1)


if __name__ == "__main__":
    main()
