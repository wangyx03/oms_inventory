"""
oms_inventory.py — Query real-time inventory from the OMS and write it to a Feishu sheet

Usage:
    export OMS_APP_KEY="your AppKey"
    export OMS_APP_SECRET="your AppSecret"
    export FEISHU_APP_ID="cli_xxxxxxxx"
    export FEISHU_APP_SECRET="your Feishu app secret"

    # List all sheets in the spreadsheet and their real sheetId
    python oms_inventory.py --list-sheets --feishu-url "https://xcn3xthf3pue.feishu.cn/sheets/UnSRsCAfGhDWkitWOwvcb8o6nRc"

    # Full overwrite write into a sheet (default behavior): whatever is queried gets written as-is
    python oms_inventory.py --feishu-url "https://xcn3xthf3pue.feishu.cn/sheets/UnSRsCAfGhDWkitWOwvcb8o6nRc" --sheet-name "Sheet3"

    # Refresh every hour (or just set WATCH_INTERVAL_SECONDS=3600 in .env instead of passing this flag)
    python oms_inventory.py --feishu-url "..." --sheet-name "Sheet3" --watch 3600

    # Export to CSV only, don't write to Feishu (useful to sanity-check data locally first)
    python oms_inventory.py --csv snapshot.csv

    # Just print, don't write anywhere
    python oms_inventory.py

Logging:
    This script just prints to stdout/stderr; it does not write its own log file.
    When deployed as a systemd service, journald captures and retains this output
    for you. See the systemd notes below for how to view logs and set retention.

API docs source: https://apidoc-oms.xlwms.com/
  - Signing algorithm: /docs/开发验签工具.md
  - Inventory endpoint: /reference/post_v1-integratedinventory-pageopen.md
Feishu write API: https://open.feishu.cn/document/server-docs/docs/sheets-v3/data-operation/write-data-to-multiple-ranges
"""

import os
import re
import sys
import csv
import json
import time
import hmac
import hashlib
import argparse
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import requests

try:
    from dotenv import load_dotenv
    _dotenv_loaded = load_dotenv()
    if not _dotenv_loaded:
        print("Notice: no .env file found in the current directory (or it wasn't loaded); "
              "falling back to system environment variables only.", file=sys.stderr)
except ImportError:
    print("Notice: python-dotenv is not installed, so .env will not be auto-loaded "
          "(pip install python-dotenv).", file=sys.stderr)


def log(msg: str = "", err: bool = False) -> None:
    """Print to stdout/stderr. When run under systemd, journald captures and
    retains this output itself, so this script does not maintain its own log file."""
    print(msg, file=sys.stderr if err else sys.stdout)


OMS_API_BASE = "https://api.xlwms.com/openapi"
INVENTORY_PATH = "/v1/integratedInventory/pageOpen"

FEISHU_TOKEN_RE = re.compile(r"feishu\.cn/sheets/([A-Za-z0-9]+)")


class AuthError(RuntimeError):
    pass


# ----------------------------------------------------------------------
# OMS (华人行OMS / Lingxing WMS OpenAPI)
# ----------------------------------------------------------------------
class OmsClient:
    def __init__(self, app_key: str, app_secret: str, base_url: str = OMS_API_BASE):
        if not app_key or not app_secret:
            raise AuthError("Missing OMS_APP_KEY / OMS_APP_SECRET, please set these environment variables")
        self.app_key = app_key
        self.app_secret = app_secret
        self.base_url = base_url
        self.session = requests.Session()

    @staticmethod
    def _canonical_json(obj: Any) -> str:
        """Recursively sort keys in dictionary order (case-insensitive), compact output.
        Corresponds to step 1 of the signing algorithm."""
        def sort_key(item):
            return item[0].lower()

        def _sort(o):
            if isinstance(o, dict):
                return {k: _sort(v) for k, v in sorted(o.items(), key=sort_key)}
            if isinstance(o, list):
                return [_sort(v) for v in o]
            return o

        return json.dumps(_sort(obj), ensure_ascii=False, separators=(",", ":"))

    def _authcode(self, data: Dict[str, Any], req_time: str) -> str:
        """Concatenate appKey + sorted data JSON + reqTime in dictionary order of the
        parameter names, then HMAC-SHA256 the result."""
        data_json = self._canonical_json(data)
        plain = f"{self.app_key}{data_json}{req_time}"
        return hmac.new(
            self.app_secret.encode("utf-8"), plain.encode("utf-8"), hashlib.sha256
        ).hexdigest()

    def _post(self, path: str, data: Dict[str, Any]) -> Dict[str, Any]:
        req_time = str(int(time.time()))
        authcode = self._authcode(data, req_time)
        url = f"{self.base_url}{path}"
        payload = {"appKey": self.app_key, "reqTime": req_time, "data": data}
        resp = self.session.post(url, params={"authcode": authcode}, json=payload, timeout=30)
        resp.raise_for_status()
        result = resp.json()
        if result.get("code") != 200:
            raise RuntimeError(f"OMS API returned an error: {result}")
        return result["data"]

    def query_inventory(
        self,
        sku_list: Optional[List[str]] = None,
        wh_code_list: Optional[List[str]] = None,
        stock_type: Optional[int] = None,
        page_size: int = 100,
    ) -> List[Dict[str, Any]]:
        all_records: List[Dict[str, Any]] = []
        page = 1
        start_time = (datetime.now() - timedelta(days=3650)).strftime("%Y-%m-%d %H:%M:%S")
        end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        while True:
            data: Dict[str, Any] = {
                "page": page,
                "pageSize": page_size,
                "timeType": "operateTime",
                "startTime": start_time,
                "endTime": end_time,
            }
            if sku_list:
                data["skuList"] = ",".join(sku_list)
            if wh_code_list:
                data["whCodeList"] = ",".join(wh_code_list)
            if stock_type is not None:
                data["stockType"] = stock_type

            result = self._post(INVENTORY_PATH, data)
            records = result.get("records", []) or []
            all_records.extend(records)

            total = result.get("total", 0)
            if page * page_size >= total or not records:
                break
            page += 1

        return all_records


def flatten_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    prod = rec.get("productStockDtl") or {}
    stock_type_raw = rec.get("stockType")
    stock_type_label = {"0": "Good", "1": "Defective"}.get(str(stock_type_raw), str(stock_type_raw))
    return {
        "SKU": rec.get("sku", ""),
        "Product Name": rec.get("productName", ""),
        "Warehouse": rec.get("whCode", ""),
        "Stock Type": stock_type_label,
        "Total Stock (Dropship)": rec.get("productTotalAmount", 0),
        "Available Stock": prod.get("availableAmount", 0),
        "Locked Stock": prod.get("lockAmount", 0),
        "In-Transit Stock": prod.get("transportAmount", 0),
    }


# ----------------------------------------------------------------------
# Feishu Sheets
# ----------------------------------------------------------------------
def parse_feishu_token(url: str) -> str:
    """Extract the spreadsheet_token from a Feishu sheet URL (doesn't rely on the
    sheet= query parameter, which can be a stale cached value)."""
    m = FEISHU_TOKEN_RE.search(url)
    if not m:
        raise ValueError(
            "Could not parse a Feishu spreadsheet_token from the URL. "
            "Expected format: https://xxx.feishu.cn/sheets/<token>..."
        )
    return m.group(1)


class FeishuSheetClient:
    TOKEN_URL = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"

    def __init__(self, app_id: str, app_secret: str):
        if not app_id or not app_secret:
            raise AuthError("Missing FEISHU_APP_ID / FEISHU_APP_SECRET, please set these environment variables")
        self.app_id = app_id
        self.app_secret = app_secret
        self._token: Optional[str] = None
        self._token_expire_at: float = 0.0
        self.session = requests.Session()

    def _get_token(self) -> str:
        if self._token and time.time() < self._token_expire_at - 60:
            return self._token
        resp = self.session.post(
            self.TOKEN_URL,
            json={"app_id": self.app_id, "app_secret": self.app_secret},
            timeout=15,
        )
        resp.raise_for_status()
        result = resp.json()
        if result.get("code") != 0:
            raise RuntimeError(f"Failed to get Feishu tenant_access_token: {result}")
        self._token = result["tenant_access_token"]
        self._token_expire_at = time.time() + result.get("expire", 7200)
        return self._token

    @staticmethod
    def _col_letter(n: int) -> str:
        letters = ""
        while n > 0:
            n, rem = divmod(n - 1, 26)
            letters = chr(65 + rem) + letters
        return letters

    def list_sheets(self, spreadsheet_token: str) -> List[Dict[str, str]]:
        token = self._get_token()
        url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/metainfo"
        resp = self.session.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=15)
        resp.raise_for_status()
        result = resp.json()
        if result.get("code") != 0:
            raise RuntimeError(f"Failed to list sheets in the Feishu spreadsheet: {result}")
        sheets = result.get("data", {}).get("sheets", []) or []
        return [{"sheetId": s.get("sheetId", ""), "title": s.get("title", "")} for s in sheets]

    def resolve_sheet_id_by_name(self, spreadsheet_token: str, sheet_name: str) -> str:
        sheets = self.list_sheets(spreadsheet_token)
        for s in sheets:
            if s["title"] == sheet_name:
                return s["sheetId"]
        available = ", ".join(f"{s['title']}({s['sheetId']})" for s in sheets)
        raise RuntimeError(f"No sheet named \"{sheet_name}\" found. Sheets in this spreadsheet: {available}")

    def write_full(self, spreadsheet_token: str, sheet_id: str, rows: List[Dict[str, Any]]) -> None:
        """Full overwrite write: header row + all data rows."""
        if not rows:
            log("No data, skipping write to Feishu sheet.")
            return
        headers = list(rows[0].keys())
        values: List[List[Any]] = [headers]
        values.extend([r[h] for h in headers] for r in rows)
        values.append([])
        values.append([f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"])

        n_rows = len(values)
        n_cols = len(headers)
        range_str = f"{sheet_id}!A1:{self._col_letter(n_cols)}{n_rows}"

        token = self._get_token()
        url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values"
        resp = self.session.put(
            url,
            headers={"Authorization": f"Bearer {token}"},
            json={"valueRange": {"range": range_str, "values": values}},
            timeout=30,
        )
        resp.raise_for_status()
        result = resp.json()
        if result.get("code") != 0:
            raise RuntimeError(f"Failed to write to Feishu sheet: {result}")
        log(f"Wrote to Feishu sheet {spreadsheet_token} ({sheet_id}), range {range_str}")


# ----------------------------------------------------------------------
# Output / CLI
# ----------------------------------------------------------------------
def print_table(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        log("No inventory data found.")
        return
    headers = list(rows[0].keys())
    widths = [max(len(str(r[h])) for r in rows + [dict(zip(headers, headers))]) for h in headers]
    line = " | ".join(h.ljust(w) for h, w in zip(headers, widths))
    lines = [line, "-" * len(line)]
    for r in rows:
        lines.append(" | ".join(str(r[h]).ljust(w) for h, w in zip(headers, widths)))
    lines.append(f"\n{len(rows)} records total, generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log("\n".join(lines))


def write_csv(rows: List[Dict[str, Any]], path: str) -> None:
    if not rows:
        log("No data, skipping CSV export.")
        return
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    log(f"Exported to {path}")


def run_once(client: OmsClient, args) -> List[Dict[str, Any]]:
    sku_list = args.sku.split(",") if args.sku else None
    wh_list = args.warehouse.split(",") if args.warehouse else None
    records = client.query_inventory(
        sku_list=sku_list,
        wh_code_list=wh_list,
        stock_type=args.stock_type,
        page_size=100,
    )
    return [flatten_record(r) for r in records]


def main():
    parser = argparse.ArgumentParser(description="Query real-time OMS inventory and write it to a Feishu sheet")
    parser.add_argument("--sku", help="Comma-separated list of SKUs")
    parser.add_argument("--warehouse", help="Comma-separated warehouse codes, e.g. M60003")
    parser.add_argument("--stock-type", type=int, choices=[0, 1], default=None,
                         help="0=Good, 1=Defective; if omitted, both are included")
    parser.add_argument("--csv", help="Path to export a CSV file")
    parser.add_argument(
        "--feishu-url",
        default=os.environ.get("FEISHU_SHEET_URL"),
        help="Feishu sheet URL, used to extract the spreadsheet_token "
        "(falls back to the FEISHU_SHEET_URL environment variable if omitted)",
    )
    parser.add_argument(
        "--sheet-name",
        default=os.environ.get("FEISHU_SHEET_NAME"),
        help="Title of the sheet to write to, e.g. Sheet3 "
        "(falls back to the FEISHU_SHEET_NAME environment variable if omitted)",
    )
    parser.add_argument(
        "--list-sheets",
        action="store_true",
        help="Just list all sheets (titles and sheetIds) in the spreadsheet, then exit "
        "(useful for verifying names)",
    )
    parser.add_argument(
        "--watch",
        type=int,
        default=int(os.environ.get("WATCH_INTERVAL_SECONDS", "0") or "0"),
        metavar="SECONDS",
        help="Refresh every N seconds (falls back to the WATCH_INTERVAL_SECONDS "
        "environment variable if omitted; default 0 = run once)",
    )
    args = parser.parse_args()

    # --list-sheets only needs Feishu credentials, not OMS credentials
    if args.list_sheets:
        if not args.feishu_url:
            log("Error: --list-sheets requires --feishu-url to also be provided", err=True)
            sys.exit(1)
        token = parse_feishu_token(args.feishu_url)
        try:
            writer = FeishuSheetClient(
                os.environ.get("FEISHU_APP_ID", ""), os.environ.get("FEISHU_APP_SECRET", "")
            )
            sheets = writer.list_sheets(token)
        except (AuthError, RuntimeError) as e:
            log(f"Error: {e}", err=True)
            sys.exit(1)
        log(f"All sheets in spreadsheet {token}:")
        for s in sheets:
            log(f"  {s['title']}  ->  sheetId={s['sheetId']}")
        sys.exit(0)

    app_key = os.environ.get("OMS_APP_KEY", "")
    app_secret = os.environ.get("OMS_APP_SECRET", "")
    try:
        client = OmsClient(app_key, app_secret)
    except AuthError as e:
        log(f"Error: {e}", err=True)
        sys.exit(1)

    feishu_writer = None
    feishu_token = feishu_sheet_id = None
    if args.feishu_url:
        if not args.sheet_name:
            log("Error: --sheet-name must also be provided when --feishu-url is given", err=True)
            sys.exit(1)
        feishu_token = parse_feishu_token(args.feishu_url)
        try:
            feishu_writer = FeishuSheetClient(
                os.environ.get("FEISHU_APP_ID", ""), os.environ.get("FEISHU_APP_SECRET", "")
            )
            feishu_sheet_id = feishu_writer.resolve_sheet_id_by_name(feishu_token, args.sheet_name)
            log(f"Resolved sheet \"{args.sheet_name}\" -> sheetId={feishu_sheet_id}")
        except (AuthError, RuntimeError) as e:
            log(f"Error: {e}", err=True)
            sys.exit(1)
    else:
        log(
            "Notice: --feishu-url was not provided (and FEISHU_SHEET_URL is not set in .env); "
            "this run will only query and print/export CSV, without writing to Feishu.",
            err=True,
        )

    def sync(rows):
        if feishu_writer:
            try:
                feishu_writer.write_full(feishu_token, feishu_sheet_id, rows)
            except Exception as e:
                log(f"Error writing to Feishu sheet: {e}", err=True)

    if args.watch > 0:
        log(f"Entering watch mode, refreshing every {args.watch} seconds. Press Ctrl+C to stop.\n")
        try:
            while True:
                rows = run_once(client, args)
                log(f"\n=== Refreshed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")
                print_table(rows)
                if args.csv:
                    write_csv(rows, args.csv)
                sync(rows)
                time.sleep(args.watch)
        except KeyboardInterrupt:
            log("\nWatch mode stopped.")
    else:
        rows = run_once(client, args)
        print_table(rows)
        if args.csv:
            write_csv(rows, args.csv)
        sync(rows)


if __name__ == "__main__":
    main()