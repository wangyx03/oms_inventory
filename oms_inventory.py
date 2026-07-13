"""
oms_inventory.py — Query real-time inventory from the OMS and write it to a Feishu sheet

Usage:
    export OMS_APP_KEY="your AppKey"
    export OMS_APP_SECRET="your AppSecret"
    export FEISHU_APP_ID="cli_xxxxxxxx"
    export FEISHU_APP_SECRET="your Feishu app secret"

    # List all sheets in the spreadsheet and their real sheetId
    python oms_inventory.py --list-sheets --feishu-url "https://xcn3xthf3pue.feishu.cn/sheets/UnSRsCAfGhDWkitWOwvcb8o6nRc"

    # Full overwrite write into a sheet (default behavior): whatever is queried gets written as-is.
    # Before writing, any leftover rows from a previous (longer) run are deleted first
    # so stale/"zombie" rows don't survive a shrinking dataset.
    python oms_inventory.py --feishu-url "https://xcn3xthf3pue.feishu.cn/sheets/UnSRsCAfGhDWkitWOwvcb8o6nRc" --sheet-name "Sheet3"

    # Same, but by sheetId directly (recommended for long-term/server use — a renamed
    # tab won't break this the way --sheet-name would)
    python oms_inventory.py --feishu-url "https://xcn3xthf3pue.feishu.cn/sheets/UnSRsCAfGhDWkitWOwvcb8o6nRc" --sheet-id "vVDz1o"

    # Refresh every hour (or just set WATCH_INTERVAL_SECONDS=3600 in .env instead of passing this flag)
    python oms_inventory.py --feishu-url "..." --sheet-name "Sheet3" --watch 3600

    # Export to CSV only, don't write to Feishu (useful to sanity-check data locally first)
    python oms_inventory.py --csv snapshot.csv

    # Just print, don't write anywhere
    python oms_inventory.py

    # Dry-run the cleanup logic: probe the sheet's current row count and print
    # what WOULD be deleted, without actually deleting or writing anything.
    # Use this once after upgrading, to sanity-check the numbers before trusting it.
    python oms_inventory.py --feishu-url "..." --sheet-id "vVDz1o" --dry-run-cleanup

Logging:
    This script just prints to stdout/stderr; it does not write its own log file.
    When deployed as a systemd service, journald captures and retains this output
    for you. See the systemd notes below for how to view logs and set retention.

Zombie-row cleanup:
    write_full() used to size its write range only off the NEW data's row count.
    If a refresh returns fewer rows than the previous refresh (e.g. some SKUs
    dropped out, a warehouse emptied, filters changed), the old rows past the
    new range were left behind untouched — "zombie rows" from a prior run.

    This version fixes that by, before writing, probing how many rows the sheet
    is actually currently using (via a values GET over a generous range) and,
    if that is larger than what we're about to write, deleting the extra rows
    with the Feishu dimension_range DELETE API. Then the fresh data is written.

    A safety guard (MAX_AUTO_DELETE_ROWS) refuses to auto-delete an implausibly
    large number of rows in one go, in case the probe misfires — this fails loud
    instead of silently wiping the sheet.

API docs source: https://apidoc-oms.xlwms.com/
  - Signing algorithm: /docs/开发验签工具.md
  - Inventory endpoint: /reference/post_v1-integratedinventory-pageopen.md
Feishu write API: https://open.feishu.cn/document/server-docs/docs/sheets-v3/data-operation/write-data-to-multiple-ranges
Feishu dimension_range (row delete) API:
  https://open.feishu.cn/document/server-docs/docs/sheets-v3/sheet-rowcol/dimension_range-1
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

# Safety guard: if the zombie-row cleanup logic ever thinks it needs to delete
# more rows than this in one go, it aborts the delete (and the write) instead
# of trusting a possibly-wrong probe. Bump this if you genuinely expect huge
# swings in row count between runs.
MAX_AUTO_DELETE_ROWS = 5000

# How far down we probe when checking "how many rows does this sheet actually
# use right now". Should comfortably exceed the largest this sheet will ever get.
PROBE_ROW_LIMIT = 10000


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

    def get_used_row_count(self, spreadsheet_token: str, sheet_id: str, max_probe_col: str) -> int:
        """Probe how many rows this sheet is CURRENTLY using (1-based count of the
        last row that has any non-empty cell), by reading a generously-sized range.

        This is what lets us detect "zombie rows" left behind by a previous run
        that wrote more rows than the current run is about to write.
        """
        token = self._get_token()
        probe_range = f"{sheet_id}!A1:{max_probe_col}{PROBE_ROW_LIMIT}"
        url = (f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/"
               f"{spreadsheet_token}/values/{probe_range}")
        resp = self.session.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=30)
        resp.raise_for_status()
        result = resp.json()
        if result.get("code") != 0:
            raise RuntimeError(f"Failed to read current row usage from Feishu sheet: {result}")
        values = result.get("data", {}).get("valueRange", {}).get("values", []) or []
        last_non_empty = 0
        for i, row in enumerate(values, start=1):
            if any(cell not in (None, "", []) for cell in (row or [])):
                last_non_empty = i
        return last_non_empty

    def delete_rows(self, spreadsheet_token: str, sheet_id: str, start_index: int, end_index: int) -> None:
        """Delete rows in the half-open interval [start_index, end_index), 0-based.
        No-op if end_index <= start_index."""
        if end_index <= start_index:
            return
        token = self._get_token()
        url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/dimension_range"
        body = {
            "dimension": {
                "sheetId": sheet_id,
                "majorDimension": "ROWS",
                "startIndex": start_index,
                "endIndex": end_index,
            }
        }
        resp = self.session.delete(
            url,
            headers={"Authorization": f"Bearer {token}"},
            json=body,
            timeout=30,
        )
        resp.raise_for_status()
        result = resp.json()
        if result.get("code") != 0:
            raise RuntimeError(f"Failed to delete leftover rows: {result}")
        log(f"Deleted rows {start_index + 1}-{end_index} (1-based) from sheet {sheet_id}")

    def cleanup_zombie_rows(
        self, spreadsheet_token: str, sheet_id: str, new_n_rows: int, n_cols: int,
        dry_run: bool = False,
    ) -> None:
        """If the sheet currently uses more rows than we're about to write, delete
        the extra trailing rows first. `new_n_rows` is the 1-based row count of the
        data we're about to write (including header/footer/timestamp rows)."""
        try:
            prev_n_rows = self.get_used_row_count(spreadsheet_token, sheet_id, self._col_letter(n_cols))
        except Exception as e:
            log(f"Warning: could not probe previous row count, skipping zombie-row "
                f"cleanup this run ({e})", err=True)
            return

        extra = prev_n_rows - new_n_rows
        if extra <= 0:
            log(f"No leftover rows to clean up (previous used rows: {prev_n_rows}, "
                f"new data rows: {new_n_rows}).")
            return

        if extra > MAX_AUTO_DELETE_ROWS:
            raise RuntimeError(
                f"Refusing to auto-delete {extra} rows (previous={prev_n_rows}, "
                f"new={new_n_rows}) — this exceeds MAX_AUTO_DELETE_ROWS="
                f"{MAX_AUTO_DELETE_ROWS} and looks like a probe error rather than "
                f"a real shrink in data. Investigate before raising the limit."
            )

        # new_n_rows is 1-based count of rows we're keeping (rows 1..new_n_rows).
        # 0-based delete range is therefore [new_n_rows, prev_n_rows).
        if dry_run:
            log(f"[dry-run] Would delete rows {new_n_rows + 1}-{prev_n_rows} (1-based) "
                f"from sheet {sheet_id} (previous used rows: {prev_n_rows}, "
                f"new data rows: {new_n_rows}). No changes made.")
            return

        self.delete_rows(spreadsheet_token, sheet_id, new_n_rows, prev_n_rows)

    def write_full(self, spreadsheet_token: str, sheet_id: str, rows: List[Dict[str, Any]]) -> None:
        """Full overwrite write: header row + all data rows.

        Before writing, checks whether the sheet currently has more rows in use
        than this write will cover, and if so, deletes the leftover ("zombie")
        rows first so a shrinking dataset doesn't leave stale data behind.
        """
        if not rows:
            log("No data, skipping write to Feishu sheet.")
            return
        headers = list(rows[0].keys())
        values: List[List[Any]] = [headers]
        values.extend([r[h] for h in headers] for r in rows)
        values.append([])
        values.append([f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"])

        n_cols = len(headers)
        n_rows = len(values)  # 1-based

        # Delete any leftover rows from a previous, longer run BEFORE writing new data.
        self.cleanup_zombie_rows(spreadsheet_token, sheet_id, n_rows, n_cols, dry_run=False)

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
        "(falls back to the FEISHU_SHEET_NAME environment variable if omitted). "
        "Ignored if --sheet-id is provided.",
    )
    parser.add_argument(
        "--sheet-id",
        default=os.environ.get("FEISHU_SHEET_ID"),
        help="Real sheetId to write to directly, e.g. vVDz1o "
        "(falls back to the FEISHU_SHEET_ID environment variable if omitted). "
        "Takes priority over --sheet-name/FEISHU_SHEET_NAME and skips the name lookup, "
        "so renaming a tab in Feishu won't break this.",
    )
    parser.add_argument(
        "--list-sheets",
        action="store_true",
        help="Just list all sheets (titles and sheetIds) in the spreadsheet, then exit "
        "(useful for verifying names)",
    )
    parser.add_argument(
        "--dry-run-cleanup",
        action="store_true",
        help="Query inventory and print what the zombie-row cleanup WOULD delete, "
        "without deleting anything or writing to Feishu. Use this once after "
        "upgrading to sanity-check the row-count probing before trusting it live.",
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
        if not args.sheet_id and not args.sheet_name:
            log("Error: either --sheet-id or --sheet-name must be provided when --feishu-url is given", err=True)
            sys.exit(1)
        feishu_token = parse_feishu_token(args.feishu_url)
        try:
            feishu_writer = FeishuSheetClient(
                os.environ.get("FEISHU_APP_ID", ""), os.environ.get("FEISHU_APP_SECRET", "")
            )
            if args.sheet_id:
                feishu_sheet_id = args.sheet_id
                log(f"Using sheetId directly: {feishu_sheet_id}")
            else:
                feishu_sheet_id = feishu_writer.resolve_sheet_id_by_name(feishu_token, args.sheet_name)
                log(f"Resolved sheet \"{args.sheet_name}\" -> sheetId={feishu_sheet_id}")
        except (AuthError, RuntimeError) as e:
            log(f"Error: {e}", err=True)
            sys.exit(1)
    elif not args.dry_run_cleanup:
        log(
            "Notice: --feishu-url was not provided (and FEISHU_SHEET_URL is not set in .env); "
            "this run will only query and print/export CSV, without writing to Feishu.",
            err=True,
        )

    if args.dry_run_cleanup:
        if not feishu_writer:
            log("Error: --dry-run-cleanup requires --feishu-url and --sheet-id/--sheet-name", err=True)
            sys.exit(1)
        rows = run_once(client, args)
        if not rows:
            log("No data returned from OMS, nothing to simulate.")
            sys.exit(0)
        headers = list(rows[0].keys())
        # Same row accounting as write_full: header + data rows + blank + timestamp row.
        n_rows = 1 + len(rows) + 1 + 1
        n_cols = len(headers)
        try:
            feishu_writer.cleanup_zombie_rows(feishu_token, feishu_sheet_id, n_rows, n_cols, dry_run=True)
        except Exception as e:
            log(f"Error during dry-run cleanup check: {e}", err=True)
            sys.exit(1)
        sys.exit(0)

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