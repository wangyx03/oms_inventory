"""
oms_inventory.py — Query real-time inventory (+ true inbound in-transit) from the
OMS and write it to a Feishu sheet

=============================================================================
WHY THIS VERSION IS DIFFERENT (read this if you're used to the old script)
=============================================================================
The OMS "综合库存" query (/v1/integratedInventory/pageOpen) has an important,
easy-to-miss behavior documented by Lingxing themselves:

    "节点起始时间-节点截止时间内，如无库存流水变动，则不会返回该库存的数据"
    (If there's no inventory-flow / movement within the queried time window,
    that stock record is simply NOT returned at all.)

This means: a SKU that has 0 on-hand stock but IS sitting "在途" (in transit /
pending receipt on an open inbound order) will not show up in that endpoint at
ALL — not as a zero row, but completely absent — because no goods-movement has
happened yet (the shipment hasn't been received). The old version of this
script drove everything off that one endpoint, so those SKUs silently
vanished from the export even though the OMS website clearly shows them with
an "在途库存" (in-transit) number.

This version fixes that by driving the SKU list from a different, warehouse-
and-movement-agnostic source — the OMS **product catalog**
(/v1/product/pagelist) — and then, for each SKU, separately looking up:
  1. on-hand stock status (same pageOpen endpoint as before), and
  2. TRUE inbound in-transit quantity, computed from open inbound orders
     (/v1/inboundOrder/pageList, status = 待入库/收货中) + their per-SKU
     packing details (/v1/inboundOrder/pageBoxSkuList), as
     quantity预报 - receivedQuantity已收货, summed per SKU/warehouse.

Any SKU that has neither on-hand stock nor open in-transit still gets one row
(with everything at 0) so the SKU column always matches the OMS product
catalog — nothing quietly disappears anymore.

=============================================================================
Usage:
    export OMS_APP_KEY="your AppKey"
    export OMS_APP_SECRET="your AppSecret"
    export FEISHU_APP_ID="cli_xxxxxxxx"
    export FEISHU_APP_SECRET="your Feishu app secret"

    # List all sheets in the spreadsheet and their real sheetId
    python oms_inventory.py --list-sheets --feishu-url "https://xcn3xthf3pue.feishu.cn/sheets/UnSRsCAfGhDWkitWOwvcb8o6nRc"

    # Full overwrite write into a sheet (default behavior): whatever is queried gets written as-is.
    python oms_inventory.py --feishu-url "https://xcn3xthf3pue.feishu.cn/sheets/UnSRsCAfGhDWkitWOwvcb8o6nRc" --sheet-id "vVDz1o"

    # Refresh every hour (or set WATCH_INTERVAL_SECONDS=3600 in .env instead)
    python oms_inventory.py --feishu-url "..." --sheet-id "vVDz1o" --watch 3600

    # Export to CSV only, don't write to Feishu (useful to sanity-check data locally first)
    python oms_inventory.py --csv snapshot.csv

    # Just print, don't write anywhere
    python oms_inventory.py

    # Skip the (slower, extra API-call-heavy) inbound in-transit computation,
    # e.g. if you just want the old-style stock-only snapshot quickly
    python oms_inventory.py --csv snapshot.csv --skip-transit

    # Dry-run the zombie-row cleanup logic (Feishu write path only)
    python oms_inventory.py --feishu-url "..." --sheet-id "vVDz1o" --dry-run-cleanup

Logging:
    This script just prints to stdout/stderr; it does not write its own log file.
    When deployed as a systemd service, journald captures and retains this output.

API docs source: https://apidoc-oms.xlwms.com/
  - Signing algorithm: /docs/开发验签工具.md
  - Inventory endpoint: /reference/post_v1-integratedinventory-pageopen.md
  - Product catalog endpoint: /reference/getproductforpageusingpost_1.md
  - Inbound order list endpoint: /reference/getorderpageusingpost.md
  - Inbound order packing-detail endpoint: /reference/pageboxskulistusingpost.md
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
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import requests

# Server runs in UTC; timestamps shown to the user (Eastern Michigan) should be
# in Eastern time instead, so "Last updated" / "generated at" reflect local time.
EASTERN_TZ = ZoneInfo("America/Detroit")


def now_eastern() -> datetime:
    return datetime.now(EASTERN_TZ)

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
PRODUCT_LIST_PATH = "/v1/product/pagelist"
INBOUND_ORDER_LIST_PATH = "/v1/inboundOrder/pageList"
INBOUND_BOX_SKU_LIST_PATH = "/v1/inboundOrder/pageBoxSkuList"

# Inbound order statuses that mean "not fully received yet" — i.e. still
# contributing to in-transit quantity. (0-新建 1-待入库 2-收货中 3-已收货
# 4-已上架 5-已取消 6-待审核 7-驳回)
OPEN_INBOUND_STATUSES = (1, 2)

FEISHU_TOKEN_RE = re.compile(r"feishu\.cn/sheets/([A-Za-z0-9]+)")


def _monthly_windows(start_dt: datetime, end_dt: datetime, max_span_days: int = 30):
    """Yield (window_start, window_end) datetime pairs covering [start_dt, end_dt],
    each spanning at most `max_span_days`. The inbound-order-list API's
    start/end-time filter appears to be designed for ~1-month windows at a
    time (larger spans, or an unbounded query, trip its internal
    "查询超过最大条数限制" cap) — so callers that need a longer lookback must
    chunk their queries into windows like this and merge the results.
    """
    cur = start_dt
    step = timedelta(days=max_span_days)
    while cur < end_dt:
        window_end = min(cur + step, end_dt)
        yield cur, window_end
        cur = window_end

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
            raise RuntimeError(f"OMS API returned an error ({path}): {result}")
        return result["data"]

    # ------------------------------------------------------------------
    # On-hand inventory (existing behavior, unchanged)
    # ------------------------------------------------------------------
    def query_inventory(
        self,
        sku_list: Optional[List[str]] = None,
        wh_code_list: Optional[List[str]] = None,
        stock_type: Optional[int] = None,
        page_size: int = 100,
    ) -> List[Dict[str, Any]]:
        """Query on-hand stock via /v1/integratedInventory/pageOpen.

        NOTE: per Lingxing's own docs, a SKU with NO inventory-flow movement in
        the queried time window will not be returned at all — this is why we
        no longer treat "absent from this call" as "zero stock" (see
        build_combined_rows / query_product_catalog for how that's handled).
        """
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

    # ------------------------------------------------------------------
    # Product catalog — the new SKU "source of truth" for the export
    # ------------------------------------------------------------------
    def query_product_catalog(
        self,
        sku_list: Optional[List[str]] = None,
        page_size: int = 100,
    ) -> List[Dict[str, Any]]:
        """Query the full product catalog via /v1/product/pagelist.

        This endpoint is NOT warehouse- or stock-flow-scoped — it simply
        returns every product SKU that exists in OMS, regardless of whether
        it currently has any stock or movement. This is what we use to decide
        which SKUs should appear in the export at all.
        """
        all_records: List[Dict[str, Any]] = []
        page = 1
        while True:
            data: Dict[str, Any] = {"page": page, "pageSize": page_size}
            if sku_list:
                data["skuList"] = sku_list
            result = self._post(PRODUCT_LIST_PATH, data)
            records = result.get("records", []) or []
            all_records.extend(records)

            total = result.get("total", 0)
            if page * page_size >= total or not records:
                break
            page += 1
        return all_records

    # ------------------------------------------------------------------
    # True inbound in-transit — computed from open inbound orders
    # ------------------------------------------------------------------
    def query_open_inbound_orders(
        self, page_size: int = 50, lookback_days: int = 60
    ) -> List[Dict[str, Any]]:
        """List inbound orders that are still open (待入库/收货中 — i.e. not
        yet fully received), across all statuses in OPEN_INBOUND_STATUSES.

        The list endpoint only accepts a single `status` value per call, so we
        call it once per status and merge results.

        IMPORTANT — two separate limits on the startTime/endTime filter:
        1. Omitting them entirely makes the server try to scan the ENTIRE
           order history at once, which trips its own internal cap and
           returns `{"code": 10002, "msg": "查询超过最大条数限制"}` (query
           exceeds max row limit) — even on page 1.
        2. The startTime/endTime window itself appears to be designed for
           ~1-month spans at a time — passing a full `lookback_days` (e.g.
           365) worth of range in one call trips the SAME error, just for a
           different reason (span too wide, not "unbounded").

        So we chunk the full lookback window into ~30-day slices via
        `_monthly_windows()`, query each slice separately (per status), and
        merge + dedupe by inboundOrderNo (a single open order could in
        principle span a chunk boundary depending on how "created" is
        indexed, though in practice each order only has one creation date).

        TIMEZONE BUFFER: `datetime.now()` here is whatever local time the
        script happens to run in, but OMS likely timestamps "入库单创建时间"
        in China Standard Time (UTC+8). If this script runs somewhere behind
        that (e.g. US Eastern, ~12-13 hours behind), an order created "today"
        in Beijing time can already be timestamped later than this script's
        idea of "now", silently excluding it from the window. We pad the
        upper bound by a couple of days to absorb that gap — querying a
        little into the future is harmless (orders that don't exist yet
        simply won't be returned).
        """
        all_orders_by_no: Dict[str, Dict[str, Any]] = {}
        end_dt = datetime.now() + timedelta(days=2)
        start_dt = end_dt - timedelta(days=lookback_days)

        for window_start, window_end in _monthly_windows(start_dt, end_dt):
            start_time = window_start.strftime("%Y-%m-%d %H:%M:%S")
            end_time = window_end.strftime("%Y-%m-%d %H:%M:%S")
            for status in OPEN_INBOUND_STATUSES:
                page = 1
                while True:
                    data: Dict[str, Any] = {
                        "page": page,
                        "pageSize": page_size,
                        "status": status,
                        "startTime": start_time,
                        "endTime": end_time,
                    }
                    result = self._post(INBOUND_ORDER_LIST_PATH, data)
                    records = result.get("records", []) or []
                    for rec in records:
                        order_no = rec.get("inboundOrderNo")
                        if order_no:
                            all_orders_by_no[order_no] = rec

                    total = result.get("total", 0)
                    if page * page_size >= total or not records:
                        break
                    page += 1

        return list(all_orders_by_no.values())

    def query_inbound_box_sku_list(
        self, inbound_order_no: str, inbound_type: int, page_size: int = 100
    ) -> List[Dict[str, Any]]:
        """Get the per-box, per-SKU packing detail for a single inbound order
        (quantity预报 vs receivedQuantity已收货 per SKU)."""
        all_boxes: List[Dict[str, Any]] = []
        page = 1
        while True:
            data: Dict[str, Any] = {
                "inboundOrderNo": inbound_order_no,
                "inboundType": inbound_type,
                "page": page,
                "pageSize": page_size,
            }
            result = self._post(INBOUND_BOX_SKU_LIST_PATH, data)
            records = result.get("records", []) or []
            all_boxes.extend(records)

            total = result.get("total", 0)
            pages = result.get("pages", 1)
            if page >= pages or not records:
                break
            page += 1
        return all_boxes

    def compute_in_transit_by_sku(
        self,
        wh_code_filter: Optional[List[str]] = None,
        sku_filter: Optional[List[str]] = None,
        lookback_days: int = 60,
    ) -> Dict[Tuple[str, str], int]:
        """Compute true in-transit quantity per (SKU, Warehouse), summed across
        all open (待入库/收货中) inbound orders.

        in_transit_qty = sum over open orders' boxes of max(quantity - receivedQuantity, 0)

        NOTE: the underlying pageBoxSkuList API has no server-side SKU filter
        (it's scoped per inbound order, not per SKU), so `sku_filter` — if
        given — is applied client-side after fetching each order's packing
        detail. This still saves nothing on API call count, but it DOES make
        --sku behave consistently with the product-catalog and stock queries,
        instead of silently ignoring the filter and returning in-transit data
        for every SKU in the warehouse.
        """
        in_transit: Dict[Tuple[str, str], int] = {}

        orders = self.query_open_inbound_orders(lookback_days=lookback_days)
        wh_filter_set = set(wh_code_filter) if wh_code_filter else None
        sku_filter_set = set(sku_filter) if sku_filter else None

        log(f"Found {len(orders)} open inbound order(s) (待入库/收货中) to inspect for in-transit quantities.")

        for order in orders:
            wh_code = order.get("whCode", "")
            if wh_filter_set is not None and wh_code not in wh_filter_set:
                continue
            order_no = order.get("inboundOrderNo")
            inbound_type = order.get("inboundType")
            if not order_no or inbound_type is None:
                continue
            try:
                boxes = self.query_inbound_box_sku_list(order_no, inbound_type)
            except Exception as e:
                log(f"Warning: failed to fetch packing detail for inbound order {order_no}: {e}", err=True)
                continue

            for box in boxes:
                for prod in box.get("productList", []) or []:
                    sku = prod.get("sku", "")
                    if not sku:
                        continue
                    if sku_filter_set is not None and sku not in sku_filter_set:
                        continue
                    qty = prod.get("quantity", 0) or 0
                    received = prod.get("receivedQuantity", 0) or 0
                    pending = max(qty - received, 0)
                    if pending <= 0:
                        continue
                    key = (sku, wh_code)
                    in_transit[key] = in_transit.get(key, 0) + pending

        return in_transit


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
        "Inbound In-Transit": 0,  # filled in later by build_combined_rows
    }


# ----------------------------------------------------------------------
# Combining product catalog + stock + true in-transit into export rows
# ----------------------------------------------------------------------
ROW_COLUMNS = [
    "SKU",
    "Product Name",
    "Warehouse",
    "Stock Type",
    "Total Stock (Dropship)",
    "Available Stock",
    "Locked Stock",
    "Inbound In-Transit",
]


def build_combined_rows(
    products: List[Dict[str, Any]],
    stock_records: List[Dict[str, Any]],
    in_transit_map: Dict[Tuple[str, str], int],
) -> List[Dict[str, Any]]:
    """Merge the three data sources into one row set, keyed by (SKU, Warehouse, Stock Type).

    - `products`: full OMS product catalog (source of truth for which SKUs exist at all)
    - `stock_records`: raw records from /v1/integratedInventory/pageOpen (on-hand stock)
    - `in_transit_map`: {(sku, warehouse): pending_qty} computed from open inbound orders

    Guarantee: every SKU in `products` appears at least once in the output,
    even if it currently has zero on-hand stock AND zero in-transit.
    """
    product_names = {p.get("sku", ""): p.get("productName", "") for p in products if p.get("sku")}
    all_master_skus = set(product_names.keys())

    combined: Dict[Tuple[str, str, str], Dict[str, Any]] = {}

    # 1. Seed with on-hand stock records
    for rec in stock_records:
        flat = flatten_record(rec)
        # Prefer the product catalog's name if we have one (more authoritative/consistent)
        if flat["SKU"] in product_names and product_names[flat["SKU"]]:
            flat["Product Name"] = product_names[flat["SKU"]]
        key = (flat["SKU"], flat["Warehouse"], flat["Stock Type"])
        combined[key] = flat

    # 2. Fold in true in-transit quantities. In-transit is inherently "Good"
    #    stock (goods not yet received can't be defective-in-inventory yet),
    #    so it's attached to the "Good" stock-type row for that SKU/warehouse,
    #    creating that row (zeroed on-hand fields) if it doesn't exist yet.
    for (sku, warehouse), qty in in_transit_map.items():
        key = (sku, warehouse, "Good")
        if key in combined:
            combined[key]["Inbound In-Transit"] = combined[key].get("Inbound In-Transit", 0) + qty
        else:
            combined[key] = {
                "SKU": sku,
                "Product Name": product_names.get(sku, ""),
                "Warehouse": warehouse,
                "Stock Type": "Good",
                "Total Stock (Dropship)": 0,
                "Available Stock": 0,
                "Locked Stock": 0,
                "Inbound In-Transit": qty,
            }

    # 3. Any catalog SKU that still hasn't shown up anywhere (no stock, no
    #    in-transit, no movement at all) gets one all-zero placeholder row,
    #    so the SKU column always matches the OMS product catalog.
    skus_with_data = {k[0] for k in combined.keys()}
    for sku in sorted(all_master_skus - skus_with_data):
        key = (sku, "-", "-")
        combined[key] = {
            "SKU": sku,
            "Product Name": product_names.get(sku, ""),
            "Warehouse": "-",
            "Stock Type": "-",
            "Total Stock (Dropship)": 0,
            "Available Stock": 0,
            "Locked Stock": 0,
            "Inbound In-Transit": 0,
        }

    rows = list(combined.values())
    rows.sort(key=lambda r: (r["SKU"], r["Warehouse"], r["Stock Type"]))
    # Make sure every row has exactly ROW_COLUMNS, in order (matters for CSV/Feishu writes)
    return [{col: r.get(col, 0 if col not in ("SKU", "Product Name", "Warehouse", "Stock Type") else "") for col in ROW_COLUMNS} for r in rows]


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
        values.append([f"Last updated: {now_eastern().strftime('%Y-%m-%d %H:%M:%S %Z')}"])

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
    lines.append(f"\n{len(rows)} records total, generated at: {now_eastern().strftime('%Y-%m-%d %H:%M:%S %Z')}")
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

    log("Fetching OMS product catalog (SKU source of truth)...")
    products = client.query_product_catalog(sku_list=sku_list)
    log(f"  -> {len(products)} product(s) in catalog"
        + (f" matching --sku filter" if sku_list else ""))

    log("Fetching on-hand stock (integratedInventory/pageOpen)...")
    stock_records = client.query_inventory(
        sku_list=sku_list,
        wh_code_list=wh_list,
        stock_type=args.stock_type,
        page_size=100,
    )
    log(f"  -> {len(stock_records)} stock record(s)")

    in_transit_map: Dict[Tuple[str, str], int] = {}
    if not args.skip_transit:
        log("Computing true inbound in-transit from open inbound orders (this "
            "calls the API once per open inbound order — may take a while)...")
        in_transit_map = client.compute_in_transit_by_sku(
            wh_code_filter=wh_list, sku_filter=sku_list, lookback_days=args.transit_lookback_days
        )
        log(f"  -> in-transit quantities computed for {len(in_transit_map)} (SKU, warehouse) pair(s)")
    else:
        log("Skipping inbound in-transit computation (--skip-transit was given); "
            "'Inbound In-Transit' column will be 0 for everything this run.")

    return build_combined_rows(products, stock_records, in_transit_map)


def main():
    parser = argparse.ArgumentParser(
        description="Query OMS product catalog + on-hand stock + true inbound in-transit, and write it to a Feishu sheet"
    )
    parser.add_argument("--sku", help="Comma-separated list of SKUs (filters product catalog + stock query)")
    parser.add_argument("--warehouse", help="Comma-separated warehouse codes, e.g. M60003 (filters stock + in-transit queries)")
    parser.add_argument("--stock-type", type=int, choices=[0, 1], default=None,
                         help="0=Good, 1=Defective; if omitted, both are included")
    parser.add_argument("--skip-transit", action="store_true",
                         help="Skip the inbound-order-based in-transit computation (faster, "
                         "but 'Inbound In-Transit' will just be 0 for everything)")
    parser.add_argument("--transit-lookback-days", type=int, default=60,
                         help="How far back (by inbound-order creation date) to look when "
                         "searching for open/未完成 inbound orders for the in-transit "
                         "computation (default: 60). The inbound-order-list API's "
                         "startTime/endTime filter appears designed for ~1-month spans, "
                         "so this window is automatically chunked into ~30-day slices "
                         "under the hood; raise this if you have inbound orders that have "
                         "been open longer than this (e.g. slow ocean freight).")
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
        "environment variable if omitted; default 0 = run once). Pass --watch 0 "
        "explicitly to force a single run even if WATCH_INTERVAL_SECONDS is set in .env.",
    )
    parser.add_argument(
        "--debug-source",
        choices=["catalog", "stock", "transit"],
        help="Bypass the merge logic entirely and dump ONE raw data source to "
        "--csv (or stdout) for inspection: 'catalog' = OMS product list "
        "(/v1/product/pagelist), 'stock' = on-hand inventory "
        "(/v1/integratedInventory/pageOpen), 'transit' = computed inbound "
        "in-transit quantities from open inbound orders. Does not write to "
        "Feishu. Use this to figure out which of the three data sources isn't "
        "returning what you expect.",
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

    # --debug-source: dump exactly one raw data source, unmerged, and exit.
    # No Feishu involved at all — this is purely for figuring out which of the
    # three sources isn't returning what you expect.
    if args.debug_source:
        sku_list = args.sku.split(",") if args.sku else None
        wh_list = args.warehouse.split(",") if args.warehouse else None

        if args.debug_source == "catalog":
            log("Fetching OMS product catalog (/v1/product/pagelist) only...")
            products = client.query_product_catalog(sku_list=sku_list)
            log(f"  -> {len(products)} product(s)")
            rows = [{"SKU": p.get("sku", ""), "Product Name": p.get("productName", "")} for p in products]

        elif args.debug_source == "stock":
            log("Fetching on-hand stock (/v1/integratedInventory/pageOpen) only...")
            stock_records = client.query_inventory(
                sku_list=sku_list, wh_code_list=wh_list, stock_type=args.stock_type, page_size=100
            )
            log(f"  -> {len(stock_records)} stock record(s)")
            rows = [flatten_record(rec) for rec in stock_records]
            for r in rows:
                r.pop("Inbound In-Transit", None)  # not populated in this raw dump

        else:  # transit
            log("Computing in-transit quantities from open inbound orders only...")
            in_transit_map = client.compute_in_transit_by_sku(
                wh_code_filter=wh_list, sku_filter=sku_list, lookback_days=args.transit_lookback_days
            )
            log(f"  -> {len(in_transit_map)} (SKU, warehouse) pair(s) with pending in-transit qty")
            rows = [
                {"SKU": sku, "Warehouse": wh, "Pending In-Transit Qty": qty}
                for (sku, wh), qty in sorted(in_transit_map.items())
            ]

        print_table(rows)
        if args.csv:
            write_csv(rows, args.csv)
        sys.exit(0)

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
            log("No data returned, nothing to simulate.")
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