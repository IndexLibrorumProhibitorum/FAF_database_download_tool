import json
import logging
import os
import threading
from datetime import datetime, date
from pathlib import Path
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from tkcalendar import DateEntry   # pip install tkcalendar

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from auth import FAFAuthClient
from client import FAFClient
from config import *
from utils import jsonapi_to_dataframe, convert_datetime_columns

logger = logging.getLogger(__name__)

CHUNK_STATE_FILE = Path("chunk_state.json")

API_MAX_PAGE_SIZE = 10_000
DEFAULT_CHUNK_PAGES = 10

# (path, elide_type_name, date_field_for_filtering)
# elide_type_name is the entity name used in filter[TYPE]=...
# date_field is None for endpoints that don't support date filtering.
ENDPOINT_META = {
    "Players": ("/data/player", "player", "createTime"),
    "Games": ("/data/game", "game", "startTime"),
    "Maps": ("/data/map", "map", "createTime"),
    "GamePlayerStats": ("/data/gamePlayerStats", "gamePlayerStats", "scoreTime"),
    "leaderboard": ("/data/leaderboard", "leaderboard", "createTime"),
    "leaderboardRatingJournal": ("/data/leaderboardRatingJournal", "leaderboardRatingJournal", "createTime"),
    "Reports": ("/data/moderationReport", "moderationReport", "createTime"),
    "Bans": ("/data/banInfo", "banInfo", "createTime"),
}

ENDPOINTS = {name: meta[0] for name, meta in ENDPOINT_META.items()}


# ---------------------------------------------------------------------------
# Chunked writers
# ---------------------------------------------------------------------------

class ChunkedParquetWriter:
    """Appends DataFrames to a single Parquet file via pyarrow."""

    def __init__(self, path: str, resume: bool = False) -> None:
        self.path = path
        self._writer: pq.ParquetWriter | None = None
        # On a fresh (non-resume) start, delete any existing file so we start clean.
        if not resume and os.path.exists(path):
            os.remove(path)

    def write(self, df: pd.DataFrame) -> None:
        table = pa.Table.from_pandas(df, preserve_index=False)
        if self._writer is None:
            self._writer = pq.ParquetWriter(self.path, table.schema)
        self._writer.write_table(table)

    def close(self) -> None:
        if self._writer:
            self._writer.close()
            self._writer = None


class ChunkedCSVWriter:
    """Appends DataFrames to a CSV file, writing the header only once."""

    def __init__(self, path: str, resume: bool = False) -> None:
        self.path = path
        if not resume:
            # Overwrite: truncate any existing file and write fresh.
            open(self.path, "w").close()
            self._header_written = False
        else:
            self._header_written = os.path.exists(path)

    def write(self, df: pd.DataFrame) -> None:
        df.to_csv(
            self.path,
            mode="a",
            index=False,
            header=not self._header_written,
        )
        self._header_written = True

    def close(self) -> None:
        pass


class ChunkedJSONWriter:
    """Streams records as a JSON array, chunk by chunk."""

    def __init__(self, path: str, resume: bool = False) -> None:
        self.path = path
        self._need_comma = False

        if resume and os.path.exists(path):
            # Trim the trailing "]\n" so we can keep appending.
            with open(self.path, "rb+") as f:
                f.seek(0, 2)
                size = f.tell()
                for back in range(1, min(20, size)):
                    f.seek(-back, 2)
                    ch = f.read(1)
                    if ch == b"]":
                        f.seek(-back, 2)
                        f.truncate()
                        break
            self._need_comma = True
        else:
            # Fresh start: overwrite any existing file.
            with open(self.path, "w", encoding="utf-8") as f:
                f.write("[\n")

    def write(self, df: pd.DataFrame) -> None:
        records = df.to_dict(orient="records")
        with open(self.path, "a", encoding="utf-8") as f:
            for rec in records:
                if self._need_comma:
                    f.write(",\n")
                f.write(json.dumps(rec, default=str))
                self._need_comma = True

    def close(self) -> None:
        with open(self.path, "a", encoding="utf-8") as f:
            f.write("\n]\n")


def make_writer(fmt: str, path: str, resume: bool):
    if fmt == "parquet":
        return ChunkedParquetWriter(path, resume=resume)
    elif fmt == "csv":
        return ChunkedCSVWriter(path, resume=resume)
    elif fmt == "json":
        return ChunkedJSONWriter(path, resume=resume)
    raise ValueError(f"Unknown format: {fmt}")


# ---------------------------------------------------------------------------
# Filter builder
# ---------------------------------------------------------------------------

def build_filter(predicates: list[str], extra: str) -> dict:
    """
    Build the FAF API filter param.

    Confirmed working format:
        filter=field=ge="2025-01-01T00:00:00Z";field=le="2025-10-31T00:00:00Z"

    Operators: =ge= =gt= =le= =lt= == !=
    AND: ;   OR: ,
    Extra predicates typed by the user are appended with ;
    """
    all_parts = list(predicates)
    raw = extra.strip()
    if raw:
        all_parts.extend(p.strip() for p in raw.split(";") if p.strip())
    if not all_parts:
        return {}
    return {"filter": ";".join(all_parts)}


def date_to_filter_value(d: date) -> str:
    """Return a date as a double-quoted ISO-8601Z string for a filter predicate."""
    return f'"{datetime(d.year, d.month, d.day).strftime("%Y-%m-%dT%H:%M:%SZ")}"'


# ---------------------------------------------------------------------------
# GUI
# ---------------------------------------------------------------------------

class FAFDataApp:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("FAF Data Downloader")
        self.root.resizable(False, False)

        self.auth_client = FAFAuthClient(
            CLIENT_ID,
            OAUTH_BASE_URL,
            REDIRECT_URI,
            SCOPES,
            TOKEN_FILE,
        )
        self.client = FAFClient(API_BASE_URL, self.auth_client)
        self._stop_event = threading.Event()

        self._build_ui()

    # ------------------------------------------------------------------
    # UI
    # ------------------------------------------------------------------

    def _build_ui(self) -> None:
        pad = {"padx": 6, "pady": 3}
        frame = ttk.Frame(self.root, padding=12)
        frame.grid(sticky="nsew")

        row = 0

        # ---- Endpoint ----
        ttk.Label(frame, text="Endpoint").grid(row=row, column=0, sticky="w", **pad)
        self.endpoint_var = tk.StringVar(value="Games")
        ttk.Combobox(
            frame,
            textvariable=self.endpoint_var,
            values=list(ENDPOINTS.keys()),
            state="readonly",
            width=28,
        ).grid(row=row, column=1, columnspan=2, sticky="ew", **pad)
        row += 1

        # ---- Page size ----
        ttk.Label(frame, text=f"Page size (max {API_MAX_PAGE_SIZE:,})").grid(row=row, column=0, sticky="w", **pad)
        self.page_size_var = tk.StringVar(value="10000")
        ttk.Entry(frame, textvariable=self.page_size_var, width=10).grid(row=row, column=1, sticky="w", **pad)
        row += 1

        # ---- Max pages ----
        ttk.Label(frame, text="Max pages (0 = unlimited)").grid(row=row, column=0, sticky="w", **pad)
        self.max_pages_var = tk.StringVar(value="0")
        ttk.Entry(frame, textvariable=self.max_pages_var, width=10).grid(row=row, column=1, sticky="w", **pad)
        row += 1

        # ---- Extra filters ----
        ttk.Label(frame, text='Extra filter (e.g. field=ge="value")').grid(row=row, column=0, sticky="w", **pad)
        self.filter_var = tk.StringVar()
        ttk.Entry(frame, textvariable=self.filter_var, width=32).grid(row=row, column=1, columnspan=2, sticky="ew", **pad)
        row += 1

        # ---- Include ----
        ttk.Label(frame, text="Include").grid(row=row, column=0, sticky="w", **pad)
        self.include_var = tk.StringVar()
        ttk.Entry(frame, textvariable=self.include_var, width=32).grid(row=row, column=1, columnspan=2, sticky="ew", **pad)
        row += 1

        # ---- Newest first ----
        self.newest_first_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            frame,
            text="Newest first (sort by -id)",
            variable=self.newest_first_var,
        ).grid(row=row, column=0, columnspan=3, sticky="w", **pad)
        row += 1

        # ---- Separator ----
        ttk.Separator(frame, orient="horizontal").grid(
            row=row, column=0, columnspan=3, sticky="ew", pady=6
        )
        row += 1

        # ---- Date range header ----
        ttk.Label(frame, text="Date range filter (field auto-detected per endpoint)", font=("", 9, "bold")).grid(
            row=row, column=0, columnspan=3, sticky="w", padx=6
        )
        row += 1

        # ---- Date from ----
        ttk.Label(frame, text="Date from").grid(row=row, column=0, sticky="w", **pad)
        self.date_from = DateEntry(
            frame, width=14, date_pattern="yyyy-mm-dd",
            background="darkblue", foreground="white",
        )
        self.date_from.delete(0, "end")
        self.date_from.grid(row=row, column=1, sticky="w", **pad)
        self._date_from_active = False
        self.date_from.bind("<<DateEntrySelected>>", lambda e: setattr(self, "_date_from_active", True))
        ttk.Button(frame, text="Clear", width=5,
                   command=self._clear_date_from).grid(row=row, column=2, **pad)
        row += 1

        # ---- Date to ----
        ttk.Label(frame, text="Date to").grid(row=row, column=0, sticky="w", **pad)
        self.date_to = DateEntry(
            frame, width=14, date_pattern="yyyy-mm-dd",
            background="darkblue", foreground="white",
        )
        self.date_to.delete(0, "end")
        self.date_to.grid(row=row, column=1, sticky="w", **pad)
        self._date_to_active = False
        self.date_to.bind("<<DateEntrySelected>>", lambda e: setattr(self, "_date_to_active", True))
        ttk.Button(frame, text="Clear", width=5,
                   command=self._clear_date_to).grid(row=row, column=2, **pad)
        row += 1

        # ---- Download all in range checkbox ----
        self.all_in_range_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            frame,
            text="Download ALL records in date range (ignores Max pages)",
            variable=self.all_in_range_var,
        ).grid(row=row, column=0, columnspan=3, sticky="w", **pad)
        row += 1

        # ---- Separator ----
        ttk.Separator(frame, orient="horizontal").grid(
            row=row, column=0, columnspan=3, sticky="ew", pady=6
        )
        row += 1

        # ---- Export format ----
        ttk.Label(frame, text="Export format").grid(row=row, column=0, sticky="w", **pad)
        self.format_var = tk.StringVar(value="csv")
        ttk.Combobox(
            frame,
            textvariable=self.format_var,
            values=["csv", "parquet", "json"],
            state="readonly",
            width=12,
        ).grid(row=row, column=1, sticky="w", **pad)
        row += 1

        # ---- Chunk size ----
        ttk.Label(frame, text="Pages per chunk").grid(row=row, column=0, sticky="w", **pad)
        self.chunk_size_var = tk.StringVar(value=str(DEFAULT_CHUNK_PAGES))
        ttk.Entry(frame, textvariable=self.chunk_size_var, width=10).grid(row=row, column=1, sticky="w", **pad)
        row += 1

        # ---- Resume ----
        self.resume_var = tk.BooleanVar()
        ttk.Checkbutton(
            frame,
            text="Resume previous download",
            variable=self.resume_var,
        ).grid(row=row, column=0, columnspan=3, sticky="w", **pad)
        row += 1

        # ---- Progress bar ----
        self.progress = ttk.Progressbar(frame, mode="indeterminate", length=360)
        self.progress.grid(row=row, column=0, columnspan=3, sticky="ew", **pad)
        row += 1

        # ---- Status label ----
        self.status_var = tk.StringVar(value="Ready.")
        ttk.Label(frame, textvariable=self.status_var, foreground="gray").grid(
            row=row, column=0, columnspan=3, sticky="w", **pad
        )
        row += 1

        # ---- Buttons ----
        btn_frame = ttk.Frame(frame)
        btn_frame.grid(row=row, column=0, columnspan=3, pady=(6, 0))

        self.download_btn = ttk.Button(btn_frame, text="Download", command=self._start_download)
        self.download_btn.pack(side="left", padx=4)

        self.stop_btn = ttk.Button(btn_frame, text="Stop", command=self._stop_download, state="disabled")
        self.stop_btn.pack(side="left", padx=4)

        frame.columnconfigure(1, weight=1)

    # ------------------------------------------------------------------
    # Date helpers
    # ------------------------------------------------------------------

    def _clear_date_from(self):
        self.date_from.delete(0, "end")
        self._date_from_active = False

    def _clear_date_to(self):
        self.date_to.delete(0, "end")
        self._date_to_active = False

    def _get_selected_date(self, entry: DateEntry, active_attr: str) -> date | None:
        """Return a date object if the user has picked a date, else None."""
        if not getattr(self, active_attr):
            return None
        raw = entry.get().strip()
        if not raw:
            return None
        try:
            return datetime.strptime(raw, "%Y-%m-%d").date()
        except ValueError:
            return None

    # ------------------------------------------------------------------
    # Download orchestration
    # ------------------------------------------------------------------

    def _start_download(self) -> None:
        self._stop_event.clear()
        self.download_btn.configure(state="disabled")
        self.stop_btn.configure(state="normal")
        self.progress.start(12)
        self.status_var.set("Starting download…")
        thread = threading.Thread(target=self._download, daemon=True)
        thread.start()

    def _stop_download(self) -> None:
        self._stop_event.set()
        self.status_var.set("Stopping after current page…")

    def _finish_ui(self, msg: str, error: bool = False) -> None:
        self.progress.stop()
        self.progress["value"] = 0
        self.download_btn.configure(state="normal")
        self.stop_btn.configure(state="disabled")
        self.status_var.set(msg)
        if error:
            messagebox.showerror("Error", msg)

    # ------------------------------------------------------------------

    def _download(self) -> None:
        try:
            endpoint_name = self.endpoint_var.get()
            endpoint = ENDPOINTS[endpoint_name]
            page_size = min(int(self.page_size_var.get()), API_MAX_PAGE_SIZE)
            chunk_pages = max(1, int(self.chunk_size_var.get()))
            fmt = self.format_var.get()
            all_in_range = self.all_in_range_var.get()

            max_pages_raw = int(self.max_pages_var.get())
            max_pages = None if (all_in_range or max_pages_raw == 0) else max_pages_raw

            # ------------------------------------------------------------------
            # Build params
            # ------------------------------------------------------------------
            params: dict = {"page[size]": page_size}

            if self.include_var.get().strip():
                params["include"] = self.include_var.get()

            if self.newest_first_var.get():
                params["sort"] = "-id"

            # Build Elide RSQL filter params.
            # Format: filter[entityType]=field=ge='value';field=lt='value'
            _, entity_type, date_field = ENDPOINT_META[endpoint_name]
            date_from = self._get_selected_date(self.date_from, "_date_from_active")
            date_to   = self._get_selected_date(self.date_to,   "_date_to_active")

            filter_predicates: list[str] = []
            if date_field and date_from:
                filter_predicates.append(f"{date_field}=ge={date_to_filter_value(date_from)}")
            if date_field and date_to:
                # =le= at 23:59:59 so the entire chosen end date is included.
                end_val = f'"{datetime(date_to.year, date_to.month, date_to.day, 23, 59, 59).strftime("%Y-%m-%dT%H:%M:%SZ")}"'
                filter_predicates.append(f"{date_field}=le={end_val}")

            filter_params = build_filter(filter_predicates, self.filter_var.get())
            params.update(filter_params)

            if all_in_range and not filter_predicates:
                self.root.after(0, lambda: self._finish_ui(
                    "Error: 'Download all in range' requires at least one date.", error=True
                ))
                return

            # ------------------------------------------------------------------
            # Resume handling
            # ------------------------------------------------------------------
            start_page = 1
            output_path = None
            resuming = False

            if self.resume_var.get() and CHUNK_STATE_FILE.exists():
                state = json.loads(CHUNK_STATE_FILE.read_text())
                start_page = state.get("next_page", 1)
                output_path = state.get("output_path")
                resuming = bool(output_path and os.path.exists(output_path))
                self.root.after(0, lambda pg=state.get("page_count", "?"): self.status_var.set(
                    f"Resuming from page {pg}…"
                ))

            if not output_path:
                output_path = self._ask_save_path(fmt)
                if not output_path:
                    self.root.after(0, lambda: self._finish_ui("Cancelled."))
                    return

            # resuming=False → writers will overwrite any existing file
            writer = make_writer(fmt, output_path, resume=resuming)

            # ------------------------------------------------------------------
            # Streaming download loop
            # ------------------------------------------------------------------
            page_buffer: list = []
            total_records = 0
            chunk_record_threshold = chunk_pages * page_size

            def progress_callback(page: int, api_total: int) -> None:
                self.root.after(0, lambda p=page, r=api_total: self.status_var.set(
                    f"Fetching… page {p+1} | {r:,} records received so far"
                ))

            try:
                for page_records in self.client.iter_pages(
                    endpoint=endpoint,
                    params=params,
                    max_pages=max_pages,
                    start_page=start_page,
                    progress_callback=progress_callback,
                    resume_file=CHUNK_STATE_FILE,
                    resume_extra={"output_path": output_path},
                ):
                    if self._stop_event.is_set():
                        break

                    page_buffer.extend(page_records)

                    if len(page_buffer) >= chunk_record_threshold:
                        self._flush_chunk(writer, page_buffer)
                        total_records += len(page_buffer)
                        page_buffer.clear()

                # Flush remainder
                if page_buffer:
                    self._flush_chunk(writer, page_buffer)
                    total_records += len(page_buffer)
                    page_buffer.clear()

            finally:
                writer.close()

            if not self._stop_event.is_set() and CHUNK_STATE_FILE.exists():
                CHUNK_STATE_FILE.unlink()

            if self._stop_event.is_set():
                msg = f"Stopped. {total_records:,} records saved to {output_path}. Resume to continue."
            else:
                msg = f"Done. {total_records:,} records saved to {output_path}"

            self.root.after(0, lambda m=msg: self._finish_ui(m))

        except Exception as exc:
            logger.exception("Download failed")
            self.root.after(0, lambda e=str(exc): self._finish_ui(f"Error: {e}", error=True))

    # ------------------------------------------------------------------

    def _flush_chunk(self, writer, records: list) -> None:
        if not records:
            return
        df = jsonapi_to_dataframe(records)
        df = convert_datetime_columns(df)
        writer.write(df)

    def _ask_save_path(self, fmt: str) -> str | None:
        filetypes = {
            "parquet": [("Parquet files", "*.parquet")],
            "csv": [("CSV files", "*.csv")],
            "json": [("JSON files", "*.json")],
        }
        result: list = []
        evt = threading.Event()

        def ask():
            path = filedialog.asksaveasfilename(
                defaultextension=f".{fmt}",
                filetypes=filetypes[fmt],
            )
            result.append(path or None)
            evt.set()

        self.root.after(0, ask)
        evt.wait()
        return result[0] if result else None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    root = tk.Tk()
    app = FAFDataApp(root)
    root.mainloop()