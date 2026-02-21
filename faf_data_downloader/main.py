import json
import logging
import os
import subprocess
import sys
import time
import threading
from datetime import datetime, date
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from tkcalendar import DateEntry   # pip install tkcalendar

from auth import FAFAuthClient
from client import FAFClient
from config import *
from utils_filters import build_filter, date_to_filter_value
from utils_history import load_settings, save_settings, load_history, append_history, \
    format_duration
from utils_dataframe import jsonapi_to_dataframe, convert_datetime_columns
from utils_filewriters import make_writer


###############################
#   INIT
###############################
logger = logging.getLogger(__name__)
ENDPOINTS = {name: meta[0] for name, meta in ENDPOINT_META.items()}

###############################
#   GUI
###############################

class FAFDataApp:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("FAF Data Downloader")
        self.root.resizable(False, False)

        self.auth_client = FAFAuthClient(
            CLIENT_ID, OAUTH_BASE_URL, REDIRECT_URI, SCOPES, TOKEN_FILE,
        )
        self.client = FAFClient(API_BASE_URL, self.auth_client)
        self._stop_event = threading.Event()
        self._last_output_path: str | None = None

        self._build_ui()
        self._load_settings_to_ui()

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _build_ui(self) -> None:
        pad = {"padx": 6, "pady": 3}

        # Notebook with two tabs: Download and History
        nb = ttk.Notebook(self.root)
        nb.pack(fill="both", expand=True, padx=8, pady=8)

        dl_frame = ttk.Frame(nb, padding=8)
        hist_frame = ttk.Frame(nb, padding=8)
        nb.add(dl_frame, text="Download")
        nb.add(hist_frame, text="History")

        self._build_download_tab(dl_frame, pad)
        self._build_history_tab(hist_frame)

    def _build_download_tab(self, frame: ttk.Frame, pad: dict) -> None:
        row = 0

        # ---- Endpoint ----
        ttk.Label(frame, text="Endpoint").grid(row=row, column=0, sticky="w", **pad)
        self.endpoint_var = tk.StringVar(value="Games")
        ttk.Combobox(
            frame, textvariable=self.endpoint_var,
            values=list(ENDPOINTS.keys()), state="readonly", width=28,
        ).grid(row=row, column=1, columnspan=2, sticky="ew", **pad)
        row += 1

        # ---- Page size ----
        ttk.Label(frame, text=f"Page size (max {API_MAX_PAGE_SIZE:,})").grid(row=row, column=0, sticky="w", **pad)
        self.page_size_var = tk.StringVar(value=str(DEFAULT_PAGE_SIZE))
        ttk.Entry(frame, textvariable=self.page_size_var, width=10).grid(row=row, column=1, sticky="w", **pad)
        row += 1

        # ---- Max pages ----
        ttk.Label(frame, text="Max pages (0 = unlimited)").grid(row=row, column=0, sticky="w", **pad)
        self.max_pages_var = tk.StringVar(value="0")
        ttk.Entry(frame, textvariable=self.max_pages_var, width=10).grid(row=row, column=1, sticky="w", **pad)
        row += 1

        # ---- Extra filter ----
        ttk.Label(frame, text='Extra filter (RSQL)').grid(row=row, column=0, sticky="w", **pad)
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
        ttk.Checkbutton(frame, text="Newest first (sort by -id)", variable=self.newest_first_var
                        ).grid(row=row, column=0, columnspan=3, sticky="w", **pad)
        row += 1

        ttk.Separator(frame, orient="horizontal").grid(row=row, column=0, columnspan=3, sticky="ew", pady=6)
        row += 1

        # ---- Date range ----
        ttk.Label(frame, text="Date range (auto-detected field per endpoint)", font=("", 9, "bold")
                  ).grid(row=row, column=0, columnspan=3, sticky="w", padx=6)
        row += 1

        ttk.Label(frame, text="Date from").grid(row=row, column=0, sticky="w", **pad)
        self.date_from = DateEntry(frame, width=14, date_pattern="yyyy-mm-dd",
                                   background="darkblue", foreground="white")
        self.date_from.delete(0, "end")
        self.date_from.grid(row=row, column=1, sticky="w", **pad)
        self._date_from_active = False
        self.date_from.bind("<<DateEntrySelected>>", lambda e: setattr(self, "_date_from_active", True))
        ttk.Button(frame, text="Clear", width=5, command=self._clear_date_from
                   ).grid(row=row, column=2, **pad)
        row += 1

        ttk.Label(frame, text="Date to").grid(row=row, column=0, sticky="w", **pad)
        self.date_to = DateEntry(frame, width=14, date_pattern="yyyy-mm-dd",
                                 background="darkblue", foreground="white")
        self.date_to.delete(0, "end")
        self.date_to.grid(row=row, column=1, sticky="w", **pad)
        self._date_to_active = False
        self.date_to.bind("<<DateEntrySelected>>", lambda e: setattr(self, "_date_to_active", True))
        ttk.Button(frame, text="Clear", width=5, command=self._clear_date_to
                   ).grid(row=row, column=2, **pad)
        row += 1

        self.all_in_range_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(frame, text="Download ALL records in date range (ignores Max pages)",
                        variable=self.all_in_range_var
                        ).grid(row=row, column=0, columnspan=3, sticky="w", **pad)
        row += 1

        ttk.Separator(frame, orient="horizontal").grid(row=row, column=0, columnspan=3, sticky="ew", pady=6)
        row += 1

        # ---- Export format ----
        ttk.Label(frame, text="Export format").grid(row=row, column=0, sticky="w", **pad)
        self.format_var = tk.StringVar(value="csv")
        ttk.Combobox(frame, textvariable=self.format_var, values=["csv", "parquet", "json"],
                     state="readonly", width=12).grid(row=row, column=1, sticky="w", **pad)
        row += 1

        # ---- Chunk pages ----
        ttk.Label(frame, text="Pages per chunk").grid(row=row, column=0, sticky="w", **pad)
        self.chunk_size_var = tk.StringVar(value=str(DEFAULT_CHUNK_PAGES))
        ttk.Entry(frame, textvariable=self.chunk_size_var, width=10).grid(row=row, column=1, sticky="w", **pad)
        row += 1

        # ---- Resume ----
        self.resume_var = tk.BooleanVar()
        ttk.Checkbutton(frame, text="Resume previous download", variable=self.resume_var
                        ).grid(row=row, column=0, columnspan=3, sticky="w", **pad)
        row += 1

        ttk.Separator(frame, orient="horizontal").grid(row=row, column=0, columnspan=3, sticky="ew", pady=4)
        row += 1

        # ---- Progress bar (determinate) ----
        self.progress = ttk.Progressbar(frame, mode="determinate", length=380, maximum=100)
        self.progress.grid(row=row, column=0, columnspan=3, sticky="ew", **pad)
        row += 1

        # ---- Status label ----
        self.status_var = tk.StringVar(value="Ready.")
        ttk.Label(frame, textvariable=self.status_var, foreground="gray", wraplength=380
                  ).grid(row=row, column=0, columnspan=3, sticky="w", **pad)
        row += 1

        # ---- Buttons ----
        btn_frame = ttk.Frame(frame)
        btn_frame.grid(row=row, column=0, columnspan=3, pady=(6, 0))

        self.download_btn = ttk.Button(btn_frame, text="Download", command=self._start_download)
        self.download_btn.pack(side="left", padx=4)

        self.stop_btn = ttk.Button(btn_frame, text="Stop", command=self._stop_download, state="disabled")
        self.stop_btn.pack(side="left", padx=4)

        self.open_btn = ttk.Button(btn_frame, text="Open output folder", command=self._open_output_file, state="disabled")
        self.open_btn.pack(side="left", padx=4)

        frame.columnconfigure(1, weight=1)

    def _build_history_tab(self, frame: ttk.Frame) -> None:
        cols = ("time", "endpoint", "records", "duration", "file")
        self.history_tree = ttk.Treeview(frame, columns=cols, show="headings", height=14)
        self.history_tree.heading("time",     text="Time")
        self.history_tree.heading("endpoint", text="Endpoint")
        self.history_tree.heading("records",  text="Records")
        self.history_tree.heading("duration", text="Duration")
        self.history_tree.heading("file",     text="Output file")

        self.history_tree.column("time",     width=130, anchor="w")
        self.history_tree.column("endpoint", width=120, anchor="w")
        self.history_tree.column("records",  width=80,  anchor="e")
        self.history_tree.column("duration", width=70,  anchor="e")
        self.history_tree.column("file",     width=250, anchor="w")

        sb = ttk.Scrollbar(frame, orient="vertical", command=self.history_tree.yview)
        self.history_tree.configure(yscrollcommand=sb.set)
        self.history_tree.pack(side="left", fill="both", expand=True)
        sb.pack(side="right", fill="y")

        self.history_tree.bind("<Double-1>", self._on_history_double_click)
        self._refresh_history_view()

    # ------------------------------------------------------------------
    # Settings persistence
    # ------------------------------------------------------------------

    def _load_settings_to_ui(self) -> None:
        s = load_settings()
        if not s:
            return
        if "endpoint" in s:
            self.endpoint_var.set(s["endpoint"])
        if "page_size" in s:
            self.page_size_var.set(str(s["page_size"]))
        if "max_pages" in s:
            self.max_pages_var.set(str(s["max_pages"]))
        if "filter" in s:
            self.filter_var.set(s["filter"])
        if "include" in s:
            self.include_var.set(s["include"])
        if "newest_first" in s:
            self.newest_first_var.set(bool(s["newest_first"]))
        if "format" in s:
            self.format_var.set(s["format"])
        if "chunk_pages" in s:
            self.chunk_size_var.set(str(s["chunk_pages"]))
        if "all_in_range" in s:
            self.all_in_range_var.set(bool(s["all_in_range"]))

    def _save_settings_from_ui(self) -> None:
        save_settings({
            "endpoint":    self.endpoint_var.get(),
            "page_size":   self.page_size_var.get(),
            "max_pages":   self.max_pages_var.get(),
            "filter":      self.filter_var.get(),
            "include":     self.include_var.get(),
            "newest_first": self.newest_first_var.get(),
            "format":      self.format_var.get(),
            "chunk_pages": self.chunk_size_var.get(),
            "all_in_range": self.all_in_range_var.get(),
        })

    # ------------------------------------------------------------------
    # History
    # ------------------------------------------------------------------

    def _refresh_history_view(self) -> None:
        for item in self.history_tree.get_children():
            self.history_tree.delete(item)
        for entry in load_history():
            self.history_tree.insert("", "end", values=(
                entry.get("time", ""),
                entry.get("endpoint", ""),
                f"{entry.get('records', 0):,}",
                entry.get("duration", ""),
                entry.get("file", ""),
            ))

    def _on_history_double_click(self, event) -> None:
        """Double-clicking a history row opens that file in Explorer/Finder."""
        sel = self.history_tree.selection()
        if not sel:
            return
        values = self.history_tree.item(sel[0], "values")
        if values:
            path = values[4]  # file column
            self._reveal_path(path)

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
    # File / folder opening
    # ------------------------------------------------------------------

    def _open_output_file(self) -> None:
        # With chunked output there's no single file — open the folder instead.
        if self._last_output_path and os.path.exists(self._last_output_path):
            self._reveal_path(self._last_output_path)

    def _open_output_folder(self) -> None:
        if self._last_output_path and os.path.exists(self._last_output_path):
            self._reveal_path(self._last_output_path)

    @staticmethod
    def _reveal_path(path: str) -> None:
        """Open a file or folder in the OS file manager."""
        try:
            if sys.platform == "win32":
                os.startfile(path)
            elif sys.platform == "darwin":
                subprocess.Popen(["open", path])
            else:
                subprocess.Popen(["xdg-open", path])
        except Exception as e:
            logger.warning("Could not open path %s: %s", path, e)

    # ------------------------------------------------------------------
    # Download orchestration
    # ------------------------------------------------------------------

    def _start_download(self) -> None:
        self._stop_event.clear()
        self.download_btn.configure(state="disabled")
        self.stop_btn.configure(state="normal")
        self.open_btn.configure(state="disabled")
        self.progress["value"] = 0
        self.status_var.set("Starting download…")
        self._save_settings_from_ui()
        thread = threading.Thread(target=self._download, daemon=True)
        thread.start()

    def _stop_download(self) -> None:
        self._stop_event.set()
        self.status_var.set("Stopping after current page…")

    def _finish_ui(self, msg: str, error: bool = False, output_path: str | None = None) -> None:
        self.progress["value"] = 0
        self.download_btn.configure(state="normal")
        self.stop_btn.configure(state="disabled")
        self.status_var.set(msg)
        if output_path and os.path.exists(output_path):
            self._last_output_path = output_path
            self.open_btn.configure(state="normal")
        if error:
            messagebox.showerror("Error", msg)

    def _update_progress(self, fetched: int, total: int | None, rate: float, elapsed: float) -> None:
        """Update the progress bar and status label from the main thread."""
        if total and total > 0:
            pct = min(100.0, fetched / total * 100)
            self.progress["value"] = pct
            remaining = ((total - fetched) / rate) if rate > 0 else 0
            eta_str = format_duration(remaining) if rate > 0 else "…"
            self.status_var.set(
                f"{fetched:,} / {total:,} records ({pct:.1f}%) — "
                f"{rate:.0f} rec/s — ETA {eta_str}"
            )
        else:
            # Total unknown — show indeterminate-style text, pulse bar
            self.progress["value"] = (self.progress["value"] + 2) % 100
            rate_str = f"{rate:.0f} rec/s" if rate > 0 else "…"
            self.status_var.set(
                f"{fetched:,} records fetched — {rate_str} — {format_duration(elapsed)} elapsed"
            )

    # ------------------------------------------------------------------

    def _download(self) -> None:
        start_time = time.monotonic()
        total_records = 0

        try:
            endpoint_name = self.endpoint_var.get()
            endpoint      = ENDPOINTS[endpoint_name]
            page_size     = min(int(self.page_size_var.get()), API_MAX_PAGE_SIZE)
            chunk_pages   = max(1, int(self.chunk_size_var.get()))
            fmt           = self.format_var.get()
            all_in_range  = self.all_in_range_var.get()

            max_pages_raw = int(self.max_pages_var.get())
            max_pages = None if (all_in_range or max_pages_raw == 0) else max_pages_raw

            # Build params
            params: dict = {"page[size]": page_size}
            if self.include_var.get().strip():
                params["include"] = self.include_var.get()
            if self.newest_first_var.get():
                params["sort"] = "-id"

            _, _entity_type, date_field = ENDPOINT_META[endpoint_name]
            date_from = self._get_selected_date(self.date_from, "_date_from_active")
            date_to   = self._get_selected_date(self.date_to,   "_date_to_active")

            filter_predicates: list[str] = []
            if date_field and date_from:
                filter_predicates.append(f"{date_field}=ge={date_to_filter_value(date_from)}")
            if date_field and date_to:
                end_val = f'"{datetime(date_to.year, date_to.month, date_to.day, 23, 59, 59).strftime("%Y-%m-%dT%H:%M:%SZ")}"'
                filter_predicates.append(f"{date_field}=le={end_val}")

            params.update(build_filter(filter_predicates, self.filter_var.get()))

            if all_in_range and not filter_predicates:
                self.root.after(0, lambda: self._finish_ui(
                    "Error: 'Download all in range' requires at least one date.", error=True))
                return

            # Resume handling
            start_page  = 1
            output_path = None
            resuming    = False

            if self.resume_var.get() and CHUNK_STATE_FILE.exists():
                state = json.loads(CHUNK_STATE_FILE.read_text())
                start_page  = state.get("next_page", 1)
                output_path = state.get("output_path")
                resuming    = bool(output_path and os.path.exists(output_path))
                self.root.after(0, lambda pg=state.get("page_count", "?"): self.status_var.set(
                    f"Resuming from page {pg}…"))

            if not output_path:
                output_path = self._ask_save_path(fmt)
                if not output_path:
                    self.root.after(0, lambda: self._finish_ui("Cancelled."))
                    return

            # Chunked output: each chunk_pages pages → a separate numbered file.
            # e.g. output.csv → output_001.csv, output_002.csv, ...
            out_path  = Path(output_path)
            out_stem  = out_path.stem
            out_suffix = out_path.suffix
            out_dir   = out_path.parent

            page_buffer: list = []
            chunk_record_threshold = chunk_pages * page_size
            chunk_index = (start_page - 1) // chunk_pages  # resume-aware chunk numbering

            def next_chunk_path(idx: int) -> str:
                return str(out_dir / f"{out_stem}_{idx + 1:03d}{out_suffix}")

            current_writer = make_writer(fmt, next_chunk_path(chunk_index), resume=resuming)

            # ── Pre-download record count probe ──────────────────────────
            # Always probe with the full params (including any filters) so we
            # get the count for the actual filtered result set, not the global
            # total.  This is cheap — it fetches only 1 record.
            self.root.after(0, lambda: self.status_var.set("Counting records…"))
            api_total: int | None = self.client.count_records(endpoint, params)
            if api_total is not None:
                logger.info("Pre-download count: %s records", api_total)
                self.root.after(0, lambda t=api_total: self.status_var.set(
                    f"{t:,} records to download — starting…"))

            def progress_callback(page: int, fetched: int, reported_total: int | None) -> None:
                # Use the pre-probed total; ignore reported_total from iter_pages
                # because it comes from the first data page's meta, which on some
                # endpoints ignores filters and returns the global count.
                elapsed = time.monotonic() - start_time
                rate    = fetched / elapsed if elapsed > 0 else 0
                self.root.after(0, lambda f=fetched, t=api_total, r=rate, e=elapsed:
                                self._update_progress(f, t, r, e))

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
                        self._flush_chunk(current_writer, page_buffer)
                        total_records += len(page_buffer)
                        page_buffer.clear()
                        current_writer.close()
                        chunk_index += 1
                        current_writer = make_writer(fmt, next_chunk_path(chunk_index), resume=False)

                # Flush any remaining records into the current (last) chunk
                if page_buffer:
                    self._flush_chunk(current_writer, page_buffer)
                    total_records += len(page_buffer)
                    page_buffer.clear()

            finally:
                current_writer.close()

            if not self._stop_event.is_set() and CHUNK_STATE_FILE.exists():
                CHUNK_STATE_FILE.unlink()

            elapsed   = time.monotonic() - start_time
            duration  = format_duration(elapsed)

            if self._stop_event.is_set():
                msg = f"Stopped. {total_records:,} records saved — {duration}. Resume to continue."
            else:
                msg = f"Done. {total_records:,} records saved — {duration}."

            # Log to history — record the folder and file pattern
            chunk_pattern = str(out_dir / f"{out_stem}_*{out_suffix}")
            append_history({
                "time":     datetime.now().strftime("%Y-%m-%d %H:%M"),
                "endpoint": endpoint_name,
                "records":  total_records,
                "duration": duration,
                "file":     chunk_pattern,
            })

            # Open-file buttons point to the output folder (multiple chunk files)
            folder = str(out_dir)
            self.root.after(0, lambda m=msg, p=folder: (
                self._finish_ui(m, output_path=p),
                self.progress.configure(value=100 if not self._stop_event.is_set() else self.progress["value"]),
                self._refresh_history_view(),
            ))

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
            "csv":     [("CSV files",     "*.csv")],
            "json":    [("JSON files",    "*.json")],
        }
        result: list = []
        evt = threading.Event()

        def ask():
            path = filedialog.asksaveasfilename(
                defaultextension=f".{fmt}", filetypes=filetypes[fmt])
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