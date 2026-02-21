# FAF Data Downloader

A desktop tool for downloading data from the [Forged Alliance Forever](https://www.faforever.com/) API. Built with Python and Tkinter, it supports pagination, date-range filtering, chunked output files, download resumption, and a history log.

> **Note on authorship:** This project was developed with substantial AI assistance. The architecture, logic, and iterative bug-fixing were done collaboratively with [Claude](https://claude.ai) (Anthropic). Although I directed the requirements, tested the tool against the live FAF API, diagnosed issues and made various changes, Claude wrote the majority of the code.

---

## Features

- **Multiple endpoints** — Players, Games, Maps, GamePlayerStats, Leaderboard, LeaderboardRatingJournal, Reports, Bans
- **Date range filtering** — pick from/to dates with a calendar picker; the correct date field is auto-detected per endpoint (`startTime`, `scoreTime`, `createTime`)
- **Chunked output** — large downloads are split into numbered files (e.g. `games_001.csv`, `games_002.csv`) so you never have one unwieldy file
- **Resume support** — interrupted downloads can be continued from where they left off
- **Export formats** — CSV, Parquet, JSON
- **Download history** — logs completed downloads with record count, duration, and file location; double-click a row to open the folder
- **Settings persistence** — remembers your last-used options between sessions
- **OAuth authentication** — full browser-based login flow against the FAF Hydra OAuth server, with automatic token refresh

<img width="550" height="658" alt="image" src="https://github.com/user-attachments/assets/82762dca-f48d-4bf1-9dd7-91efb6a41b9b" />


---

## Requirements

```
Python 3.11+
requests
pandas
pyarrow
tkcalendar
```

Install dependencies:

```bash
pip install requests pandas pyarrow tkcalendar
```

---

## Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/faf-data-downloader.git
   cd faf-data-downloader
   ```

2. Run the tool:
   ```bash
   python main.py
   ```

On first run, a browser window will open for FAF login. The token is saved locally and refreshed automatically.

---

## Usage

### Basic download

1. Select an **endpoint** from the dropdown
2. Set **page size** (max 10,000) and **max pages** (0 = unlimited)
3. Click **Download**, choose an output file, and wait

### Date range filtering

Enable date filtering by selecting a **Date from** and/or **Date to**. The correct field for each endpoint is applied automatically:

| Endpoint | Filter field |
|---|---|
| Games | `startTime` |
| GamePlayerStats | `scoreTime` |
| Players, Maps, Bans, Reports | `createTime` |

Check **Download ALL records in date range** to ignore the max pages limit and fetch everything in the range.

### Extra filters

The **Extra filter** field accepts raw RSQL predicates in the format the FAF API expects:

```
victory==DOMINATION;ranked==true
```

Multiple conditions are separated by `;` (AND). These are appended to any date filters already applied.

### Chunked output

**Pages per chunk** controls how many pages are written to each output file before starting a new one. With `page size = 1000` and `pages per chunk = 10`, each file holds ~10,000 records. Output files are named `yourfile_001.csv`, `yourfile_002.csv`, etc.

### Resuming

If a download is stopped (via the Stop button or a crash), check **Resume previous download** on the next run. The tool picks up from the last completed page and appends to the existing chunk files.

---

## License

MIT
