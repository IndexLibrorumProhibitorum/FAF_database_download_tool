from pathlib import Path

# General Auth
OAUTH_BASE_URL = "https://hydra.faforever.com"
API_BASE_URL = "https://api.faforever.com"
TOKEN_FILE = "token.json"

# Specific Auth, set in FAF's gitops-stack/apps/ory-hydra/values.yaml file
CLIENT_ID = "8ff5c14f-60e2-41b9-b594-a641dc5013be"
REDIRECT_URI = "http://localhost:8080/"
SCOPES = "openid offline upload_avatar administrative_actions read_sensible_userdata manage_vault"

# Globals - Paths
CHUNK_STATE_FILE  = Path("chunk_state.json")
SETTINGS_FILE     = Path("settings.json")
HISTORY_FILE      = Path("download_history.json")

# Globals - API settings
API_MAX_PAGE_SIZE = 10_000
DEFAULT_CHUNK_PAGES = 10
DEFAULT_PAGE_SIZE = 10000

# Endpoint dictionary
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

# Settings keys that are persisted to disk
SETTINGS_KEYS = [
    "endpoint", "page_size", "max_pages", "filter", "include",
    "newest_first", "format", "chunk_pages", "all_in_range",
]
