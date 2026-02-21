# General Auth
OAUTH_BASE_URL = "https://hydra.faforever.com"
API_BASE_URL = "https://api.faforever.com"
TOKEN_FILE = "token.json"

# Specific Auth, set in FAF's gitops-stack/apps/ory-hydra/values.yaml file
CLIENT_ID = "8ff5c14f-60e2-41b9-b594-a641dc5013be"
REDIRECT_URI = "http://localhost:8080/"
SCOPES = "openid offline upload_avatar administrative_actions read_sensible_userdata manage_vault"
