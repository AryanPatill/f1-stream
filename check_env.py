"""Structural check on .env connection strings. Prints no secrets."""
import os
import urllib.parse as u

from dotenv import load_dotenv

load_dotenv()

for name in ("SUPABASE_DB_URL", "SUPABASE_DB_URL_RO"):
    raw = os.environ.get(name, "")
    if not raw:
        print(f"{name}: NOT SET")
        continue
    p = u.urlparse(raw)
    print(
        f"{name}: user={p.username} host={p.hostname} "
        f"port={p.port} db={p.path} pwd_len={len(p.password or '')} "
        f"at_count={raw.count('@')}"
    )