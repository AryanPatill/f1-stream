"""Build the fixtures for step 7's verification: one valid file and
three that must each be rejected for a different reason."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

OUT = Path("test_uploads")
OUT.mkdir(exist_ok=True)

valid = pd.DataFrame(
    [
        {
            "driver": "TST",
            "lap": lap,
            "sector": sector,
            "event_time": round(lap * 90.0 + sector * 30.0, 3),
            "sector_time": 30.0,
            "compound": "SOFT",
            "notes": "this column should be discarded",
        }
        for lap in range(1, 6)
        for sector in (1, 2, 3)
    ]
)
valid.to_csv(OUT / "valid.csv", index=False)

valid.drop(columns=["sector_time"]).to_csv(OUT / "missing_column.csv", index=False)

(OUT / "fake.csv").write_bytes(b"PAR1" + b"\x00" * 512)

(OUT / "wrong_ext.txt").write_text("driver,lap\nTST,1\n", encoding="utf-8")

print(f"Fixtures written to {OUT.resolve()}")
for p in sorted(OUT.iterdir()):
    print(f"   {p.name}  ({p.stat().st_size:,} bytes)")