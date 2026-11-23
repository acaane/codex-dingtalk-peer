"""Keep the original demo command available."""
import sys

from scripts.run.demo import demo


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    raise SystemExit(demo())
