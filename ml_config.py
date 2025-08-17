import os
from dotenv import load_dotenv
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

# Zig thresholds (tune in your .env or here)
ZZ_ABS_THRESHOLD = float(os.getenv("ZZ_ABS_THRESHOLD", "1.0"))
ZZ_REVERSAL_FRACTION = float(os.getenv("ZZ_REVERSAL_FRACTION", "0.5"))
NO_RETURN_PROBA = float(os.getenv("NO_RETURN_PROBA", "0.80"))
