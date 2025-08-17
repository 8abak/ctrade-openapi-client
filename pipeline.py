import time
from datetime import datetime, timezone
from sqlalchemy import create_engine
from ml_config import DATABASE_URL, ZZ_ABS_THRESHOLD
from zigzag_labeler import process_day
from feature_engineer import compute_zig_features, compute_tick_features_and_labels
from trainer import train_on_zig

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

def run_loop():
    while True:
        day = datetime.now(timezone.utc).date()
        new_zigs = process_day(day, threshold=ZZ_ABS_THRESHOLD)
        for zid in new_zigs:
            compute_zig_features(zid)
            compute_tick_features_and_labels(zid)
            train_on_zig(zid)
        time.sleep(2)

if __name__ == "__main__":
    run_loop()
