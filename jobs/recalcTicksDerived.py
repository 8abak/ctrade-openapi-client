#!/usr/bin/env python3
import psycopg2

DBName = "trading"
DBUser = "babak"
DBPassword = "babak33044"
DBHost = "localhost"
DBPort = 5432

BatchSize = 10000  # small, safe chunk size

def GetConnection():
    return psycopg2.connect(
        dbname=DBName,
        user=DBUser,
        password=DBPassword,
        host=DBHost,
        port=DBPort,
    )

class KalmanFilter1D:
    def __init__(self, InitialValue: float, ProcessNoise: float, MeasurementNoise: float):
        self.X = InitialValue
        self.P = 1.0
        self.Q = ProcessNoise
        self.R = MeasurementNoise

    def Update(self, Measurement: float) -> float:
        self.P = self.P + self.Q
        K = self.P / (self.P + self.R)
        self.X = self.X + K * (Measurement - self.X)
        self.P = (1 - K) * self.P
        return self.X

def Main():
    conn = GetConnection()
    cur = conn.cursor()

    # Find id range
    cur.execute("SELECT MIN(id), MAX(id) FROM ticks;")
    row = cur.fetchone()
    if row is None or row[0] is None:
        print("No rows in ticks; nothing to do.")
        cur.close()
        conn.close()
        return

    minId, maxId = row
    print(f"Recalculating derived fields from id {minId} to {maxId}...")

    NormalKalman = None
    FastKalman = None
    SlowKalman = None

    lastId = minId - 1
    processed = 0

    while True:
        cur.execute(
            """
            SELECT id, bid, ask
            FROM ticks
            WHERE id > %s
            ORDER BY id
            LIMIT %s;
            """,
            (lastId, BatchSize),
        )
        rows = cur.fetchall()
        if not rows:
            break

        # Initialize Kalmans on the very first tick
        if NormalKalman is None:
            firstBid, firstAsk = rows[0][1], rows[0][2]
            firstMidRaw = (firstBid + firstAsk) / 2.0
            NormalKalman = KalmanFilter1D(firstMidRaw, ProcessNoise=0.02,  MeasurementNoise=0.5)
            FastKalman   = KalmanFilter1D(firstMidRaw, ProcessNoise=0.10,  MeasurementNoise=0.5)
            SlowKalman   = KalmanFilter1D(firstMidRaw, ProcessNoise=0.005, MeasurementNoise=0.5)

        updates = []

        for tickId, bid, ask in rows:
            lastId = tickId

            midRaw = (bid + ask) / 2.0
            midRounded = round(midRaw, 2)
            spread = round(ask - bid, 2)

            kal_normal = NormalKalman.Update(midRaw)
            kal_fast   = FastKalman.Update(midRaw)
            kal_slow   = SlowKalman.Update(midRaw)

            kal_fast_resid = midRaw - kal_fast
            kal_slow_resid = midRaw - kal_slow

            updates.append(
                (
                    midRounded,
                    kal_normal,
                    spread,
                    kal_fast,
                    kal_slow,
                    kal_fast_resid,
                    kal_slow_resid,
                    tickId,
                )
            )

        cur.executemany(
            """
            UPDATE ticks
            SET mid = %s,
                kal = %s,
                spread = %s,
                kal_fast = %s,
                kal_slow = %s,
                kal_fast_resid = %s,
                kal_slow_resid = %s
            WHERE id = %s;
            """,
            updates,
        )
        conn.commit()

        processed += len(rows)
        print(f"Updated {processed} rows so far (up to id {lastId})...")

    print("✅ Finished recalculating all derived fields.")
    cur.close()
    conn.close()

if __name__ == "__main__":
    Main()
