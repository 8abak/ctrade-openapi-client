from __future__ import annotations

from array import array as NativeArray
import math
import random
import statistics
import struct
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np

from datavis.research.fresh_numeric_spool import (
    NUMERIC_SPOOL_DIRECTORY_PREFIX,
    FloatSeriesSpool,
    NumericSpoolCapacityError,
    NumericSpoolStateError,
)
from datavis.research.fresh_protocol import canonical_hash


_TEST_MAXIMUM_BYTES = 64 * 1024 * 1024


class FreshNumericSpoolTests(unittest.TestCase):
    def setUp(self) -> None:
        self.parent = Path(__file__).resolve().parent

    def _directories(self) -> tuple[Path, ...]:
        return tuple(
            path
            for path in self.parent.iterdir()
            if path.is_dir()
            and path.name.startswith(NUMERIC_SPOOL_DIRECTORY_PREFIX)
        )

    def test_exact_median_quantile_order_and_cleanup(self):
        before = self._directories()
        values = [9.0, -2.0, 4.0, 4.0, 11.0, 0.25]
        with FloatSeriesSpool(
            self.parent,
            maximum_bytes=_TEST_MAXIMUM_BYTES,
        ) as spool:
            directory = spool.directory
            for value in values:
                spool.append("series", value)
            spool.append("other", 3.0)

            self.assertEqual(spool.count("series"), len(values))
            self.assertEqual(tuple(spool.values("series")), tuple(values))
            self.assertEqual(spool.median("series"), 4.0)
            self.assertEqual(
                spool.quantile("series", 0.90),
                float(
                    np.quantile(
                        np.asarray(values),
                        0.90,
                        method="linear",
                    )
                ),
            )
            self.assertIsNone(spool.median("missing"))
            self.assertIsNone(spool.quantile("missing", 0.5))
            self.assertTrue(directory.exists())

        self.assertFalse(directory.exists())
        self.assertEqual(self._directories(), before)

    def test_buffers_flush_across_boundaries_and_keep_duplicates(self):
        values = tuple(float(index % 17) for index in range(10_000))
        with FloatSeriesSpool(
            self.parent,
            maximum_bytes=_TEST_MAXIMUM_BYTES,
        ) as spool:
            for value in values:
                spool.append("many", value)
            self.assertEqual(spool.count("many"), len(values))
            self.assertEqual(tuple(spool.values("many")), values)
            self.assertEqual(
                spool.median("many"),
                float(np.median(np.asarray(values))),
            )

    def test_python311_array_without_clear_supports_flush_and_merge(self):
        class Python311Array(NativeArray):
            clear = None

        def python311_array(typecode, initializer=()):
            return Python311Array(typecode, initializer)

        values = (9.0, -0.0, 7.0, 0.0, 5.0, -0.0, 3.0, 0.0, 1.0)
        with (
            patch(
                "datavis.research.fresh_numeric_spool.array",
                new=python311_array,
            ),
            patch(
                "datavis.research.fresh_numeric_spool._BUFFER_VALUES",
                1,
            ),
            patch(
                "datavis.research.fresh_numeric_spool._SORT_RUN_VALUES",
                1,
            ),
            patch(
                "datavis.research.fresh_numeric_spool._MERGE_FAN_IN",
                2,
            ),
            FloatSeriesSpool(
                self.parent,
                maximum_bytes=_TEST_MAXIMUM_BYTES,
            ) as spool,
        ):
            for value in values:
                spool.append("series", value)
            self.assertEqual(tuple(spool.values("series")), values)
            self.assertEqual(
                spool.median("series"),
                float(statistics.median(values)),
            )

    def test_invalid_values_probabilities_and_lifecycle_are_rejected(self):
        spool = FloatSeriesSpool(
            self.parent,
            maximum_bytes=_TEST_MAXIMUM_BYTES,
        )
        with self.assertRaisesRegex(NumericSpoolStateError, "context manager"):
            spool.append("series", 1.0)
        with spool:
            for value in (True, math.inf, math.nan, "1"):
                with self.subTest(value=value):
                    with self.assertRaises(ValueError):
                        spool.append("series", value)  # type: ignore[arg-type]
            for probability in (-0.1, 1.1, math.nan, True):
                with self.subTest(probability=probability):
                    with self.assertRaises(ValueError):
                        spool.quantile("series", probability)  # type: ignore[arg-type]
        with self.assertRaisesRegex(NumericSpoolStateError, "closed"):
            spool.count("series")

    def test_maximum_bytes_is_required_and_validated(self):
        for value in (0, -1, True, 1.5, None):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    FloatSeriesSpool(
                        self.parent,
                        maximum_bytes=value,  # type: ignore[arg-type]
                    )
        with self.assertRaises(TypeError):
            FloatSeriesSpool(self.parent)  # type: ignore[call-arg]

    def test_append_capacity_rejection_is_exact_and_transactional(self):
        with FloatSeriesSpool(self.parent, maximum_bytes=16) as spool:
            spool.append("series", -0.0)
            spool.append("series", 2.0)
            before = (
                spool.count("series"),
                spool.stored_bytes,
                spool.temporary_bytes,
                spool.owned_bytes,
                spool.keys,
            )
            with self.assertRaises(NumericSpoolCapacityError) as raised:
                spool.append("new-series", 3.0)
            self.assertEqual(raised.exception.maximum_bytes, 16)
            self.assertEqual(
                (
                    spool.count("series"),
                    spool.stored_bytes,
                    spool.temporary_bytes,
                    spool.owned_bytes,
                    spool.keys,
                ),
                before,
            )
            self.assertEqual(spool.count("new-series"), 0)
            self.assertEqual(tuple(spool.values("series")), (-0.0, 2.0))

    def test_order_statistic_preflights_temporary_bytes_without_mutation(self):
        with FloatSeriesSpool(self.parent, maximum_bytes=31) as spool:
            spool.append("series", 2.0)
            spool.append("series", 1.0)
            directory = spool.directory
            with self.assertRaises(NumericSpoolCapacityError):
                spool.median("series")
            self.assertEqual(spool.stored_bytes, 16)
            self.assertEqual(spool.temporary_bytes, 0)
            self.assertEqual(spool.owned_bytes, 16)
            self.assertEqual(
                tuple(directory.glob(".sort-*.f64")),
                (),
            )
            self.assertEqual(tuple(spool.values("series")), (2.0, 1.0))

        with FloatSeriesSpool(self.parent, maximum_bytes=32) as spool:
            spool.append("series", 2.0)
            spool.append("series", 1.0)
            self.assertEqual(spool.median("series"), 1.5)
            self.assertEqual(spool.temporary_bytes, 0)
            self.assertEqual(spool.owned_bytes, 16)

    def test_primary_flush_failure_rolls_back_append_and_partial_file(self):
        spool = FloatSeriesSpool(
            self.parent,
            maximum_bytes=_TEST_MAXIMUM_BYTES,
        )
        spool.__enter__()
        directory = spool.directory

        def fail_after_partial_write(destination, values):
            destination.write(memoryview(values).cast("B")[:4])
            raise OSError("injected numeric write failure")

        try:
            with (
                patch(
                    "datavis.research.fresh_numeric_spool._BUFFER_VALUES",
                    1,
                ),
                patch(
                    "datavis.research.fresh_numeric_spool._write_array",
                    side_effect=fail_after_partial_write,
                ),
            ):
                with self.assertRaisesRegex(OSError, "injected"):
                    spool.append("series", 7.0)
            self.assertEqual(spool.count("series"), 0)
            self.assertEqual(spool.keys, ())
            self.assertEqual(spool.stored_bytes, 0)
            self.assertEqual(spool.temporary_bytes, 0)
            self.assertEqual(tuple(directory.iterdir()), ())
        finally:
            spool.close()
        self.assertFalse(directory.exists())

    def test_external_run_failure_removes_partial_temporary_files(self):
        spool = FloatSeriesSpool(
            self.parent,
            maximum_bytes=_TEST_MAXIMUM_BYTES,
        )
        spool.__enter__()
        directory = spool.directory
        try:
            for value in (3.0, 1.0, 2.0):
                spool.append("series", value)
            self.assertEqual(tuple(spool.values("series")), (3.0, 1.0, 2.0))

            def fail_after_partial_write(destination, values):
                destination.write(memoryview(values).cast("B")[:4])
                raise OSError("injected sorted-run failure")

            with patch(
                "datavis.research.fresh_numeric_spool._write_array",
                side_effect=fail_after_partial_write,
            ):
                with self.assertRaisesRegex(OSError, "sorted-run"):
                    spool.median("series")
            self.assertEqual(spool.stored_bytes, 24)
            self.assertEqual(spool.temporary_bytes, 0)
            self.assertEqual(spool.owned_bytes, 24)
            self.assertEqual(tuple(directory.glob(".sort-*.f64")), ())
            self.assertEqual(tuple(spool.values("series")), (3.0, 1.0, 2.0))
        finally:
            spool.close()
        self.assertFalse(directory.exists())

    def test_median_preserves_statistics_signed_zero_bits_and_hash(self):
        cases = (
            (-0.0,),
            (-0.0, -0.0),
            (-1.0, -0.0, 0.0),
            (-0.0, 0.0, -0.0),
            (0.0, -0.0),
        )
        for ordinal, values in enumerate(cases):
            with self.subTest(values=values):
                expected = float(statistics.median(values))
                with FloatSeriesSpool(
                    self.parent,
                    maximum_bytes=_TEST_MAXIMUM_BYTES,
                ) as spool:
                    for value in values:
                        spool.append(f"series-{ordinal}", value)
                    actual = spool.median(f"series-{ordinal}")
                assert actual is not None
                self.assertEqual(
                    struct.pack(">d", actual),
                    struct.pack(">d", expected),
                )
                self.assertEqual(
                    canonical_hash({"median": actual}),
                    canonical_hash({"median": expected}),
                )

    def test_stable_signed_zero_order_is_preserved_across_sorted_runs(self):
        values = (-3.0, -0.0, 4.0, 0.0, -0.0, 8.0, 0.0)
        expected = float(statistics.median(values))
        with (
            patch(
                "datavis.research.fresh_numeric_spool._SORT_RUN_VALUES",
                2,
            ),
            patch(
                "datavis.research.fresh_numeric_spool._RUN_READ_VALUES",
                1,
            ),
            FloatSeriesSpool(
                self.parent,
                maximum_bytes=_TEST_MAXIMUM_BYTES,
            ) as spool,
        ):
            for value in values:
                spool.append("series", value)
            actual = spool.median("series")
        assert actual is not None
        self.assertEqual(
            struct.pack(">d", actual),
            struct.pack(">d", expected),
        )
        self.assertEqual(
            canonical_hash({"median": actual}),
            canonical_hash({"median": expected}),
        )

    def test_many_runs_use_bounded_stable_merge_and_exact_peak_preflight(self):
        from datavis.research.fresh_numeric_spool import _FloatRunReader

        class AuditedRunReader(_FloatRunReader):
            active = 0
            maximum_active = 0

            def __init__(self, path, value_count):
                super().__init__(path, value_count)
                self._audit_closed = False
                type(self).active += 1
                type(self).maximum_active = max(
                    type(self).maximum_active,
                    type(self).active,
                )

            def close(self):
                if not self._audit_closed:
                    self._audit_closed = True
                    type(self).active -= 1
                super().close()

        values = (9.0, -0.0, 7.0, 0.0, 5.0, -0.0, 3.0, 0.0, 1.0)
        patches = (
            patch(
                "datavis.research.fresh_numeric_spool._SORT_RUN_VALUES",
                1,
            ),
            patch(
                "datavis.research.fresh_numeric_spool._RUN_READ_VALUES",
                1,
            ),
            patch(
                "datavis.research.fresh_numeric_spool._MERGE_FAN_IN",
                2,
            ),
            patch(
                "datavis.research.fresh_numeric_spool._FloatRunReader",
                AuditedRunReader,
            ),
        )
        with patches[0], patches[1], patches[2], patches[3]:
            with FloatSeriesSpool(self.parent, maximum_bytes=207) as spool:
                for value in values:
                    spool.append("series", value)
                with self.assertRaises(NumericSpoolCapacityError):
                    spool.median("series")
                self.assertEqual(spool.temporary_bytes, 0)
                self.assertEqual(
                    tuple(spool.directory.glob(".sort-*.f64")),
                    (),
                )

            with FloatSeriesSpool(self.parent, maximum_bytes=208) as spool:
                for value in values:
                    spool.append("series", value)
                actual_median = spool.median("series")
                actual_quantile = spool.quantile("series", 0.37)
                self.assertEqual(spool.temporary_bytes, 0)

        assert actual_median is not None
        assert actual_quantile is not None
        self.assertEqual(
            struct.pack(">d", actual_median),
            struct.pack(">d", float(statistics.median(values))),
        )
        self.assertEqual(
            struct.pack(">d", actual_quantile),
            struct.pack(
                ">d",
                float(
                    np.quantile(
                        np.asarray(values, dtype=np.float64),
                        0.37,
                        method="linear",
                    )
                ),
            ),
        )
        self.assertLessEqual(AuditedRunReader.maximum_active, 2)
        self.assertEqual(AuditedRunReader.active, 0)

    def test_missing_owned_run_is_deaccounted_while_failing_closed(self):
        with FloatSeriesSpool(
            self.parent,
            maximum_bytes=_TEST_MAXIMUM_BYTES,
        ) as spool:
            run_path = spool.directory / ".sort-owned-test.f64"
            run_path.write_bytes(struct.pack(">d", 1.0))
            spool._temporary_bytes = 8
            owned_runs = {run_path: 8}
            run_path.unlink()
            with self.assertRaisesRegex(RuntimeError, "disappeared"):
                spool._discard_run(
                    (run_path, 1, 8),
                    owned_runs=owned_runs,
                    missing_is_error=True,
                )
            self.assertEqual(owned_runs, {})
            self.assertEqual(spool.temporary_bytes, 0)
            self.assertEqual(spool.owned_bytes, 0)

    def test_randomized_median_and_quantile_are_bit_exact(self):
        rng = random.Random(20260725)
        probabilities = (0.0, 0.01, 0.10, 0.37, 0.50, 0.90, 0.99, 1.0)
        for case_index in range(20):
            count = rng.randrange(1, 80)
            values = [
                rng.uniform(-10_000.0, 10_000.0)
                for _ in range(count)
            ]
            # Exercise duplicate ordering without introducing signed-zero
            # ambiguity into NumPy's intentionally unstable partition.
            if count > 4:
                values[-1] = values[1]
                values[-2] = values[1]
            with FloatSeriesSpool(
                self.parent,
                maximum_bytes=_TEST_MAXIMUM_BYTES,
            ) as spool:
                for value in values:
                    spool.append("series", value)
                actual_median = spool.median("series")
                assert actual_median is not None
                expected_median = float(statistics.median(values))
                self.assertEqual(
                    struct.pack(">d", actual_median),
                    struct.pack(">d", expected_median),
                )
                for probability in probabilities:
                    with self.subTest(
                        case_index=case_index,
                        probability=probability,
                    ):
                        actual = spool.quantile("series", probability)
                        assert actual is not None
                        expected = float(
                            np.quantile(
                                np.asarray(values, dtype=np.float64),
                                probability,
                                method="linear",
                            )
                        )
                        self.assertEqual(
                            struct.pack(">d", actual),
                            struct.pack(">d", expected),
                        )


if __name__ == "__main__":
    unittest.main()
