from __future__ import annotations

import gc
import re
import unittest
import weakref
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import patch

import datavis.research.fresh_spool as spool_module
from datavis.research.fresh_spool import (
    KeyedObjectSpool,
    SPOOL_DIRECTORY_PREFIX,
    SpoolStateError,
)


@dataclass(eq=True)
class EmptyPayload:
    pass


@dataclass(eq=True)
class RichPayload:
    name: str
    values: tuple


class IntendedFailure(Exception):
    pass


class FreshSpoolTests(unittest.TestCase):
    def setUp(self):
        self.parent = Path(__file__).resolve().parent

    def test_round_trip_empty_stream_and_dataclass_like_objects(self):
        with KeyedObjectSpool(self.parent) as spool:
            directory = spool.directory
            self.assertEqual(directory.parent, self.parent)
            self.assertTrue(directory.name.startswith(SPOOL_DIRECTORY_PREFIX))

            spool.register_key("empty-stream")
            spool.append("payloads", EmptyPayload())
            spool.append("payloads", RichPayload("sample", (1, 2, 3)))

            self.assertEqual(
                spool.inventory,
                (("empty-stream", 0), ("payloads", 2)),
            )
            with spool.load("empty-stream") as objects:
                self.assertEqual(tuple(objects), ())
            with spool.load("payloads") as objects:
                self.assertEqual(
                    tuple(objects),
                    (EmptyPayload(), RichPayload("sample", (1, 2, 3))),
                )

        self.assertFalse(directory.exists())

    def test_parent_directory_is_optional(self):
        with patch(
            "datavis.research.fresh_spool.tempfile.gettempdir",
            return_value=str(self.parent),
        ):
            with KeyedObjectSpool() as spool:
                directory = spool.directory
                spool.append("candidate", EmptyPayload())

        self.assertEqual(directory.parent, self.parent)
        self.assertFalse(directory.exists())

    def test_interleaved_keys_preserve_per_key_order_and_duplicates(self):
        with KeyedObjectSpool(self.parent) as spool:
            spool.append("z-candidate", RichPayload("z-first", (1,)))
            spool.append("a-candidate", RichPayload("a-first", (2,)))
            spool.append("z-candidate", RichPayload("z-second", (3,)))
            duplicate = RichPayload("duplicate", (4,))
            spool.append("a-candidate", duplicate)
            spool.append("a-candidate", duplicate)

            self.assertEqual(spool.keys, ("a-candidate", "z-candidate"))
            self.assertEqual(
                spool.inventory,
                (("a-candidate", 3), ("z-candidate", 2)),
            )
            with spool.load("z-candidate") as objects:
                self.assertEqual(
                    tuple(objects),
                    (
                        RichPayload("z-first", (1,)),
                        RichPayload("z-second", (3,)),
                    ),
                )
            with spool.load("a-candidate") as objects:
                self.assertEqual(
                    tuple(objects),
                    (
                        RichPayload("a-first", (2,)),
                        RichPayload("duplicate", (4,)),
                        RichPayload("duplicate", (4,)),
                    ),
                )

    def test_arbitrary_candidate_id_is_never_used_as_a_path(self):
        unsafe_key = "../../candidate/with\\separators:*?<>|"
        with KeyedObjectSpool(self.parent) as spool:
            spool.append(unsafe_key, RichPayload("safe", ()))

            files = tuple(spool.directory.iterdir())
            self.assertEqual(len(files), 1)
            self.assertEqual(files[0].parent, spool.directory)
            self.assertRegex(files[0].name, re.compile(r"^[0-9a-f]{64}\.pickle$"))
            self.assertNotIn("candidate", files[0].name)

    def test_append_does_not_retain_the_callers_object(self):
        with KeyedObjectSpool(self.parent) as spool:
            payload = RichPayload("collectable", (1, 2))
            reference = weakref.ref(payload)
            spool.append("candidate", payload)

            del payload
            gc.collect()

            self.assertIsNone(reference())
            with spool.load("candidate") as objects:
                self.assertEqual(
                    tuple(objects),
                    (RichPayload("collectable", (1, 2)),),
                )

    def test_large_round_trip_never_uses_whole_object_buffer_helpers(self):
        payload = RichPayload("large", tuple(range(200_000)))
        with (
            patch.object(
                spool_module.pickle,
                "dumps",
                side_effect=AssertionError("whole pickle buffer used"),
            ),
            patch.object(
                spool_module.zlib,
                "compress",
                side_effect=AssertionError("whole compression buffer used"),
            ),
            patch.object(
                spool_module.pickle,
                "loads",
                side_effect=AssertionError("whole pickle load used"),
            ),
            patch.object(
                spool_module.zlib,
                "decompress",
                side_effect=AssertionError("whole decompression buffer used"),
            ),
        ):
            with KeyedObjectSpool(self.parent) as spool:
                spool.append("candidate", payload)
                with spool.load("candidate") as objects:
                    self.assertEqual(tuple(objects), (payload,))

    def test_failed_streaming_append_rolls_back_bytes_and_inventory(self):
        original_write = spool_module._CompressedPickleWriter.write
        with KeyedObjectSpool(self.parent) as spool:
            first = RichPayload("first", tuple(range(1_000)))
            spool.append("candidate", first)
            path = next(spool.directory.iterdir())
            original_bytes = path.read_bytes()

            def write_then_fail(writer, payload):
                original_write(writer, payload)
                raise IntendedFailure("streaming append failed")

            with patch.object(
                spool_module._CompressedPickleWriter,
                "write",
                autospec=True,
                side_effect=write_then_fail,
            ):
                with self.assertRaisesRegex(
                    IntendedFailure,
                    "streaming append failed",
                ):
                    spool.append(
                        "candidate",
                        RichPayload("failed", tuple(range(10_000))),
                    )

            self.assertEqual(spool.count("candidate"), 1)
            self.assertEqual(path.read_bytes(), original_bytes)
            with spool.load("candidate") as objects:
                self.assertEqual(tuple(objects), (first,))

    def test_only_one_key_can_be_loaded_at_a_time(self):
        with KeyedObjectSpool(self.parent) as spool:
            spool.append("first", RichPayload("one", ()))
            spool.append("second", RichPayload("two", ()))

            with spool.load("first") as first_objects:
                self.assertEqual(next(first_objects), RichPayload("one", ()))
                with self.assertRaisesRegex(
                    SpoolStateError,
                    "only one key",
                ):
                    with spool.load("second"):
                        pass
                with self.assertRaisesRegex(
                    SpoolStateError,
                    "cannot append",
                ):
                    spool.append("second", RichPayload("three", ()))

            with spool.load("second") as second_objects:
                self.assertEqual(tuple(second_objects), (RichPayload("two", ()),))

    def test_cleanup_occurs_when_user_code_raises(self):
        directory = None
        with self.assertRaises(IntendedFailure):
            with KeyedObjectSpool(self.parent) as spool:
                spool.append("candidate", RichPayload("one", ()))
                directory = spool.directory
                raise IntendedFailure("abort research batch")

        assert directory is not None
        self.assertFalse(directory.exists())

    def test_use_outside_context_and_reentry_are_rejected(self):
        spool = KeyedObjectSpool(self.parent)
        with self.assertRaisesRegex(SpoolStateError, "context manager"):
            spool.append("candidate", EmptyPayload())

        with spool:
            pass

        with self.assertRaisesRegex(SpoolStateError, "entered only once"):
            with spool:
                pass
        with self.assertRaisesRegex(SpoolStateError, "closed"):
            spool.append("candidate", EmptyPayload())


if __name__ == "__main__":
    unittest.main()
