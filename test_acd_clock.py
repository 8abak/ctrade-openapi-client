from datetime import date, datetime, timezone

from datavis.app import _latest_locked_acd_session, _new_york_acd_window


def test_new_york_acd_window_is_0930_to_0945_with_dst() -> None:
    opening_start, opening_end = _new_york_acd_window(date(2026, 9, 15))

    assert opening_start == datetime(2026, 9, 15, 13, 30, tzinfo=timezone.utc)
    assert opening_end == datetime(2026, 9, 15, 13, 45, tzinfo=timezone.utc)


def test_second_new_york_quarter_hour_activates_new_acd() -> None:
    before_lock = datetime(2026, 9, 15, 13, 44, tzinfo=timezone.utc)
    at_lock = datetime(2026, 9, 15, 13, 45, tzinfo=timezone.utc)

    previous_day, _, _ = _latest_locked_acd_session(before_lock)
    current_day, _, _ = _latest_locked_acd_session(at_lock)

    assert previous_day == date(2026, 9, 14)
    assert current_day == date(2026, 9, 15)


def test_monday_before_lock_carries_friday_acd() -> None:
    monday_before_lock = datetime(2026, 9, 21, 13, 40, tzinfo=timezone.utc)

    session_day, _, _ = _latest_locked_acd_session(monday_before_lock)

    assert session_day == date(2026, 9, 18)
