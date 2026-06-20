from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from admin_projects import EchoStatsError, load_echo_stats, render_echo_stats


SCHEMA = """
CREATE TABLE users (user_id INTEGER PRIMARY KEY, is_adult INTEGER, is_banned INTEGER);
CREATE TABLE queue (user_id INTEGER PRIMARY KEY);
CREATE TABLE active_chats (user_id INTEGER PRIMARY KEY);
CREATE TABLE dialogs (dialog_id INTEGER PRIMARY KEY);
CREATE TABLE reports (report_id INTEGER PRIMARY KEY);
"""


class EchoStatsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database_path = Path(self.temp_dir.name) / "echo.db"
        connection = sqlite3.connect(self.database_path)
        try:
            connection.executescript(SCHEMA)
            connection.executemany(
                "INSERT INTO users VALUES (?, ?, ?)",
                [(1, 1, 0), (2, 0, 1)],
            )
            connection.execute("INSERT INTO queue VALUES (1)")
            connection.executemany("INSERT INTO active_chats VALUES (?)", [(1,), (2,)])
            connection.executemany("INSERT INTO dialogs VALUES (?)", [(1,), (2,), (3,)])
            connection.execute("INSERT INTO reports VALUES (1)")
            connection.commit()
        finally:
            connection.close()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_loads_core_stats_from_older_database(self) -> None:
        stats = load_echo_stats(self.database_path)

        self.assertEqual(stats["users"], 2)
        self.assertEqual(stats["adults"], 1)
        self.assertEqual(stats["active_dialogs"], 1)
        self.assertEqual(stats["dialogs"], 3)
        self.assertEqual(stats["sources"], [])

    def test_loads_and_escapes_advertising_sources(self) -> None:
        connection = sqlite3.connect(self.database_path)
        try:
            connection.execute("CREATE TABLE start_sources (user_id INTEGER, source TEXT)")
            connection.executemany(
                "INSERT INTO start_sources VALUES (?, ?)",
                [(1, "channel&one"), (2, "channel&one")],
            )
            connection.commit()
        finally:
            connection.close()

        stats = load_echo_stats(self.database_path)
        text = render_echo_stats(stats, now=datetime(2026, 6, 20, 12, 30))

        self.assertEqual(stats["sources"], [("channel&one", 2)])
        self.assertIn("channel&amp;one", text)
        self.assertIn("20.06.2026 12:30", text)

    def test_missing_database_has_clear_error(self) -> None:
        with self.assertRaisesRegex(EchoStatsError, "База данных не найдена"):
            load_echo_stats(Path(self.temp_dir.name) / "missing.db")


if __name__ == "__main__":
    unittest.main()
