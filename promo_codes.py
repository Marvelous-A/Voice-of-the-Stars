import re
import secrets
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


PROMO_CODE_RE = re.compile(r"^[A-Z0-9][A-Z0-9_-]{2,31}$")
PROMO_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"


class PromoCodeError(Exception):
    """Base promo code storage error."""


class DuplicatePromoCode(PromoCodeError):
    """Raised when an admin tries to create an existing promo code."""


@dataclass(frozen=True)
class PromoActivationResult:
    ok: bool
    code: str
    reason: str = ""
    sessions: int = 0
    balance: int = 0


def normalize_promo_code(raw: str) -> str:
    code = re.sub(r"\s+", "", (raw or "").strip().upper())
    if not PROMO_CODE_RE.fullmatch(code):
        raise ValueError("Promo code must contain 3-32 latin letters, digits, hyphen or underscore.")
    return code


def _now_iso() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


class PromoCodeStore:
    def __init__(self, path: str = "promo_codes.sqlite3"):
        self.path = Path(path)
        self.init_schema()

    def _connect(self) -> sqlite3.Connection:
        if self.path.parent != Path("."):
            self.path.parent.mkdir(parents=True, exist_ok=True)
        con = sqlite3.connect(self.path, timeout=20, isolation_level=None)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA foreign_keys = ON")
        con.execute("PRAGMA busy_timeout = 20000")
        con.execute("PRAGMA journal_mode = WAL")
        return con

    @contextmanager
    def _connection(self):
        con = self._connect()
        try:
            yield con
        finally:
            con.close()

    def init_schema(self) -> None:
        with self._connection() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS promo_codes (
                    code TEXT PRIMARY KEY,
                    sessions INTEGER NOT NULL CHECK (sessions > 0),
                    max_activations INTEGER NOT NULL CHECK (max_activations > 0),
                    expires_at TEXT,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    note TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    disabled_at TEXT
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS promo_activations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    username TEXT NOT NULL DEFAULT '',
                    full_name TEXT NOT NULL DEFAULT '',
                    sessions INTEGER NOT NULL CHECK (sessions > 0),
                    activated_at TEXT NOT NULL,
                    UNIQUE(code, user_id),
                    FOREIGN KEY(code) REFERENCES promo_codes(code)
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS promo_balances (
                    user_id TEXT PRIMARY KEY,
                    sessions INTEGER NOT NULL DEFAULT 0 CHECK (sessions >= 0),
                    updated_at TEXT NOT NULL
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS promo_ledger (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT NOT NULL,
                    delta INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    code TEXT,
                    created_at TEXT NOT NULL
                )
                """
            )

    def create_code(
        self,
        *,
        code: str | None = None,
        sessions: int,
        max_activations: int = 1,
        expires_at: str | None = None,
        note: str = "",
        prefix: str = "STAR",
    ) -> dict:
        sessions = int(sessions)
        max_activations = int(max_activations)
        if sessions <= 0:
            raise ValueError("sessions must be positive")
        if max_activations <= 0:
            raise ValueError("max_activations must be positive")
        if expires_at and _parse_dt(expires_at) is None:
            raise ValueError("expires_at must be ISO datetime")

        if code:
            candidates = [normalize_promo_code(code)]
        else:
            safe_prefix = self._safe_prefix(prefix)
            candidates = [
                f"{safe_prefix}-{self._random_suffix(6)}" if safe_prefix else self._random_suffix(8)
                for _ in range(100)
            ]

        created_at = _now_iso()
        with self._connection() as con:
            for idx, candidate in enumerate(candidates):
                try:
                    con.execute(
                        """
                        INSERT INTO promo_codes
                            (code, sessions, max_activations, expires_at, note, created_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (candidate, sessions, max_activations, expires_at, note.strip(), created_at),
                    )
                    return self.get_code(candidate) or {
                        "code": candidate,
                        "sessions": sessions,
                        "max_activations": max_activations,
                        "expires_at": expires_at,
                        "note": note.strip(),
                        "created_at": created_at,
                        "is_active": 1,
                        "activations_count": 0,
                    }
                except sqlite3.IntegrityError:
                    if code or idx == len(candidates) - 1:
                        raise DuplicatePromoCode(candidate)
        raise PromoCodeError("could not create promo code")

    def get_code(self, raw_code: str) -> dict | None:
        code = normalize_promo_code(raw_code)
        with self._connection() as con:
            row = con.execute(
                """
                SELECT
                    p.*,
                    COUNT(a.id) AS activations_count
                FROM promo_codes p
                LEFT JOIN promo_activations a ON a.code = p.code
                WHERE p.code = ?
                GROUP BY p.code
                """,
                (code,),
            ).fetchone()
        return dict(row) if row else None

    def list_codes(self, *, include_inactive: bool = False, limit: int = 20) -> list[dict]:
        where = "" if include_inactive else "WHERE p.is_active = 1"
        with self._connection() as con:
            rows = con.execute(
                f"""
                SELECT
                    p.*,
                    COUNT(a.id) AS activations_count
                FROM promo_codes p
                LEFT JOIN promo_activations a ON a.code = p.code
                {where}
                GROUP BY p.code
                ORDER BY p.created_at DESC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()
        return [dict(row) for row in rows]

    def disable_code(self, raw_code: str) -> bool:
        code = normalize_promo_code(raw_code)
        with self._connection() as con:
            cur = con.execute(
                """
                UPDATE promo_codes
                SET is_active = 0, disabled_at = ?
                WHERE code = ? AND is_active = 1
                """,
                (_now_iso(), code),
            )
        return cur.rowcount > 0

    def activate_code(
        self,
        raw_code: str,
        *,
        user_id: str,
        username: str = "",
        full_name: str = "",
    ) -> PromoActivationResult:
        try:
            code = normalize_promo_code(raw_code)
        except ValueError:
            return PromoActivationResult(ok=False, code="", reason="invalid_code")

        user_id = str(user_id)
        now = _now_iso()
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            promo = con.execute("SELECT * FROM promo_codes WHERE code = ?", (code,)).fetchone()
            if not promo:
                con.execute("ROLLBACK")
                return PromoActivationResult(ok=False, code=code, reason="not_found")
            if not int(promo["is_active"]):
                con.execute("ROLLBACK")
                return PromoActivationResult(ok=False, code=code, reason="inactive")
            expires_at = _parse_dt(promo["expires_at"])
            if expires_at and expires_at < datetime.now():
                con.execute("ROLLBACK")
                return PromoActivationResult(ok=False, code=code, reason="expired")

            existing = con.execute(
                "SELECT 1 FROM promo_activations WHERE code = ? AND user_id = ?",
                (code, user_id),
            ).fetchone()
            if existing:
                con.execute("ROLLBACK")
                return PromoActivationResult(ok=False, code=code, reason="already_used")

            activations_count = con.execute(
                "SELECT COUNT(*) FROM promo_activations WHERE code = ?",
                (code,),
            ).fetchone()[0]
            if int(activations_count) >= int(promo["max_activations"]):
                con.execute("ROLLBACK")
                return PromoActivationResult(ok=False, code=code, reason="exhausted")

            sessions = int(promo["sessions"])
            con.execute(
                """
                INSERT INTO promo_activations
                    (code, user_id, username, full_name, sessions, activated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (code, user_id, username or "", full_name or "", sessions, now),
            )
            con.execute(
                """
                INSERT INTO promo_balances (user_id, sessions, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    sessions = sessions + excluded.sessions,
                    updated_at = excluded.updated_at
                """,
                (user_id, sessions, now),
            )
            con.execute(
                """
                INSERT INTO promo_ledger (user_id, delta, reason, code, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (user_id, sessions, "activation", code, now),
            )
            balance = con.execute(
                "SELECT sessions FROM promo_balances WHERE user_id = ?",
                (user_id,),
            ).fetchone()[0]
            con.execute("COMMIT")
            return PromoActivationResult(
                ok=True,
                code=code,
                sessions=sessions,
                balance=int(balance),
            )
        except sqlite3.IntegrityError:
            con.execute("ROLLBACK")
            return PromoActivationResult(ok=False, code=code, reason="already_used")
        except Exception:
            con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def get_balance(self, user_id: str) -> int:
        with self._connection() as con:
            row = con.execute(
                "SELECT sessions FROM promo_balances WHERE user_id = ?",
                (str(user_id),),
            ).fetchone()
        return int(row["sessions"]) if row else 0

    def consume_session(self, user_id: str) -> int | None:
        user_id = str(user_id)
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute(
                "SELECT sessions FROM promo_balances WHERE user_id = ?",
                (user_id,),
            ).fetchone()
            if not row or int(row["sessions"]) <= 0:
                con.execute("ROLLBACK")
                return None
            new_balance = int(row["sessions"]) - 1
            now = _now_iso()
            con.execute(
                "UPDATE promo_balances SET sessions = ?, updated_at = ? WHERE user_id = ?",
                (new_balance, now, user_id),
            )
            con.execute(
                """
                INSERT INTO promo_ledger (user_id, delta, reason, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (user_id, -1, "session_spent", now),
            )
            con.execute("COMMIT")
            return new_balance
        except Exception:
            con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def summary(self) -> dict:
        with self._connection() as con:
            active_codes = con.execute(
                "SELECT COUNT(*) FROM promo_codes WHERE is_active = 1"
            ).fetchone()[0]
            total_codes = con.execute("SELECT COUNT(*) FROM promo_codes").fetchone()[0]
            activations = con.execute("SELECT COUNT(*) FROM promo_activations").fetchone()[0]
            balances = con.execute(
                "SELECT COALESCE(SUM(sessions), 0) FROM promo_balances"
            ).fetchone()[0]
        return {
            "active_codes": int(active_codes),
            "total_codes": int(total_codes),
            "activations": int(activations),
            "unused_sessions": int(balances or 0),
        }

    @staticmethod
    def _random_suffix(length: int) -> str:
        return "".join(secrets.choice(PROMO_ALPHABET) for _ in range(length))

    @staticmethod
    def _safe_prefix(prefix: str) -> str:
        prefix = re.sub(r"[^A-Z0-9_-]+", "", (prefix or "").upper()).strip("-_")
        return prefix[:16]
