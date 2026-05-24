"""PostgreSQL bootstrap helpers for BeeAware.

This module creates the minimal schema needed by the stream ingestion layer
and exposes a small repository compatible with the current camera workflow.
"""

from __future__ import annotations

import os
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from getpass import getpass
from pathlib import Path
from typing import Iterator, Optional
from urllib.parse import urlparse

import psycopg2


SCHEMA_SQL = """
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS users (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	email VARCHAR(255) NOT NULL UNIQUE,
	password_hash VARCHAR(255) NOT NULL,
	first_name VARCHAR(100) NULL,
	last_name VARCHAR(100) NULL,
	ui_theme VARCHAR(50) NULL,
	is_active BOOLEAN NOT NULL DEFAULT TRUE,
	created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS cameras (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	name VARCHAR(255) NULL,
	rtsp_url VARCHAR(1024) NOT NULL UNIQUE,
	status VARCHAR(50) NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS detections (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	camera_id UUID NOT NULL REFERENCES cameras(id) ON DELETE CASCADE,
	reviewed_by UUID NULL REFERENCES users(id),
	hornet_count INTEGER NULL,
	confidence_score DOUBLE PRECISION NULL,
	is_false_positive BOOLEAN NOT NULL DEFAULT FALSE,
	detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cameras_rtsp_url ON cameras (rtsp_url);
CREATE INDEX IF NOT EXISTS idx_detections_camera_id ON detections (camera_id);

ALTER TABLE detections ADD COLUMN IF NOT EXISTS snapshot_path VARCHAR(1024) NULL;
ALTER TABLE cameras ADD COLUMN IF NOT EXISTS user_id UUID REFERENCES users(id) ON DELETE SET NULL;
ALTER TABLE users ADD COLUMN IF NOT EXISTS notifications_read_at TIMESTAMPTZ NULL;
"""


@dataclass(frozen=True)
class PostgresSettings:
	host: str
	port: int
	database: str
	user: str
	password: str


def _resolve_password(raw_password: str) -> str:
	if raw_password:
		return raw_password

	# Allow interactive password input when env vars are not configured.
	if sys.stdin is not None and sys.stdin.isatty():
		return getpass("PostgreSQL password: ")

	return raw_password


def _load_env_file(env_path: Path) -> None:
	if not env_path.exists():
		return

	# Read file bytes and try multiple encodings to avoid UnicodeDecodeError
	raw = None
	try:
		raw = env_path.read_bytes()
	except Exception:
		return

	text = None
	for enc in ("utf-8", "cp1252", "latin-1"):
		try:
			text = raw.decode(enc)
			break
		except UnicodeDecodeError:
			continue
	if text is None:
		# fallback with replacement to avoid crashing
		text = raw.decode("utf-8", errors="replace")

	for raw_line in text.splitlines():
		line = raw_line.strip()
		if not line or line.startswith("#") or "=" not in line:
			continue

		key, value = line.split("=", 1)
		key = key.strip()
		value = value.strip().strip('"').strip("'")
		if key and key not in os.environ:
			os.environ[key] = value


def _load_environment() -> None:
	project_root = Path(__file__).resolve().parents[2]
	_load_env_file(project_root / ".env")
	_load_env_file(project_root / "BeeAware-BackEnd" / ".env")
	_load_env_file(project_root / "BeeAware-BackEnd" / "DB" / ".env")


def _read_database_url(database_url: str) -> PostgresSettings:
	parsed = urlparse(database_url)
	if parsed.scheme not in {"postgres", "postgresql"}:
		raise ValueError("DATABASE_URL must use the postgres or postgresql scheme")

	if not parsed.hostname or not parsed.path:
		raise ValueError("DATABASE_URL is missing host or database name")

	return PostgresSettings(
		host=parsed.hostname,
		port=parsed.port or 5432,
		database=parsed.path.lstrip("/"),
		user=parsed.username or "postgres",
		password=parsed.password or "",
	)


def get_postgres_settings() -> PostgresSettings:
	"""Load PostgreSQL settings from the environment.

	Supports either DATABASE_URL or the standard PG* variables.
	"""

	_load_environment()

	database_url = os.getenv("DATABASE_URL")
	if database_url:
		return _read_database_url(database_url)

	return PostgresSettings(
		host=os.getenv("PGHOST", "localhost"),
		port=int(os.getenv("PGPORT", "5432")),
		database=os.getenv("PGDATABASE", "beeaware"),
		user=os.getenv("PGUSER", "postgres"),
		password=_resolve_password(os.getenv("PGPASSWORD", "")),
	)


@contextmanager
def get_connection() -> Iterator[psycopg2.extensions.connection]:
	settings = get_postgres_settings()
	try:
		os.environ.setdefault("PGCLIENTENCODING", "UTF8")
		connection = psycopg2.connect(
			host=settings.host,
			port=settings.port,
			dbname=settings.database,
			user=settings.user,
			password=settings.password,
			options="-c lc_messages=C",
		)
	except UnicodeDecodeError as ude:
		# Provide a clearer message for common encoding issues (accented passwords, non-UTF8 .env)
		hint = (
			"PostgreSQL connection failed: the server locale is set to Windows-1252 (French) but "
			"psycopg2 expects UTF-8. This usually means the password is wrong.\n\n"
			"If you forgot your PostgreSQL password, run as Administrator:\n"
			"  powershell -ExecutionPolicy Bypass -File BeeAware-BackEnd/reset_pg_password.ps1\n\n"
			"Original error: " + str(ude)
		)
		raise RuntimeError(hint) from ude
	except Exception:
		# Re-raise other connection errors normally
		raise

	try:
		yield connection
	finally:
		connection.close()


def initialize_database() -> None:
	"""Create the base schema required by the application."""

	with get_connection() as connection:
		with connection.cursor() as cursor:
			cursor.execute(SCHEMA_SQL)
		connection.commit()


class PostgresCameraRepository:
	"""Repository used by the stream ingestion bootstrap."""

	def delete_camera(self, camera_id: str) -> bool:
		with get_connection() as connection:
			with connection.cursor() as cursor:
				cursor.execute(
					"""
					DELETE FROM cameras WHERE id = %s
					""",
					(camera_id,),
				)
				deleted = cursor.rowcount > 0
			connection.commit()
		return deleted

	def get_all_active_cameras(self):
		with get_connection() as connection:
			with connection.cursor() as cursor:
				cursor.execute(
					"""
					SELECT c.id, c.name, c.rtsp_url, c.status, c.user_id
					FROM cameras c
					WHERE COALESCE(c.status, 'active') = 'active'
					ORDER BY c.created_at ASC
					"""
				)
				return [
					{"id": str(row[0]), "name": row[1] or "Caméra sans nom", "rtsp_url": row[2], "status": "online", "user_id": str(row[3]) if row[3] else None}
					for row in cursor.fetchall()
				]

	def get_camera_by_id(self, camera_id: str):
		with get_connection() as connection:
			with connection.cursor() as cursor:
				cursor.execute(
					"""
					SELECT id, name, rtsp_url, status, user_id
					FROM cameras
					WHERE id = %s
					""",
					(camera_id,),
				)
				row = cursor.fetchone()
				if not row:
					return None
				return {"id": str(row[0]), "name": row[1] or "Caméra sans nom", "rtsp_url": row[2], "status": "online", "user_id": str(row[3]) if row[3] else None}

	def add_camera(self, name: str, url: str, cam_id: str = None, user_id: Optional[str] = None):
		with get_connection() as connection:
			with connection.cursor() as cursor:
				if cam_id:
					cursor.execute(
						"""
						INSERT INTO cameras (id, name, rtsp_url, status, user_id)
						VALUES (%s::uuid, %s, %s, 'active', %s::uuid)
						ON CONFLICT (rtsp_url)
						DO UPDATE SET
							name = COALESCE(%s, cameras.name),
							status = 'active'
						RETURNING id
						""",
						(cam_id, name, url, user_id, name),
					)
				else:
					cursor.execute(
						"""
						INSERT INTO cameras (name, rtsp_url, status, user_id)
						VALUES (%s, %s, 'active', %s::uuid)
						ON CONFLICT (rtsp_url)
						DO UPDATE SET
							name = COALESCE(%s, cameras.name),
							status = 'active'
						RETURNING id
						""",
						(name, url, user_id, name),
					)
				row = cursor.fetchone()
			connection.commit()
		return str(row[0]) if row else None

	def get_all_cameras(self):
		with get_connection() as connection:
			with connection.cursor() as cursor:
				cursor.execute(
					"""
					SELECT c.id, c.name, c.rtsp_url, c.status, c.created_at, c.user_id, u.email as user_email
					FROM cameras c
					LEFT JOIN users u ON u.id = c.user_id
					ORDER BY c.created_at DESC
					"""
				)
				return [
					{
						"id": str(row[0]),
						"name": row[1] or "Sans nom",
						"rtsp_url": row[2],
						"status": row[3] or "inactive",
						"created_at": row[4].isoformat() if row[4] else None,
						"user_id": str(row[5]) if row[5] else None,
						"user_email": row[6],
					}
					for row in cursor.fetchall()
				]


class PostgresRepository(PostgresCameraRepository):
	"""Combined repository for cameras, users and detections."""

	def create_user(self, email: str, password_hash: str, first_name: Optional[str] = None, last_name: Optional[str] = None, ui_theme: Optional[str] = None):
		with get_connection() as connection:
			with connection.cursor() as cursor:
				cursor.execute(
					"""
					INSERT INTO users (email, password_hash, first_name, last_name, ui_theme)
					VALUES (%s, %s, %s, %s, %s)
					ON CONFLICT (email) DO NOTHING
					RETURNING id
					""",
					(email, password_hash, first_name, last_name, ui_theme),
				)
				row = cursor.fetchone()
			connection.commit()
		return row[0] if row else None

	def add_detection(self, camera_id: str, reviewed_by: Optional[str], hornet_count: Optional[int], confidence_score: Optional[float], is_false_positive: bool = False, detected_at: Optional[str] = None, snapshot_path: Optional[str] = None):
		with get_connection() as connection:
			with connection.cursor() as cursor:
				cursor.execute(
					"""
					INSERT INTO detections (camera_id, reviewed_by, hornet_count, confidence_score, is_false_positive, detected_at, snapshot_path)
					VALUES (%s, %s, %s, %s, %s, COALESCE(%s, NOW()), %s)
					RETURNING id
					""",
					(camera_id, reviewed_by, hornet_count, confidence_score, is_false_positive, detected_at, snapshot_path),
				)
				row = cursor.fetchone()
			connection.commit()
		return row[0] if row else None

	def get_detections(self, camera_id: Optional[str] = None, limit: int = 50):
		with get_connection() as connection:
			with connection.cursor() as cursor:
				if camera_id:
					cursor.execute(
						"""
						SELECT d.id, d.camera_id, d.hornet_count, d.confidence_score, d.detected_at, d.snapshot_path
						FROM detections d
						WHERE d.camera_id = %s
						ORDER BY d.detected_at DESC
						LIMIT %s
						""",
						(camera_id, limit),
					)
				else:
					cursor.execute(
						"""
						SELECT d.id, d.camera_id, d.hornet_count, d.confidence_score, d.detected_at, d.snapshot_path
						FROM detections d
						ORDER BY d.detected_at DESC
						LIMIT %s
						""",
						(limit,),
					)
				return [
					{
						"id": str(row[0]),
						"camera_id": str(row[1]),
						"hornet_count": row[2],
						"confidence_score": row[3],
						"detected_at": row[4].isoformat() if row[4] else None,
						"snapshot_path": row[5],
					}
					for row in cursor.fetchall()
				]

	def clear_detections(self, camera_id: Optional[str] = None):
		with get_connection() as connection:
			with connection.cursor() as cursor:
				if camera_id:
					cursor.execute("DELETE FROM detections WHERE camera_id = %s", (camera_id,))
				else:
					cursor.execute("DELETE FROM detections")
			connection.commit()

	def admin_delete_user(self, user_id: str):
		with get_connection() as connection:
			with connection.cursor() as cursor:
				cursor.execute("DELETE FROM cameras WHERE user_id = %s", (user_id,))
				cursor.execute("DELETE FROM users WHERE id = %s", (user_id,))
			connection.commit()

	def admin_delete_camera(self, camera_id: str):
		with get_connection() as connection:
			with connection.cursor() as cursor:
				cursor.execute("DELETE FROM cameras WHERE id = %s", (camera_id,))
			connection.commit()

	def admin_delete_detection(self, detection_id: str):
		with get_connection() as connection:
			with connection.cursor() as cursor:
				cursor.execute("DELETE FROM detections WHERE id = %s", (detection_id,))
			connection.commit()

	def mark_notifications_read(self, email: str):
		with get_connection() as connection:
			with connection.cursor() as cursor:
				cursor.execute(
					"""
					UPDATE users SET notifications_read_at = NOW()
					WHERE email = %s
					""",
					(email,)
				)
			connection.commit()

	def get_user_by_email(self, email: str):
		with get_connection() as connection:
			with connection.cursor() as cursor:
				cursor.execute(
					"""
					SELECT id, email, password_hash, first_name, last_name, ui_theme, is_active, created_at, notifications_read_at
					FROM users
					WHERE email = %s
					""",
					(email,)
				)
				row = cursor.fetchone()
				if not row:
					return None
				return {
					"id": row[0],
					"email": row[1],
					"password_hash": row[2],
					"first_name": row[3],
					"last_name": row[4],
					"ui_theme": row[5],
					"is_active": row[6],
					"created_at": row[7],
					"notifications_read_at": row[8].isoformat() if row[8] else None,
				}

	def get_all_users(self):
		with get_connection() as connection:
			with connection.cursor() as cursor:
				cursor.execute(
					"""
					SELECT id, email, first_name, last_name, is_active, created_at
					FROM users
					ORDER BY created_at DESC
					"""
				)
				return [
					{
						"id": str(row[0]),
						"email": row[1],
						"first_name": row[2],
						"last_name": row[3],
						"is_active": row[4],
						"created_at": row[5].isoformat() if row[5] else None,
					}
					for row in cursor.fetchall()
				]
def get_database_repository() -> Optional[PostgresCameraRepository]:
	try:
		initialize_database()
	except Exception as exc:
		print(f"[ERROR] PostgreSQL initialisation failed: {exc}")
		return None

	return PostgresRepository()


def main() -> None:
	initialize_database()
	print("[INFO] PostgreSQL schema initialised successfully.")


if __name__ == "__main__":
	main()
