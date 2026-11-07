"""
Módulo de tareas / recordatorios para el bot.

Características:
- Crear tareas con título, descripción y fecha/hora del recordatorio.
- Listar, consultar, actualizar, completar y eliminar tareas.
- Enviar automáticamente el recordatorio al canal donde se creó la tarea.
- Persistencia en SQLite (por defecto) o PostgreSQL (recomendado en Render).
- Si el bot estaba apagado cuando llegó la hora, al iniciar envía las tareas
  pendientes que ya vencieron.

Variables de entorno:
- TASK_DB_PATH: ruta del archivo SQLite. Por defecto "data/tasks.db".
- DATABASE_URL: URL de PostgreSQL. Si existe, tiene prioridad sobre SQLite.
- TASK_TIMEZONE: zona horaria para interpretar las fechas. Por defecto
  "America/Bogota". Ejemplos: "America/Bogota", "America/Mexico_City",
  "Europe/Madrid", "UTC".
- TASK_CHECK_INTERVAL: segundos entre revisiones de tareas vencidas.
  Por defecto 30.

Comandos:
- $task add <titulo> | <descripcion> | <fecha> <hora>
- $task list [pending|sent|done|cancelled|all]
- $task show <id>
- $task update <id> titulo=... | descripcion=... | fecha=... | estado=...
- $task done <id>
- $task delete <id>

Ejemplos:
- $task add Comprar leche | Leche entera y pan | 25/12/2026 18:30
- $task add "Pagar arriendo" | "Transferencia bancaria" | 01/01/2027 09:00
- $task update 3 titulo=Nuevo título | fecha=26/12/2026 10:00
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import shlex
import sqlite3
import threading
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import discord
from discord.ext import commands

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # pragma: no cover - solo se usa si DATABASE_URL está definido
    psycopg = None
    dict_row = None


logger = logging.getLogger(__name__)

STATUS_PENDING = "pending"
STATUS_SENT = "sent"
STATUS_DONE = "done"
STATUS_CANCELLED = "cancelled"

STATUS_LABELS = {
    STATUS_PENDING: "⏳ Pendiente",
    STATUS_SENT: "✅ Enviada",
    STATUS_DONE: "🏁 Completada",
    STATUS_CANCELLED: "🚫 Cancelada",
}

STATUS_ALIASES = {
    "pending": STATUS_PENDING,
    "pendiente": STATUS_PENDING,
    "pendientes": STATUS_PENDING,
    "sent": STATUS_SENT,
    "enviada": STATUS_SENT,
    "enviado": STATUS_SENT,
    "enviadas": STATUS_SENT,
    "done": STATUS_DONE,
    "completada": STATUS_DONE,
    "completado": STATUS_DONE,
    "completadas": STATUS_DONE,
    "cancelled": STATUS_CANCELLED,
    "canceled": STATUS_CANCELLED,
    "cancelada": STATUS_CANCELLED,
    "cancelado": STATUS_CANCELLED,
    "canceladas": STATUS_CANCELLED,
}

MAX_TITLE_LENGTH = 200
MAX_DESCRIPTION_LENGTH = 1800

DATETIME_FORMATS = (
    "%d/%m/%Y %H:%M",
    "%d/%m/%Y %H:%M:%S",
    "%d-%m-%Y %H:%M",
    "%d-%m-%Y %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d %H:%M:%S",
    "%Y/%m/%d %H:%M",
    "%Y/%m/%d %H:%M:%S",
    "%d/%m/%Y %I:%M %p",
    "%d/%m/%Y %I:%M%p",
    "%d-%m-%Y %I:%M %p",
    "%Y-%m-%d %I:%M %p",
)

DATE_TOKEN_RE = re.compile(r"^\d{1,4}[/-]\d{1,2}[/-]\d{1,4}$")
TIME_TOKEN_RE = re.compile(r"^\d{1,2}:\d{2}(:\d{2})?(\s*[ap]\.?\s*m\.?)?$", re.IGNORECASE)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_iso(value: datetime) -> str:
    """Convierte un datetime a ISO UTC con el mismo formato para SQLite y PostgreSQL."""
    return value.astimezone(timezone.utc).isoformat(timespec="seconds")


def from_iso(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    return datetime.fromisoformat(str(value)).astimezone(timezone.utc)


def normalize_status(value: str) -> str:
    key = value.strip().lower()
    status = STATUS_ALIASES.get(key)
    if status is None:
        valid = ", ".join(sorted({STATUS_PENDING, STATUS_SENT, STATUS_DONE, STATUS_CANCELLED}))
        raise ValueError(f"Estado no válido: {value!r}. Usa uno de: {valid}.")
    return status


def parse_status_filter(value: str) -> Optional[str]:
    key = value.strip().lower()
    if key in {"all", "todas", "todos", "cualquiera", "cualesquiera"}:
        return None
    return normalize_status(value)


def format_local(value: datetime, tz: ZoneInfo) -> str:
    tz_name = getattr(tz, "key", str(tz))
    return f"{value.astimezone(tz).strftime('%d/%m/%Y %H:%M')} ({tz_name})"


def _clean_part(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        value = value[1:-1].strip()
    return value


def _split_pipe_task(parts: list[str]) -> tuple[str, str, str]:
    """
    Separa los campos de un formato con pipes.

    Acepta tanto "titulo | descripcion | 25/12/2026 18:30" como
    "titulo | descripcion | 25/12/2026 | 18:30".
    """
    if (
        len(parts) >= 3
        and DATE_TOKEN_RE.match(parts[-2])
        and TIME_TOKEN_RE.match(parts[-1])
    ):
        due_raw = f"{parts[-2]} {parts[-1]}"
        middle = parts[1:-2]
    else:
        due_raw = parts[-1]
        middle = parts[1:-1]

    title = parts[0]
    description = " | ".join(middle).strip()
    return title, description, due_raw


def parse_due_at(value: str, tz: ZoneInfo) -> datetime:
    """Interpreta una fecha/hora escrita por el usuario y la devuelve en UTC."""
    raw = " ".join(value.strip().split())
    if not raw:
        raise ValueError("Debes indicar la fecha y la hora del recordatorio.")
    if ":" not in raw:
        raise ValueError(
            "La fecha debe incluir la hora. Ejemplo: 25/12/2026 18:30."
        )

    # Normaliza "6:30 p. m." / "6:30pm" a "6:30 PM" para strptime.
    raw = re.sub(
        r"\b([ap])\.?\s*m\.?",
        lambda match: match.group(1).upper() + "M",
        raw,
        flags=re.IGNORECASE,
    )

    parsed: Optional[datetime] = None

    # Formatos ISO: 2026-12-25T18:30, 2026-12-25 18:30, con o sin offset.
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        parsed = None

    if parsed is None:
        for fmt in DATETIME_FORMATS:
            try:
                parsed = datetime.strptime(raw, fmt)
                break
            except ValueError:
                continue

    if parsed is None:
        raise ValueError(
            "No pude interpretar la fecha. Usa formatos como "
            "25/12/2026 18:30, 25-12-2026 18:30 o 2026-12-25 18:30."
        )

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=tz)
    return parsed.astimezone(timezone.utc)


def parse_task_input(raw: str, tz: ZoneInfo) -> tuple[str, str, datetime]:
    """
    Devuelve (titulo, descripcion, due_at_utc).

    Formato recomendado: titulo | descripcion | fecha hora
    También acepta: titulo fecha hora  (sin descripción)
    """
    raw = raw.strip()
    if not raw:
        raise ValueError(
            "Debes indicar la tarea. Ejemplo: "
            "$task add Comprar leche | Leche y pan | 25/12/2026 18:30"
        )

    if "|" in raw:
        parts = [_clean_part(part) for part in raw.split("|")]
        if len(parts) < 2:
            raise ValueError(
                "Usa el formato: titulo | descripcion | fecha hora. "
                "Ejemplo: Comprar leche | Leche y pan | 25/12/2026 18:30"
            )
        title, description, due_raw = _split_pipe_task(parts)
        if not title:
            raise ValueError("El título de la tarea no puede estar vacío.")
        return title, description, parse_due_at(due_raw, tz)

    try:
        tokens = shlex.split(raw)
    except ValueError as exc:
        raise ValueError(f"Revisa las comillas del texto: {exc}") from exc

    if len(tokens) < 2:
        raise ValueError(
            "Usa el formato: titulo | descripcion | fecha hora. "
            "Ejemplo: Comprar leche | Leche y pan | 25/12/2026 18:30"
        )
    # Intenta tomar las dos últimas palabras como fecha y hora; si no,
    # intenta con la última (para formatos ISO con T).
    for amount in (2, 1):
        if len(tokens) <= amount:
            continue
        candidate = " ".join(tokens[-amount:])
        try:
            due_at = parse_due_at(candidate, tz)
        except ValueError:
            continue
        title = " ".join(tokens[:-amount]).strip()
        if title:
            return title, "", due_at

    raise ValueError(
        "No pude separar la fecha. Usa el formato: "
        "titulo | descripcion | fecha hora. "
        "Ejemplo: Comprar leche | Leche y pan | 25/12/2026 18:30"
    )


UPDATE_KEYS = {
    "titulo": "title",
    "title": "title",
    "descripcion": "description",
    "descripción": "description",
    "description": "description",
    "desc": "description",
    "fecha": "due_at",
    "due": "due_at",
    "due_at": "due_at",
    "recordatorio": "due_at",
    "estado": "status",
    "status": "status",
}


def parse_update_args(raw: str, tz: ZoneInfo) -> dict[str, Any]:
    """
    Devuelve un diccionario con los campos a actualizar.

    Formato recomendado:
      titulo=Nuevo | descripcion=Nueva | fecha=25/12/2026 18:30 | estado=pendiente

    También acepta el formato posicional:
      Nuevo título | Nueva descripción | 25/12/2026 18:30
    """
    raw = raw.strip()
    if not raw:
        raise ValueError(
            "Indica qué quieres actualizar. Ejemplo: "
            "titulo=Nuevo título | fecha=25/12/2026 18:30"
        )

    fields: dict[str, Any] = {}

    if "=" in raw:
        for part in raw.split("|"):
            part = part.strip()
            if not part:
                continue
            if "=" not in part:
                raise ValueError(
                    "Cada campo debe tener el formato campo=valor. "
                    "Ejemplo: titulo=Nuevo título | fecha=25/12/2026 18:30"
                )
            key, value = part.split("=", 1)
            field = UPDATE_KEYS.get(key.strip().lower())
            if field is None:
                raise ValueError(
                    f"Campo no reconocido: {key.strip()!r}. "
                    "Usa titulo, descripcion, fecha o estado."
                )
            value = _clean_part(value.strip())
            if field == "title":
                if not value:
                    raise ValueError("El título no puede estar vacío.")
                fields[field] = value
            elif field == "description":
                fields[field] = value
            elif field == "due_at":
                fields[field] = parse_due_at(value, tz)
            elif field == "status":
                fields[field] = normalize_status(value)

        if not fields:
            raise ValueError("No indicaste ningún campo para actualizar.")
        return fields

    if "|" in raw:
        parts = [_clean_part(part) for part in raw.split("|")]

        if len(parts) == 2 and parts[1]:
            if parts[0]:
                fields["title"] = parts[0]
            try:
                fields["due_at"] = parse_due_at(parts[1], tz)
            except ValueError:
                fields["description"] = parts[1]
        elif len(parts) >= 3:
            if parts[0]:
                fields["title"] = parts[0]

            if (
                DATE_TOKEN_RE.match(parts[-2])
                and TIME_TOKEN_RE.match(parts[-1])
            ):
                due_raw = f"{parts[-2]} {parts[-1]}"
                middle = parts[1:-2]
            else:
                due_raw = parts[-1]
                middle = parts[1:-1]

            description = " | ".join(middle).strip()
            if description:
                fields["description"] = description
            if due_raw:
                fields["due_at"] = parse_due_at(due_raw, tz)

        if not fields:
            raise ValueError("No indicaste ningún campo para actualizar.")
        return fields

    # Un único valor: se intenta interpretar como fecha; si no, como título.
    try:
        return {"due_at": parse_due_at(raw, tz)}
    except ValueError:
        return {"title": raw}


@dataclass(frozen=True)
class Task:
    id: int
    title: str
    description: str
    due_at: datetime
    channel_id: int
    guild_id: Optional[int]
    user_id: int
    status: str
    created_at: datetime
    updated_at: datetime
    sent_at: Optional[datetime] = None

    @classmethod
    def from_row(cls, row: Any) -> "Task":
        data = dict(row)
        return cls(
            id=int(data["id"]),
            title=str(data["title"]),
            description=str(data["description"] or ""),
            due_at=from_iso(data["due_at"]),  # type: ignore[arg-type]
            channel_id=int(data["channel_id"]),
            guild_id=int(data["guild_id"]) if data.get("guild_id") is not None else None,
            user_id=int(data["user_id"]),
            status=str(data["status"]),
            created_at=from_iso(data["created_at"]),  # type: ignore[arg-type]
            updated_at=from_iso(data["updated_at"]),  # type: ignore[arg-type]
            sent_at=from_iso(data.get("sent_at")),
        )


class TaskStorage:
    """
    Persistencia de tareas.

    - Sin DATABASE_URL: SQLite en TASK_DB_PATH.
    - Con DATABASE_URL: PostgreSQL (recomendado en Render, cuyo disco es
      efímero y puede perder el archivo SQLite al reiniciar/redeployar).
    """

    def __init__(self, *, database_url: str = "", db_path: str = "data/tasks.db") -> None:
        self.database_url = (database_url or "").strip()
        if self.database_url.startswith("postgres://"):
            self.database_url = "postgresql://" + self.database_url[len("postgres://") :]
        self.db_path = (db_path or "data/tasks.db").strip()
        self._sqlite: Optional[sqlite3.Connection] = None
        self._lock = threading.Lock()

    @classmethod
    def from_env(cls) -> "TaskStorage":
        return cls(
            database_url=os.getenv("DATABASE_URL", ""),
            db_path=os.getenv("TASK_DB_PATH", "data/tasks.db"),
        )

    @property
    def is_postgres(self) -> bool:
        return bool(self.database_url)

    def init(self) -> None:
        if self.is_postgres:
            if psycopg is None:
                raise RuntimeError(
                    "DATABASE_URL está definido pero psycopg no está instalado. "
                    "Ejecuta: pip install 'psycopg[binary]'"
                )
            self._execute(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    id BIGSERIAL PRIMARY KEY,
                    title TEXT NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    due_at TEXT NOT NULL,
                    channel_id BIGINT NOT NULL,
                    guild_id BIGINT,
                    user_id BIGINT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    sent_at TEXT
                )
                """
            )
            self._execute(
                "CREATE INDEX IF NOT EXISTS idx_tasks_due ON tasks (status, due_at)"
            )
            return

        path = Path(self.db_path).expanduser()
        if str(path) != ":memory:":
            path.parent.mkdir(parents=True, exist_ok=True)

        self._sqlite = sqlite3.connect(str(path), check_same_thread=False)
        self._sqlite.row_factory = sqlite3.Row
        with self._lock:
            self._sqlite.execute("PRAGMA journal_mode=WAL")
            self._sqlite.execute("PRAGMA synchronous=NORMAL")
            self._sqlite.execute(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    due_at TEXT NOT NULL,
                    channel_id INTEGER NOT NULL,
                    guild_id INTEGER,
                    user_id INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    sent_at TEXT
                )
                """
            )
            self._sqlite.execute(
                "CREATE INDEX IF NOT EXISTS idx_tasks_due ON tasks (status, due_at)"
            )
            self._sqlite.commit()

    def close(self) -> None:
        if self._sqlite is not None:
            with suppress(Exception):
                self._sqlite.close()
            self._sqlite = None

    def _sql(self, sql: str) -> str:
        return sql.replace("?", "%s") if self.is_postgres else sql

    def _fetchone(self, sql: str, params: tuple[Any, ...] = ()) -> Optional[dict[str, Any]]:
        if self.is_postgres:
            assert psycopg is not None and dict_row is not None
            with psycopg.connect(self.database_url) as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(self._sql(sql), params)
                    row = cur.fetchone()
                    return dict(row) if row is not None else None

        if self._sqlite is None:
            raise RuntimeError("La base de datos no está inicializada.")
        with self._lock:
            row = self._sqlite.execute(sql, params).fetchone()
            return dict(row) if row is not None else None

    def _fetchall(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        if self.is_postgres:
            assert psycopg is not None and dict_row is not None
            with psycopg.connect(self.database_url) as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(self._sql(sql), params)
                    return [dict(row) for row in cur.fetchall()]

        if self._sqlite is None:
            raise RuntimeError("La base de datos no está inicializada.")
        with self._lock:
            rows = self._sqlite.execute(sql, params).fetchall()
            return [dict(row) for row in rows]

    def _execute(self, sql: str, params: tuple[Any, ...] = ()) -> int:
        if self.is_postgres:
            assert psycopg is not None
            with psycopg.connect(self.database_url) as conn:
                with conn.cursor() as cur:
                    cur.execute(self._sql(sql), params)
                    return cur.rowcount

        if self._sqlite is None:
            raise RuntimeError("La base de datos no está inicializada.")
        with self._lock:
            cur = self._sqlite.execute(sql, params)
            self._sqlite.commit()
            return cur.rowcount

    def _insert(self, sql: str, params: tuple[Any, ...]) -> int:
        if self.is_postgres:
            assert psycopg is not None and dict_row is not None
            with psycopg.connect(self.database_url) as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(self._sql(sql + " RETURNING id"), params)
                    row = cur.fetchone()
                    if row is None:
                        raise RuntimeError("No se pudo crear la tarea.")
                    return int(row["id"])

        if self._sqlite is None:
            raise RuntimeError("La base de datos no está inicializada.")
        with self._lock:
            cur = self._sqlite.execute(sql, params)
            self._sqlite.commit()
            if cur.lastrowid is None:
                raise RuntimeError("No se pudo crear la tarea.")
            return int(cur.lastrowid)

    def create_task(
        self,
        *,
        title: str,
        description: str,
        due_at: datetime,
        channel_id: int,
        guild_id: Optional[int],
        user_id: int,
    ) -> Task:
        now = to_iso(utc_now())
        task_id = self._insert(
            """
            INSERT INTO tasks
                (title, description, due_at, channel_id, guild_id, user_id,
                 status, created_at, updated_at, sent_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
            """,
            (
                title,
                description,
                to_iso(due_at),
                channel_id,
                guild_id,
                user_id,
                STATUS_PENDING,
                now,
                now,
            ),
        )
        task = self.get_task(task_id)
        if task is None:
            raise RuntimeError("La tarea se creó pero no se pudo recuperar.")
        return task

    def get_task(self, task_id: int) -> Optional[Task]:
        row = self._fetchone("SELECT * FROM tasks WHERE id = ?", (task_id,))
        return Task.from_row(row) if row is not None else None

    def list_tasks(
        self,
        *,
        guild_id: Optional[int] = None,
        user_id: Optional[int] = None,
        status: Optional[str] = None,
        limit: int = 25,
    ) -> list[Task]:
        where: list[str] = []
        params: list[Any] = []

        if guild_id is not None:
            where.append("guild_id = ?")
            params.append(guild_id)
        if user_id is not None:
            where.append("user_id = ?")
            params.append(user_id)
        if status is not None:
            where.append("status = ?")
            params.append(status)

        sql = "SELECT * FROM tasks"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY due_at ASC LIMIT ?"
        params.append(max(1, min(int(limit), 100)))

        rows = self._fetchall(sql, tuple(params))
        return [Task.from_row(row) for row in rows]

    def get_due_tasks(self, now: Optional[datetime] = None, *, limit: int = 50) -> list[Task]:
        due = to_iso(now or utc_now())
        rows = self._fetchall(
            """
            SELECT * FROM tasks
            WHERE status = ? AND due_at <= ?
            ORDER BY due_at ASC
            LIMIT ?
            """,
            (STATUS_PENDING, due, max(1, min(int(limit), 200))),
        )
        return [Task.from_row(row) for row in rows]

    def update_task(self, task_id: int, **fields: Any) -> bool:
        allowed = {"title", "description", "due_at", "status"}
        unknown = set(fields) - allowed
        if unknown:
            raise ValueError(f"Campos no permitidos: {', '.join(sorted(unknown))}")

        if not fields:
            return False

        sets: list[str] = []
        params: list[Any] = []
        for key, value in fields.items():
            if key == "due_at":
                value = to_iso(value)
            elif key == "status":
                value = normalize_status(str(value))
            sets.append(f"{key} = ?")
            params.append(value)

        sets.append("updated_at = ?")
        params.append(to_iso(utc_now()))
        params.append(task_id)

        sql = f"UPDATE tasks SET {', '.join(sets)} WHERE id = ?"
        return self._execute(sql, tuple(params)) > 0

    def mark_sent(self, task_id: int, sent_at: Optional[datetime] = None) -> bool:
        now = to_iso(sent_at or utc_now())
        return (
            self._execute(
                """
                UPDATE tasks
                SET status = ?, sent_at = ?, updated_at = ?
                WHERE id = ? AND status = ?
                """,
                (STATUS_SENT, now, now, task_id, STATUS_PENDING),
            )
            > 0
        )

    def delete_task(self, task_id: int) -> bool:
        return self._execute("DELETE FROM tasks WHERE id = ?", (task_id,)) > 0


class TasksCog(commands.Cog, name="Tareas"):
    """Comandos y planificador de recordatorios."""

    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self.storage = TaskStorage.from_env()

        tz_name = os.getenv("TASK_TIMEZONE", "America/Bogota").strip() or "America/Bogota"
        try:
            self.tz = ZoneInfo(tz_name)
            self.tz_name = tz_name
        except ZoneInfoNotFoundError:
            logger.warning(
                "Zona horaria %r no encontrada; se usará UTC. "
                "Revisa la variable TASK_TIMEZONE.",
                tz_name,
            )
            self.tz = timezone.utc  # type: ignore[assignment]
            self.tz_name = "UTC"

        try:
            interval = float(os.getenv("TASK_CHECK_INTERVAL", "30"))
        except ValueError:
            interval = 30.0
        self.check_interval = max(5.0, interval)

        self._scheduler_task: Optional[asyncio.Task[None]] = None

    async def cog_load(self) -> None:
        self.storage.init()
        self._scheduler_task = asyncio.create_task(
            self._reminder_loop(), name="task-reminder-loop"
        )
        logger.info(
            "Módulo de tareas cargado (persistencia: %s, zona: %s).",
            "PostgreSQL" if self.storage.is_postgres else f"SQLite ({self.storage.db_path})",
            self.tz_name,
        )

    async def cog_unload(self) -> None:
        if self._scheduler_task is not None:
            self._scheduler_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._scheduler_task
            self._scheduler_task = None
        self.storage.close()

    # ------------------------------------------------------------------
    # Planificador
    # ------------------------------------------------------------------
    async def _reminder_loop(self) -> None:
        try:
            await self.bot.wait_until_ready()
        except asyncio.CancelledError:
            raise

        while not self.bot.is_closed():
            try:
                await self._check_due_tasks()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Error revisando las tareas vencidas.")

            await asyncio.sleep(self.check_interval)

    async def _check_due_tasks(self) -> None:
        tasks = self.storage.get_due_tasks(utc_now())
        for task in tasks:
            sent = await self._send_reminder(task)
            if sent:
                self.storage.mark_sent(task.id)
                logger.info("Recordatorio enviado para la tarea #%s.", task.id)

    async def _send_reminder(self, task: Task) -> bool:
        channel = self.bot.get_channel(task.channel_id)
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(task.channel_id)
            except discord.HTTPException as exc:
                logger.warning(
                    "No se pudo obtener el canal %s de la tarea #%s: %s",
                    task.channel_id,
                    task.id,
                    exc,
                )
                return False

        embed = discord.Embed(
            title=f"⏰ Recordatorio: {task.title[:230]}",
            description=task.description[:4000] or "_Sin descripción_",
            color=discord.Color.orange(),
            timestamp=task.due_at,
        )
        embed.add_field(
            name="📅 Fecha",
            value=format_local(task.due_at, self.tz),
            inline=True,
        )
        embed.add_field(name="🆔 Tarea", value=f"#{task.id}", inline=True)
        embed.set_footer(text=f"Recordatorio creado con {self.bot.user}")

        try:
            await channel.send(  # type: ignore[union-attr]
                content=f"<@{task.user_id}>",
                embed=embed,
                allowed_mentions=discord.AllowedMentions(
                    users=True, roles=False, everyone=False
                ),
            )
        except discord.HTTPException as exc:
            logger.warning(
                "No se pudo enviar el recordatorio de la tarea #%s al canal %s: %s",
                task.id,
                task.channel_id,
                exc,
            )
            return False

        return True

    # ------------------------------------------------------------------
    # Utilidades de comandos
    # ------------------------------------------------------------------
    @staticmethod
    def _validate_text(title: str, description: str) -> None:
        if not title.strip():
            raise ValueError("El título no puede estar vacío.")
        if len(title) > MAX_TITLE_LENGTH:
            raise ValueError(
                f"El título es demasiado largo (máximo {MAX_TITLE_LENGTH} caracteres)."
            )
        if len(description) > MAX_DESCRIPTION_LENGTH:
            raise ValueError(
                "La descripción es demasiado larga "
                f"(máximo {MAX_DESCRIPTION_LENGTH} caracteres)."
            )

    @staticmethod
    def _can_manage(ctx: commands.Context, task: Task) -> bool:
        if task.user_id == ctx.author.id:
            return True
        if ctx.guild is None or task.guild_id != ctx.guild.id:
            return False
        permissions = getattr(ctx.author, "guild_permissions", None)
        return bool(
            permissions
            and (permissions.administrator or permissions.manage_messages)
        )

    async def _get_manageable_task(
        self, ctx: commands.Context, task_id: int
    ) -> Optional[Task]:
        task = self.storage.get_task(task_id)
        if task is None:
            await ctx.reply(
                f"❌ No existe una tarea con el ID #{task_id}.",
                mention_author=False,
            )
            return None
        if not self._can_manage(ctx, task):
            await ctx.reply(
                "❌ No puedes modificar esta tarea porque no es tuya.",
                mention_author=False,
            )
            return None
        return task

    def _task_embed(self, task: Task, *, color: Optional[discord.Color] = None) -> discord.Embed:
        embed = discord.Embed(
            title=f"📌 {task.title[:240]}",
            description=task.description[:4000] or "_Sin descripción_",
            color=color or discord.Color.blurple(),
        )
        embed.add_field(name="🆔 ID", value=f"#{task.id}", inline=True)
        embed.add_field(
            name="📅 Recordatorio",
            value=format_local(task.due_at, self.tz),
            inline=True,
        )
        embed.add_field(
            name="📊 Estado",
            value=STATUS_LABELS.get(task.status, task.status),
            inline=True,
        )
        if task.sent_at is not None:
            embed.add_field(
                name="📨 Enviada",
                value=format_local(task.sent_at, self.tz),
                inline=False,
            )
        embed.set_footer(
            text=(
                f"Creada: {format_local(task.created_at, self.tz)} · "
                f"Zona horaria: {self.tz_name}"
            )
        )
        return embed

    # ------------------------------------------------------------------
    # Comandos
    # ------------------------------------------------------------------
    @commands.group(
        name="task",
        aliases=["tarea", "tareas", "recordatorio", "recordatorios"],
        invoke_without_command=True,
    )
    async def task_group(self, ctx: commands.Context) -> None:
        """Muestra la ayuda del módulo de tareas."""
        prefix = ctx.prefix or "$"
        embed = discord.Embed(
            title="📋 Módulo de tareas",
            description=(
                "Guarda recordatorios y el bot los enviará automáticamente "
                "al canal donde los creaste."
            ),
            color=discord.Color.blurple(),
        )
        embed.add_field(
            name="➕ Crear",
            value=(
                f"`{prefix}task add Titulo | Descripcion | 25/12/2026 18:30`\n"
                f"`{prefix}task add Titulo | 25/12/2026 18:30`"
            ),
            inline=False,
        )
        embed.add_field(
            name="📋 Listar",
            value=(
                f"`{prefix}task list` (pendientes)\n"
                f"`{prefix}task list all` (todas)\n"
                f"`{prefix}task list sent|done|cancelled`"
            ),
            inline=False,
        )
        embed.add_field(
            name="✏️ Actualizar",
            value=(
                f"`{prefix}task update 3 titulo=Nuevo | fecha=26/12/2026 09:00`\n"
                f"`{prefix}task update 3 | | 26/12/2026 09:00`\n"
                f"`{prefix}task update 3 estado=completada`"
            ),
            inline=False,
        )
        embed.add_field(
            name="🔎 Consultar / borrar",
            value=(
                f"`{prefix}task show 3`\n"
                f"`{prefix}task done 3`\n"
                f"`{prefix}task delete 3`"
            ),
            inline=False,
        )
        embed.set_footer(
            text=f"Zona horaria: {self.tz_name} · Fechas: DD/MM/AAAA HH:MM"
        )
        await ctx.send(embed=embed)

    @task_group.command(name="add", aliases=["crear", "nueva", "agregar"])
    async def task_add(self, ctx: commands.Context, *, raw: str = "") -> None:
        """Crea una tarea con recordatorio."""
        if not raw.strip():
            await ctx.reply(
                "❌ Uso: `$task add Titulo | Descripcion | 25/12/2026 18:30`",
                mention_author=False,
            )
            return

        try:
            title, description, due_at = parse_task_input(raw, self.tz)
            self._validate_text(title, description)
        except ValueError as exc:
            await ctx.reply(f"❌ {exc}", mention_author=False)
            return

        if due_at <= utc_now():
            await ctx.reply(
                "❌ La fecha y hora del recordatorio debe estar en el futuro. "
                f"Zona horaria: {self.tz_name}.",
                mention_author=False,
            )
            return

        task = self.storage.create_task(
            title=title.strip(),
            description=description.strip(),
            due_at=due_at,
            channel_id=ctx.channel.id,
            guild_id=ctx.guild.id if ctx.guild else None,
            user_id=ctx.author.id,
        )

        embed = self._task_embed(task, color=discord.Color.green())
        await ctx.reply(
            f"✅ Tarea #{task.id} creada. Te recordaré en este canal.",
            embed=embed,
            mention_author=False,
        )

    @task_group.command(name="list", aliases=["lista", "ls", "listar"])
    async def task_list(self, ctx: commands.Context, filtro: str = "pending") -> None:
        """Lista tus tareas."""
        try:
            status = parse_status_filter(filtro)
        except ValueError as exc:
            await ctx.reply(f"❌ {exc}", mention_author=False)
            return

        tasks = self.storage.list_tasks(
            guild_id=ctx.guild.id if ctx.guild else None,
            user_id=ctx.author.id,
            status=status,
            limit=25,
        )

        if not tasks:
            label = "con ese filtro" if status else "registradas"
            await ctx.reply(
                f"No tienes tareas {label}.",
                mention_author=False,
            )
            return

        filter_label = STATUS_LABELS.get(status, "Todas") if status else "Todas"
        embed = discord.Embed(
            title="📋 Tus tareas",
            description=f"Filtro: **{filter_label}**",
            color=discord.Color.blurple(),
        )

        for task in tasks:
            description = task.description.strip() or "Sin descripción"
            if len(description) > 100:
                description = description[:97] + "..."
            value = (
                f"📅 {format_local(task.due_at, self.tz)}\n"
                f"📊 {STATUS_LABELS.get(task.status, task.status)}\n"
                f"{description}"
            )
            embed.add_field(
                name=f"#{task.id} · {task.title[:120]}",
                value=value[:1024],
                inline=False,
            )

        if len(tasks) >= 25:
            embed.set_footer(text="Mostrando las primeras 25 tareas.")
        else:
            embed.set_footer(text=f"Zona horaria: {self.tz_name}")

        await ctx.send(embed=embed)

    @task_group.command(name="show", aliases=["ver", "detalle"])
    async def task_show(self, ctx: commands.Context, task_id: int) -> None:
        """Muestra el detalle de una tarea."""
        task = await self._get_manageable_task(ctx, task_id)
        if task is None:
            return
        await ctx.send(embed=self._task_embed(task))

    @task_group.command(name="update", aliases=["editar", "actualizar", "modificar"])
    async def task_update(
        self, ctx: commands.Context, task_id: int, *, raw: str = ""
    ) -> None:
        """Actualiza el título, la descripción, la fecha o el estado de una tarea."""
        task = await self._get_manageable_task(ctx, task_id)
        if task is None:
            return

        if not raw.strip():
            await ctx.reply(
                "❌ Uso: `$task update 3 titulo=Nuevo | "
                "descripcion=Nueva | fecha=26/12/2026 09:00 | estado=pendiente`",
                mention_author=False,
            )
            return

        try:
            fields = parse_update_args(raw, self.tz)
            if "title" in fields or "description" in fields:
                self._validate_text(
                    fields.get("title", task.title),
                    fields.get("description", task.description),
                )
        except ValueError as exc:
            await ctx.reply(f"❌ {exc}", mention_author=False)
            return

        new_status = fields.get("status", task.status)
        if (
            "due_at" in fields
            and fields["due_at"] <= utc_now()
            and new_status == STATUS_PENDING
        ):
            await ctx.reply(
                "❌ La nueva fecha debe estar en el futuro. "
                f"Zona horaria: {self.tz_name}.",
                mention_author=False,
            )
            return

        updated = self.storage.update_task(task.id, **fields)
        if not updated:
            await ctx.reply("❌ No se pudo actualizar la tarea.", mention_author=False)
            return

        updated_task = self.storage.get_task(task.id)
        if updated_task is None:
            await ctx.reply("❌ La tarea ya no existe.", mention_author=False)
            return

        await ctx.reply(
            f"✅ Tarea #{task.id} actualizada.",
            embed=self._task_embed(updated_task, color=discord.Color.blue()),
            mention_author=False,
        )

    @task_group.command(name="done", aliases=["completar", "completada"])
    async def task_done(self, ctx: commands.Context, task_id: int) -> None:
        """Marca una tarea como completada."""
        task = await self._get_manageable_task(ctx, task_id)
        if task is None:
            return

        self.storage.update_task(task.id, status=STATUS_DONE)
        updated = self.storage.get_task(task.id)
        kwargs: dict[str, Any] = {}
        if updated is not None:
            kwargs["embed"] = self._task_embed(updated, color=discord.Color.green())
        await ctx.reply(
            f"🏁 Tarea #{task.id} marcada como completada.",
            mention_author=False,
            **kwargs,
        )

    @task_group.command(name="delete", aliases=["borrar", "eliminar"])
    async def task_delete(self, ctx: commands.Context, task_id: int) -> None:
        """Elimina una tarea."""
        task = await self._get_manageable_task(ctx, task_id)
        if task is None:
            return

        if not self.storage.delete_task(task.id):
            await ctx.reply("❌ No se pudo eliminar la tarea.", mention_author=False)
            return

        await ctx.reply(
            f"🗑️ Tarea #{task.id} eliminada.",
            mention_author=False,
        )


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(TasksCog(bot))
