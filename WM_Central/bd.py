"""
bd.py - Base de datos SQLite de WM_Central.

La base de datos es un único archivo (por defecto watermanagement.db).
Se puede cambiar la ruta con la variable de entorno WM_BD
(nos servirá en Railway para guardarla en un volumen).

Tablas:
    estaciones (id, ubicacion, bloqueada)
    operarios  (id, nombre)
    riegos     (id, estacion, operario, inicio, duracion_s, volumen_l, motivo)

El ESTADO de cada estación (disponible, fuga...) NO se guarda: depende de si
está conectada ahora mismo, así que al arrancar Central todas están DESCONECTADA.

Cada función abre y cierra su propia conexión, porque Central usa varios hilos
y una conexión de sqlite3 solo se puede usar desde el hilo que la creó.
"""

import os
import sqlite3

RUTA_BD = os.environ.get("WM_BD", "watermanagement.db")

# Operarios que se dan de alta la primera vez que se crea la base de datos
OPERARIOS_INICIALES = [
    ("FO-12", "Operario 12"),
    ("FO-15", "Operario 15"),
    ("FO-20", "Operario 20"),
]

# Estaciones que se dan de alta la primera vez (las demás, desde el menú de Central)
ESTACIONES_INICIALES = [
    ("WS-01", "River Park"),
    ("WS-02", "Parque Lo Morant"),
    ("WS-03", "Parque El Palmeral"),
    ("WS-04", "Parque Canalejas"),
]


def conectar():
    return sqlite3.connect(RUTA_BD)


def crear_tablas():
    """Crea las tablas si no existen y mete los operarios de ejemplo."""
    con = conectar()
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS estaciones (
                id        TEXT PRIMARY KEY,
                ubicacion TEXT NOT NULL,
                bloqueada INTEGER NOT NULL DEFAULT 0
            )""")
        con.execute("""
            CREATE TABLE IF NOT EXISTS operarios (
                id     TEXT PRIMARY KEY,
                nombre TEXT NOT NULL
            )""")
        con.execute("""
            CREATE TABLE IF NOT EXISTS riegos (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                estacion   TEXT NOT NULL,
                operario   TEXT NOT NULL,
                inicio     TEXT NOT NULL,
                duracion_s INTEGER,
                volumen_l  REAL,
                motivo     TEXT
            )""")
        # INSERT OR IGNORE: si el operario ya existe, no hace nada
        con.executemany("INSERT OR IGNORE INTO operarios (id, nombre) VALUES (?, ?)",
                        OPERARIOS_INICIALES)
        con.executemany("INSERT OR IGNORE INTO estaciones (id, ubicacion) VALUES (?, ?)",
                        ESTACIONES_INICIALES)
        con.commit()
    finally:
        con.close()


# --------------------------------------------------------------- ESTACIONES

def cargar_estaciones():
    """Devuelve una lista de (id, ubicacion, bloqueada) con todas las estaciones."""
    con = conectar()
    try:
        filas = con.execute("SELECT id, ubicacion, bloqueada FROM estaciones").fetchall()
    finally:
        con.close()
    return [(id_ws, ubicacion, bool(bloqueada)) for id_ws, ubicacion, bloqueada in filas]


def guardar_estacion(id_estacion, ubicacion):
    """Da de alta una estación nueva, o actualiza su ubicación si ya existía."""
    con = conectar()
    try:
        con.execute("""
            INSERT INTO estaciones (id, ubicacion) VALUES (?, ?)
            ON CONFLICT(id) DO UPDATE SET ubicacion = excluded.ubicacion""",
                    (id_estacion, ubicacion))
        con.commit()
    finally:
        con.close()


def guardar_bloqueo(id_estacion, bloqueada):
    """Guarda si una estación está bloqueada (fuera de servicio) o no."""
    con = conectar()
    try:
        con.execute("UPDATE estaciones SET bloqueada = ? WHERE id = ?",
                    (1 if bloqueada else 0, id_estacion))
        con.commit()
    finally:
        con.close()


# ---------------------------------------------------------------- OPERARIOS

def existe_operario(id_operario):
    con = conectar()
    try:
        fila = con.execute("SELECT 1 FROM operarios WHERE id = ?", (id_operario,)).fetchone()
    finally:
        con.close()
    return fila is not None


def cargar_operarios():
    """Devuelve la lista de IDs de operarios dados de alta."""
    con = conectar()
    try:
        filas = con.execute("SELECT id FROM operarios ORDER BY id").fetchall()
    finally:
        con.close()
    return [fila[0] for fila in filas]


# ------------------------------------------------------------------- RIEGOS

def guardar_riego(estacion, operario, inicio, duracion_s, volumen_l, motivo):
    """Guarda un riego terminado en el historial."""
    con = conectar()
    try:
        con.execute("""
            INSERT INTO riegos (estacion, operario, inicio, duracion_s, volumen_l, motivo)
            VALUES (?, ?, ?, ?, ?, ?)""",
                    (estacion, operario, inicio, duracion_s, volumen_l, motivo))
        con.commit()
    finally:
        con.close()

def agua_por_estacion():
    """Devuelve {id_estacion: litros} con el agua total de los riegos terminados."""
    con = conectar()
    try:
        filas = con.execute("""
            SELECT estacion, COALESCE(SUM(volumen_l), 0) FROM riegos GROUP BY estacion""").fetchall()
    finally:
        con.close()
    return {estacion: litros for estacion, litros in filas}