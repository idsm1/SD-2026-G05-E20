"""
WM_Central - Servidor central del sistema de Water Management.

Uso:  python WM_Central.py <puerto> [ip_broker:puerto_broker]
      python WM_Central.py 5050 localhost:9092

      (el broker de Kafka todavía no se usa; se añade en la siguiente fase)

Tramas que entiende (campos separados por '#'):
    REGISTRO#<ID_ESTACION>#<UBICACION>    -> el monitor da de alta / conecta su estación
    FUGA#<ID_ESTACION>                    -> el monitor avisa de una avería
    RECUPERADA#<ID_ESTACION>              -> el monitor avisa de que la avería se ha resuelto
    PING#<ID_ESTACION>                    -> latido del monitor cada 5 s (para saber que seguimos vivos)

Respuestas:
    STATUS#OK#<mensaje>
    STATUS#ERROR#<motivo>

La conexión con cada monitor se queda ABIERTA. Si se cae, la estación pasa a DESCONECTADA.
Las estaciones y los operarios se guardan en SQLite (bd.py). Al arrancar, las estaciones
de la base de datos aparecen como DESCONECTADA hasta que su monitor vuelva a conectar.
"""

import os
import re
import socket
import sys
import threading
from datetime import datetime

import bd
from protocolo import enviar, recibir, construir_trama, separar_trama

# ---------------------------------------------------------- CONFIGURACIÓN

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 5050
BROKER = sys.argv[2] if len(sys.argv) > 2 else 'localhost:9092'   # para la fase de Kafka
SERVER = "0.0.0.0"          # escucha en todas las interfaces de red
ADDR = (SERVER, PORT)
MAX_CONEXIONES = 10

PATRON_ID = re.compile(r'^WS-\d+$')   # ej: WS-04

# Estados posibles de una estación
DISPONIBLE = "DISPONIBLE"
REGANDO = "REGANDO"
FUGA = "FUGA"
FUERA_SERVICIO = "FUERA_SERVICIO"
DESCONECTADA = "DESCONECTADA"

# Colores para el panel (códigos ANSI)
COLORES = {
    DISPONIBLE: "\033[92m",       # verde
    REGANDO: "\033[92m",          # verde
    FUGA: "\033[91m",             # rojo
    FUERA_SERVICIO: "\033[93m",   # naranja (amarillo en consola)
    DESCONECTADA: "\033[90m",     # gris
}
RESET = "\033[0m"

conexiones_activas = 0
estaciones = {}             # ID_ESTACION -> {ubicacion, estado, bloqueada, addr}
lock = threading.Lock()     # protege el contador y el diccionario de estaciones


# ------------------------------------------------------------------ PANEL

def mostrar_panel():
    """Muestra el estado de todas las estaciones. Llamar SIN tener el lock."""
    with lock:
        copia = {id_ws: dict(datos) for id_ws, datos in estaciones.items()}

    hora = datetime.now().strftime("%H:%M:%S")
    print()
    print("=" * 56)
    print(f"  PANEL DE CONTROL - WM_Central            {hora}")
    print("=" * 56)
    if not copia:
        print("  (no hay estaciones registradas)")
    for id_ws in sorted(copia):
        datos = copia[id_ws]
        color = COLORES.get(datos["estado"], "")
        print(f"  {color}{id_ws:<8} {datos['estado']:<15}{RESET} {datos['ubicacion']}")
    print("=" * 56)
    print()


# ---------------------------------------------------------------- SERVICIOS

def servicio_registro(id_estacion, ubicacion, addr):
    """Valida los datos de la estación y la marca como DISPONIBLE.
    Devuelve (respuesta, registrada_ok)."""
    if not id_estacion:
        return construir_trama("STATUS", "ERROR", "Falta el ID de la estacion"), False
    if not PATRON_ID.match(id_estacion):
        return construir_trama("STATUS", "ERROR",
                               "ID no valido. Formato esperado: WS-<numero>"), False
    if not ubicacion:
        return construir_trama("STATUS", "ERROR", "Falta la ubicacion"), False

    with lock:
        if id_estacion in estaciones and estaciones[id_estacion]["estado"] != DESCONECTADA:
            return construir_trama("STATUS", "ERROR",
                                   f"La estacion {id_estacion} ya esta conectada"), False
        nueva = id_estacion not in estaciones
        bloqueada = estaciones[id_estacion]["bloqueada"] if not nueva else False
        estaciones[id_estacion] = {"ubicacion": ubicacion, "bloqueada": bloqueada,
                                   "estado": FUERA_SERVICIO if bloqueada else DISPONIBLE,
                                   "addr": addr}

    bd.guardar_estacion(id_estacion, ubicacion)   # alta nueva o actualizar ubicación
    print(f"[REGISTRO] {id_estacion} ({ubicacion}) "
          f"{'dada de alta' if nueva else 'reconectada'} desde {addr[0]}:{addr[1]}")
    return construir_trama("STATUS", "OK", "Estacion registrada correctamente"), True


def cambiar_estado(id_estacion, nuevo_estado):
    """Cambia el estado de una estación que ya existe.
    Si se pide DISPONIBLE pero la estación está bloqueada, queda FUERA_SERVICIO."""
    with lock:
        if id_estacion not in estaciones:
            return False
        if nuevo_estado == DISPONIBLE and estaciones[id_estacion]["bloqueada"]:
            nuevo_estado = FUERA_SERVICIO
        estaciones[id_estacion]["estado"] = nuevo_estado
    return True


def procesar(trama, addr, id_conexion):
    """Parsea la trama y decide qué servicio la atiende.
    id_conexion es la estación registrada en esta conexión (o None).
    Devuelve (respuesta, id_conexion actualizado)."""
    campos = separar_trama(trama)
    comando = campos[0].upper()

    if comando == "REGISTRO":
        if len(campos) != 3:
            return construir_trama("STATUS", "ERROR",
                                   "Formato incorrecto. Usa REGISTRO#<ID>#<UBICACION>"), id_conexion
        if id_conexion is not None:
            return construir_trama("STATUS", "ERROR",
                                   f"Esta conexion ya es la estacion {id_conexion}"), id_conexion
        respuesta, ok = servicio_registro(campos[1], campos[2], addr)
        return respuesta, (campos[1] if ok else None)

    # El resto de comandos solo se aceptan después de registrarse
    if id_conexion is None:
        return construir_trama("STATUS", "ERROR", "Primero hay que registrarse"), id_conexion

    if comando == "PING":
        return construir_trama("STATUS", "OK", "PONG"), id_conexion

    if comando in ("FUGA", "RECUPERADA"):
        if len(campos) != 2 or campos[1] != id_conexion:
            return construir_trama("STATUS", "ERROR",
                                   f"Formato incorrecto. Usa {comando}#{id_conexion}"), id_conexion
        if comando == "FUGA":
            cambiar_estado(id_conexion, FUGA)
            print(f"[AVERIA] {id_conexion}: FUGA detectada por el monitor")
        else:
            cambiar_estado(id_conexion, DISPONIBLE)
            print(f"[AVERIA] {id_conexion}: averia resuelta, vuelve a estar disponible")
        return construir_trama("STATUS", "OK", f"{comando} recibido"), id_conexion

    return construir_trama("STATUS", "ERROR", f"Comando desconocido: {comando}"), id_conexion


# ------------------------------------------------------- HILO POR CLIENTE

def atender_cliente(conn, addr):
    global conexiones_activas
    print(f"[NUEVA CONEXIÓN] {addr} conectado.")
    id_conexion = None      # qué estación es este monitor (se sabe al registrarse)
    try:
        while True:
            trama = recibir(conn)
            if trama is None:          # el monitor ha cerrado o se ha caído
                break

            es_ping = trama.upper().startswith("PING")
            if not es_ping:                    # el latido no se muestra, sería cada 5 s
                print(f"[{addr}] Trama recibida: {trama}")
            respuesta, id_conexion = procesar(trama, addr, id_conexion)
            if not es_ping:
                print(f"[{addr}] Respuesta: {respuesta}")
            enviar(conn, respuesta)

            if respuesta.startswith("STATUS#OK") and not es_ping:   # algo ha cambiado
                mostrar_panel()
    except (ConnectionResetError, BrokenPipeError, OSError):
        pass
    finally:
        conn.close()
        print(f"[DESCONEXIÓN] {addr} ({id_conexion or 'sin registrar'})")
        if id_conexion is not None:
            cambiar_estado(id_conexion, DESCONECTADA)
            mostrar_panel()
        with lock:
            conexiones_activas -= 1


# ------------------------------------------------------------------ MAIN

def start():
    global conexiones_activas
    os.system("")   # en Windows activa los colores ANSI en la consola

    # Base de datos: crear tablas si hace falta y cargar las estaciones conocidas
    bd.crear_tablas()
    for id_ws, ubicacion, bloqueada in bd.cargar_estaciones():
        estaciones[id_ws] = {"ubicacion": ubicacion, "bloqueada": bloqueada,
                             "estado": DESCONECTADA, "addr": None}

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(ADDR)
    server.listen()

    print("[STARTING] WM_Central inicializándose...")
    print(f"[LISTENING] Escuchando en el puerto {PORT}")
    print(f"[INFO] Broker de Kafka (siguiente fase): {BROKER}")
    print(f"[BD] {bd.RUTA_BD}: {len(estaciones)} estaciones, "
          f"operarios: {', '.join(bd.cargar_operarios())}")
    mostrar_panel()

    try:
        while True:
            conn, addr = server.accept()
            with lock:
                hay_sitio = conexiones_activas < MAX_CONEXIONES
                if hay_sitio:
                    conexiones_activas += 1

            if hay_sitio:
                hilo = threading.Thread(target=atender_cliente,
                                        args=(conn, addr), daemon=True)
                hilo.start()
            else:
                print(f"[RECHAZADO] {addr}: demasiadas conexiones")
                enviar(conn, construir_trama("STATUS", "ERROR",
                                             "Demasiadas conexiones. Intentalo mas tarde"))
                conn.close()
    except KeyboardInterrupt:
        print("\n[STOP] WM_Central detenido")
    finally:
        server.close()


if __name__ == "__main__":
    start()