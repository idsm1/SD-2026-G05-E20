"""
WM_Central - Servidor central del sistema de Water Management.

Uso:  py WM_Central.py [puerto]      (por defecto 5050)

Trama que entiende (campos separados por '#'):
    REGISTRO#<ID_ESTACION>#<UBICACION>

Respuestas:
    STATUS#OK#Estacion registrada correctamente
    STATUS#ERROR#<motivo>
"""

import re
import socket
import sys
import threading
from datetime import datetime

# ------------------------------------------------------------- PROTOCOLO
# Cada mensaje va precedido de una CABECERA de 64 bytes con su longitud,
# así el receptor sabe exactamente cuántos bytes tiene que leer.

HEADER = 64
FORMAT = 'utf-8'
SEPARADOR = '#'


def enviar(conn, texto):
    """Envía un mensaje de texto precedido de su cabecera de longitud."""
    datos = texto.encode(FORMAT)
    cabecera = str(len(datos)).encode(FORMAT)
    cabecera += b' ' * (HEADER - len(cabecera))   # rellenar hasta 64 bytes
    conn.sendall(cabecera + datos)


def recibir_exacto(conn, n):
    """Lee exactamente n bytes del socket (recv puede devolver menos)."""
    datos = b''
    while len(datos) < n:
        trozo = conn.recv(n - len(datos))
        if not trozo:          # el otro extremo ha cerrado la conexión
            return None
        datos += trozo
    return datos


def recibir(conn):
    """Recibe un mensaje completo. Devuelve None si la conexión se ha cerrado."""
    cabecera = recibir_exacto(conn, HEADER)
    if cabecera is None:
        return None
    try:
        longitud = int(cabecera.decode(FORMAT).strip())
    except ValueError:
        return None            # cabecera corrupta: tratamos como desconexión
    datos = recibir_exacto(conn, longitud)
    if datos is None:
        return None
    return datos.decode(FORMAT)


def construir_trama(*campos):
    return SEPARADOR.join(campos)


# ---------------------------------------------------------- CONFIGURACIÓN

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 5050
SERVER = "0.0.0.0"          # escucha en todas las interfaces de red
ADDR = (SERVER, PORT)
MAX_CONEXIONES = 10

PATRON_ID = re.compile(r'^WS-\d+$')   # ej: WS-04

conexiones_activas = 0
estaciones = {}             # ID_ESTACION -> {ubicacion, addr, hora}
lock = threading.Lock()     # protege el contador y el registro compartidos


# ---------------------------------------------------------------- SERVICIOS

def servicio_registro(id_estacion, ubicacion, addr):
    """Valida los datos de la estación y la guarda en el registro."""
    id_estacion = id_estacion.strip()
    ubicacion = ubicacion.strip()

    if not id_estacion:
        return construir_trama("STATUS", "ERROR", "Falta el ID de la estacion")
    if not PATRON_ID.match(id_estacion):
        return construir_trama("STATUS", "ERROR",
                               "ID no valido. Formato esperado: WS-<numero>")
    if not ubicacion:
        return construir_trama("STATUS", "ERROR", "Falta la ubicacion")

    hora = datetime.now().strftime("%H:%M:%S")
    with lock:
        ya_existia = id_estacion in estaciones
        estaciones[id_estacion] = {"ubicacion": ubicacion,
                                   "addr": addr, "hora": hora}
        total = len(estaciones)

    print("-" * 50)
    print(f"[REGISTRO] Estacion {'actualizada' if ya_existia else 'conectada'}")
    print(f"   ID        : {id_estacion}")
    print(f"   Ubicacion : {ubicacion}")
    print(f"   Direccion : {addr[0]}:{addr[1]}")
    print(f"   Hora      : {hora}")
    print(f"   Estaciones registradas: {total}")
    print("-" * 50)

    return construir_trama("STATUS", "OK", "Estacion registrada correctamente")


def procesar(trama, addr):
    """Parsea la trama por '#' y decide qué servicio la atiende."""
    campos = trama.strip().split(SEPARADOR)
    comando = campos[0].strip().upper()

    if comando == "REGISTRO":
        if len(campos) != 3:
            return construir_trama("STATUS", "ERROR",
                                   "Formato incorrecto. Usa REGISTRO#<ID>#<UBICACION>")
        return servicio_registro(campos[1], campos[2], addr)
    else:
        return construir_trama("STATUS", "ERROR", f"Comando desconocido: {comando}")


# ------------------------------------------------------- HILO POR CLIENTE

def atender_cliente(conn, addr):
    global conexiones_activas
    print(f"[NUEVA CONEXIÓN] {addr} conectado.")
    try:
        while True:
            trama = recibir(conn)
            if trama is None:          # el cliente ha cerrado (close())
                print(f"[DESCONEXIÓN] {addr} ha cerrado la conexión")
                break

            print(f"[{addr}] Trama recibida: {trama}")
            respuesta = procesar(trama, addr)
            print(f"[{addr}] Respuesta: {respuesta}")
            enviar(conn, respuesta)
    except (ConnectionResetError, BrokenPipeError):
        print(f"[ERROR] Se perdió la conexión con {addr}")
    finally:
        conn.close()
        with lock:
            conexiones_activas -= 1
            print(f"[CONEXIONES ACTIVAS] {conexiones_activas}/{MAX_CONEXIONES}")


# ------------------------------------------------------------------ MAIN

def start():
    global conexiones_activas
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(ADDR)
    server.listen()

    ip_local = socket.gethostbyname(socket.gethostname())
    print("[STARTING] WM_Central inicializándose...")
    print(f"[LISTENING] Escuchando en el puerto {PORT} (IP de esta máquina: {ip_local})")
    print(f"[INFO] Máximo de estaciones simultáneas: {MAX_CONEXIONES}")

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
                print(f"[CONEXIONES ACTIVAS] {conexiones_activas}/{MAX_CONEXIONES}")
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
