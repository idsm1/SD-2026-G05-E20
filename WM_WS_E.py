import random
import socket
import sys
import threading
import time

CABECERA = 64

MONITOR = sys.argv[1] if len(sys.argv) > 1 else 'localhost'
P_MONITOR = int(sys.argv[2]) if len(sys.argv) > 2 else 5050
DOCKER = sys.argv[3] if len(sys.argv) > 3 else 'localhost'
P_DOCKER = sys.argv[4] if len(sys.argv) > 4 else 9092


def enviar(conn,texto):
    datos = texto
    cabecera = str(len(datos))
    cabecera += b' ' * (CABECERA - len(cabecera))
    conn.sendall(cabecera + datos)

def recibir_exacto(conn, n):
    datos = b''
    while len(datos) < n:
        trozo = conn.recv(n - len(datos))
        if not trozo:
            return None
        datos += trozo
    return datos

def recibir(conn):
    cabecera = recibir_exacto(conn, CABECERA)
    if cabecera is None:
        return None
    longitud = int(cabecera.strip())
    datos = recibir_exacto(conn, longitud)
    if datos is None:
        return None
    return datos.decode('utf-8')


id_estacion = None
salud_ok = True
bloqueada = False
monitor_conectado = False
regando = False
motivo_parada = None
fin_riego = threading.Event()
riego_actual = None
productor = None
lock = threading.Lock()

def estado_act():
    if not monitor_conectado:
        return "DESCONECTADA"
    if not salud_ok:
        return "fuga"
    if bloqueada:
        return "Fuera de servicio"
    if regando:
        return "Regando"
    return "Disponible"

def regar(operario,duracion):
    global regando, riego_actual
    with lock:
        estado = estado_act()
        if estado != "Disponible":
            return f"La estación está en estado {estado}"
        regando = True
        fin_riego.clear()
        riego_actual = threading.Thread(target=riego, args=(operario,duracion), daemon=True)
        riego_actual.start()
    return none

def parar_riego(motivo):
    global motivo_parada
    with lock:
        if not regando:
            return False
        motivo_parada = motivo
    fin_riego.set()
    return True

