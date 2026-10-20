"""
WM_WS_E - Engine de la estación de riego.

Uso:  python WM_WS_E.py <ip_broker> <puerto_broker> <ip_monitor> <puerto_monitor>
      python WM_WS_E.py localhost 9092 localhost 6000

      (el broker de Kafka todavía no se usa; se añade en la siguiente fase)

Qué hace:
  1. Se conecta a su monitor. El monitor le envía su ID:  ID#WS-04
  2. Responde a cada SALUD del monitor con OK, o con KO si hay una fuga simulada.
  3. Menú por teclado para simular/arreglar una fuga y para probar el riego.
  4. Si el monitor se cae, para el riego y se reintenta conectar cada 3 s.
"""

import random
import socket
import sys
import threading
import time

from protocolo import enviar, recibir, separar_trama

MONITOR = sys.argv[1] if len(sys.argv) > 1 else 'localhost'
P_MONITOR = int(sys.argv[2]) if len(sys.argv) > 2 else 5050
KAFKA = sys.argv[3] if len(sys.argv) > 3 else 'localhost'
P_KAFKA = int(sys.argv[4]) if len(sys.argv) > 4 else 9092

if len(sys.argv) < 5:
    print("Uso: python WM_WS_E.py <ip_broker> <puerto_broker> <ip_monitor> <puerto_monitor>")
    sys.exit(1)

BROKER = f"{sys.argv[1]}:{sys.argv[2]}"   # para la fase de Kafka
MONITOR = sys.argv[3]
P_MONITOR = int(sys.argv[4])

DURACION = 60          # duración por defecto de un riego (s)
CAUDAL_MIN = 5.0       # L/min
CAUDAL_MAX = 10.0

id_estacion = None
salud_ok = True               # False = fuga simulada (se responde KO al monitor)
bloqueada = False             # lo usará Central para poner "Fuera de servicio"
monitor_conectado = False
regando = False
motivo_parada = None
fin_riego = threading.Event() # se activa para cortar el riego antes de tiempo
riego_actual = None
lock = threading.Lock()


def estado_act():
    """Estado actual de la estación. Llamar CON el lock cogido."""
    if not monitor_conectado:
        return "DESCONECTADA"
    if not salud_ok:
        return "FUGA"
    if bloqueada:
        return "FUERA_SERVICIO"
    if regando:
        return "REGANDO"
    return "DISPONIBLE"


# ------------------------------------------------------------------ RIEGO

def regar(operario, duracion):
    """Empieza un riego en un hilo aparte. Devuelve None si ha ido bien o el motivo del error."""
    global regando, riego_actual
    with lock:
        estado = estado_act()
        if estado != "DISPONIBLE":
            return f"La estación está en estado {estado}"
        regando = True
        fin_riego.clear()
        riego_actual = threading.Thread(target=riego, args=(operario, duracion), daemon=True)
        riego_actual.start()
    return None


def parar_riego(motivo):
    """Corta el riego en curso. Devuelve False si no se estaba regando."""
    global motivo_parada
    with lock:
        if not regando:
            return False
        motivo_parada = motivo
    fin_riego.set()
    return True


def riego(operario, duracion):
    global regando
    print(f"\n[RIEGO] Electroválvula abierta | operario {operario} | límite: {duracion}s")

    volumen = 0.0
    inicio = time.time()
    motivo = "Fin de tiempo"

    while True:
        if fin_riego.wait(timeout=1):       # espera 1 s o hasta que nos paren
            with lock:
                motivo = motivo_parada
            break

        caudal = random.uniform(CAUDAL_MIN, CAUDAL_MAX)
        volumen = volumen + caudal / 60     # L/min -> litros en 1 segundo
        transcurrido = int(time.time() - inicio)

        print(f"[RIEGO] {caudal:5.1f} L/min | {volumen:6.1f} L | {transcurrido}/{duracion} s")

        if transcurrido >= duracion:
            break

    duracion_real = int(time.time() - inicio)
    with lock:
        regando = False
    print(f"[RIEGO] Electroválvula cerrada ({motivo}) | {volumen:.1f} L en {duracion_real} s")


# ------------------------------------------------------- CONEXIÓN MONITOR

def conectar_mon():
    """Se conecta al monitor y espera a que le diga su ID. Reintenta cada 3 s."""
    while True:
        try:
            conn = socket.create_connection((MONITOR, P_MONITOR), timeout=5)
            conn.settimeout(None)
            print(f"[MONITOR] Conectado a {MONITOR}:{P_MONITOR}, esperando ID...")
            trama = recibir(conn)
            campos = separar_trama(trama) if trama else []
            if len(campos) == 2 and campos[0] == "ID" and campos[1]:
                return conn, campos[1]
            print(f"[MONITOR] Respuesta inesperada: {trama}")
            conn.close()
        except OSError as e:
            print(f"[MONITOR] No disponible en {MONITOR}:{P_MONITOR} ({e}). Reintento en 3 s")
        time.sleep(3)


def atender_mon(conn):
    """Responde a los SALUD del monitor hasta que se corte la conexión."""
    try:
        while True:
            trama = recibir(conn)
            if trama is None:
                break
            if trama.strip().upper() == "SALUD":
                with lock:
                    respuesta = "OK" if salud_ok else "KO"
                enviar(conn, respuesta)
    except OSError:
        pass
    finally:
        conn.close()


def monitor(conn):
    """Hilo que mantiene la conexión con el monitor (y la recupera si se cae)."""
    global monitor_conectado
    while True:
        with lock:
            monitor_conectado = True
        atender_mon(conn)
        with lock:
            monitor_conectado = False
        print("\n[MONITOR] Conexión perdida. Se para el riego")
        parar_riego("SIN_MONITOR")

        conn, nuevo_id = conectar_mon()
        if nuevo_id != id_estacion:
            print(f"AVISO: el Monitor dice ser {nuevo_id}; seguimos como {id_estacion}")
        print("Reconectado")

def cambio_estado():
    global salud_ok
    with lock:
        salud_ok = not salud_ok
        ok = salud_ok
    if ok:
        print("Arreglado: Vuelve a responder OK")
    else:
        print("Fuga: Mandando KO al monitor")
        parar_riego("FUGA")

def sol_riego():
    if estado_act() != "Disponible":
        print(f"No se puede regar por: estado {estado_act()}")
        return

    operario = input("ID operario: ").strip()
    texto = input(f"Duracion en segundos [{DURACION}]:").strip()
    duracion = int(texto) if texto.isdigit() else DURACION
    print("Solicitud enviada")
    
