"""
WM_WS_E - Engine de la estación de riego.

Uso:  python WM_WS_E.py <ip_broker> <puerto_broker> <ip_monitor> <puerto_monitor>
      python WM_WS_E.py localhost 9092 localhost 6000

Qué hace:
  1. Se conecta a su monitor. El monitor le envía su ID:  ID#WS-04
  2. Responde a cada SALUD del monitor con OK, o con KO si hay una fuga simulada.
  3. KAFKA: escucha las órdenes de Central (wm.ordenes) que son para su estación:
         START     -> abre la electroválvula y empieza a regar
         STOP      -> para el riego
         BLOQUEAR  -> para el riego y queda fuera de servicio
         ACTIVAR   -> vuelve a estar disponible
         DENEGADO  -> Central ha denegado el riego que se pidió desde este menú
     Mientras riega, envía cada segundo el caudal y el volumen (wm.telemetria).
     Al terminar, envía el resumen del riego (wm.eventos_ws).
  4. Menú por teclado: simular/arreglar una fuga, pedir un riego a Central, parar el riego.
  5. Si el monitor se cae, para el riego y reintenta conectar cada 3 s.
"""

import random
import socket
import sys
import threading
import time
import uuid
from datetime import datetime

from config import (TOPIC_SOLICITUDES, TOPIC_ORDENES, TOPIC_TELEMETRIA, TOPIC_EVENTOS_WS,
                    DISPONIBLE, REGANDO, FUGA, FUERA_SERVICIO, DESCONECTADA,
                    crear_topics, crear_productor, crear_consumidor)
from protocolo import enviar, recibir, separar_trama

# ---------------------------------------------------------- CONFIGURACIÓN

if len(sys.argv) < 5:
    print("Uso: python WM_WS_E.py <ip_broker> <puerto_broker> <ip_monitor> <puerto_monitor>")
    sys.exit(1)

BROKER = f"{sys.argv[1]}:{sys.argv[2]}"
MONITOR = sys.argv[3]
P_MONITOR = int(sys.argv[4])

CAUDAL_MIN = 5.0       # L/min
CAUDAL_MAX = 10.0
ESPERA_CENTRAL = 10    # s que esperamos la respuesta de Central a una petición de riego

# ------------------------------------------------------- ESTADO DEL ENGINE

id_estacion = None
salud_ok = True               # False = fuga simulada (se responde KO al monitor)
bloqueada = False             # Central nos ha puesto "Fuera de servicio"
monitor_conectado = False
regando = False
riego_id_actual = None        # id del riego en curso (para ignorar órdenes repetidas)
motivo_parada = None
fin_riego = threading.Event() # se activa para cortar el riego antes de tiempo
lock = threading.Lock()

productor = None              # productor de Kafka
kafka_listo = threading.Event()        # se activa cuando ya escuchamos las órdenes
solicitud_pendiente = None    # id de la petición de riego hecha desde el menú
respuesta_central = threading.Event()  # se activa cuando Central contesta a esa petición


def estado_act():
    """Estado actual de la estación. Llamar CON el lock cogido."""
    if not monitor_conectado:
        return DESCONECTADA
    if not salud_ok:
        return FUGA
    if bloqueada:
        return FUERA_SERVICIO
    if regando:
        return REGANDO
    return DISPONIBLE


# ------------------------------------------------------------- KAFKA: ENVIAR

def publicar(topic, mensaje):
    """Envía un mensaje a Kafka (la clave es el ID de la estación)."""
    if productor is None:
        print(f"[KAFKA] Sin conexión con el broker: no se puede enviar a {topic}")
        return
    try:
        productor.send(topic, key=id_estacion, value=mensaje)
        productor.flush()
    except Exception as e:
        print(f"[KAFKA] Error enviando a {topic}: {e}")


# ------------------------------------------------------------------ RIEGO

def regar(operario, duracion, riego_id):
    """Empieza un riego en un hilo aparte. Devuelve None si ha ido bien o el motivo del error."""
    global regando, riego_id_actual
    with lock:
        estado = estado_act()
        if estado != DISPONIBLE:
            return f"La estación está en estado {estado}"
        regando = True
        riego_id_actual = riego_id
        fin_riego.clear()
        hilo = threading.Thread(target=riego, args=(operario, duracion, riego_id), daemon=True)
        hilo.start()
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


def riego(operario, duracion, riego_id):
    """Hilo del riego: cada segundo mide el caudal y se lo manda a Central."""
    global regando, riego_id_actual
    print(f"\n[RIEGO] Electroválvula ABIERTA | operario {operario} | límite: {duracion} s")

    volumen = 0.0
    inicio = time.time()
    hora_inicio = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
        publicar(TOPIC_TELEMETRIA, {"ws": id_estacion, "riego_id": riego_id, "fo": operario,
                                    "caudal": round(caudal, 2), "volumen": round(volumen, 2),
                                    "segundos": transcurrido, "duracion": duracion,
                                    "hora": time.time()})

        if transcurrido >= duracion:
            break

    duracion_real = int(time.time() - inicio)
    with lock:
        regando = False
        riego_id_actual = None
    print(f"[RIEGO] Electroválvula CERRADA ({motivo}) | {volumen:.1f} L en {duracion_real} s")

    # Resumen final para Central (que se lo reenvía al operario y lo guarda en la BD)
    publicar(TOPIC_EVENTOS_WS, {"tipo": "FIN_RIEGO", "ws": id_estacion, "riego_id": riego_id,
                                "fo": operario, "inicio": hora_inicio,
                                "volumen": round(volumen, 2), "duracion": duracion_real,
                                "motivo": motivo})


# ------------------------------------------------------ KAFKA: ÓRDENES

def procesar_orden(orden):
    """Ejecuta una orden de Central dirigida a esta estación (o a todas, ws = '*')."""
    global bloqueada
    tipo = orden["tipo"]

    if tipo == "START":
        with lock:
            repetida = orden["riego_id"] == riego_id_actual
        if repetida:
            return
        print(f"\n[CENTRAL] Orden de riego: operario {orden['fo']}, {orden['duracion']} s")
        error = regar(orden["fo"], int(orden["duracion"]), orden["riego_id"])
        if error:
            print(f"[CENTRAL] No se puede regar: {error}")
            publicar(TOPIC_EVENTOS_WS, {"tipo": "RECHAZADO", "ws": id_estacion,
                                        "riego_id": orden["riego_id"], "fo": orden["fo"],
                                        "motivo": error})

    elif tipo == "STOP":
        print("\n[CENTRAL] Orden de parar el riego")
        parar_riego("Parado desde Central")

    elif tipo == "BLOQUEAR":
        print("\n[CENTRAL] *** Estación BLOQUEADA: Fuera de servicio ***")
        with lock:
            bloqueada = True
        parar_riego("Estacion bloqueada por Central")

    elif tipo == "ACTIVAR":
        print("\n[CENTRAL] Estación ACTIVADA: vuelve a estar disponible")
        with lock:
            bloqueada = False

    elif tipo == "DENEGADO":
        print(f"\n[CENTRAL] Riego DENEGADO: {orden['motivo']}")

    # Si es la respuesta a la petición que hicimos desde el menú, despertamos al menú
    if orden.get("riego_id") is not None and orden["riego_id"] == solicitud_pendiente:
        respuesta_central.set()


def conectar_kafka():
    """Se conecta al broker. Si no está, reintenta cada 5 s."""
    global productor
    while True:
        try:
            crear_topics(BROKER)
            productor = crear_productor(BROKER)
            # Cada estación tiene su propio grupo: así TODAS reciben las órdenes
            # (luego cada una se queda solo con las suyas)
            consumidor = crear_consumidor(BROKER, [TOPIC_ORDENES],
                                          f"engine-{id_estacion}", "latest")
            return consumidor
        except Exception as e:
            print(f"[KAFKA] Broker {BROKER} no disponible ({e}). Reintento en 5 s")
            time.sleep(5)


def escuchar_ordenes():
    """Hilo que lee las órdenes de Central y se queda solo con las de esta estación."""
    consumidor = conectar_kafka()
    while True:
        lotes = consumidor.poll(timeout_ms=1000, max_records=1)
        if not kafka_listo.is_set() and consumidor.assignment():
            print(f"[KAFKA] Conectado al broker {BROKER}, esperando órdenes de Central")
            kafka_listo.set()
        for mensajes in lotes.values():
            for msg in mensajes:
                orden = msg.value
                if orden.get("ws") in (id_estacion, "*"):
                    try:
                        procesar_orden(orden)
                    except (KeyError, ValueError, TypeError) as e:
                        print(f"[KAFKA] Orden mal formada: {orden} ({e})")
        if lotes:
            consumidor.commit()


def pedir_riego():
    """Riego pedido por un operario en el menú de la estación: se pide a Central,
    que comprueba todo y, si está bien, nos manda la orden START."""
    global solicitud_pendiente
    operario = input("ID del operario (ej: FO-12): ").strip().upper()
    try:
        duracion = int(input("Duración del riego en segundos: ").strip())
    except ValueError:
        print("La duración tiene que ser un número")
        return

    solicitud_pendiente = str(uuid.uuid4())[:8]
    respuesta_central.clear()
    publicar(TOPIC_SOLICITUDES, {"id": solicitud_pendiente, "fo": operario, "ws": id_estacion,
                                 "duracion": duracion, "origen": "WS", "hora": time.time()})
    print(f"Petición enviada a Central. Esperando respuesta (máx. {ESPERA_CENTRAL} s)...")

    if not respuesta_central.wait(timeout=ESPERA_CENTRAL):
        print("*** Imposible conectar con WM_Central ***")
        print("    (la petición queda en Kafka: si Central vuelve en menos de 30 s, la atenderá)")
    solicitud_pendiente = None


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
        parar_riego("Sin conexion con el monitor")

        conn, nuevo_id = conectar_mon()
        if nuevo_id != id_estacion:
            print(f"[MONITOR] AVISO: el monitor dice ser {nuevo_id}; seguimos como {id_estacion}")
        print("[MONITOR] Reconectado")


# ------------------------------------------------------------------- MENÚ

def cambiar_fuga():
    """Simula una fuga (el monitor recibirá KO) o la arregla."""
    global salud_ok
    with lock:
        salud_ok = not salud_ok
        hay_fuga = not salud_ok
    if hay_fuga:
        print("*** FUGA SIMULADA: el monitor recibirá KO ***")
        if parar_riego("FUGA"):
            print("El riego se corta inmediatamente por la fuga")
    else:
        print("Fuga arreglada: el monitor vuelve a recibir OK")


def menu():
    while True:
        with lock:
            estado = estado_act()
        print(f"\n--- {id_estacion} | estado: {estado} ---")
        print("1. Simular fuga / arreglar fuga")
        print("2. Pedir un riego (operario en la estación)")
        print("3. Parar riego")
        print("4. Salir")
        opcion = input("> ").strip()

        if opcion == "1":
            cambiar_fuga()
        elif opcion == "2":
            pedir_riego()
        elif opcion == "3":
            if not parar_riego("Parada manual en la estacion"):
                print("No se está regando")
        elif opcion == "4":
            break
        else:
            print("Opción no válida")


# ------------------------------------------------------------------ MAIN

def main():
    global id_estacion
    conn, id_estacion = conectar_mon()
    print(f"[MONITOR] Soy la estación {id_estacion}")

    threading.Thread(target=monitor, args=(conn,), daemon=True).start()
    threading.Thread(target=escuchar_ordenes, daemon=True).start()

    print(f"[KAFKA] Conectando con el broker {BROKER}...")
    try:
        # No enseñamos el menú hasta poder recibir órdenes.
        # (wait con timeout dentro de un bucle para que Ctrl+C funcione en Windows)
        while not kafka_listo.wait(timeout=0.5):
            pass
        menu()
    except (KeyboardInterrupt, EOFError):
        pass
    print("\n[STOP] Engine detenido")


if __name__ == "__main__":
    main()
