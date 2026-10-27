"""
WM_FO - Aplicación del operario de campo (Field Operator).

Uso:  python WM_FO.py <ip_broker> <puerto_broker> <ID_OPERARIO> [fichero_solicitudes]
      python WM_FO.py localhost 9092 FO-12
      python WM_FO.py localhost 9092 FO-12 solicitudes.txt

Todo va por KAFKA (el FO no usa sockets):
    envía wm.solicitudes     -> "quiero regar en WS-01 durante 60 s"
    lee   wm.notificaciones  -> respuestas de Central para ESTE operario:
                                AUTORIZADO, DENEGADO, PROGRESO, RESUMEN, AVISO
    lee   wm.estado_red      -> estado de todas las estaciones (para ver cuáles están libres)

Fichero de solicitudes: una por línea,  <ID_ESTACION>;<duracion_en_segundos>
    WS-01;15
    WS-02;20
Se envía la primera, se espera a que termine (bien o mal), se esperan 4 s y se envía la siguiente.
"""

import os
import sys
import threading
import time
import uuid

from config import (TOPIC_SOLICITUDES, TOPIC_NOTIFICACIONES, TOPIC_ESTADO_RED, DISPONIBLE,
                    crear_topics, crear_productor, crear_consumidor)

# ---------------------------------------------------------- CONFIGURACIÓN

if len(sys.argv) < 4:
    print("Uso: python WM_FO.py <ip_broker> <puerto_broker> <ID_OPERARIO> [fichero]")
    sys.exit(1)

BROKER = f"{sys.argv[1]}:{sys.argv[2]}"
ID_FO = sys.argv[3].upper()
FICHERO = sys.argv[4] if len(sys.argv) > 4 else None

ESPERA_RESPUESTA = 10    # s que esperamos a que Central autorice o deniegue
ESPERA_ENTRE_FICHERO = 4  # s entre una solicitud del fichero y la siguiente
MARGEN_FIN = 30         # s de más que esperamos el resumen después de la duración del riego

# ---------------------------------------------------------------- ESTADO

productor = None
kafka_listo = threading.Event()   # se activa cuando ya escuchamos las notificaciones
estado_red = []                   # última lista de estaciones recibida de Central
lock = threading.Lock()

# Solicitudes que estamos esperando:  id -> {"respuesta": Event, "fin": Event, "autorizado": bool}
#   "respuesta" se activa con AUTORIZADO o DENEGADO
#   "fin"       se activa cuando el riego acaba (RESUMEN) o si se ha denegado
pendientes = {}


# ------------------------------------------------------------- KAFKA: LEER

def mostrar_notificacion(n):
    """Muestra por pantalla un mensaje de Central y despierta a quien lo esté esperando."""
    tipo = n["tipo"]
    ws = n["ws"]

    if tipo == "AUTORIZADO":
        print(f"\n[CENTRAL] AUTORIZADO en {ws}: {n['mensaje']}")
    elif tipo == "DENEGADO":
        print(f"\n[CENTRAL] DENEGADO en {ws}: {n['mensaje']}")
    elif tipo == "PROGRESO":
        print(f"[RIEGO {ws}] {n['mensaje']}")
    elif tipo == "RESUMEN":
        print(f"\n[CENTRAL] RESUMEN {ws}: {n['mensaje']}")
    elif tipo == "AVISO":
        print(f"\n[CENTRAL] AVISO {ws}: {n['mensaje']}")

    with lock:
        espera = pendientes.get(n.get("riego_id"))
    if espera is None:
        return
    if tipo == "AUTORIZADO":
        espera["autorizado"] = True
        espera["respuesta"].set()
    elif tipo == "DENEGADO":
        espera["respuesta"].set()
        espera["fin"].set()
    elif tipo == "RESUMEN":
        espera["fin"].set()


def escuchar_kafka(consumidor):
    """Hilo que lee las notificaciones de este operario y el estado de la red."""
    global estado_red
    while True:
        lotes = consumidor.poll(timeout_ms=1000, max_records=1)
        if not kafka_listo.is_set() and consumidor.assignment():
            kafka_listo.set()
        for mensajes in lotes.values():
            for msg in mensajes:
                try:
                    if msg.topic == TOPIC_ESTADO_RED:
                        with lock:
                            estado_red = msg.value["estaciones"]
                    elif msg.value.get("fo") == ID_FO:      # solo las de este operario
                        mostrar_notificacion(msg.value)
                except (KeyError, ValueError, TypeError) as e:
                    print(f"[KAFKA] Mensaje mal formado: {msg.value} ({e})")
        if lotes:
            consumidor.commit()


def conectar_kafka():
    """Se conecta al broker. Si no está, reintenta cada 5 s."""
    global productor
    while True:
        try:
            crear_topics(BROKER)
            productor = crear_productor(BROKER)
            # Grupo propio de este operario: si cerramos la app y la volvemos a abrir,
            # Kafka nos da los mensajes que llegaron mientras estaba cerrada
            return crear_consumidor(BROKER, [TOPIC_NOTIFICACIONES, TOPIC_ESTADO_RED],
                                    f"fo-{ID_FO}", "latest")
        except Exception as e:
            print(f"[KAFKA] Broker {BROKER} no disponible ({e}). Reintento en 5 s")
            time.sleep(5)


# ------------------------------------------------------------ SOLICITUDES

def solicitar_riego(ws, duracion):
    """Envía una solicitud a Central y espera la respuesta.
    Devuelve el diccionario de espera (para saber si se autorizó) o None si Central no contesta."""
    id_solicitud = str(uuid.uuid4())[:8]
    espera = {"respuesta": threading.Event(), "fin": threading.Event(), "autorizado": False}
    with lock:
        pendientes[id_solicitud] = espera

    try:
        productor.send(TOPIC_SOLICITUDES, key=ID_FO,
                       value={"id": id_solicitud, "fo": ID_FO, "ws": ws, "duracion": duracion,
                              "origen": "FO", "hora": time.time()})
        productor.flush()
    except Exception as e:
        print(f"*** Imposible conectar con WM_Central (error de Kafka: {e}) ***")
        return None

    print(f"Solicitud enviada: regar en {ws} durante {duracion} s. Esperando a Central...")
    if not espera["respuesta"].wait(timeout=ESPERA_RESPUESTA):
        print("*** Imposible conectar con WM_Central ***")
        print("    (la solicitud queda en Kafka: si Central vuelve en menos de 30 s, la atenderá)")
        return None
    return espera


def pedir_riego_manual():
    ws = input("Estación (ej: WS-01): ").strip().upper()
    try:
        duracion = int(input("Duración del riego en segundos: ").strip())
    except ValueError:
        print("La duración tiene que ser un número")
        return
    solicitar_riego(ws, duracion)
    # El progreso y el resumen se irán mostrando solos mientras usamos el menú


def leer_fichero(nombre):
    """Lee el fichero y devuelve una lista de (estacion, duracion). Ignora líneas vacías y '#'."""
    solicitudes = []
    with open(nombre, encoding="utf-8") as f:
        for numero, linea in enumerate(f, start=1):
            linea = linea.strip()
            if not linea or linea.startswith("#"):
                continue
            partes = linea.split(";")
            try:
                solicitudes.append((partes[0].strip().upper(), int(partes[1])))
            except (IndexError, ValueError):
                print(f"  Línea {numero} no válida, se ignora: {linea}")
    return solicitudes


def ejecutar_fichero(nombre):
    """Envía las solicitudes del fichero de una en una: espera a que cada riego termine
    (bien o mal), espera 4 s y pasa a la siguiente."""
    if not os.path.exists(nombre):
        print(f"No existe el fichero {nombre}")
        return
    solicitudes = leer_fichero(nombre)
    print(f"\n[FICHERO] {len(solicitudes)} solicitudes en {nombre}")

    for i, (ws, duracion) in enumerate(solicitudes, start=1):
        print(f"\n[FICHERO] Solicitud {i}/{len(solicitudes)}: {ws} durante {duracion} s")
        espera = solicitar_riego(ws, duracion)
        if espera is not None and espera["autorizado"]:
            # Esperamos el resumen final del riego
            if not espera["fin"].wait(timeout=duracion + MARGEN_FIN):
                print(f"[FICHERO] No ha llegado el resumen de {ws}; pasamos a la siguiente")
        if i < len(solicitudes):
            print(f"[FICHERO] Siguiente solicitud en {ESPERA_ENTRE_FICHERO} s...")
            time.sleep(ESPERA_ENTRE_FICHERO)
    print("\n[FICHERO] Todas las solicitudes del fichero procesadas")


# ------------------------------------------------------------------- MENÚ

def ver_estaciones():
    with lock:
        lista = list(estado_red)
    if not lista:
        print("Todavía no se ha recibido el estado de la red (¿está WM_Central en marcha?)")
        return
    print(f"\n  {'ID':<8} {'ESTADO':<16} UBICACION")
    for e in lista:
        marca = "  <- libre" if e["estado"] == DISPONIBLE else ""
        print(f"  {e['ws']:<8} {e['estado']:<16} {e['ubicacion']}{marca}")
    libres = sum(1 for e in lista if e["estado"] == DISPONIBLE)
    print(f"  {libres} estación(es) disponible(s)")


def menu():
    while True:
        print(f"\n--- Operario {ID_FO} ---")
        print("1. Ver estaciones")
        print("2. Pedir un riego")
        print("3. Ejecutar fichero de solicitudes")
        print("4. Salir")
        opcion = input("> ").strip()

        if opcion == "1":
            ver_estaciones()
        elif opcion == "2":
            pedir_riego_manual()
        elif opcion == "3":
            nombre = input("Nombre del fichero (ej: solicitudes.txt): ").strip()
            ejecutar_fichero(nombre)
        elif opcion == "4":
            break
        else:
            print("Opción no válida")


# ------------------------------------------------------------------ MAIN

def main():
    print(f"[KAFKA] Conectando con el broker {BROKER}...")
    consumidor = conectar_kafka()
    threading.Thread(target=escuchar_kafka, args=(consumidor,), daemon=True).start()

    try:
        # Esperamos a estar escuchando antes de pedir nada (si no, nos perderíamos la respuesta)
        while not kafka_listo.wait(timeout=0.5):
            pass
        print(f"[KAFKA] Conectado. Operario {ID_FO} listo")

        if FICHERO:
            ejecutar_fichero(FICHERO)
        menu()
    except (KeyboardInterrupt, EOFError):
        pass
    print("\n[STOP] WM_FO detenido")


if __name__ == "__main__":
    main()
