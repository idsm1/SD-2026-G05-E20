"""
WM_Central - Servidor central del sistema de Water Management.

Uso:  python WM_Central.py <puerto> <ip_broker:puerto_broker>
      python WM_Central.py 5050 localhost:9092

SOCKETS (con los monitores). Tramas (campos separados por '#'):
    REGISTRO#<ID_ESTACION>#<UBICACION>    -> el monitor da de alta / conecta su estación
    FUGA#<ID_ESTACION>                    -> el monitor avisa de una avería
    RECUPERADA#<ID_ESTACION>              -> el monitor avisa de que la avería se ha resuelto
    PING#<ID_ESTACION>                    -> latido del monitor cada 5 s
  Respuestas:  STATUS#OK#<mensaje>   o   STATUS#ERROR#<motivo>

KAFKA (con los engines y los FO). Central:
    lee   wm.solicitudes   -> peticiones de riego (de un FO o del menú de una estación)
    lee   wm.telemetria    -> caudal y volumen de cada estación que riega (cada segundo)
    lee   wm.eventos_ws    -> fin de riego / riego rechazado por la estación
    envía wm.ordenes       -> START / DENEGADO a las estaciones
    envía wm.notificaciones-> AUTORIZADO / DENEGADO / PROGRESO / RESUMEN / AVISO a cada FO
    envía wm.estado_red    -> lista de estaciones con su estado (para que los FO la vean)

Las estaciones, operarios y riegos terminados se guardan en SQLite (bd.py). Al arrancar,
las estaciones de la base de datos aparecen como DESCONECTADA hasta que su monitor conecta.
"""

import os
import re
import socket
import sys
import threading
import time
from datetime import datetime

import bd
from config import (TOPIC_SOLICITUDES, TOPIC_ORDENES, TOPIC_TELEMETRIA, TOPIC_EVENTOS_WS,
                    TOPIC_NOTIFICACIONES, TOPIC_ESTADO_RED,
                    DISPONIBLE, REGANDO, FUGA, FUERA_SERVICIO, DESCONECTADA,
                    crear_topics, crear_productor, crear_consumidor)
from protocolo import enviar, recibir, construir_trama, separar_trama

# ---------------------------------------------------------- CONFIGURACIÓN

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 5050
BROKER = sys.argv[2] if len(sys.argv) > 2 else 'localhost:9092'
SERVER = "0.0.0.0"          # escucha en todas las interfaces de red
ADDR = (SERVER, PORT)
MAX_CONEXIONES = 10

PATRON_ID = re.compile(r'^WS-\d+$')   # ej: WS-04
CADUCIDAD_SOLICITUD = 30    # s: una solicitud más vieja se deniega (el FO ya se ha cansado de esperar)
REFRESCO_PANEL = 3          # s entre repintados del panel mientras alguna estación riega

# Colores de FONDO para el panel (códigos ANSI). 30 = letra negra, 5 = parpadeo
COLORES = {
    DISPONIBLE: "\033[30;42m",        # fondo verde
    REGANDO: "\033[5;30;42m",         # fondo verde parpadeando
    FUGA: "\033[30;41m",              # fondo rojo
    FUERA_SERVICIO: "\033[30;43m",    # fondo naranja (amarillo en consola)
    DESCONECTADA: "\033[30;47m",      # fondo gris
}
RESET = "\033[0m"

conexiones_activas = 0
# ID_ESTACION -> {ubicacion, estado, bloqueada, addr, riego}
# riego = None, o el riego en curso: {id, fo, duracion, caudal, volumen, segundos}
estaciones = {}
lock = threading.Lock()     # protege el contador y el diccionario de estaciones
productor = None            # productor de Kafka (None mientras no hay conexión con el broker)


# ------------------------------------------------------------- KAFKA: ENVIAR

def publicar(topic, clave, mensaje):
    """Envía un mensaje a Kafka y espera a que el broker lo confirme."""
    if productor is None:
        print(f"[KAFKA] Sin conexión con el broker: no se puede enviar a {topic}")
        return
    try:
        productor.send(topic, key=clave, value=mensaje)
        productor.flush()
    except Exception as e:
        print(f"[KAFKA] Error enviando a {topic}: {e}")


def notificar(fo, tipo, ws, texto, **extra):
    """Envía una notificación a un operario (FO). extra = datos adicionales (volumen...)."""
    mensaje = {"fo": fo, "tipo": tipo, "ws": ws, "mensaje": texto}
    mensaje.update(extra)
    publicar(TOPIC_NOTIFICACIONES, fo, mensaje)


def publicar_estado_red(copia):
    """Envía a todos los FO la lista de estaciones con su estado."""
    if productor is None:      # todavía sin Kafka: no avisamos, ya se publicará luego
        return
    lista = [{"ws": id_ws, "ubicacion": datos["ubicacion"], "estado": datos["estado"]}
             for id_ws, datos in sorted(copia.items())]
    publicar(TOPIC_ESTADO_RED, "red", {"estaciones": lista})


# ------------------------------------------------------------------ PANEL

def mostrar_panel():
    """Muestra el estado de todas las estaciones y lo publica a los FO.
    Llamar SIN tener el lock."""
    with lock:
        copia = {id_ws: dict(datos) for id_ws, datos in estaciones.items()}

    hora = datetime.now().strftime("%H:%M:%S")
    print()
    print("=" * 78)
    print(f"  PANEL DE CONTROL - WM_Central                                     {hora}")
    print("=" * 78)
    print(f"  {'ID':<7} {'ESTADO':<17} {'UBICACION':<18} {'CAUDAL':>11} {'VOLUMEN':>9}  OPERARIO")
    print("-" * 78)
    if not copia:
        print("  (no hay estaciones registradas)")
    for id_ws in sorted(copia):
        datos = copia[id_ws]
        estado = datos["estado"]
        texto_estado = "Fuera de Servicio" if estado == FUERA_SERVICIO else estado
        color = COLORES.get(estado, "")
        linea = f"  {color} {id_ws:<6} {texto_estado:<17}{RESET} {datos['ubicacion'][:18]:<18}"
        riego = datos["riego"]
        if estado == REGANDO and riego is not None:
            linea += (f" {riego['caudal']:5.1f} L/min {riego['volumen']:7.1f} L  {riego['fo']}"
                      f"  ({riego['segundos']}/{riego['duracion']} s)")
        print(linea)
    print("=" * 78)
    print()

    publicar_estado_red(copia)


def refrescar_panel():
    """Hilo: cada pocos segundos
      - si alguna estación riega, repinta el panel (caudal y volumen en tiempo real)
      - si no, solo vuelve a publicar el estado de la red (para los FO que acaban de abrir)"""
    while True:
        time.sleep(REFRESCO_PANEL)
        with lock:
            alguna_regando = any(d["estado"] == REGANDO for d in estaciones.values())
            copia = {id_ws: dict(datos) for id_ws, datos in estaciones.items()}
        if alguna_regando:
            mostrar_panel()
        elif productor is not None:
            publicar_estado_red(copia)


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
        # Si tenía un riego a medias (p. ej. se cayó solo el monitor), lo conservamos
        riego = estaciones[id_estacion]["riego"] if not nueva else None
        if riego is not None:
            estado = REGANDO
        elif bloqueada:
            estado = FUERA_SERVICIO
        else:
            estado = DISPONIBLE
        estaciones[id_estacion] = {"ubicacion": ubicacion, "bloqueada": bloqueada,
                                   "estado": estado, "addr": addr, "riego": riego}

    bd.guardar_estacion(id_estacion, ubicacion)   # alta nueva o actualizar ubicación
    print(f"[REGISTRO] {id_estacion} ({ubicacion}) "
          f"{'dada de alta' if nueva else 'reconectada'} desde {addr[0]}:{addr[1]}")

    if bloqueada:
        publicar(TOPIC_ORDENES, id_estacion, {"tipo": "BLOQUEAR", "ws": id_estacion})

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


def avisar_si_regando(id_estacion, texto):
    """Si la estación tenía un riego en curso, avisa al operario que lo pidió."""
    with lock:
        riego = estaciones[id_estacion]["riego"] if id_estacion in estaciones else None
    if riego is not None:
        notificar(riego["fo"], "AVISO", id_estacion, texto, riego_id=riego["id"])


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
            avisar_si_regando(id_conexion, f"FUGA detectada en {id_conexion}: el riego se corta")
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
            avisar_si_regando(id_conexion, f"Se ha perdido la conexión con {id_conexion}")
            mostrar_panel()
        with lock:
            conexiones_activas -= 1


# ---------------------------------------------------- KAFKA: SOLICITUDES

def denegar(solicitud, motivo):
    """Avisa al operario (y a la estación, si la petición vino de su menú) de que no se riega."""
    print(f"[SOLICITUD] DENEGADA: {motivo}")
    notificar(solicitud["fo"], "DENEGADO", solicitud["ws"], motivo, riego_id=solicitud["id"])
    if solicitud.get("origen") == "WS":
        publicar(TOPIC_ORDENES, solicitud["ws"],
                 {"tipo": "DENEGADO", "ws": solicitud["ws"],
                  "riego_id": solicitud["id"], "motivo": motivo})


def procesar_solicitud(solicitud):
    """Un operario pide regar. Se comprueba todo y se autoriza o se deniega.
    solicitud = {id, fo, ws, duracion, origen ('FO' o 'WS'), hora}"""
    ws = solicitud["ws"]
    fo = solicitud["fo"]
    duracion = int(solicitud["duracion"])
    print(f"\n[SOLICITUD] {fo} pide regar en {ws} durante {duracion} s "
          f"(desde {'la app FO' if solicitud.get('origen') == 'FO' else 'el menú de la estación'})")

    # 1. ¿Es demasiado vieja? (p. ej. se pidió mientras Central estaba caída)
    if time.time() - solicitud.get("hora", time.time()) > CADUCIDAD_SOLICITUD:
        denegar(solicitud, "Solicitud caducada (WM_Central no estaba disponible)")
        return

    # 2. ¿Existe el operario?
    if not bd.existe_operario(fo):
        denegar(solicitud, f"El operario {fo} no esta dado de alta")
        return

    if duracion <= 0:
        denegar(solicitud, "La duracion tiene que ser mayor que 0")
        return

    # 3. ¿La estación existe y está disponible? Si lo está, la reservamos (REGANDO)
    with lock:
        if ws not in estaciones:
            motivo = f"La estacion {ws} no existe"
        elif estaciones[ws]["estado"] != DISPONIBLE:
            motivo = f"La estacion {ws} no esta disponible ({estaciones[ws]['estado']})"
        else:
            motivo = None
            estaciones[ws]["estado"] = REGANDO
            estaciones[ws]["riego"] = {"id": solicitud["id"], "fo": fo, "duracion": duracion,
                                       "caudal": 0.0, "volumen": 0.0, "segundos": 0}
    if motivo is not None:
        denegar(solicitud, motivo)
        return

    # 4. Todo correcto: avisamos al operario y damos la orden a la estación
    print(f"[SOLICITUD] AUTORIZADA: {ws} empieza a regar para {fo}")
    notificar(fo, "AUTORIZADO", ws, f"Riego autorizado en {ws} durante {duracion} s",
              riego_id=solicitud["id"])
    publicar(TOPIC_ORDENES, ws, {"tipo": "START", "ws": ws, "riego_id": solicitud["id"],
                                 "fo": fo, "duracion": duracion})
    mostrar_panel()


# ----------------------------------------------- KAFKA: TELEMETRÍA Y EVENTOS

def procesar_telemetria(dato):
    """La estación nos manda cada segundo cómo va el riego.
    dato = {ws, riego_id, fo, caudal, volumen, segundos, duracion, hora}"""
    ws = dato["ws"]
    with lock:
        if ws not in estaciones:
            return
        riego = estaciones[ws]["riego"]
        if riego is None:
            # Central se ha reiniciado en mitad de un riego: lo recuperamos con estos datos
            # (solo si el dato es reciente y la estación está conectada y sin avería)
            reciente = time.time() - dato.get("hora", 0) < 10
            if not reciente or estaciones[ws]["estado"] not in (DISPONIBLE, REGANDO):
                return
            riego = {"id": dato["riego_id"], "fo": dato["fo"], "duracion": dato["duracion"]}
            estaciones[ws]["riego"] = riego
            estaciones[ws]["estado"] = REGANDO
            print(f"[RIEGO] Recuperado el riego en curso de {ws} (operario {dato['fo']})")
        elif riego["id"] != dato["riego_id"]:
            return                     # dato de un riego antiguo: no nos interesa
        riego["caudal"] = dato["caudal"]
        riego["volumen"] = dato["volumen"]
        riego["segundos"] = dato["segundos"]

    # Al operario le reenviamos el progreso de SU riego
    notificar(dato["fo"], "PROGRESO", ws,
              f"{dato['caudal']:.1f} L/min | {dato['volumen']:.1f} L | "
              f"{dato['segundos']}/{dato['duracion']} s",
              riego_id=dato["riego_id"], caudal=dato["caudal"], volumen=dato["volumen"],
              segundos=dato["segundos"])


def terminar_riego(ws, riego_id):
    """Quita el riego de la estación y, si seguía REGANDO, la deja libre otra vez."""
    with lock:
        if ws not in estaciones:
            return
        riego = estaciones[ws]["riego"]
        if riego is not None and riego["id"] != riego_id:
            return                     # es de otro riego: no tocamos el actual
        estaciones[ws]["riego"] = None
        if estaciones[ws]["estado"] == REGANDO:
            estaciones[ws]["estado"] = (FUERA_SERVICIO if estaciones[ws]["bloqueada"]
                                        else DISPONIBLE)


def procesar_evento(evento):
    """La estación avisa de que un riego ha terminado o de que no ha podido empezar.
    FIN_RIEGO = {tipo, ws, riego_id, fo, inicio, volumen, duracion, motivo}
    RECHAZADO = {tipo, ws, riego_id, fo, motivo}"""
    ws = evento["ws"]
    fo = evento["fo"]

    if evento["tipo"] == "FIN_RIEGO":
        print(f"\n[RIEGO] {ws} ha terminado ({evento['motivo']}): "
              f"{evento['volumen']:.1f} L en {evento['duracion']} s")
        terminar_riego(ws, evento["riego_id"])
        bd.guardar_riego(ws, fo, evento["inicio"], evento["duracion"],
                         evento["volumen"], evento["motivo"])
        notificar(fo, "RESUMEN", ws,
                  f"Riego terminado ({evento['motivo']}): {evento['volumen']:.1f} L "
                  f"en {evento['duracion']} s",
                  riego_id=evento["riego_id"], volumen=evento["volumen"],
                  duracion=evento["duracion"], motivo=evento["motivo"])
        mostrar_panel()

    elif evento["tipo"] == "RECHAZADO":
        print(f"\n[RIEGO] {ws} ha rechazado el riego: {evento['motivo']}")
        terminar_riego(ws, evento["riego_id"])
        notificar(fo, "DENEGADO", ws, f"La estacion ha rechazado el riego: {evento['motivo']}",
                  riego_id=evento["riego_id"])
        mostrar_panel()

# --------------------------------------------- MENÚ: PARAR / REANUDAR ESTACIONES

def elegir_estaciones(texto):
    """Convierte lo que escribe el usuario en (lista de IDs, destino de la orden).
    'todas' o '*' -> todas las estaciones conocidas, y la orden va a '*'.
    Devuelve (None, None) si la estación no existe."""
    texto = texto.strip().upper()
    with lock:
        if texto in ("*", "TODAS"):
            return sorted(estaciones), "*"
        if texto in estaciones:
            return [texto], texto
    return None, None


def bloquear_estaciones(texto):
    """Parar: la estación (o todas) pasa a Fuera de Servicio y no acepta riegos.
    Si estaba regando, el engine corta el riego y nos manda el FIN_RIEGO."""
    ids, destino = elegir_estaciones(texto)
    if ids is None:
        print(f"[MENU] La estacion {texto.strip().upper()} no existe")
        return
    regando = []
    with lock:
        for id_ws in ids:
            datos = estaciones[id_ws]
            datos["bloqueada"] = True
            if datos["estado"] in (DISPONIBLE, REGANDO):
                datos["estado"] = FUERA_SERVICIO
            # (si está en FUGA o DESCONECTADA se queda así; al volver pasará a Fuera de Servicio)
            if datos["riego"] is not None:
                regando.append((id_ws, datos["riego"]))
    for id_ws in ids:
        bd.guardar_bloqueo(id_ws, True)
    for id_ws, riego in regando:
        notificar(riego["fo"], "AVISO", id_ws,
                  f"{id_ws} ha sido puesta fuera de servicio por Central: el riego se corta",
                  riego_id=riego["id"])
    publicar(TOPIC_ORDENES, destino, {"tipo": "BLOQUEAR", "ws": destino})
    print(f"[MENU] Fuera de servicio: {', '.join(ids) if ids else '(ninguna)'}")
    mostrar_panel()


def activar_estaciones(texto):
    """Reanudar: la estación (o todas) vuelve a estar disponible."""
    ids, destino = elegir_estaciones(texto)
    if ids is None:
        print(f"[MENU] La estacion {texto.strip().upper()} no existe")
        return
    with lock:
        for id_ws in ids:
            datos = estaciones[id_ws]
            datos["bloqueada"] = False
            if datos["estado"] == FUERA_SERVICIO:
                datos["estado"] = DISPONIBLE
    for id_ws in ids:
        bd.guardar_bloqueo(id_ws, False)
    publicar(TOPIC_ORDENES, destino, {"tipo": "ACTIVAR", "ws": destino})
    print(f"[MENU] Reactivadas: {', '.join(ids) if ids else '(ninguna)'}")
    mostrar_panel()


def cortar_riego(texto):
    """Corta el riego en curso de una estación, que queda disponible para otro riego."""
    id_ws = texto.strip().upper()
    with lock:
        riego = estaciones[id_ws]["riego"] if id_ws in estaciones else None
    if riego is None:
        print(f"[MENU] {id_ws} no esta regando")
        return
    notificar(riego["fo"], "AVISO", id_ws, f"Central ha cortado el riego en {id_ws}",
              riego_id=riego["id"])
    publicar(TOPIC_ORDENES, id_ws, {"tipo": "STOP", "ws": id_ws, "riego_id": riego["id"]})
    print(f"[MENU] Orden STOP enviada a {id_ws} (el resumen llegará al terminar)")


def menu_central():
    """Hilo con el menú de Central. Si no hay teclado (p. ej. en Docker sin -it), se cierra."""
    time.sleep(1)       # que salgan antes los mensajes de arranque
    while True:
        print("\n--- MENU WM_Central ---")
        print("1. Parar estacion (Fuera de Servicio)   [ID o 'todas']")
        print("2. Reanudar estacion                     [ID o 'todas']")
        print("3. Cortar el riego en curso de una estacion")
        print("4. Ver panel")
        try:
            opcion = input("> ").strip()
            if opcion == "1":
                bloquear_estaciones(input("Estacion (ej: WS-01, o 'todas'): "))
            elif opcion == "2":
                activar_estaciones(input("Estacion (ej: WS-01, o 'todas'): "))
            elif opcion == "3":
                cortar_riego(input("Estacion (ej: WS-01): "))
            elif opcion == "4":
                mostrar_panel()
            elif opcion:
                print("Opcion no valida")
        except EOFError:
            print("[MENU] Sin entrada por teclado: el menu de Central queda desactivado")
            return

# ------------------------------------------------------ KAFKA: HILO LECTOR

def conectar_kafka():
    """Se conecta al broker. Si no está, reintenta cada 5 s (los sockets siguen funcionando)."""
    global productor
    while True:
        try:
            crear_topics(BROKER)
            productor = crear_productor(BROKER)
            # Grupo "central" y 'earliest': si Central se cae, al volver lee lo que se perdió
            consumidor = crear_consumidor(BROKER,
                                          [TOPIC_SOLICITUDES, TOPIC_TELEMETRIA, TOPIC_EVENTOS_WS],
                                          "central", "earliest")
            print(f"[KAFKA] Conectado al broker {BROKER}")
            return consumidor
        except Exception as e:
            print(f"[KAFKA] Broker {BROKER} no disponible ({e}). Reintento en 5 s")
            time.sleep(5)


def escuchar_kafka():
    """Hilo que lee los mensajes de Kafka y llama a la función que toca según el topic."""
    consumidor = conectar_kafka()
    mostrar_panel()
    while True:
        # max_records=1: procesamos de uno en uno y confirmamos (commit) cada uno
        lotes = consumidor.poll(timeout_ms=1000, max_records=1)
        for mensajes in lotes.values():
            for msg in mensajes:
                try:
                    if msg.topic == TOPIC_SOLICITUDES:
                        procesar_solicitud(msg.value)
                    elif msg.topic == TOPIC_TELEMETRIA:
                        procesar_telemetria(msg.value)
                    elif msg.topic == TOPIC_EVENTOS_WS:
                        procesar_evento(msg.value)
                except (KeyError, ValueError, TypeError) as e:
                    print(f"[KAFKA] Mensaje mal formado en {msg.topic}: {msg.value} ({e})")
        if lotes:
            consumidor.commit()        # ya está procesado: Kafka no nos lo volverá a dar


# ------------------------------------------------------------------ MAIN

def start():
    global conexiones_activas
    os.system("")   # en Windows activa los colores ANSI en la consola

    # Base de datos: crear tablas si hace falta y cargar las estaciones conocidas
    bd.crear_tablas()
    for id_ws, ubicacion, bloqueada in bd.cargar_estaciones():
        estaciones[id_ws] = {"ubicacion": ubicacion, "bloqueada": bloqueada,
                             "estado": DESCONECTADA, "addr": None, "riego": None}

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(ADDR)
    server.listen()

    print("[STARTING] WM_Central inicializándose...")
    print(f"[LISTENING] Escuchando en el puerto {PORT}")
    print(f"[BD] {bd.RUTA_BD}: {len(estaciones)} estaciones, "
          f"operarios: {', '.join(bd.cargar_operarios())}")

    threading.Thread(target=escuchar_kafka, daemon=True).start()
    threading.Thread(target=refrescar_panel, daemon=True).start()
    threading.Thread(target=menu_central, daemon=True).start()

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
