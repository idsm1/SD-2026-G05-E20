import random
import socket
import sys
import threading
import time

CABECERA = 64

MONITOR = sys.argv[1] if len(sys.argv) > 1 else 'localhost'
P_MONITOR = int(sys.argv[2]) if len(sys.argv) > 2 else 5050
KAFKA = sys.argv[3] if len(sys.argv) > 3 else 'localhost'
P_KAFKA = int(sys.argv[4]) if len(sys.argv) > 4 else 9092

DURACION = 60
CAUDAL_MIN = 5.0
CAUDAL_MAX = 10.0

def enviar(conn,texto):
    datos = texto.encode('utf-8')
    cabecera = str(len(datos)).encode('utf-8')
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
    return None

def parar_riego(motivo):
    global motivo_parada
    with lock:
        if not regando:
            return False
        motivo_parada = motivo
    fin_riego.set()
    return True

def riego(operario, duracion):
    global regando
    print(f"Electroválvula abierta | operario {operario} | límite: {duracion}s")

    volumen = 0.0
    inicio = time.time()
    motivo = "Fin de tiempo"

    while True:
        if fin_riego.wait(timeout=1):
            with lock:
                motivo = motivo_parada
            break

        caudal = random.uniform(CAUDAL_MIN,CAUDAL_MAX)
        volumen = volumen + caudal/60
        transcurrido = int (time.time() - inicio)

        print(f"{caudal:5.1f}L/min | {volumen:6.1f}L | {transcurrido}/{duracion} s")

        if transcurrido >= duracion:
            break

    duracion_real = int(time.time() - inicio)
    with lock:
        regando = False
    print(f"Electroválvula cerrada ({motivo}) | {volumen:.1f}L en {duracion_real}s")

def conectar_mon():
    while True:
        try:
            conn = socket.create_connection((MONITOR, P_MONITOR), timeout=5)
            conn.settimeout(None)
            print(f"Conectado a {MONITOR}:{P_MONITOR}, esperando ID...")
            trama = recibir(conn)
            campos = trama.split("#") if trama else []
            if len(campos) == 2 and campos[0] == "ID" and campos[1]:
                return conn, campos[1]
            print(f"Respuesta inesperada: {trama}")
            conn.close()

        except OSError as e:
            print(f"No disponible en {MONITOR}:{P_MONITOR} ({e}). Reintento en 3 s")
        time.sleep(3)

def atender_mon(conn):
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
    global monitor_conectado
    while True:
        with lock:
            monitor_conectado = True
        atender_mon(conn)
        with lock:
            monitor_conectado = False
        print("Conexión perdida. Se para el riego")
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
    