"""
WM_WS_M - Monitor de la estación de riego.

Uso:  python WM_WS_M.py <puerto_para_engine> <ip_central> <puerto_central> <ID> [ubicacion...]
      python WM_WS_M.py 6000 localhost 5050 WS-04 River Park

      (si no se indica la ubicación se pide por teclado)

Qué hace:
  1. Se conecta a WM_Central y registra la estación. Esa conexión se queda ABIERTA.
  2. Abre un servidor en <puerto_para_engine> y espera a que se conecte su Engine.
  3. Cuando el Engine se conecta le envía su ID:   ID#WS-04
  4. Cada segundo le pregunta:  SALUD   -> el Engine responde OK o KO
  5. Si responde KO, no responde en 3 s o se desconecta  -> FUGA#WS-04 a Central
     Cuando vuelve a responder OK                         -> RECUPERADA#WS-04 a Central
  6. Cada 5 s envía PING#WS-04 a Central (latido). Si Central se ha caído o reiniciado,
     así nos enteramos enseguida, nos volvemos a registrar y le recordamos si hay fuga.
"""

import socket
import sys
import threading
import time

from protocolo import enviar, recibir, construir_trama, separar_trama

# ---------------------------------------------------------- CONFIGURACIÓN

if len(sys.argv) < 5:
    print("Uso: python WM_WS_M.py <puerto_para_engine> <ip_central> <puerto_central> <ID> [ubicacion]")
    sys.exit(1)

PUERTO_ENGINE = int(sys.argv[1])
CENTRAL = sys.argv[2]
P_CENTRAL = int(sys.argv[3])
ID_ESTACION = sys.argv[4]
UBICACION = ' '.join(sys.argv[5:]) if len(sys.argv) > 5 else None

INTERVALO_SALUD = 1      # segundos entre comprobaciones
TIMEOUT_SALUD = 3        # segundos que esperamos la respuesta del engine
INTERVALO_LATIDO = 5     # segundos entre PING a Central

conn_central = None      # socket con WM_Central (se queda abierto)
en_fuga = False          # último estado que hemos comunicado a Central
lock_central = threading.RLock()   # el socket de Central lo usan dos hilos (salud y latido)


# ------------------------------------------------------- CONEXIÓN CENTRAL

def pedir_campo(texto):
    while True:
        valor = input(texto).strip()
        if not valor:
            print("  El campo no puede estar vacío")
        elif '#' in valor:
            print("  El campo no puede contener el carácter '#'")
        else:
            return valor


def conectar_central():
    """Se conecta a Central y registra la estación.
    Si Central no está, reintenta cada 3 s. Si Central rechaza el registro, termina."""
    global conn_central
    while True:
        try:
            conn = socket.create_connection((CENTRAL, P_CENTRAL), timeout=5)
            conn.settimeout(None)
            enviar(conn, construir_trama("REGISTRO", ID_ESTACION, UBICACION))
            respuesta = recibir(conn)
        except OSError as e:
            print(f"[CENTRAL] No disponible en {CENTRAL}:{P_CENTRAL} ({e}). Reintento en 3 s")
            time.sleep(3)
            continue

        if respuesta is None:
            print("[CENTRAL] Ha cerrado la conexión sin responder. Reintento en 3 s")
            conn.close()
            time.sleep(3)
            continue

        campos = separar_trama(respuesta)
        if len(campos) >= 3 and campos[0] == "STATUS" and campos[1] == "OK":
            print(f"[CENTRAL] Registrada como {ID_ESTACION} -> {campos[2]}")
            conn_central = conn
            return

        # Central nos ha rechazado (ID no válido, ya conectada...): no tiene sentido reintentar
        print(f"[CENTRAL] Registro rechazado: {respuesta}")
        conn.close()
        sys.exit(1)


def avisar_central(comando):
    """Envía FUGA, RECUPERADA o PING a Central y lee su respuesta.
    Si la conexión se ha caído, se reconecta y lo vuelve a enviar."""
    mensaje = construir_trama(comando, ID_ESTACION)
    with lock_central:              # mensaje y respuesta tienen que ir juntos
        while True:
            try:
                enviar(conn_central, mensaje)
                respuesta = recibir(conn_central)
                if respuesta is not None:
                    if comando != "PING":
                        print(f"[CENTRAL] {mensaje} -> {respuesta}")
                    return
            except OSError:
                pass
            print("[CENTRAL] Conexión perdida. Reconectando...")
            conn_central.close()
            conectar_central()
            # Central nos acaba de registrar como DISPONIBLE: si teníamos fuga, se lo recordamos
            if en_fuga and comando != "FUGA":
                avisar_central("FUGA")


def latido_central():
    """Hilo que envía PING a Central cada 5 s para detectar si se ha caído."""
    while True:
        time.sleep(INTERVALO_LATIDO)
        avisar_central("PING")


# -------------------------------------------------------- VIGILAR ENGINE

def actualizar_salud(sana, motivo):
    """Avisa a Central solo cuando cambia el estado (sana <-> fuga)."""
    global en_fuga
    if not sana and not en_fuga:
        en_fuga = True
        print(f"[SALUD] *** AVERIA: {motivo} ***")
        avisar_central("FUGA")
    elif sana and en_fuga:
        en_fuga = False
        print("[SALUD] El engine vuelve a responder OK. Averia resuelta")
        avisar_central("RECUPERADA")


def vigilar_engine(conn, addr):
    """Envía SALUD cada segundo al engine conectado hasta que se desconecte."""
    print(f"[ENGINE] Conectado desde {addr[0]}:{addr[1]}")
    try:
        enviar(conn, construir_trama("ID", ID_ESTACION))
        conn.settimeout(TIMEOUT_SALUD)
        comprobaciones = 0

        while True:
            enviar(conn, "SALUD")
            try:
                respuesta = recibir(conn)
            except socket.timeout:
                actualizar_salud(False, f"el engine no responde en {TIMEOUT_SALUD} s")
                continue

            if respuesta is None:
                break                         # el engine se ha cerrado
            if respuesta == "OK":
                actualizar_salud(True, "")
            else:
                actualizar_salud(False, "el engine ha respondido KO")

            comprobaciones += 1
            if comprobaciones % 10 == 0:      # cada 10 s, para que se vea que sigue vivo
                print(f"[SALUD] {comprobaciones} comprobaciones. Estado: "
                      f"{'FUGA' if en_fuga else 'OK'}")
            time.sleep(INTERVALO_SALUD)
    except OSError:
        pass
    finally:
        conn.close()

    actualizar_salud(False, "el engine se ha desconectado")


# ------------------------------------------------------------------ MAIN

def main():
    global UBICACION
    if '#' in ID_ESTACION:
        print("El ID no puede contener el carácter '#'")
        return
    if UBICACION is None:
        UBICACION = pedir_campo("Ubicación (ej: River Park): ")

    conectar_central()
    threading.Thread(target=latido_central, daemon=True).start()

    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    servidor.bind(("0.0.0.0", PUERTO_ENGINE))
    servidor.listen()
    print(f"[ENGINE] Esperando al engine de {ID_ESTACION} en el puerto {PUERTO_ENGINE}...")

    try:
        while True:
            conn, addr = servidor.accept()     # un engine cada vez
            vigilar_engine(conn, addr)
            print(f"[ENGINE] Esperando a que el engine vuelva a conectarse...")
    except KeyboardInterrupt:
        print("\n[STOP] Monitor detenido")
    finally:
        servidor.close()
        if conn_central:
            conn_central.close()


if __name__ == "__main__":
    main()