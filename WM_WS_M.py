"""
WM_WS_M - Módulo monitor de la estación de riego (cliente).

Uso:  py WM_WS_M.py [ip_central] [puerto] [id_estacion] [ubicacion...]
      (por defecto localhost 5050; si no se indican ID o ubicación se piden por teclado)

Ejemplo:
      py WM_WS_M.py 192.168.1.20 5050 WS-04 River Park
"""

import socket
import sys

# ------------------------------------------------------------- PROTOCOLO
# Cada mensaje va precedido de una CABECERA de 64 bytes con su longitud,
# así el receptor sabe exactamente cuántos bytes tiene que leer.

HEADER = 64
FORMAT = 'utf-8'
SEPARADOR = '#'


def enviar(conn, texto):
    datos = texto.encode(FORMAT)
    cabecera = str(len(datos)).encode(FORMAT)
    cabecera += b' ' * (HEADER - len(cabecera))   # rellenar hasta 64 bytes
    conn.sendall(cabecera + datos)


def recibir_exacto(conn, n):
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
    longitud = int(cabecera.decode(FORMAT).strip())
    datos = recibir_exacto(conn, longitud)
    if datos is None:
        return None
    return datos.decode(FORMAT)


def construir_trama(*campos):
    return SEPARADOR.join(campos)


# ---------------------------------------------------------- CONFIGURACIÓN

HOST = sys.argv[1] if len(sys.argv) > 1 else 'localhost'
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 5050
ID_ESTACION = sys.argv[3] if len(sys.argv) > 3 else None
UBICACION = ' '.join(sys.argv[4:]) if len(sys.argv) > 4 else None


def pedir_campo(texto):
    """Pide un campo por teclado asegurando que no esté vacío ni contenga '#'."""
    while True:
        valor = input(texto).strip()
        if not valor:
            print("  El campo no puede estar vacío")
        elif SEPARADOR in valor:
            print(f"  El campo no puede contener el carácter '{SEPARADOR}'")
        else:
            return valor


def mostrar_respuesta(respuesta):
    campos = respuesta.split(SEPARADOR)
    if len(campos) >= 3 and campos[0] == "STATUS":
        estado, mensaje = campos[1], SEPARADOR.join(campos[2:])
        if estado == "OK":
            print(f">> [OK] {mensaje}")
        else:
            print(f">> [{estado}] {mensaje}")
    else:
        print(">> Respuesta no reconocida:", respuesta)


def main():
    id_estacion = ID_ESTACION or pedir_campo("ID de la estación (ej: WS-04): ")
    ubicacion = UBICACION or pedir_campo("Ubicación (ej: River Park): ")

    if SEPARADOR in id_estacion or SEPARADOR in ubicacion:
        print(f"Los campos no pueden contener el carácter '{SEPARADOR}'")
        return

    cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        cliente.connect((HOST, PORT))
    except (ConnectionRefusedError, socket.gaierror, OSError) as e:
        print(f"No se pudo conectar con WM_Central en {HOST}:{PORT} -> {e}")
        return

    try:
        print(f"Conectado a WM_Central en {HOST}:{PORT}")
        trama = construir_trama("REGISTRO", id_estacion, ubicacion)
        print(f"Enviando: {trama}")
        enviar(cliente, trama)

        respuesta = recibir(cliente)
        if respuesta is None:
            print("WM_Central ha cerrado la conexión sin responder")
        else:
            print(f"Recibido: {respuesta}")
            mostrar_respuesta(respuesta)
    except (ConnectionResetError, BrokenPipeError):
        print("Se perdió la conexión con WM_Central")
    except KeyboardInterrupt:
        print("\nCancelado")
    finally:
        cliente.close()        # cierre limpio de la conexión
        print("Conexión cerrada")


if __name__ == "__main__":
    main()
