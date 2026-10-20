"""
protocolo.py - Funciones de envío y recepción por sockets.

Lo usan WM_Central, WM_WS_M y WM_WS_E, así está escrito una sola vez.

Cada mensaje va precedido de una CABECERA de 64 bytes con su longitud,
así el receptor sabe exactamente cuántos bytes tiene que leer.
Los campos del mensaje van separados por '#'.

    Ejemplo:  "25" + 62 espacios  +  "REGISTRO#WS-04#River Park"
              (cabecera, 64 bytes)    (datos, 25 bytes)
"""

CABECERA = 64
FORMATO = 'utf-8'
SEPARADOR = '#'


def enviar(conn, texto):
    """Envía un mensaje de texto precedido de su cabecera de longitud."""
    datos = texto.encode(FORMATO)
    cabecera = str(len(datos)).encode(FORMATO)
    cabecera += b' ' * (CABECERA - len(cabecera))   # rellenar hasta 64 bytes
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
    cabecera = recibir_exacto(conn, CABECERA)
    if cabecera is None:
        return None
    try:
        longitud = int(cabecera.decode(FORMATO).strip())
    except ValueError:
        return None            # cabecera corrupta: lo tratamos como desconexión
    datos = recibir_exacto(conn, longitud)
    if datos is None:
        return None
    return datos.decode(FORMATO)


def construir_trama(*campos):
    """construir_trama("FUGA", "WS-04") -> "FUGA#WS-04" """
    return SEPARADOR.join(campos)


def separar_trama(trama):
    """separar_trama("FUGA#WS-04") -> ["FUGA", "WS-04"]"""
    return [campo.strip() for campo in trama.strip().split(SEPARADOR)]