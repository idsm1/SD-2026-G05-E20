import socket
import sys

CABECERA = 64

def enviar(conn, texto):
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
    """Recibe un mensaje completo. Devuelve None si la conexión se ha cerrado."""
    cabecera = recibir_exacto(conn, CABECERA)
    if cabecera is None:
        return None
    longitud = int(cabecera.strip())
    datos = recibir_exacto(conn, longitud)
    if datos is None:
        return None
    return datos.decode('utf-8')

CENTRAL = sys.argv[1] if len(sys.argv) > 1 else 'localhost'
P_CENTRAL = int(sys.argv[2]) if len(sys.argv) > 2 else 5050
ID_ESTACION = sys.argv[3] if len(sys.argv) > 3 else None
UBICACION = ' '.join(sys.argv[4:]) if len(sys.argv) > 4 else None

def pedir_campo(texto):
    while True:
        valor = input(texto).strip()
        if not valor:
            print("  El campo no puede estar vacío")
        else:
            return valor


def mostrar_respuesta(respuesta):
    campos = respuesta.split('#')
    if len(campos) >= 3 and campos[0] == "STATUS":
        estado, mensaje = campos[1], campos[2]
        if estado == "OK":
            print(f">> [OK] {mensaje}")
        else:
            print(f">> [{estado}] {mensaje}")
    else:
        print(">> Respuesta no reconocida:", respuesta)


def main():
    id_estacion = ID_ESTACION or pedir_campo("ID de la estación (ej: WS-04): ")
    ubicacion = UBICACION or pedir_campo("Ubicación (ej: River Park): ")

    if '#' in id_estacion or '#' in ubicacion:
        print("Los campos no pueden contener el carácter '#'")
        return

    cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        cliente.connect((CENTRAL, P_CENTRAL))
    except (OSError) as e:
        print(f"No se pudo conectar con WM_Central en {CENTRAL}:{P_CENTRAL} -> {e}")
        return

    try:
        print(f"Conectado a WM_Central en {CENTRAL}:{P_CENTRAL}")
        msg = (f"REGISTRO#{id_estacion}#{ubicacion}")
        print(f"Enviando: {msg}")
        enviar(cliente, msg)

        respuesta = recibir(cliente)
        if respuesta is None:
            print("WM_Central ha cerrado la conexión sin responder")
        else:
            print(f"Recibido: {respuesta}")
            mostrar_respuesta(respuesta)
    except (ConnectionResetError, BrokenPipeError):
        print("Se perdió la conexión con WM_Central")
    finally:
        cliente.close()
        print("Conexión cerrada")


if __name__ == "__main__":
    main()
