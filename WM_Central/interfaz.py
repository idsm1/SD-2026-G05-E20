"""
interfaz.py - Pantalla de WM_Central que se actualiza en su sitio (sin desplazarse).

La consola se divide en zonas fijas:

    +--------------------------------------------------------------+
    | WM_Central | puerto | Kafka | monitores | hora    (cabecera) |
    |                                                              |
    | ID  ESTADO  UBICACION  AGUA TOTAL  CAUDAL ...   (panel)      |
    | WS-01 ...                                                    |
    |                                                              |
    | ULTIMOS EVENTOS                                              |
    | 13:59:01  [REGISTRO] WS-01 ...           (los más recientes) |
    | ------------------------------------------------------------ |
    | 1 Iniciar riego  2 Parar ...                    (menú)       |
    | > _                                             (teclado)    |
    +--------------------------------------------------------------+

Cada vez que algo cambia se vuelven a escribir las líneas en su sitio con códigos ANSI
(mover el cursor a una fila y borrarla), así los valores cambian sin que la pantalla
haga scroll. La línea donde se escribe (">") no se toca, para no borrar lo que el
usuario está tecleando.

Si la salida NO es una terminal (p. ej. "docker logs" o la salida redirigida a un
fichero), se usa el modo simple: cada evento se imprime en una línea normal.
"""

import os
import re
import shutil
import sys
import threading
from collections import deque
from datetime import datetime

# Modo pantalla solo si escribimos en una terminal de verdad
PANTALLA = sys.stdout.isatty()

# Códigos ANSI
ESC = "\033["
RESET = ESC + "0m"
NEGRITA = ESC + "1m"
TENUE = ESC + "2m"
INVERSO = ESC + "7m"
GUARDAR_CURSOR = "\0337"
RESTAURAR_CURSOR = "\0338"
BORRAR_PANTALLA = ESC + "2J"
BORRAR_LINEA = ESC + "2K"

# Color de la etiqueta de cada tipo de evento: "[AVERIA] ..." sale en rojo, etc.
COLOR_ETIQUETA = {
    "[AVERIA]": ESC + "1;31m", "[TIMEOUT]": ESC + "1;31m", "[DESCONEXIÓN]": ESC + "33m",
    "[REGISTRO]": ESC + "1;32m", "[CONEXIÓN]": ESC + "32m", "[SOCKET]": TENUE,
    "[SOLICITUD]": ESC + "1;36m", "[RIEGO]": ESC + "1;34m", "[KAFKA]": ESC + "35m",
    "[MENU]": ESC + "1;33m", "[RECHAZADO]": ESC + "1;31m", "[STARTING]": NEGRITA,
    "[LISTENING]": NEGRITA, "[BD]": NEGRITA, "[STOP]": ESC + "1;31m",
}

_PATRON_ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]|\x1b[78]")

_lock = threading.RLock()
_eventos = deque(maxlen=300)
_cabecera = "WM_Central"
_panel = []
_menu = []
_tamano_anterior = None


def _mover(fila):
    return f"{ESC}{fila};1H"


def _cortar(texto, ancho):
    """Corta un texto a 'ancho' caracteres visibles sin romper los códigos de color."""
    resultado = []
    visibles = 0
    i = 0
    while i < len(texto) and visibles < ancho:
        m = _PATRON_ANSI.match(texto, i)
        if m:
            resultado.append(m.group())
            i = m.end()
        else:
            resultado.append(texto[i])
            visibles += 1
            i += 1
    return "".join(resultado) + RESET


def iniciar():
    """Prepara la consola (en Windows activa los colores ANSI) y pinta la pantalla."""
    os.system("")
    if PANTALLA:
        sys.stdout.write(BORRAR_PANTALLA)
        dibujar()


def terminar():
    """Deja el cursor abajo del todo para que la terminal quede usable al salir."""
    if PANTALLA:
        filas = shutil.get_terminal_size((100, 30)).lines
        sys.stdout.write(_mover(filas) + RESET + "\n")
        sys.stdout.flush()


def cabecera(texto):
    global _cabecera
    with _lock:
        _cabecera = texto


def menu(lineas):
    global _menu
    with _lock:
        _menu = list(lineas)
    dibujar()


def log(texto):
    """Añade un evento (con la hora). En modo simple, se imprime sin más."""
    texto = texto.strip()
    linea = f" {datetime.now().strftime('%H:%M:%S')}  {texto}"
    if not PANTALLA:
        print(linea, flush=True)
        return
    etiqueta = texto.split("]")[0] + "]" if texto.startswith("[") else ""
    if etiqueta in COLOR_ETIQUETA:
        linea = (f" {TENUE}{datetime.now().strftime('%H:%M:%S')}{RESET}  "
                 f"{COLOR_ETIQUETA[etiqueta]}{etiqueta}{RESET}{texto[len(etiqueta):]}")
    with _lock:
        _eventos.append(linea)
    dibujar()


def panel(lineas):
    """Sustituye las líneas del panel de estaciones. En modo simple, se imprimen."""
    global _panel
    if not PANTALLA:
        print("\n".join(lineas), flush=True)
        return
    with _lock:
        _panel = list(lineas)
    dibujar()


def dibujar():
    """Vuelve a escribir todas las zonas en su sitio (menos la línea del teclado)."""
    global _tamano_anterior
    if not PANTALLA:
        return
    with _lock:
        ancho, filas = shutil.get_terminal_size((100, 30))
        fila_teclado = filas - 1          # la última fila se deja libre para el salto de línea
        salida = [GUARDAR_CURSOR]
        if (ancho, filas) != _tamano_anterior:      # han cambiado el tamaño de la ventana
            salida.append(BORRAR_PANTALLA)
            _tamano_anterior = (ancho, filas)

        arriba = [INVERSO + NEGRITA + " " + _cabecera.ljust(ancho - 1), ""]
        arriba += _panel
        arriba += ["", NEGRITA + " ÚLTIMOS EVENTOS" + RESET]
        abajo = [TENUE + "─" * (ancho - 1) + RESET] + _menu

        huecos = fila_teclado - 1 - len(arriba) - len(abajo)
        eventos = list(_eventos)[-huecos:] if huecos > 0 else []
        eventos += [""] * (max(huecos, 0) - len(eventos))

        for fila, texto in enumerate(arriba + eventos + abajo, start=1):
            if fila >= fila_teclado:
                break
            salida.append(_mover(fila) + BORRAR_LINEA + _cortar(texto, ancho - 1))
        salida.append(RESTAURAR_CURSOR)
        sys.stdout.write("".join(salida))
        sys.stdout.flush()


def redibujar_todo():
    """Borra la consola y lo vuelve a pintar todo (por si se ha descolocado algo)."""
    global _tamano_anterior
    with _lock:
        _tamano_anterior = None
    dibujar()


def pedir(texto):
    """Pide un dato por teclado en la línea de abajo (como input(), pero sin
    descolocar la pantalla). Lanza EOFError si no hay teclado."""
    if not PANTALLA:
        return input(texto)
    with _lock:
        filas = shutil.get_terminal_size((100, 30)).lines
        sys.stdout.write(_mover(filas - 1) + BORRAR_LINEA + NEGRITA + texto + RESET)
        sys.stdout.write(_mover(filas) + BORRAR_LINEA + _mover(filas - 1) + ESC
                         + f"{len(texto) + 1}G")
        sys.stdout.flush()
    return input()