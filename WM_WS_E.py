import random
import socket
import sys
import threading
import time

CABECERA = 64

MONITOR = sys.argv[1] if len(sys.argv) > 1 else 'localhost'
P_MONITOR = int(sys.argv[2]) if len(sys.argv) > 2 else 5050
DOCKER = sys.argv[3] if len(sys.argv) > 3 else 'localhost'
P_DOCKER = sys.argv[4] if len(sys.argv) > 4 else 9092


def enviar(conn,texto):
    datos = texto
    cabecera = str(len(datos))
    cabecera += b' ' * (CABECERA - len(cabecera))
    conn.sendall(cabecera + datos)


