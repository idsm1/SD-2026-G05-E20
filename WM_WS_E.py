"""
WM_WS_E - Motor de la estación de riego

Uso: py WM_WS_E.py [ip_monitor] [puerto] [ip_broker] [puerto_broker]
"""

import socket
import sys


#--------------------------------------Parámetros

MONITOR = sys.argv[1] if len(sys.argv) > 1 else 'localhost'
P_MONITOR = int(sys.argv[2]) if len(sys.argv) > 2 else 5050
DOCKER = sys.argv[3] if len(sys.argv) > 3 else 'localhost'
P_DOCKER = sys.argv[4] if len(sys.argv) > 4 else 9092


