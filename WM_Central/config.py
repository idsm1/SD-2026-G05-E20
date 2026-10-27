"""
config.py - Nombres de los topics de Kafka y estados de las estaciones.

Hay una copia idéntica en WM_Central/, WM_WS/ y WM_FO/ para que cada carpeta
se pueda construir con Docker por separado. Si se cambia algo, hay que
cambiarlo en las tres.
"""

# ------------------------------------------------------------------ TOPICS
TOPIC_SOLICITUDES = "wm.solicitudes"      # FO / engine (riego manual) -> Central
TOPIC_ORDENES = "wm.ordenes"              # Central -> engines (START, STOP, BLOQUEAR, ACTIVAR)
TOPIC_TELEMETRIA = "wm.telemetria"        # engine -> Central (caudal y volumen cada segundo)
TOPIC_EVENTOS_WS = "wm.eventos_ws"        # engine -> Central (fin de riego, orden rechazada)
TOPIC_NOTIFICACIONES = "wm.notificaciones"  # Central -> FO (autorizado, progreso, resumen...)
TOPIC_ESTADO_RED = "wm.estado_red"        # Central -> todos los FO (estado de las estaciones)

TODOS_LOS_TOPICS = [TOPIC_SOLICITUDES, TOPIC_ORDENES, TOPIC_TELEMETRIA,
                    TOPIC_EVENTOS_WS, TOPIC_NOTIFICACIONES, TOPIC_ESTADO_RED]

# --------------------------------------------------------- ESTADOS DE UNA WS
DISPONIBLE = "DISPONIBLE"
REGANDO = "REGANDO"
FUGA = "FUGA"
FUERA_SERVICIO = "FUERA_SERVICIO"
DESCONECTADA = "DESCONECTADA"
