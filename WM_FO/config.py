"""
config.py - Todo lo que comparten los módulos para usar Kafka:
nombres de los topics, estados de las estaciones y funciones para conectarse.

Hay una copia idéntica en WM_Central/, WM_WS/ y WM_FO/ para que cada carpeta
se pueda construir con Docker por separado. Si se cambia algo, hay que
cambiarlo en las tres.

Todos los mensajes de Kafka van en JSON (un diccionario de Python).
"""

from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient
from kafka.serializer import DefaultSerializer, JsonSerializer

# ------------------------------------------------------------------ TOPICS
TOPIC_SOLICITUDES = "wm.solicitudes"      # FO / engine (riego manual) -> Central
TOPIC_ORDENES = "wm.ordenes"              # Central -> engines (START, STOP, BLOQUEAR, ACTIVAR, DENEGADO)
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


# ----------------------------------------------------- FUNCIONES DE KAFKA

def crear_topics(broker):
    """Crea en Kafka los topics que todavía no existan.
    La llaman todos los módulos al arrancar, así da igual cuál arranque primero."""
    admin = KafkaAdminClient(bootstrap_servers=broker)
    try:
        existentes = admin.list_topics()
        faltan = [t for t in TODOS_LOS_TOPICS if t not in existentes]
        if faltan:
            # raise_errors=False: si otro módulo lo ha creado a la vez, no pasa nada
            admin.create_topics(faltan, raise_errors=False)
    finally:
        admin.close()


def crear_productor(broker):
    """Productor que envía diccionarios de Python convertidos a JSON.
    acks='all': Kafka confirma que ha guardado el mensaje (no se pierde)."""
    return KafkaProducer(
        bootstrap_servers=broker,
        acks="all",
        key_serializer=DefaultSerializer(),    # la clave es texto (ej: "WS-01")
        value_serializer=JsonSerializer(),     # el mensaje es un diccionario -> JSON
    )


def crear_consumidor(broker, topics, grupo, desde):
    """Consumidor que recibe los mensajes JSON ya convertidos a diccionario.

    grupo: nombre del consumer group. Kafka recuerda por dónde se quedó cada grupo,
           así que si el programa se cae, al volver sigue donde lo dejó.
    desde: 'earliest' (la primera vez lee todo lo que haya) o
           'latest'   (la primera vez solo lee lo nuevo).
    enable_auto_commit=False: confirmamos a mano (commit) DESPUÉS de procesar cada
           mensaje. Si nos caemos a mitad, ese mensaje se vuelve a leer.
    """
    return KafkaConsumer(
        *topics,
        bootstrap_servers=broker,
        group_id=grupo,
        auto_offset_reset=desde,
        enable_auto_commit=False,
        value_deserializer=JsonSerializer(),   # JSON -> diccionario
    )
