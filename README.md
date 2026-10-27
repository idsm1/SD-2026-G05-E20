# SD-2026-G05-E20 · WaterManagement

Práctica de Sistemas Distribuidos (UA, curso 2026-27). Autores en `Nombres.txt`.

## Estructura

| Carpeta | Contenido |
|---|---|
| `WM_Central/` | Central (`WM_Central.py`), base de datos SQLite (`bd.py`) |
| `WM_WS/` | Estación de riego: monitor (`WM_WS_M.py`) y engine (`WM_WS_E.py`) |
| `WM_FO/` | Aplicación del operario (`WM_FO.py`) |
| `prompts/` | Prompts de IA usados en el desarrollo |

`protocolo.py` (tramas por sockets) y `config.py` (topics y estados) están copiados
en cada carpeta para que cada módulo se construya por separado con Docker.
Si se cambian, hay que cambiarlos en todas las copias.

## Ejecución en local (sin Docker)

```
docker compose up -d                      # Kafka en localhost:9092

cd WM_Central
python WM_Central.py 5050 localhost:9092

cd WM_WS
python WM_WS_M.py 6004 localhost 5050 WS-04 River Park
python WM_WS_E.py localhost 9092 localhost 6004

cd WM_FO
python WM_FO.py localhost 9092 FO-12
python WM_FO.py localhost 9092 FO-12 solicitudes.txt    # modo fichero
```

Operarios dados de alta al crear la base de datos: `FO-12`, `FO-15`, `FO-20`.

## Mensajes de Kafka (JSON)

| Topic | Quién → quién | Contenido |
|---|---|---|
| `wm.solicitudes` | FO / menú WS → Central | `id, fo, ws, duracion, origen (FO/WS), hora` |
| `wm.ordenes` | Central → engines | `tipo (START/STOP/BLOQUEAR/ACTIVAR/DENEGADO), ws (o *), riego_id, fo, duracion` |
| `wm.telemetria` | engine → Central | `ws, riego_id, fo, caudal, volumen, segundos, duracion, hora` (cada segundo) |
| `wm.eventos_ws` | engine → Central | `tipo (FIN_RIEGO/RECHAZADO), ws, riego_id, fo, inicio, volumen, duracion, motivo` |
| `wm.notificaciones` | Central → FO | `fo, tipo (AUTORIZADO/DENEGADO/PROGRESO/RESUMEN/AVISO), ws, mensaje, riego_id...` |
| `wm.estado_red` | Central → todos los FO | `estaciones: [{ws, ubicacion, estado}]` |

Consumer groups: Central `central` (earliest), cada engine `engine-<WS>`, cada operario `fo-<FO>`.
Cada mensaje se confirma (commit) después de procesarlo, así si un módulo se cae no pierde nada.
