FROM python:3.13-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Se indica el programa al arrancar: WM_WS_M.py o WM_WS_E.py + sus argumentos
ENTRYPOINT ["python"]
CMD ["WM_WS_M.py"]