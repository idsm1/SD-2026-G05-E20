FROM python:3.13-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    WM_BD=/data/watermanagement.db

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# La base de datos se guarda en /data para no perderla al recrear el contenedor
VOLUME /data
EXPOSE 5050

ENTRYPOINT ["python", "WM_Central.py"]
CMD ["5050", "localhost:9092"]