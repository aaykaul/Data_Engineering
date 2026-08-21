FROM apache/spark:3.5.5

WORKDIR /app

COPY src/ ./src/
COPY data/ ./data/

CMD ["/opt/spark/bin/spark-submit", "/app/src/main.py"]