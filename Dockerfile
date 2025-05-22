
FROM python:3.9.21

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2 requests retry-requests requests_cache

WORKDIR /app
COPY kiva_ingest.py kiva_ingest.py 

ENTRYPOINT ["/bin/bash"]