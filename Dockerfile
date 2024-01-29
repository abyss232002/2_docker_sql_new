FROM python:3.9.1

# todo: need to install wget to get data from URL and gzip to unzip
RUN apt-get install wget
RUN apt-get install gzip

# todo: install pandas,Pyarrow(was throwing warning),sqlalchemy and psycopg2 to connect to postgres
RUN pip install pandas Pyarrow sqlalchemy psycopg2

WORKDIR /app
COPY ingest-data-green-taxi.py ingest-data-green-taxi.py

ENTRYPOINT [ "python", "ingest-data-green-taxi.py" ]