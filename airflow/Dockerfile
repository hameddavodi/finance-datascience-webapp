ARG PYTHON_VERSION=3.10

FROM apache/airflow:2.7.3

COPY requirements.txt /requirements.txt

RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt

ENV PYTHONBUFFERED = 1

WORKDIR /app


COPY . .

EXPOSE 8080 
