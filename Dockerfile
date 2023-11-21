ARG PYTHON_VERSION=3.10

FROM python:${PYTHON_VERSION}-bullseye

ENV PYTHONBUFFERED = 1

WORKDIR /app

COPY req1.txt requirements.txt

RUN pip install -r requirements.txt

COPY backend .

EXPOSE 8000 