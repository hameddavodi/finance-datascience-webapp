ARG PYTHON_VERSION=3.10

FROM python:${PYTHON_VERSION}-bullseye


ENV PYTHONBUFFERED = 1

WORKDIR /app

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY backend .

EXPOSE 8000 

CMD ["python3", "manage.py", "runserver", "0.0.0.0:8000"]
