#!/bin/bash

python3 manage.py migrate
celery -A core worker -l INFO 
python3 manage.py runserver 0.0.0.0:8000
