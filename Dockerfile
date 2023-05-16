FROM flyingjoe/uvicorn-gunicorn-fastapi:python3.10
# FROM python:3.10

COPY ./requirements.txt /app
RUN pip install --no-cache-dir --upgrade -r ./requirements.txt

COPY ./src/ /app
ENV TZ="Pacific/Auckland"
# Runs with defualt port 80
ENV MAX_WORKERS 1
