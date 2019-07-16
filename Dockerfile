FROM python:3.7-slim

USER root
#COPY . ./app
WORKDIR ./app
ADD . /app

RUN pip install -r requirements.txt

CMD ["python", "run.py"]

