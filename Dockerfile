FROM python:3.7-slim

USER root
COPY . ./app
WORKDIR ./app
ADD . /app

RUN pip install -r requirements.txt

# just runs the process once
CMD ["python", "run_process.py"]

#runs flask and makes the endpoint available
#CMD ["python", "run.py"]

