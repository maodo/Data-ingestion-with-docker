# Dockerfile

FROM python:3.9.1 
#the base image to start from
RUN apt-get install wget
RUN pip install pandas sqlalchemy fastparquet psycopg2
#run a command to install python packages

WORKDIR /app 
#change the working directory - it's like cd command in linux 
COPY ingest_data.py ingest_data.py 
# copy the file from current folder in the host machine to the working directory

ENTRYPOINT [ "python", "ingest_data.py" ] 
# run the python ingest_data.py command when we use docker run command
