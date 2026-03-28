FROM python:3.10

RUN apt-get update && apt-get install -y default-jdk

ENV JAVA_HOME=/usr/lib/jvm/default-java

WORKDIR /app

COPY . .

RUN pip install pyspark pandas faker

COPY postgresql.jar /app/postgresql.jar

ENV CLASSPATH=/app/postgresql.jar

CMD ["python", "scripts/spark_job.py"]