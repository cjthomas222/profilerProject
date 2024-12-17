FROM openjdk:8-jre-slim

RUN apt-get update && apt-get install -y python3 python3-pip wget && \
    apt-get clean

RUN wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz && \
    tar -xzf spark-3.5.0-bin-hadoop3.tgz && \
    mv spark-3.5.0-bin-hadoop3 /opt/spark && \
    rm spark-3.5.0-bin-hadoop3.tgz

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Install Python dependencies
RUN pip3 install pyspark pytest

# Set working directory
WORKDIR /app

# Copy application files
COPY script.py .
COPY test_profile_fields.py .
COPY fl_1.csv .

# Run pytest instead of the script
CMD ["pytest", "test_profile_fields.py"]
