# Use the official Apache Spark image from Docker Hub
FROM bitnami/spark:latest

# Set the working directory inside the container
WORKDIR /app

# Install any necessary dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the project files into the container
COPY . /app/

# Set the environment variables
ENV SPARK_HOME=/opt/bitnami/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Expose the ports for Spark
EXPOSE 4040 7077 8080

# Start the Spark application
CMD ["python", "manage.py", "runserver"]
