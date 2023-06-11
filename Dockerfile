FROM apache/spark-py

# Set the working directory
WORKDIR /app

# Copy the Python script and requirements.txt file
COPY pyspark_test.py .
COPY requirements.txt .

# Copy the XML files
COPY xml_files /app/xml_files

# Switch to root user
USER root

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Download and install spark-xml
RUN curl -L -o spark-xml.jar https://repo1.maven.org/maven2/com/databricks/spark-xml_2.12/0.12.0/spark-xml_2.12-0.12.0.jar \
    && mv spark-xml.jar $SPARK_HOME/jars/

# Switch back to the default user
USER 185

# # Run the Python script
# CMD ["spark-submit", "--master", "local[*]", "pyspark_test.py"]
