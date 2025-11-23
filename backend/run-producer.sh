#!/bin/bash

# Build the project first
echo "Building the project..."
mvn clean package

# Run the producer client
echo "Starting Producer Client..."
java -cp target/media-upload-service-1.0.0.jar com.mediaupload.producer.ProducerClient

