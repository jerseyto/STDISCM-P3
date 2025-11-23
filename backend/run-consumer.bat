@echo off

REM Build the project first
echo Building the project...
call mvn clean package

REM Run the consumer server
echo Starting Consumer Server...
java -cp target\media-upload-service-1.0.0.jar com.mediaupload.consumer.ConsumerServer

