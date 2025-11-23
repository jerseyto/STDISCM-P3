NOTE: Run "taskkill /F /IM java.exe" first to make sure no Java processes are running.

FOR CONSUMER
1. Install Maven: Go to the Apache Maven Download Page.
2. Add it to Environment Variables.
3. Open terminal, navigate to the backend folder, then run "mvn clean package"
4. Run "java -cp target\media-upload-service-1.0.0.jar com.mediaupload.consumer.ConsumerServer"
5. Enter consumer server port
6. Enter number of consumer threads
7. Enter max queue size input

FOR FRONTEND
1. Open terminal, navigate to the frontend folder, then run "npm install"
2. After installing the dependencies, run "npm run dev"
3. Open http://localhost:3000

FOR PRODUCER
1. Open terminal, navigate to the backend folder, then run "java -cp target\media-upload-service-1.0.0.jar com.mediaupload.producer.ProducerClient"
2. Enter consumer server host
3. Enter consumer server port
4. Enter number of producer threads
5. Enter folder path of "testvideos"
