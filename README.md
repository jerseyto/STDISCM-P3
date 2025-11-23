NOTE: Run "taskkill /F /IM java.exe" first to make sure no Java processes are running.

FOR CONSUMER
1. Open terminal, navigate to the backend folder, then run "mvn clean package"
2. Run "java -cp target\media-upload-service-1.0.0.jar com.mediaupload.consumer.ConsumerServer"
3. Enter consumer server port
4. Enter number of consumer threads
5. Enter max queue size input

FOR FRONTEND
1. Open terminal, navigate to the frontend folder, then run "npm run dev"
2. Open http://localhost:3000

FOR PRODUCER
1. Open terminal, navigate to the backend folder, then run "java -cp target\media-upload-service-1.0.0.jar com.mediaupload.producer.ProducerClient"
2. Enter consumer server host
3. Enter consumer server port
4. Enter number of producer threads
5. Enter folder path of "testvideos"
