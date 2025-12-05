// package com.mediaupload.consumer;

// import com.google.gson.Gson;
// import com.mediaupload.proto.*;
// import io.grpc.Server;
// import io.grpc.ServerBuilder;
// import io.grpc.stub.StreamObserver;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
// import spark.Spark;

// import java.io.*;
// import java.nio.file.Files;
// import java.nio.file.Path;
// import java.nio.file.Paths;
// import java.util.*;
// import java.util.concurrent.*;
// import java.util.stream.Collectors;

// public class ConsumerServer {
//     private static final Logger logger = LoggerFactory.getLogger(ConsumerServer.class);
//     private Server server;
//     private final int port;
//     private final String uploadDir;
//     private final BlockingQueue<VideoUploadTask> queue;
//     private final int maxQueueSize;
//     private final ExecutorService consumerThreadPool;
//     private final int consumerThreadCount;
//     private final Map<String, VideoMetadata> uploadedVideos;
//     // private final Set<String> videoHashes; // For duplicate detection
//     private final Map<String, String> videoHashToId;
    
//     public ConsumerServer(int port, int consumerThreads, int maxQueueSize) {
//         this.port = port;
//         this.maxQueueSize = maxQueueSize;
//         this.consumerThreadCount = consumerThreads;
//         this.queue = new LinkedBlockingQueue<>(maxQueueSize);
//         this.consumerThreadPool = Executors.newFixedThreadPool(consumerThreads);
//         this.uploadedVideos = new ConcurrentHashMap<>();
//         // this.videoHashes = ConcurrentHashMap.newKeySet();
//         this.videoHashToId = new ConcurrentHashMap<>();
//         this.uploadDir = "uploads";
        
//         // Create upload directory
//         try {
//             Files.createDirectories(Paths.get(uploadDir));
//         } catch (IOException e) {
//             logger.error("Failed to create upload directory", e);
//         }
//     }
    
//     public void start() throws IOException {
//         try {
//             logger.info("Starting gRPC server on port {}...", port);
//             server = ServerBuilder.forPort(port)
//                     .addService(new MediaUploadServiceImpl())
//                     .build();
            
//             server.start();
            
//             // Verify server is actually running
//             if (server.isShutdown() || server.isTerminated()) {
//                 throw new IOException("gRPC server failed to start - server is in shutdown state");
//             }
            
//             logger.info("Consumer Server started, listening on port {}", port);
//             logger.info("gRPC server is ready to accept connections");
//             logger.info("Server state - Shutdown: {}, Terminated: {}", server.isShutdown(), server.isTerminated());
//         } catch (Exception e) {
//             logger.error("Failed to start gRPC server on port {}", port, e);
//             if (server != null) {
//                 try {
//                     server.shutdown();
//                 } catch (Exception ex) {
//                     logger.error("Error shutting down failed server", ex);
//                 }
//             }
//             throw new IOException("Failed to start gRPC server: " + e.getMessage(), e);
//         }
        
//         // Start consumer threads
//         for (int i = 0; i < consumerThreadCount; i++) {
//             consumerThreadPool.submit(new VideoConsumer());
//         }
        
//         // Start REST API server for frontend
//         startRestApi();
        
//         Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//             logger.info("Shutting down Consumer Server...");
//             ConsumerServer.this.stop();
//         }));
//     }
    
//     private void startRestApi() {
//         Spark.port(8080);
//         Spark.staticFiles.externalLocation(new File(uploadDir).getAbsolutePath());
        
//         Gson gson = new Gson();
        
//         // Enable CORS
//         Spark.before((request, response) -> {
//             response.header("Access-Control-Allow-Origin", "*");
//             response.header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
//             response.header("Access-Control-Allow-Headers", "Content-Type, Authorization");
//         });
        
//         // Get list of uploaded videos
//         Spark.get("/api/videos", (req, res) -> {
//             res.type("application/json");
//             List<VideoInfo> videoList = uploadedVideos.values().stream()
//                     .map(metadata -> new VideoInfo(
//                             metadata.getFileId(),
//                             metadata.getFilename(),
//                             metadata.getFileSize(),
//                             metadata.getUploadTime(),
//                             metadata.getFilePath()
//                     ))
//                     .sorted((a, b) -> Long.compare(b.uploadTime, a.uploadTime))
//                     .collect(Collectors.toList());
//             return gson.toJson(videoList);
//         });
        
//         // Get video metadata
//         Spark.get("/api/videos/:id", (req, res) -> {
//             String id = req.params("id");
//             VideoMetadata metadata = uploadedVideos.get(id);
//             if (metadata == null) {
//                 res.status(404);
//                 return gson.toJson(Map.of("error", "Video not found"));
//             }
//             res.type("application/json");
//             return gson.toJson(new VideoInfo(
//                     metadata.getFileId(),
//                     metadata.getFilename(),
//                     metadata.getFileSize(),
//                     metadata.getUploadTime(),
//                     metadata.getFilePath()
//             ));
//         });
        
//         logger.info("REST API started on port 8080");
//     }
    
//     private void stop() {
//         if (server != null) {
//             server.shutdown();
//         }
//         consumerThreadPool.shutdown();
//         try {
//             if (!consumerThreadPool.awaitTermination(30, TimeUnit.SECONDS)) {
//                 consumerThreadPool.shutdownNow();
//             }
//         } catch (InterruptedException e) {
//             consumerThreadPool.shutdownNow();
//         }
//         Spark.stop();
//     }
    
//     public void blockUntilShutdown() throws InterruptedException {
//         if (server != null) {
//             server.awaitTermination();
//         }
//     }
    
//     private boolean compressVideo(File inputFile, File outputFile) {
//         try {
//             // NOTE: This assumes ffmpeg.exe is in the project root (backend folder)
//             // Command: ffmpeg -i input -vcodec libx264 -crf 28 -preset ultrafast output
//             ProcessBuilder pb = new ProcessBuilder(
//                 "ffmpeg.exe", 
//                 "-y", // Overwrite output if exists
//                 "-i", inputFile.getAbsolutePath(),
//                 "-vcodec", "libx264",
//                 "-crf", "35", // Higher CRF = More Compression (Lower Quality)
//                 "-preset", "medium", // Fast processing for demo
//                 outputFile.getAbsolutePath()
//             );
            
//             pb.redirectErrorStream(true); // Merge stdout and stderr
//             Process process = pb.start();
            
//             // Read output to prevent blocking
//             try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
//                 String line;
//                 while ((line = reader.readLine()) != null) {
//                     // Uncomment to see ffmpeg logs: 
//                     // System.out.println(line);
//                 }
//             }
            
//             int exitCode = process.waitFor();
//             return exitCode == 0;
            
//         } catch (Exception e) {
//             logger.error("Compression failed (Is ffmpeg.exe in the backend folder?)", e);
//             return false;
//         }
//     }

//     private class MediaUploadServiceImpl extends MediaUploadServiceGrpc.MediaUploadServiceImplBase {
//         @Override
//         public StreamObserver<VideoChunk> uploadVideo(StreamObserver<UploadResponse> responseObserver) {
//             return new StreamObserver<VideoChunk>() {
//                 private String filename;
//                 private String fileHash;
//                 private ByteArrayOutputStream videoData = new ByteArrayOutputStream();
//                 private final String fileId = UUID.randomUUID().toString();
                
//                 @Override
//                 public void onNext(VideoChunk chunk) {
//                     if (chunk.getChunkIndex() == 0) {
//                         filename = chunk.getFilename();
//                         fileHash = chunk.getHash();
                        
//                         // Check for duplicates
//                         if (videoHashToId.containsKey(fileHash)) {
//                             responseObserver.onNext(UploadResponse.newBuilder()
//                                     .setSuccess(false)
//                                     .setMessage("Duplicate video detected")
//                                     .setIsDuplicate(true)
//                                     .build());
//                             responseObserver.onCompleted();
//                             return;
//                         }
//                     }
                    
//                     try {
//                         chunk.getData().writeTo(videoData);
//                     } catch (IOException e) {
//                         logger.error("Error writing chunk", e);
//                         responseObserver.onError(e);
//                         return;
//                     }
                    
//                     if (chunk.getIsLastChunk()) {
//                         // Check if queue is full
//                         if (queue.remainingCapacity() == 0) {
//                             responseObserver.onNext(UploadResponse.newBuilder()
//                                     .setSuccess(false)
//                                     .setMessage("Queue is full")
//                                     .setQueueFull(true)
//                                     .build());
//                             responseObserver.onCompleted();
//                             return;
//                         }
                        
//                         videoHashToId.put(fileHash, fileId); 

//                         // Add to queue
//                         VideoUploadTask task = new VideoUploadTask(fileId, filename, videoData.toByteArray(), fileHash);
//                         if (!queue.offer(task)) {
//                             responseObserver.onNext(UploadResponse.newBuilder()
//                                     .setSuccess(false)
//                                     .setMessage("Queue is full, video dropped")
//                                     .setQueueFull(true)
//                                     .build());
//                             responseObserver.onCompleted();
//                             return;
//                         }
                        
//                         responseObserver.onNext(UploadResponse.newBuilder()
//                                 .setSuccess(true)
//                                 .setMessage("Video queued successfully")
//                                 .setFileId(fileId)
//                                 .setQueueFull(false)
//                                 .setIsDuplicate(false)
//                                 .build());
//                         responseObserver.onCompleted();
//                     }
//                 }
                
//                 @Override
//                 public void onError(Throwable t) {
//                     logger.error("Error in upload stream", t);
//                 }
                
//                 @Override
//                 public void onCompleted() {
//                     // Already completed in onNext
//                 }
//             };
//         }
        
//         @Override
//         public void checkQueueStatus(QueueStatusRequest request, StreamObserver<QueueStatusResponse> responseObserver) {
//             try {
//                 logger.info("Received checkQueueStatus request");
//                 QueueStatusResponse response = QueueStatusResponse.newBuilder()
//                         .setQueueFull(queue.remainingCapacity() == 0)
//                         .setQueueSize(queue.size())
//                         .setMaxQueueSize(maxQueueSize)
//                         .build();
//                 logger.info("Sending queue status: size={}, max={}, full={}", 
//                         queue.size(), maxQueueSize, queue.remainingCapacity() == 0);
//                 responseObserver.onNext(response);
//                 responseObserver.onCompleted();
//                 logger.info("checkQueueStatus completed successfully");
//             } catch (Exception e) {
//                 logger.error("Error in checkQueueStatus", e);
//                 responseObserver.onError(io.grpc.Status.INTERNAL
//                         .withDescription("Server error: " + e.getMessage())
//                         .withCause(e)
//                         .asRuntimeException());
//             }
//         }
//     }
    
//     private class VideoConsumer implements Runnable {
//         @Override
//         public void run() {
//             while (!Thread.currentThread().isInterrupted()) {
//                 try {
//                     VideoUploadTask task = queue.take(); // Blocking call
//                     processVideo(task);
//                 } catch (InterruptedException e) {
//                     Thread.currentThread().interrupt();
//                     break;
//                 } catch (Exception e) {
//                     logger.error("Error processing video", e);
//                 }
//             }
//         }
        
//         // private void processVideo(VideoUploadTask task) {
//         //     try {
//         //         String filePath = Paths.get(uploadDir, task.getFileId() + "_" + task.getFilename()).toString();
                
//         //         // Save video file
//         //         try (FileOutputStream fos = new FileOutputStream(filePath)) {
//         //             fos.write(task.getData());
//         //         }
                
//         //         // Add to hash set for duplicate detection
//         //         videoHashes.add(task.getHash());
                
//         //         String webUrl = "http://localhost:8080/" + task.getFileId() + "_" + task.getFilename();

//         //         // Create metadata
//         //         VideoMetadata metadata = new VideoMetadata(
//         //                 task.getFileId(),
//         //                 task.getFilename(),
//         //                 task.getData().length,
//         //                 System.currentTimeMillis(),
//         //                 webUrl
//         //         );
                
//         //         uploadedVideos.put(task.getFileId(), metadata);
                
//         //         logger.info("Video processed: {} (ID: {})", task.getFilename(), task.getFileId());
//         //     } catch (IOException e) {
//         //         logger.error("Error saving video file", e);
//         //     }
//         // }

//         private void processVideo(VideoUploadTask task) {
//             try {
//                 // 1. Save RAW file first (temp)
//                 String rawName = "raw_" + task.getFileId() + "_" + task.getFilename();
//                 File rawFile = new File(uploadDir, rawName);
//                 try (FileOutputStream fos = new FileOutputStream(rawFile)) {
//                     fos.write(task.getData());
//                 }
                
//                 logger.info("Processing {}: Saved raw file ({} bytes). Starting compression...", task.getFilename(), rawFile.length());
                
//                 // 2. Define Compressed File Output
//                 String compressedName = task.getFileId() + "_" + task.getFilename();
//                 File compressedFile = new File(uploadDir, compressedName);
                
//                 // 3. Attempt Compression
//                 boolean compressed = compressVideo(rawFile, compressedFile);
                
//                 File finalFile;
//                 if (compressed) {
//                     logger.info("Compression SUCCESS. Old: {} -> New: {}", rawFile.length(), compressedFile.length());
//                     rawFile.delete(); // Delete the big file to save space
//                     finalFile = compressedFile;
//                 } else {
//                     logger.warn("Compression FAILED (or ffmpeg missing). Using raw file.");
//                     // Rename raw to final if compression failed
//                     File fallbackFile = new File(uploadDir, compressedName);
//                     rawFile.renameTo(fallbackFile);
//                     finalFile = fallbackFile;
//                 }

//                 // 4. Update Metadata
//                 String webUrl = "http://localhost:8080/" + finalFile.getName();
//                 VideoMetadata metadata = new VideoMetadata(
//                         task.getFileId(), task.getFilename(),
//                         finalFile.length(), System.currentTimeMillis(), webUrl
//                 );
                
//                 uploadedVideos.put(task.getFileId(), metadata);
//                 logger.info("Video ready: {}", task.getFilename());
                
//             } catch (Exception e) {
//                 logger.error("Error processing video", e);
//             }
//         }
//     }
    
//     // Data classes
//     private static class VideoUploadTask {
//         private final String fileId;
//         private final String filename;
//         private final byte[] data;
//         private final String hash;
        
//         public VideoUploadTask(String fileId, String filename, byte[] data, String hash) {
//             this.fileId = fileId;
//             this.filename = filename;
//             this.data = data;
//             this.hash = hash;
//         }
        
//         public String getFileId() { return fileId; }
//         public String getFilename() { return filename; }
//         public byte[] getData() { return data; }
//         public String getHash() { return hash; }
//     }
    
//     private static class VideoMetadata {
//         private final String fileId;
//         private final String filename;
//         private final long fileSize;
//         private final long uploadTime;
//         private final String filePath;
        
//         public VideoMetadata(String fileId, String filename, long fileSize, long uploadTime, String filePath) {
//             this.fileId = fileId;
//             this.filename = filename;
//             this.fileSize = fileSize;
//             this.uploadTime = uploadTime;
//             this.filePath = filePath;
//         }
        
//         public String getFileId() { return fileId; }
//         public String getFilename() { return filename; }
//         public long getFileSize() { return fileSize; }
//         public long getUploadTime() { return uploadTime; }
//         public String getFilePath() { return filePath; }
//     }
    
//     private static class VideoInfo {
//         public String id;
//         public String filename;
//         public long size;
//         public long uploadTime;
//         public String filePath;
        
//         public VideoInfo(String id, String filename, long size, long uploadTime, String filePath) {
//             this.id = id;
//             this.filename = filename;
//             this.size = size;
//             this.uploadTime = uploadTime;
//             this.filePath = filePath;
//         }
//     }
    
//     public static void main(String[] args) throws IOException, InterruptedException {
//         Scanner scanner = new Scanner(System.in);
//         try {
//             System.out.print("Enter consumer server port (default 50051): ");
//             String portStr = scanner.nextLine().trim();
//             int port = portStr.isEmpty() ? 50051 : Integer.parseInt(portStr);
            
//             System.out.print("Enter number of consumer threads (c): ");
//             int consumerThreads = Integer.parseInt(scanner.nextLine().trim());
            
//             System.out.print("Enter max queue size (q): ");
//             int maxQueueSize = Integer.parseInt(scanner.nextLine().trim());
            
//             ConsumerServer server = new ConsumerServer(port, consumerThreads, maxQueueSize);
//             server.start();
//             server.blockUntilShutdown();
//         } finally {
//             scanner.close();
//         }
//     }
// }


package com.mediaupload.consumer;

import com.google.gson.Gson;
import com.mediaupload.proto.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Spark;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class ConsumerServer {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerServer.class);
    private Server server;
    private final int port;
    private final String uploadDir;
    private final BlockingQueue<VideoUploadTask> queue;
    private final int maxQueueSize;
    private final ExecutorService consumerThreadPool;
    private final int consumerThreadCount;
    private final Map<String, VideoMetadata> uploadedVideos;
    
    private final ConcurrentHashMap<String, String> videoHashToId;
    private final ConcurrentHashMap<String, String> filenameToId;
    
    private final Map<String, String> idToFilename; 
    
    public ConsumerServer(int port, int consumerThreads, int maxQueueSize) {
        this.port = port;
        this.maxQueueSize = maxQueueSize;
        this.consumerThreadCount = consumerThreads;
        this.queue = new LinkedBlockingQueue<>(maxQueueSize);
        this.consumerThreadPool = Executors.newFixedThreadPool(consumerThreads);
        this.uploadedVideos = new ConcurrentHashMap<>();
        this.videoHashToId = new ConcurrentHashMap<>();
        this.filenameToId = new ConcurrentHashMap<>(); 
        this.idToFilename = new ConcurrentHashMap<>();
        this.uploadDir = "uploads";
        
        try {
            Files.createDirectories(Paths.get(uploadDir));
        } catch (IOException e) {
            logger.error("Failed to create upload directory", e);
        }
    }
    
    public void start() throws IOException {
        try {
            logger.info("Starting gRPC server on port {}...", port);
            server = ServerBuilder.forPort(port)
                    .addService(new MediaUploadServiceImpl())
                    .build();
            server.start();
            logger.info("Consumer Server started on port {}", port);
        } catch (Exception e) {
            logger.error("Failed to start gRPC server", e);
            throw new IOException(e);
        }
        
        for (int i = 0; i < consumerThreadCount; i++) {
            consumerThreadPool.submit(new VideoConsumer());
        }
        
        startRestApi();
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
    }
    
    private void startRestApi() {
        Spark.port(8080);
        Spark.staticFiles.externalLocation(new File(uploadDir).getAbsolutePath());
        Gson gson = new Gson();
        
        Spark.before((req, res) -> {
            res.header("Access-Control-Allow-Origin", "*");
            res.header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
            res.header("Access-Control-Allow-Headers", "Content-Type, Authorization");
        });
        
        Spark.get("/api/videos", (req, res) -> {
            res.type("application/json");
            
            return gson.toJson(uploadedVideos.values().stream()
                    .map(meta -> new VideoInfo(
                        meta.getFileId(),
                        meta.getFilename(),
                        meta.getFileSize(),
                        meta.getUploadTime(),
                        meta.getFilePath()
                    ))
                    .sorted((a, b) -> Long.compare(b.uploadTime, a.uploadTime))
                    .collect(Collectors.toList()));
        });

        Spark.get("/api/videos/:id", (req, res) -> {
            String id = req.params("id");
            VideoMetadata meta = uploadedVideos.get(id);
            if(meta == null) {
                res.status(404);
                return "{}";
            }
            res.type("application/json");
            return gson.toJson(new VideoInfo(meta.fileId, meta.filename, meta.fileSize, meta.uploadTime, meta.filePath));
        });
    }
    
    private void stop() {
        if (server != null) server.shutdown();
        consumerThreadPool.shutdownNow();
        Spark.stop();
    }
    
    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) server.awaitTermination();
    }
    
    // --- COMPRESSION HELPER ---
    private boolean compressVideo(File inputFile, File outputFile) {
        try {
            ProcessBuilder pb = new ProcessBuilder(
                "ffmpeg.exe", 
                "-y", 
                "-i", inputFile.getAbsolutePath(),
                "-vcodec", "libx264",
                "-crf", "35",
                "-preset", "medium", 
                outputFile.getAbsolutePath()
            );
            
            pb.redirectErrorStream(true);
            Process process = pb.start();
            
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {}
            }
            
            int exitCode = process.waitFor();
            return exitCode == 0;
            
        } catch (Exception e) {
            logger.error("Compression failed", e);
            return false;
        }
    }

    private class MediaUploadServiceImpl extends MediaUploadServiceGrpc.MediaUploadServiceImplBase {
        @Override
        public StreamObserver<VideoChunk> uploadVideo(StreamObserver<UploadResponse> responseObserver) {
            return new StreamObserver<VideoChunk>() {
                private String filename;
                private String fileHash;
                private ByteArrayOutputStream videoData = new ByteArrayOutputStream();
                private final String fileId = UUID.randomUUID().toString();
                
                private boolean isDuplicate = false;
                private String existingDuplicateId = "";
                
                private boolean reservedHash = false;
                private boolean reservedFilename = false;

                @Override
                public void onNext(VideoChunk chunk) {
                    if (chunk.getChunkIndex() == 0) {
                        filename = chunk.getFilename();
                        fileHash = chunk.getHash();
                        
                        String previousId = videoHashToId.putIfAbsent(fileHash, fileId);
                        
                        if (previousId != null) {
                           
                            isDuplicate = true;
                            existingDuplicateId = previousId;
                            logger.info("Duplicate detected {}. Draining stream...", filename);
                        } else {
                            
                            reservedHash = true;
                            
                            if (filenameToId.containsKey(filename)) {
                                String baseName = filename;
                                String extension = "";
                                int dotIndex = filename.lastIndexOf('.');
                                if (dotIndex >= 0) {
                                    baseName = filename.substring(0, dotIndex);
                                    extension = filename.substring(dotIndex);
                                }

                                int counter = 2;
                                while (filenameToId.containsKey(filename)) {
                                    filename = baseName + " (" + counter + ")" + extension;
                                    counter++;
                                }
                                logger.info("Renaming collision to {}", filename);
                            }
                            
                            filenameToId.put(filename, fileId);
                            reservedFilename = true;
                            idToFilename.put(fileId, filename);
                        }
                    }

                    if (isDuplicate) {
                         if (chunk.getIsLastChunk()) {
                             responseObserver.onNext(UploadResponse.newBuilder()
                                    .setSuccess(true)
                                    .setMessage("Duplicate detected (Skipped)")
                                    .setFileId(existingDuplicateId)
                                    .setIsDuplicate(true)
                                    .build());
                             responseObserver.onCompleted();
                         }
                         return; 
                    }
                    // -----------------------------

                    try {
                        chunk.getData().writeTo(videoData);
                    } catch (IOException e) {
                        cleanupLocks();
                        responseObserver.onError(e);
                        return;
                    }
                    
                    if (chunk.getIsLastChunk()) {
                        if (queue.remainingCapacity() == 0) {
                            cleanupLocks();
                            responseObserver.onNext(UploadResponse.newBuilder().setSuccess(false).setMessage("Queue Full").setQueueFull(true).build());
                            responseObserver.onCompleted();
                            return;
                        }
                        
                      

                        VideoUploadTask task = new VideoUploadTask(fileId, filename, videoData.toByteArray(), fileHash);
                        
                        if (queue.offer(task)) {
                            responseObserver.onNext(UploadResponse.newBuilder().setSuccess(true).setMessage("Queued").setFileId(fileId).build());
                        } else {
                            
                            cleanupLocks();
                            responseObserver.onNext(UploadResponse.newBuilder().setSuccess(false).setMessage("Queue Full").build());
                        }
                        responseObserver.onCompleted();
                    }
                }
                
                private void cleanupLocks() {
                    if (reservedHash) videoHashToId.remove(fileHash);
                    if (reservedFilename) filenameToId.remove(filename);
                    if (reservedFilename) idToFilename.remove(fileId);
                }
                
                @Override 
                public void onError(Throwable t) { 
                    cleanupLocks(); 
                    logger.error("Upload stream error", t); 
                }
                
                @Override public void onCompleted() {}
            };
        }
        
        @Override
        public void checkQueueStatus(QueueStatusRequest req, StreamObserver<QueueStatusResponse> res) {
             res.onNext(QueueStatusResponse.newBuilder()
                     .setQueueFull(queue.remainingCapacity() == 0)
                     .setQueueSize(queue.size())
                     .setMaxQueueSize(maxQueueSize).build());
             res.onCompleted();
        }
    }
    
    private class VideoConsumer implements Runnable {
        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    processVideo(queue.take());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        
        private void processVideo(VideoUploadTask task) {
            try {
               
                String rawName = "raw_" + task.getFileId() + "_" + task.getFilename();
                File rawFile = new File(uploadDir, rawName);
                try (FileOutputStream fos = new FileOutputStream(rawFile)) {
                    fos.write(task.getData());
                }
                
                logger.info("Processing {}: Saved raw file. Compressing...", task.getFilename());
               
                String compressedName = task.getFileId() + "_" + task.getFilename();
                File compressedFile = new File(uploadDir, compressedName);
                boolean compressed = compressVideo(rawFile, compressedFile);
                
                File finalFile;
              
                if (compressed && compressedFile.length() < rawFile.length()) {
                    logger.info("Compression SUCCESS & SMALLER. Old: {} -> New: {}", rawFile.length(), compressedFile.length());
                    rawFile.delete(); 
                    finalFile = compressedFile;
                } else {
                    if (compressed) {
                        logger.info("Compression SUCCESS but BIGGER. Keeping Original. Old: {} -> New: {}", rawFile.length(), compressedFile.length());
                        compressedFile.delete(); 
                    } else {
                        logger.warn("Compression FAILED (or ffmpeg missing). Using raw file.");
                    }
                    
                    File fallbackFile = new File(uploadDir, compressedName);
                    rawFile.renameTo(fallbackFile);
                    finalFile = fallbackFile;
                }
                // ------------------

                String webUrl = "http://localhost:8080/" + finalFile.getName();
                VideoMetadata metadata = new VideoMetadata(
                        task.getFileId(), task.getFilename(),
                        finalFile.length(), System.currentTimeMillis(), webUrl
                );
                
                uploadedVideos.put(task.getFileId(), metadata);
                logger.info("Video ready: {}", task.getFilename());
                
            } catch (Exception e) {
                logger.error("Error processing video", e);
            }
        }
    }
    
    private static class VideoUploadTask {
        String fileId, filename, hash; byte[] data;
        VideoUploadTask(String id, String f, byte[] d, String h) { fileId=id; filename=f; data=d; hash=h; }
        String getFileId() { return fileId; } String getFilename() { return filename; } byte[] getData() { return data; } String getHash() { return hash; }
    }
    
    private static class VideoMetadata {
        String fileId, filename, filePath; long fileSize, uploadTime;
        VideoMetadata(String id, String f, long s, long t, String p) { fileId=id; filename=f; fileSize=s; uploadTime=t; filePath=p; }
        String getFileId() { return fileId; } String getFilename() { return filename; } long getFileSize() { return fileSize; } long getUploadTime() { return uploadTime; } String getFilePath() { return filePath; }
    }
    
    private static class VideoInfo {
        public String id, filename, filePath; public long size, uploadTime;
        public VideoInfo(String id, String f, long s, long t, String p) { id=id; filename=f; size=s; uploadTime=t; filePath=p; }
    }
    
    public static void main(String[] args) throws IOException, InterruptedException {
        Scanner s = new Scanner(System.in);
        int port=50051, threads=4, q=10;
        try {
            System.out.print("Enter port (50051): "); String p = s.nextLine().trim();
            if(!p.isEmpty()) port=Integer.parseInt(p);
            System.out.print("Enter threads: "); String t = s.nextLine().trim();
            if(!t.isEmpty()) threads=Integer.parseInt(t);
            System.out.print("Enter max queue length: "); String qs = s.nextLine().trim();
            if(!qs.isEmpty()) q=Integer.parseInt(qs);
            
            ConsumerServer sv = new ConsumerServer(port, threads, q);
            sv.start();
            sv.blockUntilShutdown();
        } finally { s.close(); }
    }
}