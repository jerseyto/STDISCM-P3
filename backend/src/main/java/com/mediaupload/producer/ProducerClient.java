package com.mediaupload.producer;

import com.google.common.hash.Hashing;
import com.mediaupload.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.net.InetSocketAddress;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;

public class ProducerClient {
    private static final Logger logger = LoggerFactory.getLogger(ProducerClient.class);
    private final ManagedChannel channel;
    private final MediaUploadServiceGrpc.MediaUploadServiceStub asyncStub;
    private final String serverHost;
    private final int serverPort;
    
    public ProducerClient(String host, int port) {
        this.serverHost = host == null || host.trim().isEmpty() ? "localhost" : host.trim();
        this.serverPort = port;

        // make sure host is just "localhost" or "192.168.x.x" (no :port)
        logger.info("Connecting to consumer server at {}:{}", this.serverHost, this.serverPort);
        logger.info("Creating gRPC channel to {}:{}", this.serverHost, this.serverPort);

        // Force Netty to use a plain InetSocketAddress directly
        InetSocketAddress address = new InetSocketAddress(this.serverHost, this.serverPort);

        this.channel = NettyChannelBuilder
                .forAddress(address)               // <— direct InetSocketAddress, no name resolver
                .usePlaintext()                    // no TLS for local testing
                .maxInboundMessageSize(100 * 1024 * 1024) // 100MB max
                .build();

        this.asyncStub = MediaUploadServiceGrpc.newStub(channel);
        logger.info("gRPC channel created successfully");


    }
    
    public void uploadVideo(String videoPath, String folderName) {
        try {
            Path path = Paths.get(videoPath);
            String filename = path.getFileName().toString();
            
            // Calculate file hash for duplicate detection
            String fileHash = calculateFileHash(path);
            
            // Read file
            byte[] fileData = Files.readAllBytes(path);
            int chunkSize = 64 * 1024; // 64KB chunks
            int totalChunks = (int) Math.ceil((double) fileData.length / chunkSize);
            
            CountDownLatch finishLatch = new CountDownLatch(1);
            
            StreamObserver<VideoChunk> requestObserver = asyncStub.uploadVideo(
                    new StreamObserver<UploadResponse>() {
                        @Override
                        public void onNext(UploadResponse response) {
                            if (response.getQueueFull()) {
                                logger.warn("Queue is full for video: {}", filename);
                            } else if (response.getIsDuplicate()) {
                                logger.info("Duplicate video detected: {}", filename);
                            } else if (response.getSuccess()) {
                                logger.info("Video uploaded successfully: {} (ID: {})", filename, response.getFileId());
                            } else {
                                logger.error("Upload failed: {}", response.getMessage());
                            }
                        }
                        
                        @Override
                        public void onError(Throwable t) {
                            logger.error("Upload error for video: {}", filename, t);
                            finishLatch.countDown();
                        }
                        
                        @Override
                        public void onCompleted() {
                            finishLatch.countDown();
                        }
                    });
            
            // Send chunks
            for (int i = 0; i < totalChunks; i++) {
                int start = i * chunkSize;
                int end = Math.min(start + chunkSize, fileData.length);
                byte[] chunk = new byte[end - start];
                System.arraycopy(fileData, start, chunk, 0, end - start);
                
                VideoChunk videoChunk = VideoChunk.newBuilder()
                        .setFilename(filename)
                        .setData(com.google.protobuf.ByteString.copyFrom(chunk))
                        .setChunkIndex(i)
                        .setIsLastChunk(i == totalChunks - 1)
                        .setHash(fileHash)
                        .build();
                
                requestObserver.onNext(videoChunk);
            }
            
            requestObserver.onCompleted();
            
            // Wait for completion
            if (!finishLatch.await(1, TimeUnit.MINUTES)) {
                logger.warn("Upload timeout for video: {}", filename);
            }
            
        } catch (Exception e) {
            logger.error("Error uploading video: {}", videoPath, e);
        }
    }
    
    private String calculateFileHash(Path path) throws IOException {
        byte[] fileBytes = Files.readAllBytes(path);
        return Hashing.sha256()
                .hashBytes(fileBytes)
                .toString();
    }
    
    public void checkQueueStatus() throws Exception {
        try {
            logger.info("Attempting to connect to gRPC server...");
            MediaUploadServiceGrpc.MediaUploadServiceBlockingStub blockingStub =
                    MediaUploadServiceGrpc.newBlockingStub(channel);
            
            // Add a timeout to avoid hanging
            blockingStub = blockingStub.withDeadlineAfter(10, TimeUnit.SECONDS);
            
            logger.info("Calling checkQueueStatus RPC...");
            QueueStatusResponse response = blockingStub.checkQueueStatus(
                    QueueStatusRequest.newBuilder().build());
            
            logger.info("RPC call successful!");
            if (response.getQueueFull()) {
                logger.warn("Queue is full. Size: {}/{}", response.getQueueSize(), response.getMaxQueueSize());
            } else {
                logger.info("Queue status: {}/{}", response.getQueueSize(), response.getMaxQueueSize());
            }
        } catch (io.grpc.StatusRuntimeException e) {
            logger.error("gRPC error: status={}, description={}, cause={}", 
                    e.getStatus().getCode(), 
                    e.getStatus().getDescription(),
                    e.getCause() != null ? e.getCause().getClass().getSimpleName() : "none");
            if (e.getCause() != null) {
                logger.error("Root cause: ", e.getCause());
            }
            throw new Exception("Failed to connect to consumer: " + 
                    (e.getStatus().getDescription() != null ? e.getStatus().getDescription() : e.getStatus().getCode().toString()), e);
        } catch (Exception e) {
            logger.error("Unexpected error during connection test: ", e);
            throw e;
        }
    }
    
    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }
    
    /**
     * Scans folder and uploads all video files that haven't been uploaded yet
     */
    private static void scanAndUploadVideos(ProducerClient client, Path folder, String folderName, 
                                           Set<String> uploadedFiles, int producerId) {
        try {
            Files.walk(folder)
                    .filter(Files::isRegularFile)
                    .filter(p -> isVideoFile(p))
                    .forEach(videoPath -> {
                        String absolutePath = videoPath.toAbsolutePath().toString();
                        
                        // Skip if already uploaded
                        if (uploadedFiles.contains(absolutePath)) {
                            logger.debug("Producer {}: Skipping already uploaded file: {}", producerId, videoPath.getFileName());
                            return;
                        }
                        
                        // Wait for file to stabilize (in case it's still being written)
                        if (waitForFileStable(videoPath)) {
                            logger.info("Producer {}: Uploading {}", producerId, videoPath.getFileName());
                            client.uploadVideo(absolutePath, folderName);
                            uploadedFiles.add(absolutePath); // Mark as uploaded
                        } else {
                            logger.warn("Producer {}: File {} appears to be still writing, will retry later", 
                                    producerId, videoPath.getFileName());
                        }
                    });
        } catch (IOException e) {
            logger.error("Producer {}: Error scanning folder: {}", producerId, e.getMessage());
        }
    }
    
    /**
     * Continuously monitors folder for new video files
     */
    private static void monitorFolderForNewVideos(ProducerClient client, Path folder, String folderName,
                                                 Set<String> uploadedFiles, int producerId) {
        logger.info("Producer {}: Continuous monitoring started for folder: {}", producerId, folder);
        
        // Poll interval: check for new files every 3 seconds
        final long pollIntervalSeconds = 3;
        
        while (!Thread.currentThread().isInterrupted()) {
            try {
                // Scan for new files
                Files.walk(folder)
                        .filter(Files::isRegularFile)
                        .filter(p -> isVideoFile(p))
                        .forEach(videoPath -> {
                            String absolutePath = videoPath.toAbsolutePath().toString();
                            
                            // Check if this is a new file we haven't uploaded yet
                            if (!uploadedFiles.contains(absolutePath)) {
                                // Wait for file to stabilize before uploading
                                if (waitForFileStable(videoPath)) {
                                    logger.info("Producer {}: New file detected, uploading: {}", 
                                            producerId, videoPath.getFileName());
                                    client.uploadVideo(absolutePath, folderName);
                                    uploadedFiles.add(absolutePath); // Mark as uploaded
                                } else {
                                    logger.debug("Producer {}: File {} still writing, will check again later", 
                                            producerId, videoPath.getFileName());
                                }
                            }
                        });
                
                // Sleep before next poll
                Thread.sleep(pollIntervalSeconds * 1000);
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.info("Producer {}: Monitoring interrupted", producerId);
                break;
            } catch (Exception e) {
                logger.error("Producer {}: Error during folder monitoring: {}", producerId, e.getMessage());
                try {
                    Thread.sleep(pollIntervalSeconds * 1000); // Continue monitoring despite error
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        
        logger.info("Producer {}: Continuous monitoring stopped", producerId);
    }
    
    /**
     * Checks if a file is a video file based on extension
     */
    private static boolean isVideoFile(Path path) {
        String name = path.toString().toLowerCase();
        return name.endsWith(".mp4") || name.endsWith(".avi") || 
               name.endsWith(".mov") || name.endsWith(".mkv") ||
               name.endsWith(".webm");
    }
    
    /**
     * Waits for a file to stabilize (stop being written to) before uploading
     * Returns true if file is stable, false if timeout
     */
    private static boolean waitForFileStable(Path filePath) {
        try {
            long stableCheckInterval = 2000; // Check every 2 seconds
            int maxChecks = 5; // Check up to 5 times (10 seconds total)
            
            long lastSize = -1;
            int stableCount = 0;
            
            for (int i = 0; i < maxChecks; i++) {
                if (!Files.exists(filePath)) {
                    return false; // File doesn't exist
                }
                
                long currentSize = Files.size(filePath);
                
                // If file size hasn't changed, it might be stable
                if (currentSize == lastSize && currentSize > 0) {
                    stableCount++;
                    if (stableCount >= 2) {
                        // File size has been stable for 2 checks (4 seconds), consider it stable
                        return true;
                    }
                } else {
                    stableCount = 0; // Reset counter if size changed
                }
                
                lastSize = currentSize;
                Thread.sleep(stableCheckInterval);
            }
            
            // If file size is non-zero and hasn't changed in last check, assume it's stable
            return lastSize > 0;
            
        } catch (Exception e) {
            logger.warn("Error checking file stability for {}: {}", filePath, e.getMessage());
            return false;
        }
    }
    
    public static void main(String[] args) throws InterruptedException {
        Scanner scanner = new Scanner(System.in);
        
        System.out.print("Enter consumer server host (default localhost): ");
        String host = scanner.nextLine().trim();
        if (host.isEmpty()) {
            host = "localhost";
        } else {
            int colonIndex = host.indexOf(':');
            if (colonIndex != -1) {
                host = host.substring(0, colonIndex);
            }
        }
        
        System.out.print("Enter consumer server port (default 50051): ");
        String portStr = scanner.nextLine().trim();
        int port = portStr.isEmpty() ? 50051 : Integer.parseInt(portStr);
        
        System.out.print("Enter number of producer threads (p): ");
        int producerThreads = Integer.parseInt(scanner.nextLine().trim());
        
        System.out.print("Enter base folder path for videos: ");
        String baseFolderPath = scanner.nextLine().trim();
        
        ProducerClient client = new ProducerClient(host, port);
        
        // Test connection before starting uploads
        System.out.println("Testing connection to consumer server...");
        try {
            client.checkQueueStatus();
            System.out.println("Connection successful! Starting uploads...");
        } catch (Exception e) {
            System.err.println("ERROR: Cannot connect to consumer server at " + host + ":" + port);
            System.err.println("Please verify:");
            System.err.println("  1. Consumer server is running (Terminal 1)");
            System.err.println("  2. Host is correct (use 'localhost' for local machine)");
            System.err.println("  3. Port matches consumer server port");
            System.err.println("  4. No firewall blocking the connection");
            System.err.println("\nError details:");
            if (e.getCause() != null) {
                System.err.println("  Root cause: " + e.getCause().getClass().getSimpleName());
                System.err.println("  Message: " + e.getCause().getMessage());
            } else {
                System.err.println("  " + e.getMessage());
            }
            if (e.getCause() instanceof java.net.ConnectException) {
                System.err.println("\nThis usually means:");
                System.err.println("  - Consumer server is not running");
                System.err.println("  - Wrong port number");
                System.err.println("  - Firewall blocking connection");
            }
            scanner.close();
            System.exit(1);
        }
        
        ExecutorService executor = Executors.newFixedThreadPool(producerThreads);
        
        // Shared set to track uploaded files across all threads (by absolute path)
        Set<String> uploadedFiles = ConcurrentHashMap.newKeySet();
        
        // Create producer threads with continuous monitoring
        for (int i = 0; i < producerThreads; i++) {
            final int threadIndex = i;
            final String folderPath = Paths.get(baseFolderPath, "folder" + (threadIndex + 1)).toString();
            
            executor.submit(() -> {
                try {
                    Path folder = Paths.get(folderPath);
                    if (!Files.exists(folder)) {
                        logger.warn("Folder does not exist: {}", folderPath);
                        return;
                    }
                    
                    logger.info("Producer {}: Starting continuous monitoring of folder: {}", threadIndex + 1, folderPath);
                    
                    // First, scan and upload existing files
                    scanAndUploadVideos(client, folder, "folder" + (threadIndex + 1), uploadedFiles, threadIndex + 1);
                    
                    // Then continuously monitor for new files
                    monitorFolderForNewVideos(client, folder, "folder" + (threadIndex + 1), uploadedFiles, threadIndex + 1);
                    
                } catch (Exception e) {
                    logger.error("Error in producer thread {}", threadIndex + 1, e);
                }
            });
        }
        
        // Keep running
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            executor.shutdown();
            try {
                client.shutdown();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
        
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    }
}
