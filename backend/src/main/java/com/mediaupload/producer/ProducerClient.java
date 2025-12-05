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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ProducerClient {
    private static final Logger logger = LoggerFactory.getLogger(ProducerClient.class);
    private final ManagedChannel channel;
    private final MediaUploadServiceGrpc.MediaUploadServiceStub asyncStub;
    private final MediaUploadServiceGrpc.MediaUploadServiceBlockingStub blockingStub;
    private final String serverHost;
    private final int serverPort;
    
    public ProducerClient(String host, int port) {
        this.serverHost = host == null || host.trim().isEmpty() ? "localhost" : host.trim();
        this.serverPort = port;

        logger.info("Connecting to consumer server at {}:{}", this.serverHost, this.serverPort);

        InetSocketAddress address = new InetSocketAddress(this.serverHost, this.serverPort);

        this.channel = NettyChannelBuilder
                .forAddress(address)
                .usePlaintext()
                .maxInboundMessageSize(100 * 1024 * 1024) // 100MB max
                .build();

        this.asyncStub = MediaUploadServiceGrpc.newStub(channel);
        this.blockingStub = MediaUploadServiceGrpc.newBlockingStub(channel);
        logger.info("gRPC channel created successfully");
    }
    
    public boolean isQueueAvailable() {
        try {
            QueueStatusResponse response = blockingStub
                    .withDeadlineAfter(5, TimeUnit.SECONDS)
                    .checkQueueStatus(QueueStatusRequest.newBuilder().build());
            
            if (response.getQueueFull()) {
                logger.warn("Queue is FULL ({}/{}). Cannot accept uploads.", 
                        response.getQueueSize(), response.getMaxQueueSize());
                return false;
            }
            
        
            double fillPercentage = (double) response.getQueueSize() / response.getMaxQueueSize();
            if (fillPercentage > 0.8) {
                logger.info("Queue is {}% full ({}/{})", 
                        (int)(fillPercentage * 100), 
                        response.getQueueSize(), 
                        response.getMaxQueueSize());
            }
            
            return true;
        } catch (Exception e) {
            logger.error("Failed to check queue status: {}", e.getMessage());
            return false; 
        }
    }
    
    public void uploadVideoWithQueueCheck(String videoPath, String folderName, int producerId) {
        int maxRetries = 5;
        int retryDelayMs = 3000; 
        
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            
            if (!isQueueAvailable()) {
                if (attempt < maxRetries) {
                    logger.info("Producer {}: Queue full, retry {}/{} in {} seconds for: {}", 
                            producerId, attempt, maxRetries, retryDelayMs / 1000, 
                            Paths.get(videoPath).getFileName());
                    try {
                        Thread.sleep(retryDelayMs);
                        retryDelayMs = (int)(retryDelayMs * 1.5); 
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    continue;
                } else {
                    logger.error("Producer {}: Queue full after {} retries. DROPPING video: {}", 
                            producerId, maxRetries, videoPath);
                    return;
                }
            }
            
            UploadResult result = uploadVideoInternal(videoPath, folderName, producerId);
            
            if (result.success  && !result.isDuplicate) {
                logger.info("Producer {}: Upload successful: {}", 
                        producerId, Paths.get(videoPath).getFileName());
                return; 
            } else if (result.isDuplicate) {
                logger.info("Producer {}: Duplicate detected (skipping): {}", 
                        producerId, Paths.get(videoPath).getFileName());
                return; 
            } else if (result.queueFull) {
               
                if (attempt < maxRetries) {
                    logger.warn("Producer {}: Queue became full during upload. Retry {}/{} in {} seconds", 
                            producerId, attempt, maxRetries, retryDelayMs / 1000);
                    try {
                        Thread.sleep(retryDelayMs);
                        retryDelayMs = (int)(retryDelayMs * 1.5); 
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    continue;
                } else {
                    logger.error("Producer {}: Queue full after {} retries. DROPPING video: {}", 
                            producerId, maxRetries, videoPath);
                    return;
                }
            } else {
               
                logger.error("Producer {}: Upload failed: {}", producerId, result.message);
                return; 
            }
        }
    }
    
    private UploadResult uploadVideoInternal(String videoPath, String folderName, int producerId) {
        try {
            Path path = Paths.get(videoPath);
            String filename = path.getFileName().toString();
            
            String fileHash = calculateFileHash(path);
            
            byte[] fileData = Files.readAllBytes(path);
            int chunkSize = 64 * 1024;
            int totalChunks = (int) Math.ceil((double) fileData.length / chunkSize);
            
            CountDownLatch finishLatch = new CountDownLatch(1);
            UploadResult result = new UploadResult();
            
            StreamObserver<VideoChunk> requestObserver = asyncStub.uploadVideo(
                    new StreamObserver<UploadResponse>() {
                        @Override
                        public void onNext(UploadResponse response) {
                            result.success = response.getSuccess();
                            result.queueFull = response.getQueueFull();
                            result.isDuplicate = response.getIsDuplicate();
                            result.message = response.getMessage();
                            result.fileId = response.getFileId();
                            
                            if (response.getQueueFull()) {
                                logger.debug("Producer {}: Server reports queue full for: {}", 
                                        producerId, filename);
                            } else if (response.getIsDuplicate()) {
                                logger.debug("Producer {}: Server reports duplicate for: {}", 
                                        producerId, filename);
                                logger.info("Duplicate video detected: {}", filename);
                            } else if (response.getSuccess()) {
                                logger.debug("Producer {}: Server accepted: {} (ID: {})", 
                                        producerId, filename, response.getFileId());
                            }
                        }
                        
                        @Override
                        public void onError(Throwable t) {
                            logger.error("Producer {}: Upload stream error for: {}", 
                                    producerId, filename, t);
                            result.success = false;
                            result.message = "Stream error: " + t.getMessage();
                            finishLatch.countDown();
                        }
                        
                        @Override
                        public void onCompleted() {
                            finishLatch.countDown();
                        }
                    });
            
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
            
            if (!finishLatch.await(2, TimeUnit.MINUTES)) {
                logger.warn("Producer {}: Upload timeout for: {}", producerId, filename);
                result.success = false;
                result.message = "Upload timeout";
            }
            
            return result;
            
        } catch (Exception e) {
            logger.error("Producer {}: Error uploading video: {}", producerId, videoPath, e);
            UploadResult result = new UploadResult();
            result.success = false;
            result.message = "Exception: " + e.getMessage();
            return result;
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
            
            QueueStatusResponse response = blockingStub
                    .withDeadlineAfter(10, TimeUnit.SECONDS)
                    .checkQueueStatus(QueueStatusRequest.newBuilder().build());
            
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
    
    private static void scanAndUploadVideos(ProducerClient client, Path folder, String folderName, 
                                           Set<String> uploadedFiles, int producerId) {
        try {
            Files.walk(folder)
                    .filter(Files::isRegularFile)
                    .filter(p -> isVideoFile(p))
                    .forEach(videoPath -> {
                        String absolutePath = videoPath.toAbsolutePath().toString();
                        
                        if (uploadedFiles.contains(absolutePath)) {
                            logger.debug("Producer {}: Skipping already uploaded file: {}", 
                                    producerId, videoPath.getFileName());
                            return;
                        }
                        
                        if (waitForFileStable(videoPath)) {
                            logger.info("Producer {}: Uploading {}", producerId, videoPath.getFileName());
                            client.uploadVideoWithQueueCheck(absolutePath, folderName, producerId);
                            uploadedFiles.add(absolutePath);
                        } else {
                            logger.warn("Producer {}: File {} appears to be still writing, will retry later", 
                                    producerId, videoPath.getFileName());
                        }
                    });
        } catch (IOException e) {
            logger.error("Producer {}: Error scanning folder: {}", producerId, e.getMessage());
        }
    }
    
    private static void monitorFolderForNewVideos(ProducerClient client, Path folder, String folderName,
                                                 Set<String> uploadedFiles, int producerId) {
        logger.info("Producer {}: Continuous monitoring started for folder: {}", producerId, folder);
        
        final long pollIntervalSeconds = 3;
        
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Files.walk(folder)
                        .filter(Files::isRegularFile)
                        .filter(p -> isVideoFile(p))
                        .forEach(videoPath -> {
                            String absolutePath = videoPath.toAbsolutePath().toString();
                            
                            if (!uploadedFiles.contains(absolutePath)) {
                                if (waitForFileStable(videoPath)) {
                                    logger.info("Producer {}: New file detected, uploading: {}", 
                                            producerId, videoPath.getFileName());
                                    client.uploadVideoWithQueueCheck(absolutePath, folderName, producerId);
                                    uploadedFiles.add(absolutePath);
                                } else {
                                    logger.debug("Producer {}: File {} still writing, will check again later", 
                                            producerId, videoPath.getFileName());
                                }
                            }
                        });
                
                Thread.sleep(pollIntervalSeconds * 1000);
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.info("Producer {}: Monitoring interrupted", producerId);
                break;
            } catch (Exception e) {
                logger.error("Producer {}: Error during folder monitoring: {}", producerId, e.getMessage());
                try {
                    Thread.sleep(pollIntervalSeconds * 1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        
        logger.info("Producer {}: Continuous monitoring stopped", producerId);
    }
    
    private static boolean isVideoFile(Path path) {
        String name = path.toString().toLowerCase();
        return name.endsWith(".mp4") || name.endsWith(".avi") || 
               name.endsWith(".mov") || name.endsWith(".mkv") ||
               name.endsWith(".webm");
    }
    
    private static boolean waitForFileStable(Path filePath) {
        try {
            long stableCheckInterval = 2000; 
            int maxChecks = 5;
            
            long lastSize = -1;
            int stableCount = 0;
            
            for (int i = 0; i < maxChecks; i++) {
                if (!Files.exists(filePath)) {
                    return false;
                }
                
                long currentSize = Files.size(filePath);
                
                if (currentSize == lastSize && currentSize > 0) {
                    stableCount++;
                    if (stableCount >= 2) {
                        return true;
                    }
                } else {
                    stableCount = 0;
                }
                
                lastSize = currentSize;
                Thread.sleep(stableCheckInterval);
            }
            
            return lastSize > 0;
            
        } catch (Exception e) {
            logger.warn("Error checking file stability for {}: {}", filePath, e.getMessage());
            return false;
        }
    }
    
    private static class UploadResult {
        boolean success = false;
        boolean queueFull = false;
        boolean isDuplicate = false;
        String message = "";
        String fileId = "";
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
        
        System.out.println("\n=== Testing connection to consumer server ===");
        try {
            client.checkQueueStatus();
            System.out.println("Connection successful! Starting uploads...\n");
        } catch (Exception e) {
            System.err.println("ERROR: Cannot connect to consumer server at " + host + ":" + port);
            System.err.println("\nPlease verify:");
            System.err.println("  1. Consumer server is running");
            System.err.println("  2. Host is correct (use 'localhost' for same machine)");
            System.err.println("  3. Port matches consumer server port");
            System.err.println("  4. No firewall blocking the connection");
            System.err.println("\nError details: " + e.getMessage());
            scanner.close();
            System.exit(1);
        }
        
        ExecutorService executor = Executors.newFixedThreadPool(producerThreads);
        Set<String> uploadedFiles = ConcurrentHashMap.newKeySet();
        
        for (int i = 0; i < producerThreads; i++) {
            final int threadIndex = i;
            final String folderPath = Paths.get(baseFolderPath, "folder" + (threadIndex + 1)).toString();
            
            executor.submit(() -> {
                try {
                    Path folder = Paths.get(folderPath);
                    if (!Files.exists(folder)) {
                        logger.warn("Producer {}: Folder does not exist: {}", threadIndex + 1, folderPath);
                        return;
                    }
                    
                    logger.info("Producer {}: Starting continuous monitoring of folder: {}", 
                            threadIndex + 1, folderPath);
                    
                    scanAndUploadVideos(client, folder, "folder" + (threadIndex + 1), 
                            uploadedFiles, threadIndex + 1);
                    
                    monitorFolderForNewVideos(client, folder, "folder" + (threadIndex + 1), 
                            uploadedFiles, threadIndex + 1);
                    
                } catch (Exception e) {
                    logger.error("Producer {}: Error in producer thread", threadIndex + 1, e);
                }
            });
        }
        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down producer client...");
            executor.shutdown();
            try {
                client.shutdown();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
        
        System.out.println("=== Producer is running. Press Ctrl+C to stop ===\n");
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    }
}