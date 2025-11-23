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
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
        
        // Create producer threads
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
                    
                    // Monitor folder for video files
                    Files.walk(folder)
                            .filter(Files::isRegularFile)
                            .filter(p -> {
                                String name = p.toString().toLowerCase();
                                return name.endsWith(".mp4") || name.endsWith(".avi") || 
                                       name.endsWith(".mov") || name.endsWith(".mkv") ||
                                       name.endsWith(".webm");
                            })
                            .forEach(videoPath -> {
                                logger.info("Producer {}: Uploading {}", threadIndex + 1, videoPath.getFileName());
                                client.uploadVideo(videoPath.toString(), "folder" + (threadIndex + 1));
                            });
                } catch (IOException e) {
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
