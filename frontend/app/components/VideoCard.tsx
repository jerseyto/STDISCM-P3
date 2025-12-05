'use client';

import { useState, useRef, useEffect } from 'react';

interface Video {
  id: string;
  filename: string;
  size: number;
  uploadTime: number;
  filePath: string;
}

interface VideoCardProps {
  video: Video;
  onHover: (videoId: string | null) => void;
  onClick: (video: Video) => void;
  isHovered: boolean;
  formatFileSize: (bytes: number) => string;
  formatDate: (timestamp: number) => string;
}

export default function VideoCard({
  video,
  onHover,
  onClick,
  isHovered,
  formatFileSize,
  formatDate,
}: VideoCardProps) {
  const [thumbnailUrl, setThumbnailUrl] = useState<string | null>(null);
  const videoRef = useRef<HTMLVideoElement>(null);
  const previewVideoRef = useRef<HTMLVideoElement>(null);

  // Get video URL for preview - ensure it's a full URL
  const videoUrl = video.filePath.startsWith('http') 
    ? video.filePath 
    : `http://localhost:8080${video.filePath.startsWith('/') ? '' : '/'}${video.filePath}`;

  useEffect(() => {
    // Create thumbnail when component mounts
    let videoElement: HTMLVideoElement | null = null;
    let isCancelled = false;

    const generateThumbnail = () => {
      videoElement = document.createElement('video');
      videoElement.src = videoUrl;
      videoElement.crossOrigin = 'anonymous';
      videoElement.muted = true;
      videoElement.preload = 'metadata';
      
      const handleLoadedData = () => {
        if (isCancelled || !videoElement) return;
        
        try {
          // Seek to 1 second or middle of video
          const seekTime = Math.min(1, videoElement.duration / 2 || 1);
          videoElement.currentTime = seekTime;
        } catch (e) {
          // If seeking fails, use first frame
          videoElement.currentTime = 0;
        }
      };

      const handleSeeked = () => {
        if (isCancelled || !videoElement) return;
        
        try {
          const canvas = document.createElement('canvas');
          const width = videoElement.videoWidth || 320;
          const height = videoElement.videoHeight || 240;
          
          canvas.width = width;
          canvas.height = height;
          
          const ctx = canvas.getContext('2d');
          if (ctx) {
            ctx.drawImage(videoElement, 0, 0, width, height);
            const thumbnail = canvas.toDataURL('image/jpeg', 0.8);
            if (!isCancelled) {
              setThumbnailUrl(thumbnail);
            }
          }
        } catch (error) {
          console.error('Error generating thumbnail:', error);
          // Set a placeholder on error
          if (!isCancelled) {
            setThumbnailUrl(null);
          }
        }
      };

      const handleError = () => {
        console.error('Error loading video for thumbnail:', videoUrl);
        if (!isCancelled) {
          setThumbnailUrl(null);
        }
      };

      videoElement.addEventListener('loadeddata', handleLoadedData);
      videoElement.addEventListener('seeked', handleSeeked);
      videoElement.addEventListener('error', handleError);
      
      // Timeout fallback - use first frame if seeking takes too long
      const timeout = setTimeout(() => {
        if (!isCancelled && videoElement && videoElement.readyState >= 2) {
          try {
            videoElement.currentTime = 0;
            const canvas = document.createElement('canvas');
            const width = videoElement.videoWidth || 320;
            const height = videoElement.videoHeight || 240;
            canvas.width = width;
            canvas.height = height;
            const ctx = canvas.getContext('2d');
            if (ctx && videoElement.videoWidth > 0) {
              ctx.drawImage(videoElement, 0, 0, width, height);
              setThumbnailUrl(canvas.toDataURL('image/jpeg', 0.8));
            }
          } catch (e) {
            console.error('Fallback thumbnail generation failed:', e);
          }
        }
      }, 3000);

      return () => {
        clearTimeout(timeout);
        if (videoElement) {
          videoElement.removeEventListener('loadeddata', handleLoadedData);
          videoElement.removeEventListener('seeked', handleSeeked);
          videoElement.removeEventListener('error', handleError);
          videoElement.src = '';
          videoElement.load();
        }
      };
    };

    const cleanup = generateThumbnail();

    return () => {
      isCancelled = true;
      if (cleanup) cleanup();
    };
  }, [videoUrl]);

  useEffect(() => {
    const previewVideo = previewVideoRef.current;
    if (previewVideo && isHovered) {
      // Load and play preview on hover
      previewVideo.load();
      previewVideo.currentTime = 0;
      previewVideo.play().catch((err) => {
        console.error('Error playing preview:', err);
      });

      // Stop after 10 seconds
      const timeout = setTimeout(() => {
        if (previewVideo) {
          previewVideo.pause();
          previewVideo.currentTime = 0;
        }
      }, 10000);

      return () => {
        clearTimeout(timeout);
        if (previewVideo) {
          previewVideo.pause();
          previewVideo.currentTime = 0;
        }
      };
    }
  }, [isHovered]);

  const handleMouseEnter = () => {
    onHover(video.id);
  };

  const handleMouseLeave = () => {
    onHover(null);
  };

  return (
    <div
      className="video-card"
      onMouseEnter={handleMouseEnter}
      onMouseLeave={handleMouseLeave}
      onClick={() => onClick(video)}
    >
      <div style={{ position: 'relative', width: '100%', height: '200px', overflow: 'hidden' }}>
        {thumbnailUrl ? (
          <img
            src={thumbnailUrl}
            alt={video.filename}
            className="video-preview"
            style={{ objectFit: 'cover', width: '100%', height: '100%', backgroundColor: '#000' }}
            onError={() => {
              // Fallback if thumbnail image fails to load - show video frame
              setThumbnailUrl(null);
            }}
          />
        ) : (
          <video
            ref={videoRef}
            src={videoUrl}
            className="video-preview"
            style={{
              objectFit: 'cover',
              width: '100%',
              height: '100%',
              backgroundColor: '#000',
            }}
            muted
            playsInline
            preload="metadata"
            poster=""
            onLoadedMetadata={(e) => {
              // Show first frame immediately by seeking to 0 and pausing
              const video = e.currentTarget;
              try {
                video.currentTime = 0.1; // Seek to 0.1 second for a visible frame
                video.pause();
                
                // Generate thumbnail from first frame
                setTimeout(() => {
                  try {
                    const canvas = document.createElement('canvas');
                    const width = video.videoWidth || 320;
                    const height = video.videoHeight || 240;
                    canvas.width = width;
                    canvas.height = height;
                    const ctx = canvas.getContext('2d');
                    if (ctx && video.videoWidth > 0) {
                      ctx.drawImage(video, 0, 0, width, height);
                      setThumbnailUrl(canvas.toDataURL('image/jpeg', 0.8));
                    }
                  } catch (err) {
                    console.error('Error generating thumbnail:', err);
                  }
                }, 300);
              } catch (err) {
                console.error('Error setting up video thumbnail:', err);
              }
            }}
            onError={(e) => {
              console.error('Error loading video:', videoUrl);
            }}
          />
        )}

        {isHovered && (
          <div className="preview-overlay">
            <video
              ref={previewVideoRef}
              className="preview-video"
              src={videoUrl}
              muted
              playsInline
              preload="metadata"
              style={{ maxWidth: '90%', maxHeight: '90%' }}
            />
          </div>
        )}
      </div>

      <div className="video-info">
        <h3>{video.filename}</h3>
        <p>Size: {formatFileSize(video.size)}</p>
        <p>Uploaded: {formatDate(video.uploadTime)}</p>
      </div>
    </div>
  );
}

