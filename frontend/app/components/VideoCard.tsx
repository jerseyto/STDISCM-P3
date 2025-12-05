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

  // Get video URL for preview
  // const videoUrl = `http://localhost:8080/${video.id}_${video.filename}`;
  const videoUrl = video.filePath;

  useEffect(() => {
    // Create thumbnail when component mounts
    const videoElement = document.createElement('video');
    videoElement.src = videoUrl;
    videoElement.crossOrigin = 'anonymous';
    videoElement.addEventListener('loadeddata', () => {
      videoElement.currentTime = 1; // Seek to 1 second
      videoElement.addEventListener('seeked', () => {
        const canvas = document.createElement('canvas');
        canvas.width = videoElement.videoWidth;
        canvas.height = videoElement.videoHeight;
        const ctx = canvas.getContext('2d');
        if (ctx) {
          ctx.drawImage(videoElement, 0, 0);
          setThumbnailUrl(canvas.toDataURL());
        }
      }, { once: true });
    });
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
            style={{ objectFit: 'cover', width: '100%', height: '100%' }}
          />
        ) : (
          <div
            className="video-preview"
            style={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              color: 'white',
              fontSize: '1.2rem',
            }}
          >
            Loading...
          </div>
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

