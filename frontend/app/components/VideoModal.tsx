'use client';

import { useEffect, useRef } from 'react';

interface Video {
  id: string;
  filename: string;
  size: number;
  uploadTime: number;
  filePath: string;
}

interface VideoModalProps {
  video: Video | null;
  onClose: () => void;
}

export default function VideoModal({ video, onClose }: VideoModalProps) {
  const videoRef = useRef<HTMLVideoElement>(null);

  useEffect(() => {
    const handleEscape = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        onClose();
      }
    };

    document.addEventListener('keydown', handleEscape);
    document.body.style.overflow = 'hidden';

    return () => {
      document.removeEventListener('keydown', handleEscape);
      document.body.style.overflow = 'unset';
    };
  }, [onClose]);

  if (!video) {
    return null;
  }

  // const videoUrl = `http://localhost:8080/${video.id}_${video.filename}`;
  const videoUrl = video.filePath;

  return (
    <div className="video-modal" onClick={onClose}>
      <div className="video-modal-content" onClick={(e) => e.stopPropagation()} style={{ position: 'relative' }} >
        <button className="video-modal-close" onClick={onClose}
        style={{
            position: 'absolute',
            top: '10px',
            right: '10px',
            zIndex: 9999, // Ensures it sits ON TOP of the video
            cursor: 'pointer',
            backgroundColor: 'rgba(0, 0, 0, 0.6)', // Semi-transparent black background
            color: 'white',
            border: 'none',
            borderRadius: '50%',
            width: '32px',
            height: '32px',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            fontSize: '20px',
            fontWeight: 'bold',
            padding: 0,
          }}>
          ×
        </button>
        <video
          ref={videoRef}
          className="video-modal-video"
          src={videoUrl}
          controls
          autoPlay
          style={{ display: 'block', width: '100%' }}
        />
        <div className="video-modal-info">
          <h2>{video.filename}</h2>
        </div>
      </div>
    </div>
  );
}

