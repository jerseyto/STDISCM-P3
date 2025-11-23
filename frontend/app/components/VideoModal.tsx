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
  video: Video;
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

  const videoUrl = `http://localhost:8080${video.filePath}`;

  return (
    <div className="video-modal" onClick={onClose}>
      <div className="video-modal-content" onClick={(e) => e.stopPropagation()}>
        <button className="video-modal-close" onClick={onClose}>
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

