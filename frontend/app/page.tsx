'use client';

import { useState, useEffect } from 'react';
import VideoCard from './components/VideoCard';
import VideoModal from './components/VideoModal';

interface Video {
  id: string;
  filename: string;
  size: number;
  uploadTime: number;
  filePath: string;
}

export default function Home() {
  const [videos, setVideos] = useState<Video[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedVideo, setSelectedVideo] = useState<Video | null>(null);
  const [hoveredVideo, setHoveredVideo] = useState<string | null>(null);

  useEffect(() => {
    fetchVideos();
    // Refresh every 5 seconds
    const interval = setInterval(fetchVideos, 5000);
    return () => clearInterval(interval);
  }, []);

  const fetchVideos = async () => {
    try {
      const response = await fetch('http://localhost:8080/api/videos');
      if (!response.ok) {
        throw new Error('Failed to fetch videos');
      }
      const data = await response.json();
      setVideos(data);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unknown error');
    } finally {
      setLoading(false);
    }
  };

  const formatFileSize = (bytes: number): string => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return Math.round(bytes / Math.pow(k, i) * 100) / 100 + ' ' + sizes[i];
  };

  const formatDate = (timestamp: number): string => {
    return new Date(timestamp).toLocaleString();
  };

  const handleVideoClick = (video: Video) => {
    setSelectedVideo(video);
  };

  const handleCloseModal = () => {
    setSelectedVideo(null);
  };

  return (
    <div className="container">
      <div className="header">
        <h1>Media Upload Service</h1>
        <p>Producer-Consumer Video Gallery</p>
      </div>

      {error && <div className="error">Error: {error}</div>}

      {loading ? (
        <div className="loading">Loading videos...</div>
      ) : (
        <>
          {videos.length === 0 ? (
            <div className="loading">No videos uploaded yet</div>
          ) : (
            <div className="video-grid">
              {videos.map((video) => (
                <VideoCard
                  key={video.id}
                  video={video}
                  onHover={setHoveredVideo}
                  onClick={handleVideoClick}
                  isHovered={hoveredVideo === video.id}
                  formatFileSize={formatFileSize}
                  formatDate={formatDate}
                />
              ))}
            </div>
          )}
        </>
      )}

      {selectedVideo && (
        <VideoModal
          video={selectedVideo}
          onClose={handleCloseModal}
        />
      )}
    </div>
  );
}

