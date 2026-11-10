export type VideoStatus = "uploaded" | "queued" | "processing" | "completed" | "failed" | "invalid";
export type JobStatus = "queued" | "running" | "retrying" | "completed" | "failed";

export interface UploadedVideo { id: string; title: string; original_filename: string; size_bytes: number; status: string; created_at: string }

export interface VideoSummary {
  video_id: string; title: string; original_filename: string; format: string;
  container: string | null; video_codec: string | null; audio_codec: string | null;
  duration_seconds: number | null; width: number | null; height: number | null;
  frame_rate: number | null; bit_rate: number | null; size_bytes: number;
  status: VideoStatus; progress: number; latest_job_id: string | null;
  error_message: string | null; created_at: string; updated_at: string;
}
export interface VideoList { items: VideoSummary[]; total: number; limit: number; offset: number }

export interface AttemptEntry {
  attempt: number; status: "running" | "succeeded" | "failed"; worker_id?: string;
  started_at?: string; finished_at?: string; error?: string;
}
export interface Job {
  id: string; video_id: string; job_type: string; status: JobStatus; progress: number;
  resolutions: number[]; attempt: number; max_attempts: number; retries_remaining: number;
  last_error: string | null; attempt_history: AttemptEntry[]; next_retry_at: string | null;
  dead_lettered: boolean; created_at: string; updated_at: string;
  started_at: string | null; completed_at: string | null;
}
export interface OutputMedia { resolution: string; height: number; width: number; size_bytes: number; content_type: string; url: string; expires_at: string }
export interface Outputs { video_id: string; count: number; outputs: OutputMedia[] }
export interface ThumbnailMedia { index: number; timestamp_seconds: number | null; width: number | null; height: number | null; status: string; error_message?: string | null; url: string | null }
export interface Thumbnails { video_id: string; status: string; count: number; thumbnails: ThumbnailMedia[] }
