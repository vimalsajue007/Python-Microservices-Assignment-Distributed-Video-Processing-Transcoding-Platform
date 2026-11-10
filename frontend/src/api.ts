import type { Job, Outputs, Thumbnails, UploadedVideo, VideoList, VideoStatus, VideoSummary } from "./types";

const BASE = "/api";

export class ApiError extends Error {
  constructor(public status: number, public code: string, message: string, public requestId?: string) {
    super(message);
  }
}

async function toError(resp: Response): Promise<ApiError> {
  try {
    const body = await resp.json();
    if (body?.error) return new ApiError(resp.status, body.error.code, body.error.message, body.request_id);
    if (body?.detail) return new ApiError(resp.status, "error", typeof body.detail === "string" ? body.detail : JSON.stringify(body.detail));
  } catch { /* not JSON */ }
  return new ApiError(resp.status, "error", `${resp.status} ${resp.statusText}`);
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  let resp: Response;
  try {
    resp = await fetch(`${BASE}${path}`, init);
  } catch {
    throw new ApiError(0, "network_error", "Cannot reach the API gateway");
  }
  if (!resp.ok) throw await toError(resp);
  return (await resp.json()) as T;
}

export const api = {
  listVideos: (p: { status?: VideoStatus | ""; q?: string; limit?: number; offset?: number }) => {
    const qs = new URLSearchParams();
    if (p.status) qs.set("status", p.status);
    if (p.q) qs.set("q", p.q);
    qs.set("limit", String(p.limit ?? 50));
    qs.set("offset", String(p.offset ?? 0));
    return request<VideoList>(`/videos?${qs}`);
  },
  getVideo: (id: string) => request<VideoSummary>(`/videos/${id}`),
  getJob: (id: string) => request<Job>(`/jobs/${id}`),
  outputs: (id: string) => request<Outputs>(`/videos/${id}/outputs`),
  thumbnails: (id: string) => request<Thumbnails>(`/videos/${id}/thumbnails`),
  process: (id: string, resolutions?: number[]) =>
    request<Job>(`/videos/${id}/process`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(resolutions?.length ? { resolutions } : {}),
    }),
  retry: (jobId: string) => request<Job>(`/jobs/${jobId}/retry`, { method: "POST" }),

  /** XHR (not fetch) so we can report upload progress. */
  upload(file: File, title: string, onProgress: (pct: number) => void): { promise: Promise<UploadedVideo>; abort: () => void } {
    const xhr = new XMLHttpRequest();
    const promise = new Promise<UploadedVideo>((resolve, reject) => {
      xhr.open("POST", `${BASE}/videos/upload`);
      xhr.upload.onprogress = (e) => e.lengthComputable && onProgress(Math.round((e.loaded / e.total) * 100));
      xhr.onerror = () => reject(new ApiError(0, "network_error", "Upload failed: cannot reach the API gateway"));
      xhr.onabort = () => reject(new ApiError(0, "aborted", "Upload cancelled"));
      xhr.onload = () => {
        let body: any = null;
        try { body = JSON.parse(xhr.responseText); } catch { /* ignore */ }
        if (xhr.status >= 200 && xhr.status < 300) return resolve(body as UploadedVideo);
        const err = body?.error;
        reject(new ApiError(xhr.status, err?.code ?? "error", err?.message ?? `Upload failed (${xhr.status})`, body?.request_id));
      };
      const form = new FormData();
      form.append("file", file);
      if (title.trim()) form.append("title", title.trim());
      xhr.send(form);
    });
    return { promise, abort: () => xhr.abort() };
  },
};
