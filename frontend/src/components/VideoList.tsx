import { useState } from "react";
import { api } from "../api";
import { bytes, duration, ago } from "../format";
import { usePolling } from "../hooks";
import type { VideoStatus } from "../types";
import { ProgressBar, StatusBadge } from "./StatusBadge";

const FILTERS: { value: VideoStatus | ""; label: string }[] = [
  { value: "", label: "All" }, { value: "processing", label: "Processing" }, { value: "completed", label: "Completed" },
  { value: "failed", label: "Failed" }, { value: "uploaded", label: "Uploaded" },
];

export function VideoList({ selected, onSelect, refreshKey }: { selected: string | null; onSelect: (id: string) => void; refreshKey: number }) {
  const [status, setStatus] = useState<VideoStatus | "">("");
  const [q, setQ] = useState("");
  const { data, error, loading } = usePolling(() => api.listVideos({ status, q: q.trim() || undefined }), 3000, [status, q, refreshKey]);

  return (
    <section className="card list">
      <div className="card-head">
        <h2 className="card-title">Library {data && <span className="count">{data.total}</span>}</h2>
        <input className="search" type="search" placeholder="Search titles…" value={q} onChange={(e) => setQ(e.target.value)} aria-label="Search videos" />
      </div>
      <div className="chips" role="tablist">
        {FILTERS.map((f) => (
          <button key={f.value} className={`chip ${status === f.value ? "on" : ""}`} onClick={() => setStatus(f.value)}>{f.label}</button>
        ))}
      </div>

      {error && !data && <div className="alert alert-error">{error.message}</div>}
      {loading && !data && <div className="skeleton-list">{[0, 1, 2].map((i) => <div key={i} className="skeleton" />)}</div>}
      {data && data.items.length === 0 && <div className="empty">No videos yet — upload your first one.</div>}

      <ul className="videos">
        {data?.items.map((v) => (
          <li key={v.video_id}>
            <button className={`video-row ${selected === v.video_id ? "selected" : ""}`} onClick={() => onSelect(v.video_id)}>
              <div className="vr-main">
                <strong className="vr-title">{v.title}</strong>
                <StatusBadge status={v.status} />
              </div>
              <div className="vr-meta muted">
                {v.height ? `${v.width}×${v.height}` : v.status === "invalid" ? "unreadable" : "analysing…"} · {duration(v.duration_seconds)} · {bytes(v.size_bytes)} · {ago(v.created_at)}
              </div>
              {(v.status === "processing" || v.status === "queued") && <ProgressBar value={v.progress} status={v.status} />}
              {v.status === "failed" && v.error_message && <div className="vr-error">{v.error_message}</div>}
            </button>
          </li>
        ))}
      </ul>
    </section>
  );
}
