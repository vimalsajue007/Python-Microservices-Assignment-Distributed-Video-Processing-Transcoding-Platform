import { useEffect, useMemo, useState } from "react";
import { api, ApiError } from "../api";
import { ago, bytes, duration } from "../format";
import { usePolling } from "../hooks";
import { useToast } from "../toast";
import type { Job, VideoSummary } from "../types";
import { ProgressBar, StatusBadge } from "./StatusBadge";

const PRESETS = [360, 480, 720, 1080];

export function VideoDetail({ videoId, onChanged }: { videoId: string; onChanged: () => void }) {
  const toast = useToast();
  const video = usePolling(() => api.getVideo(videoId), 2000, [videoId]);
  const v = video.data;
  const active = v?.status === "processing" || v?.status === "queued";
  const jobId = v?.latest_job_id ?? null;

  const job = usePolling<Job | null>(() => (jobId ? api.getJob(jobId) : Promise.resolve(null)), 1500, [jobId]);
  const thumbs = usePolling(() => api.thumbnails(videoId), active || v?.status === "uploaded" ? 2500 : 15000, [videoId]);
  const outputs = usePolling(() => api.outputs(videoId), active ? 2500 : 15000, [videoId, v?.status]);

  if (video.error && !v) return <section className="card detail"><div className="alert alert-error">{video.error.message}</div></section>;
  if (!v) return <section className="card detail"><div className="skeleton tall" /></section>;

  return (
    <section className="card detail">
      <header className="detail-head">
        <div>
          <h2 className="detail-title">{v.title}</h2>
          <p className="muted">{v.original_filename} · uploaded {ago(v.created_at)}</p>
        </div>
        <StatusBadge status={v.status} />
      </header>

      <div className="facts">
        <Fact label="Resolution" value={v.width ? `${v.width}×${v.height}` : "—"} />
        <Fact label="Duration" value={duration(v.duration_seconds)} />
        <Fact label="Size" value={bytes(v.size_bytes)} />
        <Fact label="Codec" value={[v.video_codec, v.audio_codec].filter(Boolean).join(" / ") || "—"} />
        <Fact label="Frame rate" value={v.frame_rate ? `${v.frame_rate.toFixed(2)} fps` : "—"} />
        <Fact label="Container" value={v.container ?? v.format} />
      </div>

      {v.status === "invalid" && <div className="alert alert-error"><strong>This file could not be analysed.</strong> {v.error_message}</div>}

      <Player videoId={videoId} outputs={outputs.data?.outputs ?? []} poster={thumbs.data?.thumbnails.find((t) => t.url)?.url ?? undefined} />

      <JobPanel video={v} job={job.data} onChanged={() => { onChanged(); video.refresh(); job.refresh(); toast("info", "Job update requested"); }} />

      <h3 className="section-title">Resolutions</h3>
      {outputs.data && outputs.data.outputs.length > 0 ? (
        <div className="renditions">
          {outputs.data.outputs.map((o) => (
            <div className="rendition" key={o.resolution}>
              <strong>{o.resolution}</strong>
              <span className="muted">{o.width}×{o.height} · {bytes(o.size_bytes)}</span>
            </div>
          ))}
        </div>
      ) : <p className="muted small">No transcoded renditions yet.</p>}

      <h3 className="section-title">Thumbnails {thumbs.data && <StatusBadge status={thumbs.data.status === "partial" ? "retrying" : thumbs.data.status} />}</h3>
      {thumbs.data && thumbs.data.thumbnails.length > 0 ? (
        <div className="thumbs">
          {thumbs.data.thumbnails.map((t) => t.url ? (
            <figure key={t.index}>
              <img src={t.url} alt={`Thumbnail ${t.index + 1}`} loading="lazy" />
              <figcaption className="muted">{duration(t.timestamp_seconds)}</figcaption>
            </figure>
          ) : (
            <figure key={t.index}><div className="thumb-missing">{t.status}</div></figure>
          ))}
        </div>
      ) : <p className="muted small">{thumbs.data?.status === "failed" ? "Thumbnail generation failed." : "Thumbnails are being generated…"}</p>}
    </section>
  );
}

function Fact({ label, value }: { label: string; value: string }) {
  return <div className="fact"><span>{label}</span><strong>{value}</strong></div>;
}

// ------------------------------------------------------------------ player
function Player({ videoId, outputs, poster }: { videoId: string; outputs: { resolution: string; url: string }[]; poster?: string }) {
  const [sel, setSel] = useState<string | null>(null);
  useEffect(() => setSel(null), [videoId]);
  const current = useMemo(() => outputs.find((o) => o.resolution === sel) ?? outputs[outputs.length - 1], [outputs, sel]);

  if (!current) {
    return (
      <div className="player empty-player">
        {poster ? <img src={poster} alt="" /> : null}
        <div className="player-overlay"><span>▶</span><small>Process the video to enable playback</small></div>
      </div>
    );
  }
  return (
    <div className="player-wrap">
      {/* key on the resolution so switching quality reloads the source but keeps the layout stable */}
      <video key={current.resolution} className="player" controls preload="metadata" poster={poster} src={current.url} />
      <div className="quality">
        {outputs.map((o) => (
          <button key={o.resolution} className={`chip ${o.resolution === current.resolution ? "on" : ""}`} onClick={() => setSel(o.resolution)}>{o.resolution}</button>
        ))}
      </div>
    </div>
  );
}

// ------------------------------------------------------------------ job panel
function JobPanel({ video, job, onChanged }: { video: VideoSummary; job: Job | null; onChanged: () => void }) {
  const toast = useToast();
  const [busy, setBusy] = useState(false);
  const [picked, setPicked] = useState<number[]>([360, 720]);
  const toggle = (r: number) => setPicked((p) => (p.includes(r) ? p.filter((x) => x !== r) : [...p, r].sort((a, b) => a - b)));

  const run = async (fn: () => Promise<unknown>, ok: string) => {
    setBusy(true);
    try { await fn(); toast("success", ok); onChanged(); }
    catch (e) { toast("error", (e as ApiError).message); }
    finally { setBusy(false); }
  };

  const canProcess = video.status === "uploaded" || video.status === "completed";
  const canRetry = job?.status === "failed";

  return (
    <div className="jobpanel">
      <div className="jp-head">
        <h3 className="section-title nomargin">Processing</h3>
        {job && <StatusBadge status={job.status} />}
      </div>

      {job ? (
        <>
          <ProgressBar value={job.progress} status={job.status} />
          <div className="jp-line muted">
            <span>{job.progress}%</span>
            <span>Attempt {job.attempt} of {job.max_attempts}</span>
            {job.status !== "completed" && job.status !== "failed" && <span>{job.retries_remaining} retr{job.retries_remaining === 1 ? "y" : "ies"} left</span>}
          </div>
          {job.status === "retrying" && job.next_retry_at && <div className="alert alert-warn">Attempt failed — retrying with backoff {job.next_retry_at ? `(next try ${ago(job.next_retry_at).replace(" ago", "")})` : ""}.</div>}
          {job.dead_lettered && <div className="alert alert-error"><strong>Dead-lettered.</strong> Every attempt failed and the job was moved to the dead-letter queue for inspection. You can retry it manually.</div>}
          {job.last_error && job.status !== "completed" && <div className="alert alert-error mono">{job.last_error}</div>}

          {job.attempt_history.length > 0 && (
            <ol className="attempts">
              {job.attempt_history.map((a) => (
                <li key={a.attempt} className={`attempt a-${a.status}`}>
                  <div className="a-head"><strong>Attempt {a.attempt}</strong><StatusBadge status={a.status} /></div>
                  <div className="muted small">{a.worker_id ?? "worker"} · {ago(a.started_at)}</div>
                  {a.error && <div className="mono small a-error">{a.error}</div>}
                </li>
              ))}
            </ol>
          )}
        </>
      ) : <p className="muted small">No processing job has been run for this video.</p>}

      {canProcess && (
        <div className="process-form">
          <span className="muted small">Output resolutions (never upscaled beyond the source)</span>
          <div className="chips">
            {PRESETS.map((r) => <button key={r} className={`chip ${picked.includes(r) ? "on" : ""}`} onClick={() => toggle(r)}>{r}p</button>)}
          </div>
          <button className="btn btn-primary" disabled={busy || picked.length === 0} onClick={() => run(() => api.process(video.video_id, picked), "Transcoding job queued")}>
            {video.status === "completed" ? "Re-process" : "Start processing"}
          </button>
        </div>
      )}
      {canRetry && job && (
        <button className="btn btn-primary" disabled={busy} onClick={() => run(() => api.retry(job.id), "Retry queued")}>Retry job</button>
      )}
    </div>
  );
}
