import { useRef, useState, type DragEvent } from "react";
import { api, ApiError } from "../api";
import { ALLOWED_EXTENSIONS, bytes, validateFile } from "../format";
import { useToast } from "../toast";
import type { UploadedVideo } from "../types";
import { ProgressBar } from "./StatusBadge";

export function UploadPanel({ onUploaded }: { onUploaded: (v: UploadedVideo) => void }) {
  const toast = useToast();
  const input = useRef<HTMLInputElement>(null);
  const abortRef = useRef<(() => void) | null>(null);
  const [file, setFile] = useState<File | null>(null);
  const [title, setTitle] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [progress, setProgress] = useState<number | null>(null);
  const [drag, setDrag] = useState(false);

  const choose = (f: File | undefined) => {
    if (!f) return;
    const problem = validateFile(f);
    setError(problem);
    if (problem) { setFile(null); return; }
    setFile(f);
    if (!title) setTitle(f.name.replace(/\.[^.]+$/, ""));
  };

  const onDrop = (e: DragEvent) => {
    e.preventDefault();
    setDrag(false);
    choose(e.dataTransfer.files?.[0]);
  };

  const submit = async () => {
    if (!file) return;
    setError(null);
    setProgress(0);
    const { promise, abort } = api.upload(file, title, setProgress);
    abortRef.current = abort;
    try {
      const video = await promise;
      toast("success", `“${video.title}” uploaded — analysing metadata & thumbnails`);
      onUploaded(video);
      setFile(null);
      setTitle("");
      if (input.current) input.current.value = "";
    } catch (e) {
      const err = e as ApiError;
      if (err.code !== "aborted") {
        setError(err.message);
        toast("error", err.message);
      }
    } finally {
      setProgress(null);
      abortRef.current = null;
    }
  };

  const uploading = progress !== null;

  return (
    <section className="card upload">
      <h2 className="card-title">Upload a video</h2>
      <div
        className={`dropzone ${drag ? "drag" : ""} ${file ? "has-file" : ""}`}
        onDragOver={(e) => { e.preventDefault(); setDrag(true); }}
        onDragLeave={() => setDrag(false)}
        onDrop={onDrop}
        onClick={() => !uploading && input.current?.click()}
        role="button"
        tabIndex={0}
        onKeyDown={(e) => (e.key === "Enter" || e.key === " ") && !uploading && input.current?.click()}
      >
        <input ref={input} type="file" hidden accept={ALLOWED_EXTENSIONS.join(",")} onChange={(e) => choose(e.target.files?.[0])} />
        <div className="dz-icon" aria-hidden>
          <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"><path d="M12 16V4m0 0l-4 4m4-4l4 4" /><path d="M4 16v3a1 1 0 001 1h14a1 1 0 001-1v-3" /></svg>
        </div>
        {file ? (
          <div><strong>{file.name}</strong><span className="muted"> · {bytes(file.size)}</span></div>
        ) : (
          <div><strong>Drop a video here</strong><span className="muted"> or click to browse</span></div>
        )}
        <small className="muted">{ALLOWED_EXTENSIONS.join("  ")} · up to 500 MB</small>
      </div>

      <label className="field">
        <span>Title</span>
        <input value={title} maxLength={200} placeholder="Give it a name" disabled={uploading} onChange={(e) => setTitle(e.target.value)} />
      </label>

      {error && <div className="alert alert-error" role="alert">{error}</div>}
      {uploading && (
        <div className="upload-progress">
          <ProgressBar value={progress ?? 0} status="running" />
          <span className="muted">{progress}% {progress === 100 ? "· finalising…" : ""}</span>
        </div>
      )}

      <div className="row">
        <button className="btn btn-primary" disabled={!file || uploading} onClick={submit}>
          {uploading ? "Uploading…" : "Upload video"}
        </button>
        {uploading && <button className="btn btn-ghost" onClick={() => abortRef.current?.()}>Cancel</button>}
      </div>
    </section>
  );
}
