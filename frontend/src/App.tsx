import { useState } from "react";
import { UploadPanel } from "./components/UploadPanel";
import { VideoDetail } from "./components/VideoDetail";
import { VideoList } from "./components/VideoList";
import { api } from "./api";
import { usePolling } from "./hooks";

export default function App() {
  const [selected, setSelected] = useState<string | null>(null);
  const [tick, setTick] = useState(0);
  const bump = () => setTick((t) => t + 1);
  const stats = usePolling(async () => {
    const [all, done, running, failed] = await Promise.all([
      api.listVideos({ limit: 1 }), api.listVideos({ status: "completed", limit: 1 }),
      api.listVideos({ status: "processing", limit: 1 }), api.listVideos({ status: "failed", limit: 1 }),
    ]);
    return { all: all.total, done: done.total, running: running.total, failed: failed.total };
  }, 5000, [tick]);

  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">
          <span className="logo" aria-hidden><svg width="18" height="18" viewBox="0 0 24 24"><path d="M8 5l12 7-12 7z" fill="currentColor" /></svg></span>
          <div><strong>Aurum</strong><small>Video processing platform</small></div>
        </div>
        <div className="stats">
          <Stat label="Videos" value={stats.data?.all} />
          <Stat label="Processing" value={stats.data?.running} accent />
          <Stat label="Completed" value={stats.data?.done} />
          <Stat label="Failed" value={stats.data?.failed} />
        </div>
      </header>

      <main className="layout">
        <div className="col-left">
          <UploadPanel onUploaded={(v) => { setSelected(v.id); bump(); }} />
          <VideoList selected={selected} onSelect={setSelected} refreshKey={tick} />
        </div>
        <div className="col-right">
          {selected ? (
            <VideoDetail key={selected} videoId={selected} onChanged={bump} />
          ) : (
            <section className="card hero-empty">
              <div className="hero-mark">▶</div>
              <h2>Select a video</h2>
              <p className="muted">Pick a video from the library to see its processing status, renditions, thumbnails and playback — or upload a new one.</p>
            </section>
          )}
        </div>
      </main>
    </div>
  );
}

function Stat({ label, value, accent }: { label: string; value?: number; accent?: boolean }) {
  return <div className={`stat ${accent ? "accent" : ""}`}><strong>{value ?? "–"}</strong><span>{label}</span></div>;
}
