const LABELS: Record<string, string> = {
  uploaded: "Uploaded", queued: "Queued", processing: "Processing", completed: "Completed",
  failed: "Failed", invalid: "Invalid", running: "Running", retrying: "Retrying", succeeded: "Succeeded",
};

export function StatusBadge({ status }: { status: string }) {
  return (
    <span className={`badge badge-${status}`}>
      <i className="dot" />
      {LABELS[status] ?? status}
    </span>
  );
}

export function ProgressBar({ value, status }: { value: number; status?: string }) {
  const active = status === "processing" || status === "running" || status === "queued" || status === "retrying";
  return (
    <div className="progress" role="progressbar" aria-valuenow={value} aria-valuemin={0} aria-valuemax={100}>
      <div className={`progress-fill ${active ? "animated" : ""} ${status === "failed" ? "failed" : ""}`} style={{ width: `${Math.max(value, active ? 4 : 0)}%` }} />
    </div>
  );
}
