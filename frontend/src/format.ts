export const ALLOWED_EXTENSIONS = [".mp4", ".m4v", ".mov", ".mkv", ".webm", ".avi"];
export const MAX_UPLOAD_BYTES = 500 * 1024 * 1024;

export function bytes(n: number): string {
  if (n < 1024) return `${n} B`;
  const units = ["KB", "MB", "GB", "TB"];
  let v = n / 1024, i = 0;
  while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
  return `${v.toFixed(v >= 100 ? 0 : 1)} ${units[i]}`;
}

export function duration(sec: number | null | undefined): string {
  if (sec == null) return "—";
  const s = Math.round(sec);
  const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), r = s % 60;
  return h ? `${h}:${String(m).padStart(2, "0")}:${String(r).padStart(2, "0")}` : `${m}:${String(r).padStart(2, "0")}`;
}

export function ago(iso: string | null | undefined): string {
  if (!iso) return "—";
  const t = Date.parse(iso.endsWith("Z") || /[+-]\d\d:?\d\d$/.test(iso) ? iso : iso + "Z");
  const d = Math.max(0, (Date.now() - t) / 1000);
  if (d < 10) return "just now";
  if (d < 60) return `${Math.floor(d)}s ago`;
  if (d < 3600) return `${Math.floor(d / 60)}m ago`;
  if (d < 86400) return `${Math.floor(d / 3600)}h ago`;
  return new Date(t).toLocaleDateString();
}

/** Client-side pre-check; the server re-validates everything (extension, MIME, magic bytes, size). */
export function validateFile(file: File): string | null {
  const ext = file.name.slice(file.name.lastIndexOf(".")).toLowerCase();
  if (!ALLOWED_EXTENSIONS.includes(ext)) return `Unsupported format "${ext || file.name}". Allowed: ${ALLOWED_EXTENSIONS.join(", ")}`;
  if (file.size === 0) return "The file is empty.";
  if (file.size > MAX_UPLOAD_BYTES) return `File is ${bytes(file.size)} — the limit is ${bytes(MAX_UPLOAD_BYTES)}.`;
  return null;
}
