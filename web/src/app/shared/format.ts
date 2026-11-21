import { KafkaMessage } from '../core/models';

/** Single-line preview for list rows. */
export function preview(v: string | null | undefined, max = 240): string {
  if (v === null || v === undefined) return 'null';
  const s = v.replace(/\s+/g, ' ');
  return s.length > max ? s.slice(0, max) + '…' : s;
}

/** Pretty-prints JSON payloads; returns other payloads unchanged. */
export function pretty(v: string | null | undefined): { text: string; json: boolean } {
  if (v === null || v === undefined) return { text: 'null', json: false };
  try {
    return { text: JSON.stringify(JSON.parse(v), null, 2), json: true };
  } catch {
    return { text: v, json: false };
  }
}

function escapeHtml(s: string): string {
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

/**
 * Minimal JSON syntax highlighter. Input is escaped first, output only adds
 * <span class="j-*"> wrappers (allowed by Angular's sanitizer).
 */
export function highlightJson(pretty: string): string {
  return escapeHtml(pretty).replace(
    /("(?:\\u[a-fA-F0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(?:true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+-]?\d+)?)/g,
    (m) => {
      let cls = 'j-num';
      if (m.startsWith('"')) cls = m.trimEnd().endsWith(':') ? 'j-key' : 'j-str';
      else if (m === 'true' || m === 'false') cls = 'j-bool';
      else if (m === 'null') cls = 'j-null';
      return `<span class="${cls}">${m}</span>`;
    },
  );
}

/** Case-insensitive substring match on key, value and headers. */
export function matches(m: KafkaMessage, q: string): boolean {
  if (!q) return true;
  q = q.toLowerCase();
  return (
    (m.key ?? '').toLowerCase().includes(q) ||
    (m.value ?? '').toLowerCase().includes(q) ||
    (m.headers ?? []).some((h) => h.key.toLowerCase().includes(q) || h.value.toLowerCase().includes(q))
  );
}

export const msgId = (m: KafkaMessage) => `${m.partition}:${m.offset}`;

const compactFmt = new Intl.NumberFormat('en', { notation: 'compact', maximumFractionDigits: 1 });
const fullFmt = new Intl.NumberFormat('en');

/** 1234567 -> "1.2M" (use `full()` for the title/tooltip). */
export const compact = (n: number | null | undefined) => (n == null ? '—' : compactFmt.format(n));
export const full = (n: number | null | undefined) => (n == null ? '' : fullFmt.format(n));

export function relativeTime(from: Date | string | null | undefined, now = Date.now()): string {
  if (!from) return 'never';
  const s = Math.max(0, Math.round((now - new Date(from).getTime()) / 1000));
  if (s < 5) return 'just now';
  if (s < 60) return `${s}s ago`;
  const m = Math.round(s / 60);
  if (m < 60) return `${m}m ago`;
  const h = Math.round(m / 60);
  if (h < 24) return `${h}h ago`;
  return `${Math.round(h / 24)}d ago`;
}

/** Stable decorative color per partition (the number is always shown too). */
const PARTITION_HUES = [152, 212, 268, 32, 188, 330, 92, 4];
export const partitionColor = (p: number) => `hsl(${PARTITION_HUES[p % PARTITION_HUES.length]} 70% 62%)`;

export type GroupStateTone = 'ok' | 'warn' | 'danger' | 'info' | '';
export function groupStateTone(state: string): GroupStateTone {
  switch (state) {
    case 'Stable':
      return 'ok';
    case 'PreparingRebalance':
    case 'CompletingRebalance':
    case 'Assigning':
    case 'Reconciling':
      return 'warn';
    case 'Dead':
      return 'danger';
    case 'Empty':
      return 'info';
    default:
      return '';
  }
}

/** True when the keyboard event target is a text input (skip global shortcuts). */
export function isTyping(e: KeyboardEvent): boolean {
  const t = e.target as HTMLElement | null;
  return !!t && (t.isContentEditable || ['INPUT', 'TEXTAREA', 'SELECT'].includes(t.tagName));
}

/** Extracts the API's {"error": "..."} message from an HttpErrorResponse-like value. */
export function errorMessage(e: unknown): string {
  const err = e as { error?: { error?: string }; message?: string; status?: number };
  if (err?.status === 0) return 'Cannot reach the kafpeek backend.';
  return err?.error?.error ?? err?.message ?? String(e);
}
