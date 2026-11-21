import { Component, computed, inject, input, output, signal } from '@angular/core';
import { DatePipe } from '@angular/common';
import { KafkaMessage } from '../core/models';
import { Icon } from './icon';
import { ToastService } from './toast';
import { SessionService } from '../core/session.service';
import { highlightJson, partitionColor, pretty, relativeTime } from './format';

/** Side panel with the full content of one message. */
@Component({
  selector: 'app-message-detail',
  imports: [DatePipe, Icon],
  host: { role: 'region', 'aria-label': 'Message details', '(keydown.escape)': 'closed.emit()' },
  template: `
    @let m = message();
    <header>
      <div class="id">
        <span class="part"><i [style.background]="color(m.partition)"></i>Partition {{ m.partition }}</span>
        <span class="mono">offset {{ m.offset }}</span>
      </div>
      <button class="btn ghost icon-only" (click)="closed.emit()" aria-label="Close details (Esc)" title="Close (Esc)">
        <app-icon name="x" />
      </button>
    </header>

    <dl class="meta">
      <dt>Timestamp</dt>
      <dd>
        <span class="mono">{{ m.timestamp | date: 'yyyy-MM-dd HH:mm:ss.SSS' }}</span>
        <span class="muted"> · {{ ago() }}</span>
      </dd>
      <dt>Key</dt>
      <dd class="key">
        <span class="mono" [class.null]="m.key === null">{{ m.key ?? 'null' }}</span>
        @if (m.keyEncoding === 'base64') {
          <span class="tag">base64</span>
        }
        @if (m.key !== null) {
          <button class="btn ghost icon-only sm" (click)="toast.copy(m.key ?? '', 'Key copied')" aria-label="Copy key">
            <app-icon name="copy" [size]="13" />
          </button>
        }
      </dd>
      <dt>Size</dt>
      <dd>
        <span class="mono">{{ size() }}</span>
        @if (m.truncated) {
          <span class="pill warn">truncated to 64 KB</span>
        }
      </dd>
    </dl>

    @if (m.headers?.length) {
      <section>
        <h3>Headers <span class="count">{{ m.headers!.length }}</span></h3>
        <div class="headers">
          @for (h of m.headers; track $index) {
            <span class="mono hk">{{ h.key }}</span><span class="mono hv">{{ h.value }}</span>
          }
        </div>
      </section>
    }

    <section class="value">
      <h3>
        Value
        @if (m.valueEncoding === 'base64') {
          <span class="tag">base64</span>
        }
        @if (val().json) {
          <span class="tag">JSON</span>
        }
        <span class="grow"></span>
        @if (val().json) {
          <button class="btn ghost sm" (click)="raw.set(!raw())" [attr.aria-pressed]="raw()">{{ raw() ? 'Pretty' : 'Raw' }}</button>
        }
        <button class="btn sm" (click)="toast.copy(m.value ?? '', 'Value copied')"><app-icon name="copy" [size]="13" />Copy</button>
        <button class="btn sm" (click)="copyAll()" title="Copy the whole message as JSON">Copy all</button>
        @if (!session.readOnly()) {
          <button
            class="btn sm"
            (click)="produceCopy()"
            [disabled]="!copyable()"
            [title]="copyable() ? 'Open the producer with this key, value and headers (e.g. to reprocess it)' : 'Binary or truncated messages cannot be copied as text'"
          >
            <app-icon name="repeat" [size]="13" />Produce copy
          </button>
        }
      </h3>
      @if (raw() || !val().json) {
        <pre class="code">{{ raw() ? m.value : val().text }}</pre>
      } @else {
        <pre class="code" [innerHTML]="highlighted()"></pre>
      }
    </section>
  `,
  styles: `
    :host {
      display: flex;
      flex-direction: column;
      gap: var(--sp-4);
      min-width: 0;
      min-height: 0;
      padding: var(--sp-4);
      overflow: auto;
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      animation: slide var(--t) var(--ease);
    }
    @keyframes slide { from { opacity: 0; transform: translateX(8px); } }
    header { display: flex; align-items: center; justify-content: space-between; }
    .id { display: flex; flex-direction: column; gap: 2px; font-size: var(--fs-sm); }
    .id .mono { color: var(--muted); font-size: var(--fs-xs); }
    .part { display: inline-flex; align-items: center; gap: 6px; font-weight: 600; }
    .part i { width: 9px; height: 9px; border-radius: 2px; }
    .meta { display: grid; grid-template-columns: 84px minmax(0, 1fr); gap: 6px var(--sp-3); margin: 0; font-size: var(--fs-sm); }
    dt { color: var(--muted); }
    dd { margin: 0; display: flex; align-items: center; gap: var(--sp-2); flex-wrap: wrap; min-width: 0; }
    dd .mono { word-break: break-all; }
    .null { color: var(--faint); font-style: italic; }
    h3 { display: flex; align-items: center; gap: var(--sp-2); font-size: var(--fs-xs); text-transform: uppercase; letter-spacing: 0.05em; color: var(--muted); margin-bottom: var(--sp-2); }
    .count { font-family: var(--mono); color: var(--text-2); }
    .grow { flex: 1; }
    .btn.sm { height: 26px; padding: 0 var(--sp-2); font-size: var(--fs-xs); text-transform: none; letter-spacing: 0; }
    .btn.sm.icon-only { width: 26px; padding: 0; }
    .headers { display: grid; grid-template-columns: max-content minmax(0, 1fr); gap: 4px var(--sp-3); font-size: var(--fs-xs); }
    .hk { color: var(--muted); }
    .hv { word-break: break-all; }
    .value { display: flex; flex-direction: column; min-height: 0; }
    .code {
      margin: 0;
      padding: var(--sp-3);
      border-radius: var(--radius-sm);
      background: var(--bg);
      border: 1px solid var(--border);
      font-family: var(--mono);
      font-size: 12.5px;
      line-height: 1.6;
      white-space: pre-wrap;
      word-break: break-word;
      tab-size: 2;
    }
    :host ::ng-deep {
      .j-key { color: var(--syn-key); }
      .j-str { color: var(--syn-str); }
      .j-num { color: var(--syn-num); }
      .j-bool { color: var(--syn-bool); }
      .j-null { color: var(--muted); font-style: italic; }
    }
  `,
})
export class MessageDetail {
  readonly message = input.required<KafkaMessage>();
  readonly closed = output<void>();

  protected readonly toast = inject(ToastService);
  protected readonly session = inject(SessionService);
  /** Only text payloads can be re-sent byte-for-byte from a text form. */
  protected readonly copyable = computed(() => {
    const m = this.message();
    return !m.truncated && m.valueEncoding !== 'base64' && m.keyEncoding !== 'base64';
  });

  protected produceCopy() {
    const m = this.message();
    this.session.openProduce({ key: m.key, value: m.value, headers: m.headers ?? [] });
  }
  protected readonly raw = signal(false);
  protected readonly color = partitionColor;

  protected readonly val = computed(() => pretty(this.message().value));
  protected readonly highlighted = computed(() => highlightJson(this.val().text));
  protected readonly ago = computed(() => relativeTime(this.message().timestamp));
  protected readonly size = computed(() => {
    const b = this.message().valueSize;
    return b < 1024 ? `${b} B` : b < 1048576 ? `${(b / 1024).toFixed(1)} KB` : `${(b / 1048576).toFixed(2)} MB`;
  });

  protected copyAll() {
    this.toast.copy(JSON.stringify(this.message(), null, 2), 'Message copied as JSON');
  }
}
