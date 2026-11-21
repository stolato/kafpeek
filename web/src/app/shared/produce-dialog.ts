import { Component, ElementRef, computed, effect, inject, input, signal, untracked, viewChild } from '@angular/core';
import { DatePipe } from '@angular/common';
import { HttpErrorResponse } from '@angular/common/http';
import { ApiService } from '../core/api.service';
import { Header, ProduceResult } from '../core/models';
import { ProducePrefill, SessionService } from '../core/session.service';
import { Icon } from './icon';
import { ToastService } from './toast';
import { errorMessage, partitionColor } from './format';

type Format = 'json' | 'text';
const MAX_BYTES = 1024 * 1024;

/**
 * Side drawer to produce a message to the current topic.
 * Opened via SessionService.openProduce() (header button or "Produce copy").
 */
@Component({
  selector: 'app-produce-dialog',
  imports: [Icon, DatePipe],
  template: `
    <dialog #dlg (close)="open.set(false)" (click)="onBackdrop($event)" aria-labelledby="produce-title" (keydown)="onKey($event)">
      <form class="drawer" (submit)="$event.preventDefault(); send()">
        <header>
          <div>
            <h2 id="produce-title">Produce message</h2>
            <div class="target mono">
              <span class="cluster" [class.prod]="session.isProd()">{{ session.cluster() }}</span>
              <app-icon name="chevron-right" [size]="12" />
              <span>{{ topic() }}</span>
            </div>
          </div>
          <button type="button" class="btn ghost icon-only" (click)="close()" aria-label="Close"><app-icon name="x" /></button>
        </header>

        <div class="body">
          @if (session.isProd()) {
            <div class="alert warn">
              <app-icon name="alert" />
              <div class="body-text">
                <strong>Production cluster</strong>
                Messages are consumed by real services as soon as they are sent.
                <label class="check confirm">
                  <input type="checkbox" [checked]="prodConfirmed()" (change)="prodConfirmed.set($any($event.target).checked)" />
                  I understand this writes to {{ session.cluster() }}
                </label>
              </div>
            </div>
          }

          <div class="row2">
            <label class="lbl">
              Partition
              <span class="field">
                <select (change)="partition.set($any($event.target).value)">
                  <option value="" [selected]="partition() === ''">Auto — by key hash (same as Java client)</option>
                  @for (p of partitionIds(); track p) {
                    <option [value]="p" [selected]="partition() === '' + p">Partition {{ p }}</option>
                  }
                </select>
              </span>
            </label>
          </div>

          <div class="lbl">
            <div class="lbl-row">
              <label for="produce-key">Key</label>
              <label class="check small"><input type="checkbox" [checked]="keyNull()" (change)="keyNull.set($any($event.target).checked)" />null</label>
            </div>
            <input
              id="produce-key"
              class="input mono"
              [disabled]="keyNull()"
              [placeholder]="keyNull() ? 'null key' : 'e.g. order-123'"
              [value]="key()"
              (input)="key.set($any($event.target).value)"
              autocomplete="off"
              spellcheck="false"
            />
          </div>

          <div class="lbl grow">
            <div class="lbl-row">
              <label for="produce-value">Value</label>
              <div class="seg" role="radiogroup" aria-label="Value format">
                <button type="button" role="radio" [attr.aria-checked]="format() === 'json'" [class.on]="format() === 'json'" (click)="format.set('json')">JSON</button>
                <button type="button" role="radio" [attr.aria-checked]="format() === 'text'" [class.on]="format() === 'text'" (click)="format.set('text')">Text</button>
              </div>
              <span class="spacer"></span>
              @if (format() === 'json' && !valueNull()) {
                <button type="button" class="btn ghost xs" (click)="prettify()" [disabled]="!!jsonError()">Format</button>
                <button type="button" class="btn ghost xs" (click)="minify()" [disabled]="!!jsonError()">Minify</button>
              }
              <label class="check small" title="A null value is a tombstone: on compacted topics it deletes the key">
                <input type="checkbox" [checked]="valueNull()" (change)="valueNull.set($any($event.target).checked)" />null (tombstone)
              </label>
            </div>
            <textarea
              #valueEl
              id="produce-value"
              class="input mono"
              [class.invalid]="!!jsonError()"
              [disabled]="valueNull()"
              [placeholder]="valueNull() ? 'null value (tombstone)' : format() === 'json' ? '{&quot;id&quot;: &quot;123&quot;, &quot;status&quot;: &quot;CREATED&quot;}' : 'plain text'"
              [value]="value()"
              (input)="value.set($any($event.target).value)"
              spellcheck="false"
              [attr.aria-invalid]="!!jsonError()"
              aria-describedby="value-hint"
            ></textarea>
            <div class="hint" id="value-hint">
              @if (jsonError(); as e) {
                <span class="bad"><app-icon name="alert-circle" [size]="13" /> Invalid JSON: {{ e }}</span>
              } @else if (format() === 'json' && value().trim() && !valueNull()) {
                <span class="good"><app-icon name="check" [size]="13" /> Valid JSON</span>
              }
              <span class="spacer"></span>
              <span class="mono" [class.bad]="bytes() > maxBytes">{{ sizeLabel() }}</span>
            </div>
          </div>

          <div class="lbl">
            <div class="lbl-row">
              <span>Headers</span>
              <span class="muted small">{{ headers().length || 'none' }}</span>
              <span class="spacer"></span>
              <button type="button" class="btn ghost xs" (click)="addHeader()"><app-icon name="plus" [size]="13" /> Add header</button>
            </div>
            @for (h of headers(); track $index; let i = $index) {
              <div class="header-row">
                <input class="input mono" placeholder="name" [value]="h.key" (input)="setHeader(i, 'key', $any($event.target).value)" [attr.aria-label]="'Header ' + (i + 1) + ' name'" />
                <input class="input mono" placeholder="value" [value]="h.value" (input)="setHeader(i, 'value', $any($event.target).value)" [attr.aria-label]="'Header ' + (i + 1) + ' value'" />
                <button type="button" class="btn ghost icon-only" (click)="removeHeader(i)" [attr.aria-label]="'Remove header ' + (i + 1)"><app-icon name="trash" [size]="14" /></button>
              </div>
            }
          </div>

          @if (error()) {
            <div class="alert danger" role="alert">
              <app-icon name="alert-circle" />
              <div class="body-text"><strong>Not sent</strong>{{ error() }}</div>
            </div>
          }

          @if (sent().length) {
            <div class="sent">
              <h3>Sent in this session</h3>
              <ul>
                @for (s of sent(); track s.offset + ':' + s.partition) {
                  <li>
                    <app-icon name="check" [size]="13" />
                    <span class="part"><i [style.background]="color(s.partition)"></i>p{{ s.partition }}</span>
                    <span class="mono">offset {{ s.offset }}</span>
                    <span class="muted">{{ s.timestamp | date: 'HH:mm:ss' }}</span>
                  </li>
                }
              </ul>
            </div>
          }
        </div>

        <footer>
          <span class="muted small">{{ mod }}↵ to send · form stays filled to send again</span>
          <span class="spacer"></span>
          <button type="button" class="btn" (click)="close()">Close</button>
          <button
            type="submit"
            class="btn primary"
            [disabled]="!canSend()"
            [title]="session.isProd() && !prodConfirmed() ? 'Confirm the production warning above first' : ''"
          >
            @if (sending()) {
              <span class="spinner sm on-accent" aria-hidden="true"></span> Sending…
            } @else {
              <app-icon name="send" [size]="14" /> Produce
            }
          </button>
        </footer>
      </form>
    </dialog>
  `,
  styles: `
    dialog {
      margin: 0 0 0 auto;
      height: 100vh;
      max-height: 100vh;
      width: min(560px, 100vw);
      padding: 0;
      border: none;
      background: transparent;
      color: var(--text);
    }
    dialog:focus, dialog:focus-visible { outline: none; }
    dialog::backdrop { background: rgba(2, 6, 23, 0.55); }
    .drawer {
      display: flex;
      flex-direction: column;
      height: 100%;
      background: var(--surface);
      border-left: 1px solid var(--border-strong);
      box-shadow: var(--shadow-pop);
      animation: slide-in var(--t) var(--ease);
    }
    @keyframes slide-in { from { transform: translateX(24px); opacity: 0; } }
    header { display: flex; justify-content: space-between; align-items: flex-start; padding: var(--sp-4) var(--sp-5); border-bottom: 1px solid var(--border); }
    h2 { font-size: var(--fs-lg); }
    .target { display: flex; align-items: center; gap: 4px; margin-top: 2px; font-size: var(--fs-sm); color: var(--text-2); }
    .cluster { color: var(--muted); }
    .cluster.prod { color: var(--warn); font-weight: 600; }
    .body { flex: 1; overflow: auto; padding: var(--sp-4) var(--sp-5); display: flex; flex-direction: column; gap: var(--sp-4); }
    .lbl { display: flex; flex-direction: column; gap: 6px; font-size: var(--fs-sm); font-weight: 500; color: var(--text-2); }
    .lbl.grow { flex: 1; min-height: 220px; }
    .lbl-row { display: flex; align-items: center; gap: var(--sp-2); }
    .spacer { flex: 1; }
    .small { font-size: var(--fs-xs); font-weight: 400; }
    .check.small { font-size: var(--fs-xs); color: var(--muted); }
    .field select { width: 100%; }
    .input {
      width: 100%;
      background: var(--bg);
      border: 1px solid var(--border-strong);
      border-radius: var(--radius-sm);
      color: var(--text);
      padding: 7px 10px;
      font-size: var(--fs-sm);
      outline: none;
      transition: border-color var(--t-fast) var(--ease);
      &:focus { border-color: var(--focus); }
      &:disabled { opacity: 0.5; }
      &.invalid { border-color: var(--danger); }
    }
    textarea.input { flex: 1; min-height: 200px; resize: vertical; line-height: 1.55; tab-size: 2; }
    .hint { display: flex; align-items: center; gap: var(--sp-2); font-size: var(--fs-xs); font-weight: 400; color: var(--muted); min-height: 16px; }
    .hint span { display: inline-flex; align-items: center; gap: 4px; }
    .good { color: var(--accent); }
    .bad { color: var(--danger); }
    .seg { display: inline-flex; border: 1px solid var(--border-strong); border-radius: var(--radius-sm); overflow: hidden; }
    .seg button { all: unset; cursor: pointer; padding: 2px 10px; font-size: var(--fs-xs); color: var(--muted); }
    .seg button.on { background: var(--accent-soft); color: var(--accent); }
    .seg button:focus-visible { outline: 2px solid var(--focus); outline-offset: -2px; }
    .btn.xs { height: 24px; padding: 0 8px; font-size: var(--fs-xs); }
    .header-row { display: grid; grid-template-columns: 1fr 1.6fr 32px; gap: var(--sp-2); }
    .alert { margin: 0; }
    .body-text { display: flex; flex-direction: column; gap: 2px; font-size: var(--fs-sm); }
    .confirm { margin-top: var(--sp-2); font-weight: 600; color: var(--text); }
    .sent h3 { font-size: var(--fs-xs); text-transform: uppercase; letter-spacing: 0.05em; color: var(--muted); margin-bottom: var(--sp-2); }
    .sent ul { list-style: none; margin: 0; padding: 0; display: flex; flex-direction: column; gap: 4px; font-size: var(--fs-xs); }
    .sent li { display: flex; align-items: center; gap: var(--sp-3); app-icon { color: var(--accent); } }
    .part { display: inline-flex; align-items: center; gap: 5px; font-family: var(--mono); }
    .part i { width: 8px; height: 8px; border-radius: 2px; }
    footer { display: flex; align-items: center; gap: var(--sp-2); padding: var(--sp-3) var(--sp-5); border-top: 1px solid var(--border); }
    .spinner.on-accent { border-color: rgba(5, 46, 22, 0.25); border-top-color: var(--on-accent); }
  `,
})
export class ProduceDialog {
  readonly topic = input.required<string>();
  readonly partitions = input(0);

  protected readonly session = inject(SessionService);
  private readonly api = inject(ApiService);
  private readonly toast = inject(ToastService);
  private readonly dlg = viewChild.required<ElementRef<HTMLDialogElement>>('dlg');
  private readonly valueEl = viewChild.required<ElementRef<HTMLTextAreaElement>>('valueEl');

  protected readonly maxBytes = MAX_BYTES;
  protected readonly color = partitionColor;
  protected readonly mod = /Mac|iPhone|iPad/.test(navigator.platform) ? '⌘' : 'Ctrl+';

  protected readonly open = signal(false);
  protected readonly partition = signal('');
  protected readonly key = signal('');
  protected readonly keyNull = signal(false);
  protected readonly value = signal('');
  protected readonly valueNull = signal(false);
  protected readonly format = signal<Format>('json');
  protected readonly headers = signal<Header[]>([]);
  protected readonly sending = signal(false);
  protected readonly error = signal('');
  protected readonly sent = signal<ProduceResult[]>([]);
  protected readonly prodConfirmed = signal(false);

  protected readonly partitionIds = computed(() => Array.from({ length: this.partitions() }, (_, i) => i));

  protected readonly jsonError = computed(() => {
    if (this.format() !== 'json' || this.valueNull() || !this.value().trim()) return '';
    try {
      JSON.parse(this.value());
      return '';
    } catch (e) {
      return (e as Error).message.replace(/^JSON\.parse: /, '');
    }
  });

  protected readonly bytes = computed(() => {
    const enc = new TextEncoder();
    let n = this.keyNull() ? 0 : enc.encode(this.key()).length;
    if (!this.valueNull()) n += enc.encode(this.value()).length;
    for (const h of this.headers()) n += enc.encode(h.key + h.value).length;
    return n;
  });

  protected readonly sizeLabel = computed(() => {
    const b = this.bytes();
    return b < 1024 ? `${b} B` : `${(b / 1024).toFixed(1)} KB`;
  });

  protected readonly canSend = computed(
    () =>
      !this.sending() &&
      !this.jsonError() &&
      this.bytes() <= MAX_BYTES &&
      this.headers().every((h) => h.key.trim() !== '' || h.value === '') &&
      (!this.session.isProd() || this.prodConfirmed()),
  );

  constructor() {
    effect(() => {
      const req = this.session.produceRequest();
      if (req) untracked(() => this.show(req.prefill));
    });
  }

  private show(prefill: ProducePrefill | null) {
    this.error.set('');
    this.prodConfirmed.set(false); // ask again every time the dialog opens
    if (prefill) {
      this.keyNull.set(prefill.key === null);
      this.key.set(prefill.key ?? '');
      this.valueNull.set(prefill.value === null);
      const v = prefill.value ?? '';
      let isJson = false;
      try {
        JSON.parse(v);
        isJson = v.trim() !== '';
      } catch {
        /* not JSON */
      }
      this.format.set(isJson ? 'json' : 'text');
      this.value.set(isJson ? JSON.stringify(JSON.parse(v), null, 2) : v);
      this.headers.set(prefill.headers.map((h) => ({ ...h })));
      this.partition.set('');
    }
    this.open.set(true);
    const d = this.dlg().nativeElement;
    if (!d.open) d.showModal();
    queueMicrotask(() => this.valueEl().nativeElement.focus());
  }

  close() {
    this.dlg().nativeElement.close();
  }

  protected send() {
    if (!this.canSend()) return;
    this.sending.set(true);
    this.error.set('');
    const p = this.partition();
    this.api
      .produce(this.topic(), {
        key: this.keyNull() ? null : this.key(),
        value: this.valueNull() ? null : this.value(),
        headers: this.headers().filter((h) => h.key.trim() !== ''),
        ...(p === '' ? {} : { partition: +p }),
      })
      .subscribe({
        next: (res) => {
          this.sending.set(false);
          this.sent.update((s) => [res, ...s].slice(0, 20));
          this.toast.show(`Sent to partition ${res.partition} at offset ${res.offset}`);
        },
        error: (e: HttpErrorResponse) => {
          this.sending.set(false);
          this.error.set(errorMessage(e));
        },
      });
  }

  protected prettify() {
    this.value.set(JSON.stringify(JSON.parse(this.value()), null, 2));
  }

  protected minify() {
    this.value.set(JSON.stringify(JSON.parse(this.value())));
  }

  protected addHeader() {
    this.headers.update((h) => [...h, { key: '', value: '' }]);
  }

  protected removeHeader(i: number) {
    this.headers.update((h) => h.filter((_, j) => j !== i));
  }

  protected setHeader(i: number, field: 'key' | 'value', v: string) {
    this.headers.update((h) => h.map((x, j) => (j === i ? { ...x, [field]: v } : x)));
  }

  protected onKey(e: KeyboardEvent) {
    if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) {
      e.preventDefault();
      this.send();
    }
  }

  protected onBackdrop(e: MouseEvent) {
    if (e.target === this.dlg().nativeElement) this.close();
  }
}
