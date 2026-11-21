import { Component, DestroyRef, computed, effect, inject, input, output, signal, untracked } from '@angular/core';
import { HttpErrorResponse } from '@angular/common/http';
import { Subscription } from 'rxjs';
import { ApiService } from '../../core/api.service';
import { KafkaMessage, Partition, SearchMode, SearchProgress } from '../../core/models';
import { Icon } from '../../shared/icon';
import { MessageDetail } from '../../shared/message-detail';
import { MessageList } from '../../shared/message-list';
import { compact, errorMessage, full, matches } from '../../shared/format';

type Outcome = 'finished' | 'limit' | 'timeout' | 'incomplete' | 'stopped' | null;

const PRESETS = [
  { label: '15 min', ms: 15 * 60_000 },
  { label: '1 h', ms: 3_600_000 },
  { label: '6 h', ms: 6 * 3_600_000 },
  { label: '24 h', ms: 24 * 3_600_000 },
];

/**
 * Messages tab.
 * - No filter + "Newest": one quick REST read of the newest N (exact, instant).
 * - Otherwise: streaming server-side search (SSE) over the newest N, a time
 *   range or the whole topic, with live results, real progress and Stop.
 */
@Component({
  selector: 'app-browse',
  imports: [Icon, MessageList, MessageDetail],
  template: `
    <div class="toolbar">
      <div class="seg" role="radiogroup" aria-label="Range to read">
        @for (m of modes; track m.id) {
          <button type="button" role="radio" [attr.aria-checked]="mode() === m.id" [class.on]="mode() === m.id" (click)="setMode(m.id)">
            {{ m.label }}
          </button>
        }
      </div>

      @switch (mode()) {
        @case ('newest') {
          @if (streaming()) {
            <label class="field" title="How many of the newest messages the server examines">
              <span class="sr-only">Scan depth</span>
              <span class="muted small">Scan</span>
              <select (change)="scan.set(+$any($event.target).value); run()">
                @for (n of scans; track n) {
                  <option [value]="n" [selected]="scan() === n">newest {{ compact(n) }}</option>
                }
              </select>
            </label>
          }
        }
        @case ('range') {
          <label class="field">
            <span class="muted small">From</span>
            <input type="datetime-local" step="1" [value]="from()" (change)="from.set($any($event.target).value)" aria-label="From (local time)" />
          </label>
          <label class="field">
            <span class="muted small">To</span>
            <input type="datetime-local" step="1" [value]="to()" (change)="to.set($any($event.target).value)" aria-label="To (local time, empty = now)" />
          </label>
          <span class="presets" role="group" aria-label="Quick ranges">
            @for (p of presets; track p.label) {
              <button type="button" class="btn ghost xs" (click)="preset(p.ms)">{{ p.label }}</button>
            }
          </span>
        }
        @case ('all') {
          <span class="muted small all-note">
            <app-icon name="alert" [size]="13" /> Reads up to <b class="mono">{{ compact(topicMessages()) }}</b> messages from Kafka
          </span>
        }
      }

      <label class="field">
        <span class="sr-only">Partition</span>
        <app-icon name="columns" [size]="14" />
        <select (change)="partition.set($any($event.target).value); autoRun()">
          <option value="" [selected]="partition() === ''">All partitions</option>
          @for (p of partitions(); track p.id) {
            <option [value]="p.id" [selected]="partition() === '' + p.id">Partition {{ p.id }} · {{ compact(p.endOffset - p.startOffset) }}</option>
          }
        </select>
      </label>

      <label class="field">
        <span class="sr-only">Maximum results</span>
        <app-icon name="list" [size]="14" />
        <select (change)="limit.set(+$any($event.target).value); autoRun()">
          @for (n of limits(); track n) {
            <option [value]="n" [selected]="limit() === n">{{ streaming() ? 'Up to ' + compact(n) + ' results' : 'Newest ' + n }}</option>
          }
        </select>
      </label>

      @if (running()) {
        <button class="btn" (click)="stop()"><app-icon name="pause" [size]="14" /> Stop</button>
      } @else {
        <button class="btn" [class.primary]="mode() !== 'newest'" (click)="run()" [disabled]="!canRun()">
          <app-icon [name]="mode() === 'newest' && !filter() ? 'refresh' : 'search'" [size]="14" />
          {{ mode() === 'newest' && !filter() ? 'Reload' : 'Search' }}
        </button>
      }

      <label class="field grow">
        <app-icon name="search" [size]="15" />
        <span class="sr-only">Search in results</span>
        <input placeholder="Search in results…" [value]="search()" (input)="search.set($any($event.target).value)" />
        @if (search()) {
          <button class="btn ghost icon-only" (click)="search.set('')" aria-label="Clear search"><app-icon name="x" [size]="14" /></button>
        }
      </label>
    </div>

    <!-- Progress / outcome -->
    <div class="status-line" role="status" aria-live="polite">
      @if (running() && streaming()) {
        <div class="progress" [attr.aria-valuenow]="pct()" aria-valuemin="0" aria-valuemax="100" role="progressbar" aria-label="Scan progress">
          <span [style.width.%]="pct()"></span>
        </div>
        <span class="mono">{{ pct() }}%</span>
        <span>
          <b class="mono match">{{ full(progress().matched) }}</b> found · <b class="mono">{{ full(progress().position) }}</b> of
          <b class="mono">{{ full(total()) }}</b> scanned · {{ seconds() }}s
          @if (rate()) {
            · <span class="mono">{{ compact(rate()) }}</span> msg/s
          }
        </span>
      } @else if (running()) {
        <span class="spinner sm" aria-hidden="true"></span><span class="muted">Loading…</span>
      } @else if (outcome() && streaming()) {
        <span>
          <b class="mono match">{{ full(progress().matched) }}</b> {{ progress().matched === 1 ? 'match' : 'matches' }} in
          <b class="mono">{{ full(progress().scanned) }}</b> messages
          <span class="muted"> · {{ elapsedLabel() }}</span>
        </span>
        @switch (outcome()) {
          @case ('limit') {
            <span class="note warn">Stopped at the limit of {{ full(limit()) }} — refine the filter or raise the limit to see more.</span>
          }
          @case ('stopped') {
            <span class="note">Stopped by you at {{ pct() }}% — showing what was found so far.</span>
          }
          @case ('timeout') {
            <span class="note warn">Stopped after the 15-minute safety limit — narrow the range or pick a partition.</span>
          }
          @case ('incomplete') {
            <span class="note warn">Incomplete: the broker stopped sending data with {{ full(unread()) }} offsets still unread ({{ pct() }}% covered) — a missing match is not conclusive; run the search again.</span>
          }
        }
        @if (search()) {
          <span class="muted"> · {{ full(filtered().length) }} shown by search</span>
        }
      } @else if (outcome()) {
        <span class="muted">Newest <b class="mono">{{ full(messages().length) }}</b> messages · {{ elapsed() }} ms</span>
        @if (search()) {
          <span class="muted"> · {{ full(filtered().length) }} shown by search</span>
        }
      } @else if (mode() !== 'newest') {
        <span class="muted">Choose the range and press <b>Search</b>.</span>
      }
    </div>

    @if (error()) {
      <div class="alert danger" role="alert">
        <app-icon name="alert-circle" />
        <div class="body"><strong>Could not read messages</strong>{{ error() }}</div>
        <button class="btn" (click)="run()">Retry</button>
      </div>
    }

    <div class="split" [class.with-detail]="selected()">
      <div class="list-wrap" [attr.aria-busy]="running()">
        <app-message-list
          [messages]="filtered()"
          [(selected)]="selected"
          label="Messages, newest first"
          timeFormat="MM-dd HH:mm:ss.SSS"
          [timeWidth]="150"
          [emptyIcon]="running() ? 'search' : 'inbox'"
          [emptyTitle]="emptyTitle()"
          [emptyText]="emptyText()"
        />
        @if (running() && !streaming()) {
          <div class="progress-bar" aria-hidden="true"></div>
        }
      </div>
      @if (selected(); as m) {
        <app-message-detail [message]="m" (closed)="selected.set(null)" />
      }
    </div>
    <p class="hint-keys">
      Reads directly — no consumer group, no offset commits. Results stream in while the server scans.
      <kbd>↑</kbd><kbd>↓</kbd> navigate · <kbd>Esc</kbd> close details
    </p>
  `,
  styleUrl: './messages.scss',
  styles: `
    :host { --split-offset: 470px; }
    .seg { display: inline-flex; height: 32px; border: 1px solid var(--border-strong); border-radius: var(--radius-sm); overflow: hidden; }
    .seg button { all: unset; cursor: pointer; padding: 0 12px; font-size: var(--fs-sm); color: var(--muted); display: inline-flex; align-items: center; }
    .seg button + button { border-left: 1px solid var(--border-strong); }
    .seg button.on { background: var(--accent-soft); color: var(--accent); font-weight: 500; }
    .seg button:focus-visible { outline: 2px solid var(--focus); outline-offset: -2px; }
    .small { font-size: var(--fs-xs); }
    .field input[type='datetime-local'] { color-scheme: dark; font-family: var(--mono); font-size: var(--fs-xs); }
    .presets { display: inline-flex; gap: 2px; }
    .btn.xs { height: 26px; padding: 0 8px; font-size: var(--fs-xs); }
    .all-note { display: inline-flex; align-items: center; gap: 5px; color: var(--warn); b { color: var(--text); } }
    .status-line {
      display: flex; align-items: center; flex-wrap: wrap; gap: var(--sp-2);
      min-height: 22px; margin: calc(-1 * var(--sp-1)) 0 var(--sp-3);
      font-size: var(--fs-xs); color: var(--text-2);
      b { font-weight: 600; }
    }
    .progress { width: 180px; height: 6px; border-radius: 3px; background: var(--surface-3); overflow: hidden; display: inline-flex; }
    .progress span { background: var(--accent); border-radius: 3px; transition: width var(--t) linear; }
    .match { color: var(--accent); }
    .note { color: var(--muted); }
    .note.warn { color: var(--warn); }
  `,
})
export class Browse {
  readonly topic = input.required<string>();
  readonly partitions = input<Partition[]>([]);
  /** Server-side CEL filter ('' = none). */
  readonly filter = input('');
  /** Emits while a read/search is in flight (drives the spinner in the filter bar). */
  readonly busy = output<boolean>();

  private readonly api = inject(ApiService);
  private req?: Subscription;
  private timer?: ReturnType<typeof setInterval>;
  private startedAt = 0;

  protected readonly modes: { id: SearchMode; label: string }[] = [
    { id: 'newest', label: 'Newest' },
    { id: 'range', label: 'Time range' },
    { id: 'all', label: 'Entire topic' },
  ];
  protected readonly presets = PRESETS;
  protected readonly scans = [1_000, 10_000, 50_000, 100_000];
  protected readonly full = full;
  protected readonly compact = compact;

  protected readonly mode = signal<SearchMode>('newest');
  protected readonly scan = signal(10_000);
  protected readonly from = signal(''); // datetime-local value (local time)
  protected readonly to = signal('');
  protected readonly partition = signal('');
  protected readonly limit = signal(100);

  protected readonly messages = signal<KafkaMessage[]>([]);
  protected readonly running = signal(false);
  protected readonly error = signal('');
  protected readonly outcome = signal<Outcome>(null);
  protected readonly progress = signal<SearchProgress>({ scanned: 0, position: 0, total: 0, matched: 0 });
  protected readonly total = signal(0);
  protected readonly elapsed = signal(0);
  protected readonly seconds = signal(0);
  protected readonly search = signal('');
  protected readonly selected = signal<KafkaMessage | null>(null);

  /** Streaming search is used for filters and for any range other than a plain "newest N". */
  protected readonly streaming = computed(() => !!this.filter() || this.mode() !== 'newest');
  protected readonly limits = computed(() => (this.streaming() ? [100, 1000, 5000, 10000] : [50, 100, 500, 1000]));
  protected readonly topicMessages = computed(() => this.partitions().reduce((n, p) => n + (p.endOffset - p.startOffset), 0));
  protected readonly pct = computed(() => {
    const t = this.total();
    return t > 0 ? Math.min(100, Math.floor((this.progress().position / t) * 100)) : 0;
  });
  protected readonly unread = computed(() => Math.max(0, this.total() - this.progress().position));
  protected readonly rate = computed(() => {
    const s = this.seconds();
    return s >= 1 ? Math.round(this.progress().scanned / s) : 0;
  });
  protected readonly elapsedLabel = computed(() => {
    const ms = this.elapsed();
    return ms < 1000 ? `${ms} ms` : `${(ms / 1000).toFixed(1)}s`;
  });
  protected readonly canRun = computed(() => this.mode() !== 'range' || !!this.from() || !!this.to());

  protected readonly filtered = computed(() => this.messages().filter((m) => matches(m, this.search())));
  protected readonly emptyTitle = computed(() => {
    if (this.running()) return this.streaming() ? 'Searching… results appear here as they are found' : 'Reading messages…';
    if (this.search() && this.messages().length) return 'Nothing matches your search';
    if (!this.outcome()) return this.mode() === 'newest' ? 'No messages' : 'Ready to search';
    return this.filter() ? 'No matches' : 'No messages in this range';
  });
  protected readonly emptyText = computed(() => {
    if (this.running() || this.search()) return '';
    if (!this.outcome() && this.mode() === 'all') return `Press Search to scan the whole topic (≈ ${compact(this.topicMessages())} messages).`;
    if (!this.outcome() && this.mode() === 'range') return 'Pick a start and/or end time, or use a quick range.';
    if (this.filter()) return 'Nothing in the scanned range matches the filter. Try a wider range or check the expression on a sample message.';
    return this.mode() === 'newest' ? 'This topic (or partition) has no messages yet.' : '';
  });

  constructor() {
    // Topic or filter changed: run again (except "entire topic", which is always explicit).
    effect(() => {
      this.topic();
      this.filter();
      untracked(() => this.autoRun());
    });
    effect(() => this.busy.emit(this.running()));
    inject(DestroyRef).onDestroy(() => this.cancel());
  }

  protected setMode(m: SearchMode) {
    if (m === this.mode()) return;
    this.cancel();
    this.mode.set(m);
    this.limit.set(m === 'newest' && !this.filter() ? 100 : 1000);
    this.messages.set([]);
    this.outcome.set(null);
    this.error.set('');
    if (m === 'range' && !this.from() && !this.to()) this.preset(3_600_000, false);
    if (m === 'newest') this.run();
  }

  protected preset(ms: number, runNow = true) {
    this.from.set(toLocalInput(new Date(Date.now() - ms)));
    this.to.set('');
    if (runNow) this.run();
  }

  /** Re-run automatically on option changes, except for whole-topic scans that were never started. */
  protected autoRun() {
    if (this.mode() === 'all' && !this.outcome() && !this.running()) return;
    if (this.mode() === 'range' && !this.canRun()) return;
    this.run();
  }

  protected run() {
    this.cancel();
    this.error.set('');
    this.outcome.set(null);
    this.messages.set([]);
    this.selected.set(null);
    this.progress.set({ scanned: 0, position: 0, total: 0, matched: 0 });
    this.total.set(0);
    this.running.set(true);
    this.startedAt = performance.now();
    this.seconds.set(0);
    this.timer = setInterval(() => this.seconds.set(Math.floor((performance.now() - this.startedAt) / 1000)), 250);
    const p = this.partition();
    const partition = p === '' ? null : +p;

    if (!this.streaming()) {
      this.req = this.api.browse(this.topic(), { limit: this.limit(), partition }).subscribe({
        next: (res) => {
          this.messages.set(res.messages);
          this.finish('finished');
        },
        error: (e: HttpErrorResponse) => {
          this.error.set(errorMessage(e));
          this.finish(null);
        },
      });
      return;
    }

    this.req = this.api
      .search(this.topic(), {
        mode: this.mode(),
        filter: this.filter(),
        partition,
        limit: this.limit(),
        scan: this.scan(),
        from: this.from() ? new Date(this.from()).toISOString() : undefined,
        to: this.to() ? new Date(this.to()).toISOString() : undefined,
      })
      .subscribe((ev) => {
        switch (ev.type) {
          case 'start':
            this.total.set(ev.total);
            break;
          case 'batch':
            this.progress.set(ev.progress);
            if (ev.messages.length) {
              // Keep newest-first; batches are small and arrive every ~250ms.
              this.messages.update((cur) => sortNewest(cur.concat(ev.messages)));
            }
            break;
          case 'done':
            this.progress.set(ev.progress);
            this.finish(ev.reason);
            this.elapsed.set(ev.elapsedMs);
            break;
          case 'failure':
            this.error.set(ev.error);
            this.finish(null);
            break;
        }
      });
  }

  /** User pressed Stop: closing the stream cancels the scan on the server; results so far are kept. */
  protected stop() {
    if (!this.running()) return;
    this.cancel();
    this.finish(this.streaming() ? 'stopped' : null);
  }

  private finish(o: Outcome) {
    this.elapsed.set(Math.round(performance.now() - this.startedAt));
    this.outcome.set(o);
    this.running.set(false);
    if (this.timer) clearInterval(this.timer);
    this.timer = undefined;
  }

  private cancel() {
    this.req?.unsubscribe();
    this.req = undefined;
    if (this.running()) this.running.set(false);
    if (this.timer) clearInterval(this.timer);
    this.timer = undefined;
  }
}

function sortNewest(list: KafkaMessage[]): KafkaMessage[] {
  return list.sort((a, b) => {
    const d = Date.parse(b.timestamp) - Date.parse(a.timestamp);
    return d !== 0 ? d : b.offset - a.offset;
  });
}

/** Date -> value for <input type="datetime-local"> in the user's local time zone. */
function toLocalInput(d: Date): string {
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}
