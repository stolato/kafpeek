import { Component, DestroyRef, computed, effect, inject, input, output, signal, untracked, viewChild } from '@angular/core';
import { Subscription } from 'rxjs';
import { ApiService } from '../../core/api.service';
import { KafkaMessage } from '../../core/models';
import { Icon } from '../../shared/icon';
import { MessageDetail } from '../../shared/message-detail';
import { MessageList } from '../../shared/message-list';
import { Sparkline } from '../../shared/sparkline';
import { compact, full, isTyping, matches } from '../../shared/format';

/** Max messages kept in memory per tab; older ones are discarded. */
const MAX_MESSAGES = 5000;
const RATE_POINTS = 60;

type Status = 'connecting' | 'live' | 'reconnecting' | 'failed';

const STATUS_TEXT: Record<Status, string> = {
  connecting: 'Connecting',
  live: 'Live',
  reconnecting: 'Reconnecting',
  failed: 'Disconnected',
};

@Component({
  selector: 'app-live-tail',
  imports: [Icon, MessageList, MessageDetail, Sparkline],
  host: { '(document:keydown)': 'onKey($event)' },
  template: `
    <div class="toolbar">
      <span class="pill status" [attr.data-status]="paused() ? 'paused' : status()" role="status">
        {{ paused() ? 'Paused' : statusText() }}
      </span>

      <button class="btn" [class.primary]="paused()" (click)="togglePause()" [attr.aria-pressed]="paused()" title="Pause / resume (Space)">
        <app-icon [name]="paused() ? 'play' : 'pause'" [size]="14" />
        {{ paused() ? 'Resume' : 'Pause' }}
        <kbd aria-hidden="true">Space</kbd>
      </button>
      <button class="btn" (click)="clear()" [disabled]="!messages().length && !buffer().length" title="Clear the list">
        <app-icon name="eraser" [size]="14" /> Clear
      </button>

      <label class="field grow">
        <app-icon name="search" [size]="15" />
        <span class="sr-only">Search in received messages</span>
        <input placeholder="Search in received messages…" [value]="search()" (input)="search.set($any($event.target).value)" />
        @if (search()) {
          <button class="btn ghost icon-only" (click)="search.set('')" aria-label="Clear search"><app-icon name="x" [size]="14" /></button>
        }
      </label>

      <div class="rate" [title]="'Messages per second over the last minute'">
        <app-sparkline [values]="rates()" [width]="90" [height]="22" />
        <span class="mono"><b>{{ compact(rate()) }}</b> msg/s</span>
      </div>
    </div>

    <div class="counters muted" aria-live="off">
      @if (filter()) {
        <span><b class="mono match">{{ full(received()) }}</b> matched of <b class="mono">{{ full(scanned()) }}</b> scanned on the server</span>
      } @else {
        <span><b class="mono">{{ full(received()) }}</b> received</span>
      }
      @if (search()) {
        <span><b class="mono">{{ full(filtered().length) }}</b> shown by search</span>
      }
      @if (messages().length >= max) {
        <span title="Oldest messages are discarded to keep the browser fast">keeping newest {{ full(max) }}</span>
      }
      @if (dropped() > 0) {
        <span class="pill warn" title="The server skipped messages because this browser could not keep up">
          {{ full(dropped()) }} dropped by server
        </span>
      }
    </div>

    @if (error()) {
      <div class="alert danger" role="alert">
        <app-icon name="alert-circle" />
        <div class="body"><strong>Live stream stopped</strong>{{ error() }}</div>
        <button class="btn" (click)="reconnect()">Reconnect</button>
      </div>
    }

    <div class="split" [class.with-detail]="selected()">
      <div class="list-wrap">
        <app-message-list
          [messages]="filtered()"
          [(selected)]="selected"
          label="Live messages, newest first"
          [emptyIcon]="status() === 'live' ? 'radio' : 'refresh'"
          [emptyTitle]="emptyTitle()"
          [emptyText]="emptyText()"
          (atTop)="atTop.set($event)"
        />
        @if (buffer().length > 0) {
          <button class="btn primary new" (click)="flush()">
            <app-icon name="arrow-up" [size]="14" />
            {{ full(buffer().length) }} new {{ buffer().length === 1 ? 'message' : 'messages' }}
          </button>
        }
      </div>
      @if (selected(); as m) {
        <app-message-detail [message]="m" (closed)="selected.set(null)" />
      }
    </div>
    <p class="hint-keys">
      <kbd>↑</kbd><kbd>↓</kbd> navigate · <kbd>Esc</kbd> close details · <kbd>Space</kbd> pause · Scrolling down holds new messages until you return to the top.
    </p>
  `,
  styleUrl: './messages.scss',
  styles: `
    :host { --split-offset: 470px; }
    .status { min-width: 100px; justify-content: center; }
    .status[data-status='live'] { background: var(--ok-soft); color: var(--ok); }
    .status[data-status='live']::before { animation: pulse 1.6s ease-out infinite; }
    .status[data-status='connecting'], .status[data-status='reconnecting'], .status[data-status='paused'] { background: var(--warn-soft); color: var(--warn); }
    .status[data-status='failed'] { background: var(--danger-soft); color: var(--danger); }
    @keyframes pulse { 0% { box-shadow: 0 0 0 0 currentColor; } 100% { box-shadow: 0 0 0 6px transparent; } }
    .rate { display: flex; align-items: center; gap: var(--sp-2); font-size: var(--fs-sm); color: var(--muted); b { color: var(--text); } }
    .match { color: var(--accent) !important; }
    .counters { display: flex; align-items: center; gap: var(--sp-4); font-size: var(--fs-xs); margin: calc(-1 * var(--sp-1)) 0 var(--sp-3); b { color: var(--text-2); font-weight: 600; } }
    .new {
      position: absolute;
      top: 44px;
      left: 50%;
      transform: translateX(-50%);
      z-index: 2;
      border-radius: 999px;
      box-shadow: var(--shadow-pop);
      animation: drop var(--t) var(--ease);
    }
    @keyframes drop { from { opacity: 0; transform: translate(-50%, -6px); } }
    kbd { margin-left: 2px; }
    .btn.primary kbd { background: transparent; color: inherit; border-color: currentColor; opacity: 0.7; }
  `,
})
export class LiveTail {
  readonly topic = input.required<string>();
  /** Server-side CEL filter ('' = none). Changing it reconnects the stream. */
  readonly filter = input('');
  /** Emits while (re)connecting the stream, e.g. right after a filter is applied. */
  readonly busy = output<boolean>();

  private readonly api = inject(ApiService);
  private readonly list = viewChild(MessageList);

  protected readonly max = MAX_MESSAGES;
  protected readonly compact = compact;
  protected readonly full = full;

  protected readonly status = signal<Status>('connecting');
  protected readonly statusText = computed(() => STATUS_TEXT[this.status()]);
  protected readonly error = signal('');
  protected readonly messages = signal<KafkaMessage[]>([]); // newest first
  /** New messages held back while paused or while the user is scrolled away from the top. */
  protected readonly buffer = signal<KafkaMessage[]>([]);
  protected readonly paused = signal(false);
  protected readonly atTop = signal(true);
  protected readonly search = signal('');
  protected readonly dropped = signal(0);
  protected readonly received = signal(0);
  protected readonly scanned = signal(0);
  protected readonly rates = signal<number[]>([]);
  protected readonly rate = computed(() => this.rates().at(-1) ?? 0);
  protected readonly selected = signal<KafkaMessage | null>(null);

  protected readonly filtered = computed(() => {
    const q = this.search();
    const all = this.messages();
    return q ? all.filter((m) => matches(m, q)) : all;
  });

  protected readonly emptyTitle = computed(() => {
    if (this.search() && this.messages().length) return 'Nothing matches your search';
    if (this.status() !== 'live') return `${this.statusText()}…`;
    return this.filter() ? 'Waiting for matching messages…' : 'Waiting for new messages…';
  });
  protected readonly emptyText = computed(() =>
    this.status() !== 'live' || this.search()
      ? ''
      : this.filter()
        ? `${full(this.scanned())} new messages checked so far. Only messages produced from now on are evaluated — use the Messages tab to search history.`
        : 'Only messages produced from now on appear here. Use the Messages tab for history.',
  );

  private sub?: Subscription;
  private windowCount = 0;

  constructor() {
    // (Re)connect whenever the topic input changes; close on destroy.
    effect((onCleanup) => {
      const topic = this.topic();
      const filter = this.filter();
      untracked(() => this.connect(topic, filter));
      onCleanup(() => this.sub?.unsubscribe());
    });

    effect(() => this.busy.emit(this.status() === 'connecting' || this.status() === 'reconnecting'));

    // Returning to the top flushes held messages (unless explicitly paused).
    effect(() => {
      if (this.atTop() && !this.paused()) untracked(() => this.flush(false));
    });

    const timer = setInterval(() => {
      const n = this.windowCount;
      this.windowCount = 0;
      this.rates.update((r) => [...r, n].slice(-RATE_POINTS));
    }, 1000);
    inject(DestroyRef).onDestroy(() => clearInterval(timer));
  }

  protected reconnect() {
    this.connect(this.topic(), this.filter());
  }

  private connect(topic: string, filter: string) {
    this.sub?.unsubscribe();
    this.clear();
    this.status.set('connecting');
    this.error.set('');
    this.sub = this.api.live(topic, filter).subscribe((ev) => {
      switch (ev.type) {
        case 'ready':
          this.status.set('live');
          this.error.set('');
          break;
        case 'reconnecting':
          this.status.set('reconnecting');
          break;
        case 'failure':
          this.status.set('failed');
          this.error.set(ev.error);
          break;
        case 'batch':
          this.onBatch(ev.messages, ev.dropped, ev.scanned);
          break;
      }
    });
  }

  private onBatch(batch: KafkaMessage[], dropped: number, scanned: number) {
    // Rate reflects topic throughput (everything the server evaluated), not just matches.
    this.windowCount += scanned || batch.length;
    this.scanned.update((n) => n + (scanned || batch.length));
    this.received.update((n) => n + batch.length);
    if (dropped) this.dropped.update((n) => n + dropped);

    const incoming = batch.slice().reverse(); // newest first
    if (this.paused() || !this.atTop()) {
      this.buffer.update((b) => incoming.concat(b).slice(0, MAX_MESSAGES));
      return;
    }
    this.messages.update((cur) => incoming.concat(cur).slice(0, MAX_MESSAGES));
  }

  /** Moves held messages into the list. */
  protected flush(scroll = true) {
    const held = this.buffer();
    if (held.length) {
      this.messages.update((cur) => held.concat(cur).slice(0, MAX_MESSAGES));
      this.buffer.set([]);
    }
    if (scroll) {
      this.paused.set(false);
      this.list()?.scrollToTop();
    }
  }

  protected togglePause() {
    if (this.paused()) {
      this.paused.set(false);
      if (this.atTop()) this.flush(false);
    } else {
      this.paused.set(true);
    }
  }

  protected clear() {
    this.messages.set([]);
    this.buffer.set([]);
    this.received.set(0);
    this.scanned.set(0);
    this.dropped.set(0);
    this.selected.set(null);
  }

  protected onKey(e: KeyboardEvent) {
    if (e.key === ' ' && !isTyping(e) && !(e.target as HTMLElement).closest('button')) {
      e.preventDefault();
      this.togglePause();
    }
  }
}
