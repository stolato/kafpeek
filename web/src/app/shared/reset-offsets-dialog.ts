import { Component, DestroyRef, ElementRef, computed, effect, inject, input, output, signal, untracked, viewChild } from '@angular/core';
import { HttpErrorResponse } from '@angular/common/http';
import { Subscription } from 'rxjs';
import { ApiService } from '../core/api.service';
import { ResetOffsetsRequest, ResetOffsetsResult, ResetStrategy } from '../core/models';
import { SessionService } from '../core/session.service';
import { Icon } from './icon';
import { ToastService } from './toast';
import { errorMessage, full, partitionColor } from './format';

const STRATEGIES: { id: ResetStrategy; label: string; hint: string }[] = [
  { id: 'earliest', label: 'Earliest', hint: 'Reprocess everything still retained' },
  { id: 'latest', label: 'Latest', hint: 'Skip everything; consume only new messages' },
  { id: 'timestamp', label: 'Date/time', hint: 'First message at or after a moment' },
  { id: 'offset', label: 'Offset', hint: 'An absolute offset on every selected partition' },
  { id: 'shift', label: 'Shift', hint: 'Move by N from the committed offset (negative = back)' },
];

/** Local "YYYY-MM-DDTHH:mm" for <input type="datetime-local">. */
function localInput(d: Date) {
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

/**
 * Side drawer to reset a consumer group's offsets on one topic. Every change
 * to the form runs a server-side dry run, so the user sees the exact plan
 * (and the lag it leads to) before applying.
 */
@Component({
  selector: 'app-reset-offsets-dialog',
  imports: [Icon],
  template: `
    <dialog #dlg (close)="onClosed()" (click)="onBackdrop($event)" aria-labelledby="reset-title" (keydown)="onKey($event)">
      <form class="drawer" (submit)="$event.preventDefault(); apply()">
        <header>
          <div>
            <h2 id="reset-title">Reset offsets</h2>
            <div class="target mono">
              <span class="cluster" [class.prod]="session.isProd()">{{ session.cluster() }}</span>
              <app-icon name="chevron-right" [size]="12" />
              <span>{{ group() }}</span>
            </div>
          </div>
          <button type="button" class="btn ghost icon-only" (click)="close()" aria-label="Close"><app-icon name="x" /></button>
        </header>

        <div class="body">
          @if (preview()?.active) {
            <div class="alert warn" role="alert">
              <app-icon name="alert" />
              <div class="body-text">
                <strong>Group is active ({{ preview()!.members }} member{{ preview()!.members === 1 ? '' : 's' }})</strong>
                Kafka only accepts an offset reset while no consumer of the group is running. Stop them, then apply.
              </div>
            </div>
          }

          <div class="row2">
            <label class="lbl">
              Topic
              <span class="field">
                <select (change)="setTopic($any($event.target).value)">
                  @for (t of topics(); track t) {
                    <option [value]="t" [selected]="topic() === t">{{ t }}</option>
                  }
                </select>
              </span>
            </label>
            <div class="lbl">
              <span>Partitions</span>
              <div class="parts" role="group" aria-label="Partitions">
                <label class="check small"><input type="checkbox" [checked]="allPartitions()" (change)="allPartitions.set($any($event.target).checked)" />All</label>
                @if (!allPartitions()) {
                  @for (p of topicPartitions(); track p) {
                    <label class="chip" [class.on]="selected().includes(p)">
                      <input type="checkbox" [checked]="selected().includes(p)" (change)="toggle(p)" />
                      <i [style.background]="color(p)"></i>{{ p }}
                    </label>
                  }
                }
              </div>
            </div>
          </div>

          <div class="lbl">
            <span>Reset to</span>
            <div class="seg" role="radiogroup" aria-label="Reset strategy">
              @for (s of strategies; track s.id) {
                <button type="button" role="radio" [attr.aria-checked]="to() === s.id" [class.on]="to() === s.id" (click)="to.set(s.id)">{{ s.label }}</button>
              }
            </div>
            <div class="hint">{{ strategyHint() }}</div>
          </div>

          @switch (to()) {
            @case ('timestamp') {
              <label class="lbl">
                Date and time <span class="muted small">(your local time)</span>
                <input class="input mono" type="datetime-local" step="1" [value]="when()" (input)="when.set($any($event.target).value)" />
              </label>
            }
            @case ('offset') {
              <label class="lbl">
                Offset
                <input class="input mono" type="number" min="0" placeholder="e.g. 1500" [value]="offset()" (input)="offset.set($any($event.target).value)" />
              </label>
            }
            @case ('shift') {
              <label class="lbl">
                Shift by
                <input class="input mono" type="number" placeholder="e.g. -100 to reprocess the last 100" [value]="offset()" (input)="offset.set($any($event.target).value)" />
              </label>
            }
          }

          <div class="preview">
            <div class="lbl-row">
              <h3>Preview</h3>
              @if (previewing()) {
                <span class="spinner sm" aria-hidden="true"></span>
              }
              <span class="spacer"></span>
              @if (preview(); as p) {
                <span class="muted small">lag {{ full(totalBefore()) }} → <strong [class.warnc]="totalAfter() > totalBefore()">{{ full(totalAfter()) }}</strong></span>
              }
            </div>
            @if (previewError(); as e) {
              <div class="hint"><span class="bad"><app-icon name="alert-circle" [size]="13" /> {{ e }}</span></div>
            } @else if (preview(); as p) {
              <table class="data mini">
                <thead>
                  <tr><th>Partition</th><th class="num">Committed</th><th class="num">New offset</th><th class="num">Lag after</th></tr>
                </thead>
                <tbody>
                  @for (c of p.changes; track c.partition) {
                    <tr>
                      <td><span class="part mono"><i [style.background]="color(c.partition)"></i>{{ c.partition }}</span></td>
                      <td class="num mono muted">{{ c.current < 0 ? '—' : full(c.current) }}</td>
                      @if (c.skipped) {
                        <td class="num muted small" colspan="2" title="No committed offset to shift from">skipped (no commit)</td>
                      } @else {
                        <td class="num mono" [class.changed]="c.target !== c.current">{{ full(c.target) }}</td>
                        <td class="num mono">{{ full(c.lagAfter) }}</td>
                      }
                    </tr>
                  }
                </tbody>
              </table>
            }
          </div>

          @if (session.isProd()) {
            <label class="check confirm">
              <input type="checkbox" [checked]="prodConfirmed()" (change)="prodConfirmed.set($any($event.target).checked)" />
              I understand this changes what {{ group() }} consumes on {{ session.cluster() }}
            </label>
          }

          @if (error()) {
            <div class="alert danger" role="alert">
              <app-icon name="alert-circle" />
              <div class="body-text"><strong>Not applied</strong>{{ error() }}</div>
            </div>
          }
        </div>

        <footer>
          <span class="muted small">{{ changedCount() }} of {{ preview()?.changes?.length ?? 0 }} partitions change</span>
          <span class="spacer"></span>
          <button type="button" class="btn" (click)="close()">Cancel</button>
          <button type="submit" class="btn primary" [disabled]="!canApply()" [title]="applyTitle()">
            @if (applying()) {
              <span class="spinner sm on-accent" aria-hidden="true"></span> Applying…
            } @else {
              <app-icon name="rotate-ccw" [size]="14" /> Reset offsets
            }
          </button>
        </footer>
      </form>
    </dialog>
  `,
  styles: `
    dialog { margin: 0 0 0 auto; height: 100vh; max-height: 100vh; width: min(600px, 100vw); padding: 0; border: none; background: transparent; color: var(--text); }
    dialog:focus, dialog:focus-visible { outline: none; }
    dialog::backdrop { background: rgba(2, 6, 23, 0.55); }
    .drawer { display: flex; flex-direction: column; height: 100%; background: var(--surface); border-left: 1px solid var(--border-strong); box-shadow: var(--shadow-pop); animation: slide-in var(--t) var(--ease); }
    @keyframes slide-in { from { transform: translateX(24px); opacity: 0; } }
    header { display: flex; justify-content: space-between; align-items: flex-start; padding: var(--sp-4) var(--sp-5); border-bottom: 1px solid var(--border); }
    h2 { font-size: var(--fs-lg); }
    .target { display: flex; align-items: center; gap: 4px; margin-top: 2px; font-size: var(--fs-sm); color: var(--text-2); }
    .cluster { color: var(--muted); }
    .cluster.prod { color: var(--warn); font-weight: 600; }
    .body { flex: 1; overflow: auto; padding: var(--sp-4) var(--sp-5); display: flex; flex-direction: column; gap: var(--sp-4); }
    .lbl { display: flex; flex-direction: column; gap: 6px; font-size: var(--fs-sm); font-weight: 500; color: var(--text-2); }
    .lbl-row { display: flex; align-items: center; gap: var(--sp-2); }
    .row2 { display: grid; grid-template-columns: 1fr 1fr; gap: var(--sp-3); }
    .spacer { flex: 1; }
    .small { font-size: var(--fs-xs); font-weight: 400; }
    .field select { width: 100%; }
    .input { width: 100%; background: var(--bg); border: 1px solid var(--border-strong); border-radius: var(--radius-sm); color: var(--text); padding: 7px 10px; font-size: var(--fs-sm); outline: none; &:focus { border-color: var(--focus); } }
    .hint { font-size: var(--fs-xs); font-weight: 400; color: var(--muted); }
    .hint span { display: inline-flex; align-items: center; gap: 4px; }
    .bad { color: var(--danger); }
    .warnc { color: var(--warn); }
    .seg { display: inline-flex; flex-wrap: wrap; border: 1px solid var(--border-strong); border-radius: var(--radius-sm); overflow: hidden; align-self: flex-start; }
    .seg button { all: unset; cursor: pointer; padding: 5px 12px; font-size: var(--fs-sm); color: var(--muted); }
    .seg button.on { background: var(--accent-soft); color: var(--accent); }
    .seg button:focus-visible { outline: 2px solid var(--focus); outline-offset: -2px; }
    .parts { display: flex; flex-wrap: wrap; gap: 6px; align-items: center; min-height: 32px; }
    .chip { display: inline-flex; align-items: center; gap: 5px; padding: 2px 8px; border: 1px solid var(--border-strong); border-radius: 999px; font-family: var(--mono); font-size: var(--fs-xs); cursor: pointer; color: var(--muted); }
    .chip.on { border-color: var(--accent); color: var(--text); }
    .chip input { display: none; }
    .chip i, .part i { width: 8px; height: 8px; border-radius: 2px; }
    .part { display: inline-flex; align-items: center; gap: 6px; }
    .preview h3 { font-size: var(--fs-xs); text-transform: uppercase; letter-spacing: 0.05em; color: var(--muted); }
    .preview { display: flex; flex-direction: column; gap: var(--sp-2); }
    .mini { font-size: var(--fs-sm); }
    .changed { color: var(--accent); font-weight: 600; }
    .alert { margin: 0; }
    .body-text { display: flex; flex-direction: column; gap: 2px; font-size: var(--fs-sm); }
    .confirm { font-weight: 600; color: var(--text); font-size: var(--fs-sm); }
    footer { display: flex; align-items: center; gap: var(--sp-2); padding: var(--sp-3) var(--sp-5); border-top: 1px solid var(--border); }
    .spinner.on-accent { border-color: rgba(5, 46, 22, 0.25); border-top-color: var(--on-accent); }
  `,
})
export class ResetOffsetsDialog {
  readonly group = input.required<string>();
  /** Topics the group has committed offsets for. */
  readonly topics = input<string[]>([]);
  /** Partition ids per topic, from the group's lag table. */
  readonly partitionsByTopic = input<Record<string, number[]>>({});
  readonly applied = output<ResetOffsetsResult>();

  protected readonly session = inject(SessionService);
  private readonly api = inject(ApiService);
  private readonly toast = inject(ToastService);
  private readonly dlg = viewChild.required<ElementRef<HTMLDialogElement>>('dlg');

  protected readonly strategies = STRATEGIES;
  protected readonly full = full;
  protected readonly color = partitionColor;

  private readonly open = signal(false);
  protected readonly topic = signal('');
  protected readonly allPartitions = signal(true);
  protected readonly selected = signal<number[]>([]);
  protected readonly to = signal<ResetStrategy>('earliest');
  protected readonly when = signal('');
  protected readonly offset = signal('');
  protected readonly prodConfirmed = signal(false);

  protected readonly preview = signal<ResetOffsetsResult | null>(null);
  protected readonly previewError = signal('');
  protected readonly previewing = signal(false);
  protected readonly applying = signal(false);
  protected readonly error = signal('');

  protected readonly topicPartitions = computed(() => this.partitionsByTopic()[this.topic()] ?? []);
  protected readonly strategyHint = computed(() => STRATEGIES.find((s) => s.id === this.to())?.hint ?? '');
  protected readonly totalBefore = computed(() => (this.preview()?.changes ?? []).reduce((n, c) => n + Math.max(c.lagBefore, 0), 0));
  protected readonly totalAfter = computed(() =>
    (this.preview()?.changes ?? []).reduce((n, c) => n + (c.skipped ? Math.max(c.lagBefore, 0) : c.lagAfter), 0),
  );
  protected readonly changedCount = computed(() => (this.preview()?.changes ?? []).filter((c) => !c.skipped && c.target !== c.current).length);

  /** The request for the current form, or null while it is incomplete. */
  private readonly request = computed<Omit<ResetOffsetsRequest, 'dryRun'> | null>(() => {
    const topic = this.topic();
    if (!topic) return null;
    const req: Omit<ResetOffsetsRequest, 'dryRun'> = { topic, to: this.to() };
    if (!this.allPartitions()) {
      if (!this.selected().length) return null;
      req.partitions = [...this.selected()].sort((a, b) => a - b);
    }
    switch (this.to()) {
      case 'timestamp': {
        const ms = new Date(this.when()).getTime();
        if (!this.when() || Number.isNaN(ms)) return null;
        req.timestamp = ms;
        break;
      }
      case 'offset':
      case 'shift': {
        const n = Number(this.offset());
        if (this.offset().trim() === '' || !Number.isInteger(n) || (this.to() === 'offset' ? n < 0 : n === 0)) return null;
        req.offset = n;
        break;
      }
    }
    return req;
  });

  protected readonly canApply = computed(
    () =>
      !this.applying() &&
      !this.previewing() &&
      !!this.request() &&
      !!this.preview() &&
      !this.preview()!.active &&
      this.changedCount() > 0 &&
      (!this.session.isProd() || this.prodConfirmed()),
  );

  protected readonly applyTitle = computed(() =>
    this.preview()?.active ? 'Stop the group’s consumers first'
    : this.session.isProd() && !this.prodConfirmed() ? 'Confirm the production warning first'
    : this.preview() && !this.changedCount() ? 'Nothing would change'
    : '',
  );

  private previewSub?: Subscription;
  private timer?: ReturnType<typeof setTimeout>;

  constructor() {
    // Debounced dry run whenever the form changes while open.
    effect(() => {
      const req = this.request();
      if (!this.open()) return;
      untracked(() => this.schedulePreview(req));
    });
    inject(DestroyRef).onDestroy(() => {
      clearTimeout(this.timer);
      this.previewSub?.unsubscribe();
    });
  }

  show(topic?: string) {
    if (this.session.readOnly()) return;
    this.topic.set(topic && this.topics().includes(topic) ? topic : (this.topics()[0] ?? ''));
    this.allPartitions.set(true);
    this.selected.set([]);
    this.to.set('earliest');
    this.when.set(localInput(new Date(Date.now() - 3_600_000)));
    this.offset.set('');
    this.prodConfirmed.set(false);
    this.error.set('');
    this.preview.set(null);
    this.open.set(true);
    const d = this.dlg().nativeElement;
    if (!d.open) d.showModal();
  }

  close() {
    this.dlg().nativeElement.close();
  }

  protected onClosed() {
    this.open.set(false);
    clearTimeout(this.timer);
    this.previewSub?.unsubscribe();
  }

  protected setTopic(t: string) {
    this.topic.set(t);
    this.selected.set([]);
  }

  protected toggle(p: number) {
    this.selected.update((s) => (s.includes(p) ? s.filter((x) => x !== p) : [...s, p]));
  }

  private schedulePreview(req: Omit<ResetOffsetsRequest, 'dryRun'> | null) {
    clearTimeout(this.timer);
    this.previewSub?.unsubscribe();
    this.previewError.set('');
    if (!req) {
      this.preview.set(null);
      this.previewing.set(false);
      return;
    }
    this.previewing.set(true);
    this.timer = setTimeout(() => {
      this.previewSub = this.api.resetOffsets(this.group(), { ...req, dryRun: true }).subscribe({
        next: (res) => {
          this.previewing.set(false);
          this.preview.set(res);
        },
        error: (e: HttpErrorResponse) => {
          this.previewing.set(false);
          this.preview.set(null);
          this.previewError.set(errorMessage(e));
        },
      });
    }, 300);
  }

  protected apply() {
    const req = this.request();
    if (!this.canApply() || !req) return;
    this.applying.set(true);
    this.error.set('');
    this.api.resetOffsets(this.group(), { ...req, dryRun: false }).subscribe({
      next: (res) => {
        this.applying.set(false);
        this.toast.show(`Offsets reset on ${res.changes.length} partition${res.changes.length === 1 ? '' : 's'} of ${res.topic}`, 3000);
        this.close();
        this.applied.emit(res);
      },
      error: (e: HttpErrorResponse) => {
        this.applying.set(false);
        this.error.set(errorMessage(e));
        this.schedulePreview(this.request()); // the group may have become active
      },
    });
  }

  protected onKey(e: KeyboardEvent) {
    if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) {
      e.preventDefault();
      this.apply();
    }
  }

  protected onBackdrop(e: MouseEvent) {
    if (e.target === this.dlg().nativeElement) this.close();
  }
}
