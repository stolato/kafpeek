import { Component, ElementRef, computed, inject, input, output, signal, viewChild } from '@angular/core';
import { HttpErrorResponse, httpResource } from '@angular/common/http';
import { ApiService } from '../core/api.service';
import { ClusterInfo, CreateTopicRequest, CreateTopicResult } from '../core/models';
import { SessionService } from '../core/session.service';
import { Icon } from './icon';
import { ToastService } from './toast';
import { errorMessage } from './format';

const NAME_RE = /^[a-zA-Z0-9._-]+$/;
const MAX_NAME = 249;
const MAX_PARTITIONS = 10000;

const RETENTION = [
  { label: '1 hour', ms: '3600000' },
  { label: '1 day', ms: '86400000' },
  { label: '7 days', ms: '604800000' },
  { label: '30 days', ms: '2592000000' },
  { label: 'Forever', ms: '-1' },
];

interface ConfigRow {
  key: string;
  value: string;
}

/** Side drawer to create a topic. Open it with show(). */
@Component({
  selector: 'app-create-topic-dialog',
  imports: [Icon],
  template: `
    <dialog #dlg (close)="open.set(false)" (click)="onBackdrop($event)" aria-labelledby="create-topic-title" (keydown)="onKey($event)">
      <form class="drawer" (submit)="$event.preventDefault(); create()">
        <header>
          <div>
            <h2 id="create-topic-title">Create topic</h2>
            <div class="target mono">
              <span class="cluster" [class.prod]="session.isProd()">{{ session.cluster() }}</span>
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
                Partitions can be added later but never removed, and the replication factor cannot be changed from here.
                <label class="check confirm">
                  <input type="checkbox" [checked]="prodConfirmed()" (change)="prodConfirmed.set($any($event.target).checked)" />
                  I understand this creates a topic on {{ session.cluster() }}
                </label>
              </div>
            </div>
          }

          <div class="lbl">
            <label for="topic-name">Name</label>
            <input
              #nameEl
              id="topic-name"
              class="input mono"
              [class.invalid]="!!nameError()"
              placeholder="e.g. orders.created.v1"
              [value]="name()"
              (input)="name.set($any($event.target).value)"
              autocomplete="off"
              spellcheck="false"
              maxlength="249"
              [attr.aria-invalid]="!!nameError()"
              aria-describedby="name-hint"
            />
            <div class="hint" id="name-hint">
              @if (nameError(); as e) {
                <span class="bad"><app-icon name="alert-circle" [size]="13" /> {{ e }}</span>
              } @else if (nameWarning(); as w) {
                <span class="warn"><app-icon name="alert" [size]="13" /> {{ w }}</span>
              } @else {
                <span>Letters, digits, "." "_" and "-" · max 249 characters</span>
              }
            </div>
          </div>

          <div class="row2">
            <div class="lbl">
              <label for="topic-partitions">Partitions</label>
              <input
                id="topic-partitions"
                class="input mono"
                type="number"
                min="1"
                [max]="maxPartitions"
                [class.invalid]="!!partitionsError()"
                placeholder="broker default"
                [value]="partitions()"
                (input)="partitions.set($any($event.target).value)"
              />
              <div class="hint">
                @if (partitionsError(); as e) {
                  <span class="bad">{{ e }}</span>
                } @else {
                  <span>Can be increased later, never decreased</span>
                }
              </div>
            </div>
            <div class="lbl">
              <label for="topic-rf">Replication factor</label>
              <input
                id="topic-rf"
                class="input mono"
                type="number"
                min="1"
                [class.invalid]="!!rfError()"
                placeholder="broker default"
                [value]="rf()"
                (input)="rf.set($any($event.target).value)"
              />
              <div class="hint">
                @if (rfError(); as e) {
                  <span class="bad">{{ e }}</span>
                } @else if (brokers(); as n) {
                  <span>Cluster has {{ n }} broker{{ n === 1 ? '' : 's' }}</span>
                }
              </div>
            </div>
          </div>

          <div class="row2">
            <label class="lbl">
              Cleanup policy
              <span class="field">
                <select (change)="cleanup.set($any($event.target).value)">
                  <option value="" [selected]="cleanup() === ''">Broker default</option>
                  <option value="delete" [selected]="cleanup() === 'delete'">delete</option>
                  <option value="compact" [selected]="cleanup() === 'compact'">compact</option>
                  <option value="compact,delete" [selected]="cleanup() === 'compact,delete'">compact,delete</option>
                </select>
              </span>
            </label>
            <label class="lbl">
              Retention
              <span class="field">
                <select (change)="retention.set($any($event.target).value)">
                  <option value="" [selected]="retention() === ''">Broker default</option>
                  @for (r of retentionOptions; track r.ms) {
                    <option [value]="r.ms" [selected]="retention() === r.ms">{{ r.label }}</option>
                  }
                </select>
              </span>
            </label>
          </div>

          <div class="lbl">
            <div class="lbl-row">
              <span>Other configs</span>
              <span class="muted small">{{ configs().length || 'none' }}</span>
              <span class="spacer"></span>
              <button type="button" class="btn ghost xs" (click)="addConfig()"><app-icon name="plus" [size]="13" /> Add config</button>
            </div>
            @for (c of configs(); track $index; let i = $index) {
              <div class="config-row">
                <input class="input mono" placeholder="e.g. min.insync.replicas" [value]="c.key" (input)="setConfig(i, 'key', $any($event.target).value)" [attr.aria-label]="'Config ' + (i + 1) + ' name'" />
                <input class="input mono" placeholder="value" [value]="c.value" (input)="setConfig(i, 'value', $any($event.target).value)" [attr.aria-label]="'Config ' + (i + 1) + ' value'" />
                <button type="button" class="btn ghost icon-only" (click)="removeConfig(i)" [attr.aria-label]="'Remove config ' + (i + 1)"><app-icon name="trash" [size]="14" /></button>
              </div>
            }
            @if (configError(); as e) {
              <div class="hint"><span class="bad">{{ e }}</span></div>
            }
          </div>

          @if (error()) {
            <div class="alert danger" role="alert">
              <app-icon name="alert-circle" />
              <div class="body-text"><strong>Not created</strong>{{ error() }}</div>
            </div>
          }
        </div>

        <footer>
          <span class="muted small">{{ mod }}↵ to create</span>
          <span class="spacer"></span>
          <button type="button" class="btn" (click)="close()">Cancel</button>
          <button
            type="submit"
            class="btn primary"
            [disabled]="!canCreate()"
            [title]="session.isProd() && !prodConfirmed() ? 'Confirm the production warning above first' : ''"
          >
            @if (creating()) {
              <span class="spinner sm on-accent" aria-hidden="true"></span> Creating…
            } @else {
              <app-icon name="plus" [size]="14" /> Create topic
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
    .lbl-row { display: flex; align-items: center; gap: var(--sp-2); }
    .row2 { display: grid; grid-template-columns: 1fr 1fr; gap: var(--sp-3); }
    .spacer { flex: 1; }
    .small { font-size: var(--fs-xs); font-weight: 400; }
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
      &.invalid { border-color: var(--danger); }
    }
    .hint { display: flex; align-items: center; gap: var(--sp-2); font-size: var(--fs-xs); font-weight: 400; color: var(--muted); min-height: 16px; }
    .hint span { display: inline-flex; align-items: center; gap: 4px; }
    .hint code { font-family: var(--mono); }
    .bad { color: var(--danger); }
    .warn { color: var(--warn); }
    .btn.xs { height: 24px; padding: 0 8px; font-size: var(--fs-xs); }
    .config-row { display: grid; grid-template-columns: 1.3fr 1fr 32px; gap: var(--sp-2); }
    .alert { margin: 0; }
    .body-text { display: flex; flex-direction: column; gap: 2px; font-size: var(--fs-sm); }
    .confirm { margin-top: var(--sp-2); font-weight: 600; color: var(--text); }
    footer { display: flex; align-items: center; gap: var(--sp-2); padding: var(--sp-3) var(--sp-5); border-top: 1px solid var(--border); }
    .spinner.on-accent { border-color: rgba(5, 46, 22, 0.25); border-top-color: var(--on-accent); }
  `,
})
export class CreateTopicDialog {
  /** Names already in the cluster, to flag duplicates before submitting. */
  readonly existing = input<string[]>([]);
  readonly created = output<CreateTopicResult>();

  protected readonly session = inject(SessionService);
  private readonly api = inject(ApiService);
  private readonly toast = inject(ToastService);
  private readonly dlg = viewChild.required<ElementRef<HTMLDialogElement>>('dlg');
  private readonly nameEl = viewChild.required<ElementRef<HTMLInputElement>>('nameEl');

  protected readonly maxPartitions = MAX_PARTITIONS;
  protected readonly retentionOptions = RETENTION;
  protected readonly mod = /Mac|iPhone|iPad/.test(navigator.platform) ? '⌘' : 'Ctrl+';

  protected readonly open = signal(false);
  protected readonly name = signal('');
  protected readonly partitions = signal('3');
  protected readonly rf = signal('');
  protected readonly cleanup = signal('');
  protected readonly retention = signal('');
  protected readonly configs = signal<ConfigRow[]>([]);
  protected readonly creating = signal(false);
  protected readonly error = signal('');
  protected readonly prodConfirmed = signal(false);

  private readonly cluster = httpResource<ClusterInfo>(() => (this.open() ? '/api/cluster' : undefined));
  protected readonly brokers = computed(() => this.cluster.value()?.brokers?.length ?? 0);

  protected readonly nameError = computed(() => {
    const n = this.name();
    if (!n) return '';
    if (n === '.' || n === '..') return `"${n}" is not a valid topic name`;
    if (n.length > MAX_NAME) return `Max ${MAX_NAME} characters`;
    if (!NAME_RE.test(n)) return 'Only letters, digits, ".", "_" and "-" are allowed';
    if (this.existing().includes(n)) return 'A topic with this name already exists';
    return '';
  });

  protected readonly nameWarning = computed(() => {
    const n = this.name();
    if (!n) return '';
    // Kafka treats "." and "_" as equal in metric names, so such topics collide.
    const norm = (s: string) => s.replace(/\./g, '_');
    const clash = this.existing().find((e) => e !== n && norm(e) === norm(n));
    if (clash) return `Collides with "${clash}" ("." and "_" are equivalent in Kafka metrics)`;
    if (n.startsWith('_')) return 'Names starting with "_" are treated as internal and hidden by default';
    return '';
  });

  protected readonly partitionsError = computed(() => {
    const v = this.partitions().trim();
    if (v === '') return '';
    const n = Number(v);
    return Number.isInteger(n) && n >= 1 && n <= MAX_PARTITIONS ? '' : `Between 1 and ${MAX_PARTITIONS}`;
  });

  protected readonly rfError = computed(() => {
    const v = this.rf().trim();
    if (v === '') return '';
    const n = Number(v);
    if (!Number.isInteger(n) || n < 1) return 'Must be at least 1';
    const b = this.brokers();
    return b && n > b ? `Cannot exceed the ${b} available broker${b === 1 ? '' : 's'}` : '';
  });

  protected readonly configError = computed(() => {
    const keys = this.configs().map((c) => c.key.trim()).filter((k) => k !== '');
    if (this.configs().some((c) => c.key.trim() === '' && c.value !== '')) return 'Config names cannot be empty';
    const reserved = [...(this.cleanup() ? ['cleanup.policy'] : []), ...(this.retention() ? ['retention.ms'] : [])];
    const dup = keys.find((k, i) => keys.indexOf(k) !== i || reserved.includes(k));
    return dup ? `"${dup}" is set more than once` : '';
  });

  protected readonly canCreate = computed(
    () =>
      !this.creating() &&
      this.name() !== '' &&
      !this.nameError() &&
      !this.partitionsError() &&
      !this.rfError() &&
      !this.configError() &&
      (!this.session.isProd() || this.prodConfirmed()),
  );

  show() {
    if (this.session.readOnly()) return;
    this.name.set('');
    this.partitions.set('3');
    this.rf.set('');
    this.cleanup.set('');
    this.retention.set('');
    this.configs.set([]);
    this.error.set('');
    this.prodConfirmed.set(false);
    this.open.set(true);
    const d = this.dlg().nativeElement;
    if (!d.open) d.showModal();
    queueMicrotask(() => this.nameEl().nativeElement.focus());
  }

  close() {
    this.open.set(false);
    this.dlg().nativeElement.close();
  }

  protected create() {
    if (!this.canCreate()) return;
    const configs: Record<string, string> = {};
    if (this.cleanup()) configs['cleanup.policy'] = this.cleanup();
    if (this.retention()) configs['retention.ms'] = this.retention();
    for (const c of this.configs()) if (c.key.trim()) configs[c.key.trim()] = c.value;

    const req: CreateTopicRequest = { name: this.name() };
    if (this.partitions().trim()) req.partitions = Number(this.partitions());
    if (this.rf().trim()) req.replicationFactor = Number(this.rf());
    if (Object.keys(configs).length) req.configs = configs;

    this.creating.set(true);
    this.error.set('');
    this.api.createTopic(req).subscribe({
      next: (res) => {
        this.creating.set(false);
        this.toast.show(`Topic ${res.name} created`);
        this.close();
        this.created.emit(res);
      },
      error: (e: HttpErrorResponse) => {
        this.creating.set(false);
        this.error.set(errorMessage(e));
      },
    });
  }

  protected addConfig() {
    this.configs.update((c) => [...c, { key: '', value: '' }]);
  }

  protected removeConfig(i: number) {
    this.configs.update((c) => c.filter((_, j) => j !== i));
  }

  protected setConfig(i: number, field: 'key' | 'value', v: string) {
    this.configs.update((c) => c.map((x, j) => (j === i ? { ...x, [field]: v } : x)));
  }

  protected onKey(e: KeyboardEvent) {
    if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) {
      e.preventDefault();
      this.create();
    }
  }

  protected onBackdrop(e: MouseEvent) {
    if (e.target === this.dlg().nativeElement) this.close();
  }
}
