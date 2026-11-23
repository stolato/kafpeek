import { Component, computed, inject, input, signal, viewChild } from '@angular/core';
import { Observable } from 'rxjs';
import { httpResource } from '@angular/common/http';
import { Router } from '@angular/router';
import { TopicDetail } from '../../core/models';
import { ApiService } from '../../core/api.service';
import { ConfirmDialog } from '../../shared/confirm-dialog';
import { ToastService } from '../../shared/toast';
import { Icon } from '../../shared/icon';
import { PageHeader } from '../../shared/page-header';
import { ProduceDialog } from '../../shared/produce-dialog';
import { SessionService } from '../../core/session.service';
import { FilterBar } from '../../shared/filter-bar';
import { refreshTracker } from '../../shared/refresh';
import { compact, errorMessage, full, partitionColor } from '../../shared/format';
import { Browse } from './browse';
import { LiveTail } from './live-tail';

const TABS = [
  { id: 'messages', label: 'Messages', icon: 'list' },
  { id: 'live', label: 'Live', icon: 'radio' },
  { id: 'partitions', label: 'Partitions', icon: 'columns' },
  { id: 'configs', label: 'Configs', icon: 'sliders' },
] as const;
type Tab = (typeof TABS)[number]['id'];

@Component({
  selector: 'app-topic-detail',
  imports: [Icon, PageHeader, FilterBar, Browse, LiveTail, ProduceDialog, ConfirmDialog],
  template: `
    <app-page-header [title]="name()" [monoTitle]="true" [crumbs]="[{ label: 'Topics', link: '/topics' }]" [tracker]="tracker">
      @if (topic.value(); as t) {
        @if (t.offlinePartitions > 0) {
          <span class="pill danger">{{ t.offlinePartitions }} offline</span>
        } @else if (t.underReplicated > 0) {
          <span class="pill warn">{{ t.underReplicated }} under-replicated</span>
        } @else {
          <span class="pill ok">Healthy</span>
        }
        @if (t.internal) {
          <span class="tag">internal</span>
        }
      }
      @if (!session.readOnly()) {
        <button class="btn primary" (click)="session.openProduce()" [disabled]="!topic.hasValue()">
          <app-icon name="send" [size]="14" /> Produce
        </button>
      }
      @if (canManage()) {
        <button class="btn ghost icon-only danger-text" (click)="deleteTopic()" [disabled]="working()" title="Delete topic" aria-label="Delete topic">
          <app-icon name="trash" [size]="15" />
        </button>
      }
    </app-page-header>
    <app-confirm-dialog />
    @if (actionError(); as e) {
      <div class="alert danger" role="alert">
        <app-icon name="alert-circle" />
        <div class="body"><strong>Action failed</strong>{{ e }}</div>
        <button class="btn ghost icon-only" (click)="actionError.set('')" aria-label="Dismiss"><app-icon name="x" [size]="14" /></button>
      </div>
    }
    @if (!session.readOnly()) {
      <app-produce-dialog [topic]="name()" [partitions]="topic.value()?.partitions ?? 0" />
    }

    @if (topic.error(); as e) {
      <div class="alert danger" role="alert">
        <app-icon name="alert-circle" />
        <div class="body"><strong>Could not load topic</strong>{{ errorMessage(e) }}</div>
        <button class="btn" (click)="topic.reload()">Retry</button>
      </div>
    }

    <section class="stats compact-stats" aria-label="Summary">
      <div class="stat">
        <div class="label">Partitions</div>
        <div class="value">{{ topic.value()?.partitions ?? '—' }}</div>
      </div>
      <div class="stat">
        <div class="label">Replication</div>
        <div class="value">{{ topic.value()?.replicationFactor ?? '—' }}</div>
      </div>
      <div class="stat">
        <div class="label">Messages</div>
        <div class="value" [title]="full(topic.value()?.messages) + ' (approximate)'">{{ compact(topic.value()?.messages) }}</div>
      </div>
      <div class="stat">
        <div class="label">Retention</div>
        <div class="value sm">{{ retention() }}</div>
      </div>
      <div class="stat">
        <div class="label">Cleanup</div>
        <div class="value sm">{{ config('cleanup.policy') ?? '—' }}</div>
      </div>
    </section>

    <div class="tabs" role="tablist" aria-label="Topic sections" (keydown)="onTabKey($event)">
      @for (t of tabs; track t.id) {
        <button
          role="tab"
          [id]="'tab-' + t.id"
          [attr.aria-selected]="active() === t.id"
          [attr.aria-controls]="'panel-' + t.id"
          [attr.tabindex]="active() === t.id ? 0 : -1"
          (click)="select(t.id)"
        >
          <app-icon [name]="t.icon" [size]="14" />
          {{ t.label }}
          @if (t.id === 'live') {
            <span class="live-dot" aria-hidden="true"></span>
          }
          @if (filterExpr() && (t.id === 'live' || t.id === 'messages')) {
            <span class="filtered" title="Server filter active">filtered</span>
          }
        </button>
      }
    </div>

    <div role="tabpanel" [id]="'panel-' + active()" [attr.aria-labelledby]="'tab-' + active()">
      @if (active() === 'messages' || active() === 'live') {
        <!-- One filter for both tabs; it lives in the URL so filtered views are shareable. -->
        <app-filter-bar [applied]="filterExpr()" [busy]="busy()" (applyFilter)="setFilter($event)" />
      }
      @switch (active()) {
        @case ('messages') {
          <app-browse [topic]="name()" [filter]="filterExpr()" [partitions]="topic.value()?.partitionList ?? []" (busy)="busy.set($event)" />
        }
        @case ('live') {
          <!-- Mounted only while visible: leaving the tab closes the SSE stream. -->
          <app-live-tail [topic]="name()" [filter]="filterExpr()" (busy)="busy.set($event)" />
        }
        @case ('partitions') {
          @if (canManage()) {
            <div class="toolbar">
              @if (addingPartitions()) {
                <label class="field">
                  <span class="muted">New total</span>
                  <input
                    #partitionsInput
                    class="num-input mono"
                    type="number"
                    [min]="(topic.value()?.partitions ?? 0) + 1"
                    [value]="newPartitions()"
                    (input)="newPartitions.set($any($event.target).value)"
                    (keydown.enter)="applyPartitions()"
                    (keydown.escape)="addingPartitions.set(false)"
                    aria-label="New partition count"
                  />
                </label>
                <button class="btn primary" (click)="applyPartitions()" [disabled]="!validNewPartitions() || working()">Add partitions</button>
                <button class="btn ghost" (click)="addingPartitions.set(false)">Cancel</button>
              } @else {
                <button class="btn" (click)="startAddPartitions()" [disabled]="working()"><app-icon name="plus" [size]="14" /> Add partitions</button>
              }
              <span class="spacer"></span>
              <button class="btn" (click)="purge()" [disabled]="working() || !topic.value()?.messages">
                <app-icon name="eraser" [size]="14" /> Purge all messages
              </button>
            </div>
          }
          <div class="table-wrap">
            <table class="data">
              <thead>
                <tr>
                  <th>Partition</th>
                  <th class="num">Leader</th>
                  <th>Replicas</th>
                  <th>In-sync</th>
                  <th class="num">Start offset</th>
                  <th class="num">End offset</th>
                  <th class="dist">Messages</th>
                  @if (canManage()) {
                    <th class="act"><span class="sr-only">Actions</span></th>
                  }
                </tr>
              </thead>
              <tbody>
                @for (p of topic.value()?.partitionList ?? []; track p.id) {
                  <tr>
                    <td><span class="part mono"><i [style.background]="color(p.id)"></i>{{ p.id }}</span></td>
                    <td class="num mono">
                      @if (p.leader < 0) {
                        <span class="pill danger">none</span>
                      } @else {
                        {{ p.leader }}
                      }
                    </td>
                    <td class="mono">{{ p.replicas.join(', ') }}</td>
                    <td>
                      @if (p.isr.length < p.replicas.length) {
                        <span class="pill warn">{{ p.isr.join(', ') || 'none' }}</span>
                      } @else {
                        <span class="pill ok">{{ p.isr.join(', ') }}</span>
                      }
                    </td>
                    <td class="num mono">{{ full(p.startOffset) }}</td>
                    <td class="num mono">{{ full(p.endOffset) }}</td>
                    <td class="dist">
                      <div class="bar-row">
                        <span class="bar" aria-hidden="true"><span [style.width.%]="skewPct(p.endOffset - p.startOffset)"></span></span>
                        <span class="mono" [title]="full(p.endOffset - p.startOffset)">{{ compact(p.endOffset - p.startOffset) }}</span>
                      </div>
                    </td>
                    @if (canManage()) {
                      <td class="act">
                        <button
                          class="btn ghost icon-only sm"
                          (click)="purge(p.id)"
                          [disabled]="working() || p.endOffset === p.startOffset"
                          [title]="'Purge partition ' + p.id"
                          [attr.aria-label]="'Purge partition ' + p.id"
                        >
                          <app-icon name="eraser" [size]="14" />
                        </button>
                      </td>
                    }
                  </tr>
                }
              </tbody>
            </table>
          </div>
          <p class="muted note">The bar compares partitions to the busiest one — uneven bars usually mean a skewed message key.</p>
        }
        @case ('configs') {
          @if (topic.hasValue() && !topic.value()?.configs) {
            <div class="alert warn" role="alert">
              <app-icon name="alert" />
              <div class="body"><strong>Configs unavailable</strong>The credentials may lack the DescribeConfigs permission.</div>
            </div>
          } @else {
            <div class="toolbar">
              <label class="field grow">
                <app-icon name="search" [size]="15" />
                <span class="sr-only">Filter configs</span>
                <input placeholder="Filter configs…" [value]="cfgQ()" (input)="cfgQ.set($any($event.target).value)" />
              </label>
              <label class="check"><input type="checkbox" [checked]="onlyOverrides()" (change)="onlyOverrides.set($any($event.target).checked)" />Only overrides</label>
              <span class="muted">{{ configs().length }} shown</span>
              @if (canManage() && !adding()) {
                <button class="btn" (click)="startAdd()" [disabled]="working()"><app-icon name="plus" [size]="14" /> Add override</button>
              }
            </div>
            <div class="table-wrap">
              <table class="data">
                <thead>
                  <tr>
                    <th>Name</th><th>Value</th><th>Source</th>
                    @if (canManage()) {
                      <th class="act"><span class="sr-only">Actions</span></th>
                    }
                  </tr>
                </thead>
                <tbody>
                  @if (adding()) {
                    <tr class="editing">
                      <td>
                        <input
                          #newCfgName
                          class="cell-input mono"
                          placeholder="config name, e.g. max.message.bytes"
                          list="topic-config-names"
                          [value]="newName()"
                          (input)="newName.set($any($event.target).value)"
                          (keydown.escape)="adding.set(false)"
                          aria-label="Config name"
                        />
                        <datalist id="topic-config-names">
                          @for (c of topic.value()?.configs ?? []; track c.name) {
                            @if (c.isDefault) {
                              <option [value]="c.name"></option>
                            }
                          }
                        </datalist>
                      </td>
                      <td>
                        <input
                          class="cell-input mono"
                          placeholder="value"
                          [value]="newValue()"
                          (input)="newValue.set($any($event.target).value)"
                          (keydown.enter)="saveNew()"
                          (keydown.escape)="adding.set(false)"
                          aria-label="Config value"
                        />
                      </td>
                      <td><span class="muted src">new override</span></td>
                      <td class="act">
                        <button class="btn ghost icon-only sm" (click)="saveNew()" [disabled]="working() || !newName().trim()" aria-label="Save override"><app-icon name="check" [size]="14" /></button>
                        <button class="btn ghost icon-only sm" (click)="adding.set(false)" aria-label="Cancel"><app-icon name="x" [size]="14" /></button>
                      </td>
                    </tr>
                  }
                  @for (c of configs(); track c.name) {
                    <tr [class.editing]="editing() === c.name">
                      <td class="mono">{{ c.name }}</td>
                      @if (editing() === c.name) {
                        <td>
                          <input
                            #editCfgValue
                            class="cell-input mono"
                            [value]="editValue()"
                            (input)="editValue.set($any($event.target).value)"
                            (keydown.enter)="saveEdit(c.name)"
                            (keydown.escape)="editing.set(null)"
                            [attr.aria-label]="'Value for ' + c.name"
                          />
                        </td>
                      } @else {
                        <td class="mono wrap" [class.override]="!c.isDefault">{{ c.sensitive ? '••••••' : c.value || '—' }}</td>
                      }
                      <td>
                        @if (c.isDefault) {
                          <span class="muted src">default</span>
                        } @else {
                          <span class="pill info plain">{{ sourceLabel(c.source) }}</span>
                        }
                      </td>
                      @if (canManage()) {
                        <td class="act">
                          @if (editing() === c.name) {
                            <button class="btn ghost icon-only sm" (click)="saveEdit(c.name)" [disabled]="working()" aria-label="Save"><app-icon name="check" [size]="14" /></button>
                            <button class="btn ghost icon-only sm" (click)="editing.set(null)" aria-label="Cancel"><app-icon name="x" [size]="14" /></button>
                          } @else {
                            @if (!c.sensitive) {
                              <button class="btn ghost icon-only sm" (click)="startEdit(c.name, c.value)" [disabled]="working()" [title]="'Edit ' + c.name" [attr.aria-label]="'Edit ' + c.name">
                                <app-icon name="pencil" [size]="13" />
                              </button>
                            }
                            @if (c.source === 'DYNAMIC_TOPIC_CONFIG') {
                              <button class="btn ghost icon-only sm" (click)="resetConfig(c.name)" [disabled]="working()" [title]="'Reset ' + c.name + ' to the default'" [attr.aria-label]="'Reset ' + c.name">
                                <app-icon name="rotate-ccw" [size]="13" />
                              </button>
                            }
                          }
                        </td>
                      }
                    </tr>
                  } @empty {
                    <tr><td [attr.colspan]="canManage() ? 4 : 3"><div class="empty"><app-icon name="sliders" [size]="24" /><strong>No configs match</strong>
                      @if (onlyOverrides()) { <span>This topic uses cluster defaults. Uncheck “Only overrides” to see them.</span> }
                    </div></td></tr>
                  }
                </tbody>
              </table>
            </div>
          }
        }
      }
    </div>
  `,
  styles: `
    .compact-stats { grid-template-columns: repeat(auto-fill, minmax(140px, 1fr)); margin-bottom: var(--sp-4); }
    .tag { margin-left: var(--sp-1); }
    .filtered { font-size: 10px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.04em; color: var(--accent); background: var(--accent-soft); padding: 1px 5px; border-radius: 4px; }
    .live-dot { width: 6px; height: 6px; border-radius: 50%; background: var(--accent); }
    .part { display: inline-flex; align-items: center; gap: 6px; }
    .part i { width: 8px; height: 8px; border-radius: 2px; }
    .dist { width: 240px; }
    .bar-row { display: flex; align-items: center; gap: var(--sp-3); }
    .bar { flex: 1; height: 6px; border-radius: 3px; background: var(--surface-3); overflow: hidden; display: inline-flex; }
    .bar span { height: 100%; border-radius: 3px; background: var(--info); }
    .bar-row .mono { width: 52px; text-align: right; }
    .note { font-size: var(--fs-xs); margin-top: var(--sp-2); }
    .override { color: var(--text); font-weight: 500; }
    .src { font-size: var(--fs-xs); }
    .toolbar .spacer { flex: 1; }
    .num-input { width: 90px; background: transparent; border: none; outline: none; color: var(--text); font-size: var(--fs-sm); }
    th.act, td.act { width: 1%; white-space: nowrap; text-align: right; }
    .btn.sm { width: 26px; height: 26px; }
    .cell-input {
      width: 100%;
      background: var(--bg);
      border: 1px solid var(--border-strong);
      border-radius: var(--radius-sm);
      color: var(--text);
      padding: 4px 8px;
      font-size: var(--fs-sm);
      outline: none;
      &:focus { border-color: var(--focus); }
    }
    tr.editing td { background: var(--accent-soft); }
  `,
})
export class TopicDetailPage {
  private readonly router = inject(Router);
  private readonly api = inject(ApiService);
  private readonly toast = inject(ToastService);
  protected readonly session = inject(SessionService);
  private readonly confirmDlg = viewChild.required(ConfirmDialog);

  /** Bound from the :name route param. */
  readonly name = input.required<string>();
  /** Bound from the ?tab= query param (deep-linkable). */
  readonly tab = input<string>();
  /** Bound from the ?filter= query param: server-side CEL expression. */
  readonly filter = input<string>();
  protected readonly filterExpr = computed(() => this.filter() ?? '');
  protected readonly busy = signal(false);

  protected readonly tabs = TABS;
  protected readonly compact = compact;
  protected readonly full = full;
  protected readonly color = partitionColor;
  protected readonly errorMessage = errorMessage;

  protected readonly topic = httpResource<TopicDetail>(() => `/api/topics/${encodeURIComponent(this.name())}`);
  protected readonly tracker = refreshTracker(this.topic, 30_000);

  protected readonly active = computed<Tab>(() => (TABS.some((t) => t.id === this.tab()) ? (this.tab() as Tab) : 'messages'));

  protected readonly cfgQ = signal('');
  protected readonly onlyOverrides = signal(true);
  protected readonly configs = computed(() => {
    const q = this.cfgQ().toLowerCase();
    return (this.topic.value()?.configs ?? []).filter(
      (c) => (!this.onlyOverrides() || !c.isDefault) && (c.name.includes(q) || c.value.toLowerCase().includes(q)),
    );
  });

  private readonly maxPartition = computed(() =>
    Math.max(1, ...(this.topic.value()?.partitionList ?? []).map((p) => p.endOffset - p.startOffset)),
  );

  protected readonly retention = computed(() => {
    const ms = Number(this.config('retention.ms'));
    if (!this.config('retention.ms')) return '—';
    if (ms < 0) return 'forever';
    const h = ms / 3_600_000;
    return h >= 48 ? `${+(h / 24).toFixed(1)} days` : h >= 1 ? `${+h.toFixed(1)} hours` : `${Math.round(ms / 60000)} min`;
  });

  // ---- topic management (hidden in read-only mode and for system topics) ----

  protected readonly canManage = computed(() => !this.session.readOnly() && this.topic.hasValue() && !this.topic.value()!.internal);
  protected readonly working = signal(false);
  protected readonly actionError = signal('');

  protected readonly addingPartitions = signal(false);
  protected readonly newPartitions = signal('');
  protected readonly validNewPartitions = computed(() => {
    const n = Number(this.newPartitions());
    return Number.isInteger(n) && n > (this.topic.value()?.partitions ?? Infinity) && n <= 10000;
  });

  protected readonly editing = signal<string | null>(null);
  protected readonly editValue = signal('');
  protected readonly adding = signal(false);
  protected readonly newName = signal('');
  protected readonly newValue = signal('');

  /** Runs a write, then reloads the topic. Errors are shown above the tabs. */
  private run<T>(req: Observable<T>, done: (res: T) => void) {
    this.working.set(true);
    this.actionError.set('');
    req.subscribe({
      next: (res) => {
        this.working.set(false);
        done(res);
        this.topic.reload();
      },
      error: (e) => {
        this.working.set(false);
        this.actionError.set(errorMessage(e));
      },
    });
  }

  /** On production clusters, every change needs an explicit confirmation. */
  private async prodGuard(what: string): Promise<boolean> {
    if (!this.session.isProd()) return true;
    return this.confirmDlg().ask({
      title: 'Production cluster',
      message: `${what}\nThis changes ${this.name()} on ${this.session.cluster()}.`,
      confirmLabel: 'Apply',
    });
  }

  protected async deleteTopic() {
    const name = this.name();
    const ok = await this.confirmDlg().ask({
      title: 'Delete topic',
      message: `This permanently deletes ${name} and all of its messages.\nConsumers and producers using it will start failing, or recreate it if auto-create is enabled.`,
      confirmLabel: 'Delete topic',
      requireText: name,
      danger: true,
    });
    if (!ok) return;
    this.run(this.api.deleteTopic(name), () => {
      this.toast.show(`Topic ${name} deleted`);
      this.router.navigate(['/topics']);
    });
  }

  protected async purge(partition?: number) {
    const name = this.name();
    const scope = partition === undefined ? 'every partition' : `partition ${partition}`;
    const ok = await this.confirmDlg().ask({
      title: partition === undefined ? 'Purge all messages' : `Purge partition ${partition}`,
      message: `This permanently deletes the messages currently in ${scope} of ${name}.\nOffsets keep counting up; consumers behind the new start offset will jump forward (auto.offset.reset). Compact-only topics cannot be purged.`,
      confirmLabel: 'Purge messages',
      requireText: name,
      danger: true,
    });
    if (!ok) return;
    this.run(this.api.purgeTopic(name, partition === undefined ? undefined : [partition]), (r) =>
      this.toast.show(`Purged ~${full(r.deleted)} messages from ${r.partitions} partition${r.partitions === 1 ? '' : 's'}`, 3000),
    );
  }

  protected startAddPartitions() {
    this.newPartitions.set(String((this.topic.value()?.partitions ?? 0) + 1));
    this.addingPartitions.set(true);
    queueMicrotask(() => (document.querySelector('input[aria-label="New partition count"]') as HTMLInputElement | null)?.select());
  }

  protected async applyPartitions() {
    if (!this.validNewPartitions() || this.working()) return;
    const n = Number(this.newPartitions());
    const cur = this.topic.value()?.partitions ?? 0;
    const ok = await this.confirmDlg().ask({
      title: `Add ${n - cur} partition${n - cur === 1 ? '' : 's'}`,
      message: `${this.name()} will go from ${cur} to ${n} partitions. This cannot be undone.\nKeyed messages will hash to different partitions from now on, which breaks per-key ordering across the change.`,
      confirmLabel: 'Add partitions',
    });
    if (!ok) return;
    this.run(this.api.setPartitions(this.name(), n), () => {
      this.addingPartitions.set(false);
      this.toast.show(`${this.name()} now has ${n} partitions`);
    });
  }

  protected startEdit(name: string, value: string) {
    this.adding.set(false);
    this.editing.set(name);
    this.editValue.set(value);
    queueMicrotask(() => (document.querySelector('tr.editing input') as HTMLInputElement | null)?.focus());
  }

  protected async saveEdit(name: string) {
    if (this.working()) return;
    const value = this.editValue();
    if (value === this.config(name)) {
      this.editing.set(null);
      return;
    }
    if (!(await this.prodGuard(`Set ${name} = ${value}`))) return;
    this.run(this.api.alterTopicConfigs(this.name(), { set: { [name]: value } }), () => {
      this.editing.set(null);
      this.toast.show(`${name} updated`);
    });
  }

  protected startAdd() {
    this.editing.set(null);
    this.newName.set('');
    this.newValue.set('');
    this.adding.set(true);
    queueMicrotask(() => (document.querySelector('tr.editing input') as HTMLInputElement | null)?.focus());
  }

  protected async saveNew() {
    const name = this.newName().trim();
    if (!name || this.working()) return;
    if (!(await this.prodGuard(`Set ${name} = ${this.newValue()}`))) return;
    this.run(this.api.alterTopicConfigs(this.name(), { set: { [name]: this.newValue() } }), () => {
      this.adding.set(false);
      this.toast.show(`${name} overridden`);
    });
  }

  protected async resetConfig(name: string) {
    const ok = await this.confirmDlg().ask({
      title: `Reset ${name}`,
      message: `Remove the topic override; ${name} falls back to the broker/cluster default.`,
      confirmLabel: 'Reset to default',
    });
    if (!ok) return;
    this.run(this.api.alterTopicConfigs(this.name(), { reset: [name] }), () => this.toast.show(`${name} reset to default`));
  }

  protected config(name: string): string | undefined {
    return this.topic.value()?.configs?.find((c) => c.name === name)?.value;
  }

  protected skewPct(n: number) {
    return (n / this.maxPartition()) * 100;
  }

  protected sourceLabel(src: string) {
    return src.replace(/_CONFIG$/, '').replace(/_/g, ' ').toLowerCase();
  }

  protected setFilter(expr: string) {
    this.router.navigate([], { queryParams: { filter: expr || null }, queryParamsHandling: 'merge' });
  }

  protected select(id: Tab) {
    this.busy.set(false);
    this.router.navigate([], { queryParams: { tab: id === 'messages' ? null : id }, queryParamsHandling: 'merge', replaceUrl: true });
  }

  /** WAI-ARIA tabs: ←/→/Home/End move between tabs. */
  protected onTabKey(e: KeyboardEvent) {
    const i = TABS.findIndex((t) => t.id === this.active());
    const next =
      e.key === 'ArrowRight' ? (i + 1) % TABS.length
      : e.key === 'ArrowLeft' ? (i - 1 + TABS.length) % TABS.length
      : e.key === 'Home' ? 0
      : e.key === 'End' ? TABS.length - 1
      : -1;
    if (next < 0) return;
    e.preventDefault();
    this.select(TABS[next].id);
    queueMicrotask(() => document.getElementById('tab-' + TABS[next].id)?.focus());
  }
}
