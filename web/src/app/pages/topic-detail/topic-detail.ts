import { Component, computed, inject, input, signal } from '@angular/core';
import { httpResource } from '@angular/common/http';
import { Router } from '@angular/router';
import { TopicDetail } from '../../core/models';
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
  imports: [Icon, PageHeader, FilterBar, Browse, LiveTail, ProduceDialog],
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
    </app-page-header>
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
            </div>
            <div class="table-wrap">
              <table class="data">
                <thead><tr><th>Name</th><th>Value</th><th>Source</th></tr></thead>
                <tbody>
                  @for (c of configs(); track c.name) {
                    <tr>
                      <td class="mono">{{ c.name }}</td>
                      <td class="mono wrap" [class.override]="!c.isDefault">{{ c.sensitive ? '••••••' : c.value || '—' }}</td>
                      <td>
                        @if (c.isDefault) {
                          <span class="muted src">default</span>
                        } @else {
                          <span class="pill info plain">{{ sourceLabel(c.source) }}</span>
                        }
                      </td>
                    </tr>
                  } @empty {
                    <tr><td colspan="3"><div class="empty"><app-icon name="sliders" [size]="24" /><strong>No configs match</strong>
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
  `,
})
export class TopicDetailPage {
  private readonly router = inject(Router);
  protected readonly session = inject(SessionService);

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
