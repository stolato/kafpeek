import { Component, computed, effect, inject, input, signal, untracked, viewChild } from '@angular/core';
import { httpResource } from '@angular/common/http';
import { RouterLink } from '@angular/router';
import { ConsumerGroup } from '../../core/models';
import { SessionService } from '../../core/session.service';
import { ResetOffsetsDialog } from '../../shared/reset-offsets-dialog';
import { Icon } from '../../shared/icon';
import { PageHeader } from '../../shared/page-header';
import { refreshTracker } from '../../shared/refresh';
import { Sparkline } from '../../shared/sparkline';
import { SortHeader, SortState, sortBy } from '../../shared/sort';
import { compact, errorMessage, full, groupStateTone } from '../../shared/format';

type Key = 'topic' | 'partition' | 'committed' | 'end' | 'lag';
const TREND_POINTS = 60; // 60 samples × 5s = last 5 minutes

@Component({
  selector: 'app-group-detail',
  imports: [RouterLink, Icon, PageHeader, Sparkline, SortHeader, ResetOffsetsDialog],
  template: `
    <app-page-header [title]="name()" [monoTitle]="true" [crumbs]="[{ label: 'Consumer groups', link: '/groups' }]" [tracker]="tracker">
      @if (group.value(); as g) {
        <span class="pill" [class]="tone()">{{ g.state }}</span>
      }
      @if (!session.readOnly() && group.value()?.topics?.length) {
        <button class="btn" (click)="resetDlg().show()" [title]="(group.value()?.members ?? 0) > 0 ? 'The group must have no running consumers to apply a reset' : ''">
          <app-icon name="rotate-ccw" [size]="14" /> Reset offsets
        </button>
      }
    </app-page-header>
    @if (!session.readOnly()) {
      <app-reset-offsets-dialog
        [group]="name()"
        [topics]="group.value()?.topics ?? []"
        [partitionsByTopic]="partitionsByTopic()"
        (applied)="group.reload()"
      />
    }

    @if (group.error(); as e) {
      <div class="alert danger" role="alert">
        <app-icon name="alert-circle" />
        <div class="body"><strong>Could not load consumer group</strong>{{ errorMessage(e) }}</div>
        <button class="btn" (click)="group.reload()">Retry</button>
      </div>
    }
    @if (group.value()?.error; as err) {
      <div class="alert warn" role="alert">
        <app-icon name="alert" />
        <div class="body"><strong>Partial data</strong>{{ err }}</div>
      </div>
    }

    <section class="stats" aria-label="Summary">
      <div class="stat">
        <div class="label"><app-icon name="users" [size]="13" />Members</div>
        <div class="value">{{ group.value()?.members ?? '—' }}</div>
      </div>
      <div class="stat">
        <div class="label"><app-icon name="layers" [size]="13" />Topics</div>
        <div class="value">{{ group.value()?.topics?.length ?? '—' }}</div>
      </div>
      <div class="stat" [class.warn]="(group.value()?.totalLag ?? 0) > 0">
        <div class="label"><app-icon name="gauge" [size]="13" />Total lag</div>
        <div class="value" [title]="full(group.value()?.totalLag)">{{ compact(group.value()?.totalLag) }}</div>
      </div>
      <div class="stat trend">
        <div class="label"><app-icon name="activity" [size]="13" />Lag trend</div>
        <div class="trend-row">
          <app-sparkline [values]="trend()" [width]="140" [height]="30" [color]="trendColor()" [zeroBased]="false" />
          <span class="hint">{{ trendLabel() }}</span>
        </div>
      </div>
    </section>

    <h2 class="section">Lag by partition</h2>
    <div class="table-wrap">
      <table class="data" [attr.aria-busy]="group.isLoading()">
        <thead>
          <tr>
            <th><app-sort-header key="topic" label="Topic" [(state)]="sort" /></th>
            <th class="num"><app-sort-header key="partition" label="Partition" [(state)]="sort" /></th>
            <th class="num"><app-sort-header key="committed" label="Committed" first="desc" [(state)]="sort" /></th>
            <th class="num"><app-sort-header key="end" label="End offset" first="desc" [(state)]="sort" /></th>
            <th class="num lag-col"><app-sort-header key="lag" label="Lag" first="desc" [(state)]="sort" /></th>
            <th>Member</th>
          </tr>
        </thead>
        <tbody>
          @for (l of rows(); track l.topic + ':' + l.partition) {
            <tr>
              <td><a class="mono" [routerLink]="['/topics', l.topic]">{{ l.topic }}</a></td>
              <td class="num">{{ l.partition }}</td>
              <td class="num mono">{{ l.committed < 0 ? '—' : full(l.committed) }}</td>
              <td class="num mono">{{ full(l.end) }}</td>
              <td class="num">
                <div class="lag">
                  <span class="bar" aria-hidden="true"><span [style.width.%]="pct(l.lag)" [class.hot]="l.lag > 0"></span></span>
                  <span class="mono" [class.lagging]="l.lag > 0">{{ l.lag < 0 ? 'n/a' : full(l.lag) }}</span>
                </div>
              </td>
              <td class="mono muted member" [title]="l.memberId ?? ''">{{ l.memberId || 'unassigned' }}</td>
            </tr>
          } @empty {
            @if (group.hasValue()) {
              <tr><td colspan="6"><div class="empty"><app-icon name="inbox" [size]="28" /><strong>No committed offsets</strong></div></td></tr>
            }
          }
        </tbody>
      </table>
    </div>
  `,
  styles: `
    td a { color: var(--text); }
    .lag-col { width: 220px; }
    .lag { display: flex; align-items: center; justify-content: flex-end; gap: var(--sp-3); }
    .bar { width: 100px; height: 6px; border-radius: 3px; background: var(--surface-3); overflow: hidden; display: inline-flex; }
    .bar span { height: 100%; border-radius: 3px; background: var(--faint); transition: width var(--t) var(--ease); }
    .bar span.hot { background: var(--warn); min-width: 3px; }
    .lagging { color: var(--warn); font-weight: 600; }
    .member { max-width: 280px; overflow: hidden; text-overflow: ellipsis; }
    .trend-row { display: flex; flex-direction: column; gap: 2px; margin-top: var(--sp-2); }
    .trend-row .hint { white-space: nowrap; }
  `,
})
export class GroupDetailPage {
  /** Bound from the :name route param. */
  readonly name = input.required<string>();

  protected readonly session = inject(SessionService);
  protected readonly resetDlg = viewChild.required(ResetOffsetsDialog);

  protected readonly group = httpResource<ConsumerGroup>(() => `/api/consumer-groups/${encodeURIComponent(this.name())}`);
  protected readonly tracker = refreshTracker(this.group, 5_000);
  protected readonly sort = signal<SortState<Key>>({ key: 'lag', dir: 'desc' });

  protected readonly compact = compact;
  protected readonly full = full;
  protected readonly errorMessage = errorMessage;

  protected readonly tone = computed(() => groupStateTone(this.group.value()?.state ?? ''));
  protected readonly rows = computed(() => sortBy(this.group.value()?.lag ?? [], this.sort(), (l, k) => l[k]));
  protected readonly partitionsByTopic = computed(() => {
    const out: Record<string, number[]> = {};
    for (const l of this.group.value()?.lag ?? []) (out[l.topic] ??= []).push(l.partition);
    for (const ps of Object.values(out)) ps.sort((a, b) => a - b);
    return out;
  });
  private readonly maxLag = computed(() => Math.max(1, ...(this.group.value()?.lag ?? []).map((l) => l.lag)));

  /** Client-side lag history, sampled on every refresh (kept while the page is open). */
  protected readonly trend = signal<number[]>([]);
  protected readonly trendLabel = computed(() => {
    const t = this.trend();
    if (t.length < 2) return 'collecting…';
    const d = t[t.length - 1] - t[0];
    const secs = (t.length - 1) * 5;
    const span = secs < 60 ? `${secs}s` : `${Math.round(secs / 60)}m`;
    if (d === 0) return `flat over ${span}`;
    return `${d > 0 ? '▲ +' : '▼ '}${compact(d)} in ${span}`;
  });
  protected readonly trendColor = computed(() => {
    const t = this.trend();
    return t.length > 1 && t[t.length - 1] > t[0] ? 'var(--warn)' : 'var(--accent)';
  });

  constructor() {
    effect(() => {
      this.name();
      untracked(() => this.trend.set([]));
    });
    effect(() => {
      const g = this.group.value();
      if (this.group.status() !== 'resolved' || !g) return;
      untracked(() => this.trend.update((t) => [...t, g.totalLag].slice(-TREND_POINTS)));
    });
  }

  protected pct(lag: number) {
    return lag <= 0 ? 0 : (Math.sqrt(lag) / Math.sqrt(this.maxLag())) * 100;
  }
}
