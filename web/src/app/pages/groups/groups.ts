import { Component, ElementRef, computed, inject, signal, viewChild } from '@angular/core';
import { httpResource } from '@angular/common/http';
import { Router, RouterLink } from '@angular/router';
import { ConsumerGroup } from '../../core/models';
import { Icon } from '../../shared/icon';
import { PageHeader } from '../../shared/page-header';
import { refreshTracker } from '../../shared/refresh';
import { SortHeader, SortState, sortBy } from '../../shared/sort';
import { compact, errorMessage, full, groupStateTone, isTyping } from '../../shared/format';

type Key = 'name' | 'state' | 'members' | 'totalLag';

@Component({
  selector: 'app-groups',
  imports: [RouterLink, Icon, PageHeader, SortHeader],
  host: { '(document:keydown)': 'onKey($event)' },
  template: `
    <app-page-header title="Consumer groups" [tracker]="tracker" />

    <section class="stats" aria-label="Summary">
      <div class="stat">
        <div class="label"><app-icon name="users" [size]="13" />Groups</div>
        <div class="value">{{ loaded() ? all().length : '—' }}</div>
      </div>
      <div class="stat">
        <div class="label"><app-icon name="activity" [size]="13" />Stable</div>
        <div class="value">{{ loaded() ? stable() : '—' }}</div>
      </div>
      <div class="stat" [class.warn]="lagging() > 0">
        <div class="label"><app-icon name="gauge" [size]="13" />Lagging</div>
        <div class="value">{{ loaded() ? lagging() : '—' }}</div>
      </div>
      <div class="stat">
        <div class="label"><app-icon name="database" [size]="13" />Total lag</div>
        <div class="value" [title]="full(totalLag())">{{ loaded() ? compact(totalLag()) : '—' }}</div>
      </div>
    </section>

    <div class="toolbar">
      <label class="field grow">
        <app-icon name="search" [size]="15" />
        <span class="sr-only">Search consumer groups</span>
        <input #search placeholder="Filter groups or topics…" [value]="q()" (input)="q.set($any($event.target).value)" />
        @if (q()) {
          <button class="btn ghost icon-only" (click)="q.set('')" aria-label="Clear filter"><app-icon name="x" [size]="14" /></button>
        } @else {
          <kbd aria-hidden="true">/</kbd>
        }
      </label>
      <label class="check"><input type="checkbox" [checked]="onlyLagging()" (change)="onlyLagging.set($any($event.target).checked)" />Only lagging</label>
      <span class="spacer"></span>
      <span class="muted" role="status">{{ rows().length }} of {{ all().length }} groups</span>
    </div>

    @if (groups.error(); as e) {
      <div class="alert danger" role="alert">
        <app-icon name="alert-circle" />
        <div class="body"><strong>Could not load consumer groups</strong>{{ errorMessage(e) }}</div>
        <button class="btn" (click)="groups.reload()">Retry</button>
      </div>
    }

    <div class="table-wrap">
      <table class="data" [attr.aria-busy]="groups.isLoading()">
        <thead>
          <tr>
            <th><app-sort-header key="name" label="Group" [(state)]="sort" /></th>
            <th><app-sort-header key="state" label="State" [(state)]="sort" /></th>
            <th class="num"><app-sort-header key="members" label="Members" first="desc" [(state)]="sort" /></th>
            <th>Topics</th>
            <th class="num lag-col"><app-sort-header key="totalLag" label="Lag" first="desc" [(state)]="sort" /></th>
          </tr>
        </thead>
        <tbody>
          @if (!loaded()) {
            @for (i of [1, 2, 3, 4]; track i) {
              <tr>
                <td><span class="skeleton" style="width: 180px"></span></td>
                <td><span class="skeleton" style="width: 60px"></span></td>
                <td><span class="skeleton" style="width: 20px; margin-left: auto"></span></td>
                <td><span class="skeleton" style="width: 120px"></span></td>
                <td><span class="skeleton" style="width: 140px; margin-left: auto"></span></td>
              </tr>
            }
          }
          @for (g of rows(); track g.name) {
            <tr class="clickable" (click)="open(g.name, $event)">
              <td>
                <a class="mono" [routerLink]="['/groups', g.name]">{{ g.name }}</a>
                @if (g.error) {
                  <app-icon name="alert" [size]="14" class="err" [title]="g.error" />
                }
              </td>
              <td><span class="pill" [class]="groupStateTone(g.state)">{{ g.state || 'Unknown' }}</span></td>
              <td class="num">{{ g.members }}</td>
              <td class="topics muted">
                @for (t of g.topics.slice(0, 3); track t) {
                  <span class="tag mono">{{ t }}</span>
                }
                @if (g.topics.length > 3) {
                  <span class="muted">+{{ g.topics.length - 3 }}</span>
                }
                @if (!g.topics.length) {
                  —
                }
              </td>
              <td class="num">
                <div class="lag">
                  <span class="bar" aria-hidden="true"><span [style.width.%]="lagPct(g.totalLag)" [class.hot]="g.totalLag > 0"></span></span>
                  <span class="mono" [class.lagging]="g.totalLag > 0" [title]="full(g.totalLag)">{{ compact(g.totalLag) }}</span>
                </div>
              </td>
            </tr>
          }
        </tbody>
      </table>
      @if (loaded() && rows().length === 0) {
        <div class="empty">
          <app-icon name="inbox" [size]="28" />
          @if (q() || onlyLagging()) {
            <strong>No groups match your filters</strong>
            <button class="btn" (click)="q.set(''); onlyLagging.set(false)">Clear filters</button>
          } @else {
            <strong>No consumer groups</strong>
            <span>Groups appear here once a consumer commits offsets.</span>
          }
        </div>
      }
    </div>
  `,
  styles: `
    td a { color: var(--text); font-weight: 500; }
    tr.clickable:hover td a { color: var(--info); }
    .err { color: var(--warn); margin-left: var(--sp-2); vertical-align: -2px; }
    .topics .tag { margin-right: 4px; }
    .lag-col { width: 220px; }
    .lag { display: flex; align-items: center; justify-content: flex-end; gap: var(--sp-3); }
    .bar { width: 110px; height: 6px; border-radius: 3px; background: var(--surface-3); overflow: hidden; display: inline-flex; }
    .bar span { height: 100%; border-radius: 3px; background: var(--faint); min-width: 0; transition: width var(--t) var(--ease); }
    .bar span.hot { background: var(--warn); min-width: 3px; }
    .lagging { color: var(--warn); font-weight: 600; }
  `,
})
export class GroupsPage {
  private readonly router = inject(Router);
  private readonly searchEl = viewChild.required<ElementRef<HTMLInputElement>>('search');

  protected readonly groups = httpResource<ConsumerGroup[]>(() => '/api/consumer-groups');
  protected readonly tracker = refreshTracker(this.groups, 10_000);

  protected readonly q = signal('');
  protected readonly onlyLagging = signal(false);
  protected readonly sort = signal<SortState<Key>>({ key: 'totalLag', dir: 'desc' });

  protected readonly compact = compact;
  protected readonly errorMessage = errorMessage;
  protected readonly full = full;
  protected readonly groupStateTone = groupStateTone;
  protected readonly loaded = computed(() => this.groups.hasValue());

  protected readonly all = computed(() => this.groups.value() ?? []);
  protected readonly stable = computed(() => this.all().filter((g) => g.state === 'Stable').length);
  protected readonly lagging = computed(() => this.all().filter((g) => g.totalLag > 0).length);
  protected readonly totalLag = computed(() => this.all().reduce((n, g) => n + g.totalLag, 0));
  private readonly maxLag = computed(() => Math.max(1, ...this.all().map((g) => g.totalLag)));

  protected readonly rows = computed(() => {
    const q = this.q().trim().toLowerCase();
    const f = this.all().filter(
      (g) =>
        (!this.onlyLagging() || g.totalLag > 0) &&
        (g.name.toLowerCase().includes(q) || g.topics.some((t) => t.toLowerCase().includes(q))),
    );
    return sortBy(f, this.sort(), (g, k) => g[k]);
  });

  protected lagPct(lag: number) {
    // sqrt scale so small lags are still visible next to huge ones
    return (Math.sqrt(lag) / Math.sqrt(this.maxLag())) * 100;
  }

  protected open(name: string, e: MouseEvent) {
    if ((e.target as HTMLElement).closest('a')) return;
    this.router.navigate(['/groups', name]);
  }

  protected onKey(e: KeyboardEvent) {
    if (e.key === '/' && !isTyping(e)) {
      e.preventDefault();
      this.searchEl().nativeElement.focus();
    }
  }

}
