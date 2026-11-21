import { Component, ElementRef, computed, signal, viewChild } from '@angular/core';
import { httpResource } from '@angular/common/http';
import { Router, RouterLink } from '@angular/router';
import { inject } from '@angular/core';
import { TopicSummary } from '../../core/models';
import { Icon } from '../../shared/icon';
import { PageHeader } from '../../shared/page-header';
import { refreshTracker } from '../../shared/refresh';
import { SortHeader, SortState, sortBy } from '../../shared/sort';
import { compact, errorMessage, full, isTyping } from '../../shared/format';

type Key = 'name' | 'partitions' | 'replicationFactor' | 'messages' | 'health';

@Component({
  selector: 'app-topics',
  imports: [RouterLink, Icon, PageHeader, SortHeader],
  host: { '(document:keydown)': 'onKey($event)' },
  template: `
    <app-page-header title="Topics" [tracker]="tracker" />

    <section class="stats" aria-label="Summary">
      <div class="stat">
        <div class="label"><app-icon name="layers" [size]="13" />Topics</div>
        <div class="value">{{ loaded() ? stats().topics : '—' }}</div>
      </div>
      <div class="stat">
        <div class="label"><app-icon name="columns" [size]="13" />Partitions</div>
        <div class="value">{{ loaded() ? stats().partitions : '—' }}</div>
      </div>
      <div class="stat">
        <div class="label"><app-icon name="database" [size]="13" />Messages</div>
        <div class="value" [title]="full(stats().messages)">{{ loaded() ? compact(stats().messages) : '—' }}</div>
      </div>
      <div class="stat" [class.warn]="stats().issues > 0">
        <div class="label"><app-icon name="alert" [size]="13" />With issues</div>
        <div class="value">{{ loaded() ? stats().issues : '—' }}</div>
      </div>
    </section>

    <div class="toolbar">
      <label class="field grow">
        <app-icon name="search" [size]="15" />
        <span class="sr-only">Search topics</span>
        <input #search placeholder="Filter topics…" [value]="q()" (input)="q.set($any($event.target).value)" />
        @if (q()) {
          <button class="btn ghost icon-only" (click)="q.set('')" aria-label="Clear filter"><app-icon name="x" [size]="14" /></button>
        } @else {
          <kbd aria-hidden="true">/</kbd>
        }
      </label>
      <label class="check"><input type="checkbox" [checked]="onlyIssues()" (change)="onlyIssues.set($any($event.target).checked)" />Only with issues</label>
      <label class="check"><input type="checkbox" [checked]="showInternal()" (change)="showInternal.set($any($event.target).checked)" />Show internal</label>
      <span class="spacer"></span>
      <span class="muted" role="status">{{ rows().length }} of {{ visibleTotal() }} topics</span>
    </div>

    @if (topics.error(); as e) {
      <div class="alert danger" role="alert">
        <app-icon name="alert-circle" />
        <div class="body"><strong>Could not load topics</strong>{{ errorMessage(e) }}</div>
        <button class="btn" (click)="topics.reload()">Retry</button>
      </div>
    }

    <div class="table-wrap">
      <table class="data" [attr.aria-busy]="topics.isLoading()">
        <thead>
          <tr>
            <th><app-sort-header key="name" label="Name" [(state)]="sort" /></th>
            <th class="num"><app-sort-header key="partitions" label="Partitions" first="desc" [(state)]="sort" /></th>
            <th class="num"><app-sort-header key="replicationFactor" label="RF" first="desc" [(state)]="sort" /></th>
            <th class="num"><app-sort-header key="messages" label="Messages" first="desc" [(state)]="sort" /></th>
            <th><app-sort-header key="health" label="Health" first="desc" [(state)]="sort" /></th>
          </tr>
        </thead>
        <tbody>
          @if (!loaded()) {
            @for (i of [1, 2, 3, 4, 5, 6]; track i) {
              <tr>
                <td><span class="skeleton" style="width: 220px"></span></td>
                <td><span class="skeleton" style="width: 30px; margin-left: auto"></span></td>
                <td><span class="skeleton" style="width: 20px; margin-left: auto"></span></td>
                <td><span class="skeleton" style="width: 50px; margin-left: auto"></span></td>
                <td><span class="skeleton" style="width: 60px"></span></td>
              </tr>
            }
          }
          @for (t of rows(); track t.name) {
            <tr class="clickable" (click)="open(t.name, $event)">
              <td>
                <a class="mono" [routerLink]="['/topics', t.name]">{{ t.name }}</a>
                @if (t.internal) {
                  <span class="tag">internal</span>
                }
              </td>
              <td class="num">{{ t.partitions }}</td>
              <td class="num">{{ t.replicationFactor }}</td>
              <td class="num mono" [title]="full(t.messages)">{{ compact(t.messages) }}</td>
              <td>
                @if (t.offlinePartitions > 0) {
                  <span class="pill danger">{{ t.offlinePartitions }} offline</span>
                } @else if (t.underReplicated > 0) {
                  <span class="pill warn">{{ t.underReplicated }} under-replicated</span>
                } @else {
                  <span class="pill ok">Healthy</span>
                }
              </td>
            </tr>
          }
        </tbody>
      </table>
      @if (loaded() && rows().length === 0) {
        <div class="empty">
          <app-icon name="inbox" [size]="28" />
          @if (q() || onlyIssues()) {
            <strong>No topics match your filters</strong>
            <button class="btn" (click)="q.set(''); onlyIssues.set(false)">Clear filters</button>
          } @else {
            <strong>No topics yet</strong>
            <span>Topics created in the cluster will show up here.</span>
          }
        </div>
      }
    </div>
  `,
  styles: `
    td a { color: var(--text); font-weight: 500; }
    tr.clickable:hover td a { color: var(--info); }
    .tag { margin-left: var(--sp-2); }
  `,
})
export class TopicsPage {
  private readonly router = inject(Router);
  private readonly searchEl = viewChild.required<ElementRef<HTMLInputElement>>('search');

  protected readonly topics = httpResource<TopicSummary[]>(() => '/api/topics');
  protected readonly tracker = refreshTracker(this.topics, 30_000);

  protected readonly q = signal('');
  protected readonly showInternal = signal(false);
  protected readonly onlyIssues = signal(false);
  protected readonly sort = signal<SortState<Key>>({ key: 'name', dir: 'asc' });

  protected readonly compact = compact;
  protected readonly errorMessage = errorMessage;
  protected readonly full = full;
  protected readonly loaded = computed(() => this.topics.hasValue());

  private readonly visible = computed(() => (this.topics.value() ?? []).filter((t) => this.showInternal() || !t.internal));
  protected readonly visibleTotal = computed(() => this.visible().length);

  protected readonly stats = computed(() => {
    const ts = this.visible();
    return {
      topics: ts.length,
      partitions: ts.reduce((n, t) => n + t.partitions, 0),
      messages: ts.reduce((n, t) => n + t.messages, 0),
      issues: ts.filter(hasIssue).length,
    };
  });

  protected readonly rows = computed(() => {
    const q = this.q().trim().toLowerCase();
    const filtered = this.visible().filter((t) => t.name.toLowerCase().includes(q) && (!this.onlyIssues() || hasIssue(t)));
    return sortBy(filtered, this.sort(), (t, k) =>
      k === 'health' ? t.offlinePartitions * 1000 + t.underReplicated : (t[k] as string | number),
    );
  });

  protected open(name: string, e: MouseEvent) {
    if ((e.target as HTMLElement).closest('a')) return; // let the link handle it (supports ctrl/cmd-click)
    this.router.navigate(['/topics', name]);
  }

  protected onKey(e: KeyboardEvent) {
    if (e.key === '/' && !isTyping(e)) {
      e.preventDefault();
      this.searchEl().nativeElement.focus();
    }
  }

}

const hasIssue = (t: TopicSummary) => t.offlinePartitions > 0 || t.underReplicated > 0;
