import { Component, DestroyRef, computed, inject, input, signal } from '@angular/core';
import { RouterLink } from '@angular/router';
import { Icon } from './icon';
import { RefreshTracker } from './refresh';
import { relativeTime } from './format';

export interface Crumb {
  label: string;
  link?: string;
}

/** Page title + breadcrumbs + freshness indicator + refresh controls. Extra actions go in <ng-content>. */
@Component({
  selector: 'app-page-header',
  imports: [RouterLink, Icon],
  template: `
    <header class="ph">
      <div class="title">
        @if (crumbs().length) {
          <nav aria-label="Breadcrumb" class="crumbs">
            @for (c of crumbs(); track c.label) {
              @if (c.link) {
                <a [routerLink]="c.link">{{ c.label }}</a>
              } @else {
                <span>{{ c.label }}</span>
              }
              <app-icon name="chevron-right" [size]="12" />
            }
          </nav>
        }
        <h1 [class.mono]="monoTitle()" [title]="title()">{{ title() }}</h1>
      </div>

      <div class="actions">
        <ng-content />
        @if (tracker(); as t) {
          <span class="fresh muted" [title]="t.lastUpdated()?.toLocaleString() ?? ''">
            @if (t.loading()) {
              Updating…
            } @else {
              Updated {{ ago() }}
            }
          </span>
          @if (t.intervalMs) {
            <button
              class="btn ghost"
              [class.active]="t.auto()"
              (click)="t.auto.set(!t.auto())"
              [attr.aria-pressed]="t.auto()"
              [title]="'Auto-refresh every ' + t.intervalMs / 1000 + 's'"
            >
              <app-icon name="clock" [size]="14" /> Auto {{ t.intervalMs / 1000 }}s
            </button>
          }
          <button class="btn icon-only" (click)="t.reload()" [disabled]="t.loading()" aria-label="Refresh" title="Refresh">
            <app-icon name="refresh" [size]="14" [class.spin]="t.loading()" />
          </button>
        }
      </div>
    </header>
  `,
  styles: `
    .ph {
      display: flex;
      align-items: flex-end;
      justify-content: space-between;
      gap: var(--sp-4);
      margin-bottom: var(--sp-5);
      flex-wrap: wrap;
    }
    .title { min-width: 0; }
    h1 {
      font-size: var(--fs-2xl);
      line-height: 1.2;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      &.mono { font-family: var(--mono); font-size: var(--fs-xl); font-weight: 600; }
    }
    .crumbs {
      display: flex;
      align-items: center;
      gap: var(--sp-1);
      font-size: var(--fs-sm);
      color: var(--muted);
      margin-bottom: var(--sp-1);
      a { color: var(--muted); }
      a:hover { color: var(--text); }
    }
    .actions { display: flex; align-items: center; gap: var(--sp-2); flex-wrap: wrap; }
    .fresh { font-size: var(--fs-xs); margin-right: var(--sp-1); }
    .spin { animation: spin 0.9s linear infinite; }
    @keyframes spin { to { transform: rotate(360deg); } }
  `,
})
export class PageHeader {
  readonly title = input.required<string>();
  readonly crumbs = input<Crumb[]>([]);
  readonly tracker = input<RefreshTracker | null>(null);
  readonly monoTitle = input(false);

  private readonly now = signal(Date.now());
  protected readonly ago = computed(() => relativeTime(this.tracker()?.lastUpdated(), this.now()));

  constructor() {
    const id = setInterval(() => this.now.set(Date.now()), 5000);
    inject(DestroyRef).onDestroy(() => clearInterval(id));
  }
}
