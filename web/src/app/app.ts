import { Component, computed, inject, viewChild } from '@angular/core';
import { SessionService } from './core/session.service';
import { RouterLink, RouterLinkActive, RouterOutlet } from '@angular/router';
import { Icon } from './shared/icon';
import { CommandPalette } from './shared/command-palette';
import { ToastHost } from './shared/toast';

@Component({
  selector: 'app-root',
  imports: [RouterOutlet, RouterLink, RouterLinkActive, Icon, CommandPalette, ToastHost],
  styleUrl: './app.scss',
  template: `
    <a class="skip" href="#main">Skip to content</a>

    <aside class="sidebar" aria-label="Main">
      <a class="brand" routerLink="/topics" aria-label="kafpeek home">
        <app-icon name="logo" [size]="22" />
        <span class="label">kaf<b>peek</b></span>
      </a>

      <button class="search" (click)="palette().show()" aria-label="Quick search (Ctrl+K)">
        <app-icon name="search" [size]="15" />
        <span class="label">Search</span>
        <kbd class="label">{{ mod }}K</kbd>
      </button>

      <nav>
        @for (n of nav; track n.link) {
          <a [routerLink]="n.link" routerLinkActive="active" #rla="routerLinkActive" [attr.aria-current]="rla.isActive ? 'page' : null" [title]="n.label">
            <app-icon [name]="n.icon" [size]="17" />
            <span class="label">{{ n.label }}</span>
          </a>
        }
      </nav>

      <div class="cluster" role="status" [attr.data-state]="health.error() ? 'down' : health.value() ? 'up' : 'pending'">
        <span class="dot" aria-hidden="true"></span>
        <span class="label">
          <span class="name mono">{{ health.value()?.cluster ?? (health.error() ? 'Disconnected' : 'Connecting…') }}</span>
          <span class="sub">
            {{ health.error() ? 'API unreachable' : 'Connected' }}
            @if (health.value()?.readOnly) {
              · <span class="ro" title="KAFKA_READ_ONLY=true: producing is disabled"><app-icon name="lock" [size]="11" /> read-only</span>
            }
          </span>
        </span>
      </div>

      @if (health.value()?.build; as b) {
        <div class="version label" [title]="buildTitle()">
          <span class="mono">{{ versionLabel(b.version) }}</span>
          @if (b.commit) {
            <span class="mono muted">· {{ b.commit }}{{ b.dirty ? '*' : '' }}</span>
          }
        </div>
      }
    </aside>

    <main id="main" class="content" tabindex="-1">
      <router-outlet />
    </main>

    <app-command-palette />
    <app-toast-host />
  `,
})
export class App {
  protected readonly palette = viewChild.required(CommandPalette);
  protected readonly session = inject(SessionService);
  protected readonly health = this.session.health;
  /** "0.1.0" -> "v0.1.0"; non-numeric builds ("dev") are shown as-is. */
  protected versionLabel(v: string) {
    return /^\d/.test(v) ? `v${v}` : v;
  }
  protected readonly buildTitle = computed(() => {
    const b = this.health.value()?.build;
    if (!b) return '';
    const parts = [`kafpeek ${b.version}`];
    if (b.commit) parts.push(`commit ${b.commit}${b.dirty ? ' (with uncommitted changes)' : ''}`);
    if (b.buildDate) parts.push(`built ${new Date(b.buildDate).toLocaleString()}`);
    return parts.join('\n');
  });
  protected readonly mod = /Mac|iPhone|iPad/.test(navigator.platform) ? '⌘' : 'Ctrl ';
  protected readonly nav = [
    { link: '/topics', label: 'Topics', icon: 'layers' },
    { link: '/groups', label: 'Consumer groups', icon: 'users' },
    { link: '/cluster', label: 'Cluster', icon: 'server' },
  ];

  constructor() {
    // Re-check connectivity periodically so the status dot stays truthful.
    setInterval(() => !document.hidden && this.health.reload(), 15000);
  }
}
