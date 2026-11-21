import { Component, ElementRef, computed, inject, signal, viewChild } from '@angular/core';
import { httpResource } from '@angular/common/http';
import { Router } from '@angular/router';
import { ConsumerGroup, TopicSummary } from '../core/models';
import { Icon } from './icon';

interface Item {
  kind: 'Page' | 'Topic' | 'Group';
  label: string;
  link: string;
  icon: string;
  hint?: string;
}

const PAGES: Item[] = [
  { kind: 'Page', label: 'Topics', link: '/topics', icon: 'layers' },
  { kind: 'Page', label: 'Consumer groups', link: '/groups', icon: 'users' },
  { kind: 'Page', label: 'Cluster', link: '/cluster', icon: 'server' },
];

/** Lower is better; null = no match. Substring beats subsequence. */
function score(text: string, q: string): number | null {
  const t = text.toLowerCase();
  const i = t.indexOf(q);
  if (i >= 0) return i === 0 ? 0 : 1 + i / 100;
  let pos = -1;
  let gaps = 0;
  for (const ch of q) {
    const next = t.indexOf(ch, pos + 1);
    if (next < 0) return null;
    gaps += next - pos - 1;
    pos = next;
  }
  return 10 + gaps;
}

/** Quick-jump dialog: ⌘K / Ctrl+K. */
@Component({
  selector: 'app-command-palette',
  imports: [Icon],
  host: { '(document:keydown)': 'onGlobalKey($event)' },
  template: `
    <dialog #dlg (close)="open.set(false)" (click)="onBackdrop($event)" aria-label="Quick search">
        <div class="box">
          <div class="input">
            <app-icon name="search" />
            <input
              #input
              placeholder="Jump to topic, consumer group or page…"
              [value]="q()"
              (input)="q.set($any($event.target).value); active.set(0)"
              (keydown)="onKey($event)"
              role="combobox"
              aria-expanded="true"
              aria-controls="cp-list"
              [attr.aria-activedescendant]="results().length ? 'cp-' + active() : null"
            />
            <kbd>esc</kbd>
          </div>
          <ul id="cp-list" role="listbox">
            @for (r of results(); track r.kind + r.label; let i = $index) {
              <li
                [id]="'cp-' + i"
                role="option"
                [attr.aria-selected]="i === active()"
                [class.active]="i === active()"
                (mousemove)="active.set(i)"
                (click)="go(r)"
              >
                <app-icon [name]="r.icon" />
                <span class="label" [class.mono]="r.kind !== 'Page'">{{ r.label }}</span>
                @if (r.hint) {
                  <span class="hint">{{ r.hint }}</span>
                }
                <span class="kind">{{ r.kind }}</span>
              </li>
            } @empty {
              <li class="none">{{ loading() ? 'Loading…' : 'No matches' }}</li>
            }
          </ul>
          <footer>
            <span><kbd>↑</kbd><kbd>↓</kbd> navigate</span>
            <span><kbd>↵</kbd> open</span>
          </footer>
        </div>
    </dialog>
  `,
  styles: `
    dialog {
      padding: 0;
      border: none;
      background: transparent;
      width: min(640px, calc(100vw - 32px));
      margin-top: 12vh;
      color: var(--text);
    }
    dialog:focus, dialog:focus-visible { outline: none; }
    dialog::backdrop { background: rgba(2, 6, 23, 0.6); backdrop-filter: blur(2px); }
    .box { background: var(--surface-2); border-radius: var(--radius-lg); box-shadow: var(--shadow-pop); overflow: hidden; animation: pop var(--t) var(--ease); }
    @keyframes pop { from { opacity: 0; transform: scale(0.98) translateY(-4px); } }
    .input {
      display: flex; align-items: center; gap: var(--sp-3);
      padding: 0 var(--sp-4); height: 52px;
      border-bottom: 1px solid var(--border);
      color: var(--muted);
      input { flex: 1; border: none; outline: none; background: transparent; font-size: var(--fs-lg); color: var(--text); }
    }
    ul { list-style: none; margin: 0; padding: var(--sp-2); max-height: min(420px, 55vh); overflow: auto; }
    li {
      display: flex; align-items: center; gap: var(--sp-3);
      height: 38px; padding: 0 var(--sp-3);
      border-radius: var(--radius-sm);
      cursor: pointer; color: var(--text-2);
      &.active { background: var(--selected); color: var(--text); app-icon { color: var(--accent); } }
      &.none { cursor: default; color: var(--muted); justify-content: center; }
    }
    .label { flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .hint { font-size: var(--fs-xs); color: var(--muted); }
    .kind { font-size: var(--fs-xs); color: var(--muted); width: 48px; text-align: right; }
    footer { display: flex; gap: var(--sp-4); padding: var(--sp-2) var(--sp-4); border-top: 1px solid var(--border); font-size: var(--fs-xs); color: var(--muted); kbd { margin-right: 2px; } }
  `,
})
export class CommandPalette {
  private readonly router = inject(Router);
  private readonly dlg = viewChild.required<ElementRef<HTMLDialogElement>>('dlg');
  private readonly input = viewChild.required<ElementRef<HTMLInputElement>>('input');

  protected readonly open = signal(false);
  protected readonly q = signal('');
  protected readonly active = signal(0);

  // Fetched only while the palette is open.
  private readonly topics = httpResource<TopicSummary[]>(() => (this.open() ? '/api/topics' : undefined));
  private readonly groups = httpResource<ConsumerGroup[]>(() => (this.open() ? '/api/consumer-groups' : undefined));
  protected readonly loading = computed(() => this.topics.isLoading() || this.groups.isLoading());

  private readonly items = computed<Item[]>(() => [
    ...PAGES,
    ...(this.topics.value() ?? [])
      .filter((t) => !t.internal)
      .map((t) => ({ kind: 'Topic' as const, label: t.name, link: `/topics/${encodeURIComponent(t.name)}`, icon: 'layers', hint: `${t.partitions}p` })),
    ...(this.groups.value() ?? []).map((g) => ({
      kind: 'Group' as const,
      label: g.name,
      link: `/groups/${encodeURIComponent(g.name)}`,
      icon: 'users',
      hint: g.totalLag ? `lag ${g.totalLag}` : undefined,
    })),
  ]);

  protected readonly results = computed(() => {
    const q = this.q().trim().toLowerCase();
    if (!q) return this.items().slice(0, 30);
    return this.items()
      .map((it) => ({ it, s: score(it.label, q) }))
      .filter((x) => x.s !== null)
      .sort((a, b) => a.s! - b.s! || a.it.label.length - b.it.label.length)
      .slice(0, 50)
      .map((x) => x.it);
  });

  show() {
    this.q.set('');
    this.active.set(0);
    this.open.set(true);
    this.dlg().nativeElement.showModal();
    this.input().nativeElement.focus();
  }

  close() {
    this.dlg().nativeElement.close();
  }

  protected onGlobalKey(e: KeyboardEvent) {
    if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') {
      e.preventDefault();
      this.open() ? this.close() : this.show();
    }
  }

  protected onKey(e: KeyboardEvent) {
    const n = this.results().length;
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      this.active.set(n ? (this.active() + 1) % n : 0);
      this.scrollActive();
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      this.active.set(n ? (this.active() - 1 + n) % n : 0);
      this.scrollActive();
    } else if (e.key === 'Enter') {
      const r = this.results()[this.active()];
      if (r) this.go(r);
    }
  }

  protected onBackdrop(e: MouseEvent) {
    if (e.target === this.dlg().nativeElement) this.close();
  }

  protected go(r: Item) {
    this.close();
    this.router.navigateByUrl(r.link);
  }

  private scrollActive() {
    queueMicrotask(() => document.getElementById('cp-' + this.active())?.scrollIntoView({ block: 'nearest' }));
  }
}
