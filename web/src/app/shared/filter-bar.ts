import { Component, ElementRef, computed, effect, inject, input, output, signal, untracked, viewChild } from '@angular/core';
import { toObservable, toSignal } from '@angular/core/rxjs-interop';
import { HttpErrorResponse } from '@angular/common/http';
import { catchError, debounceTime, distinctUntilChanged, map, of, startWith, switchMap } from 'rxjs';
import { ApiService } from '../core/api.service';
import { FilterError } from '../core/models';
import { Icon } from './icon';
import { isTyping } from './format';

/** Each result carries the expression it refers to, so a stale (debounced) result is never trusted. */
type Check = { expr: string } & ({ state: 'empty' } | { state: 'pending' } | { state: 'valid' } | ({ state: 'invalid' } & FilterError));

const RECENT_KEY = 'kafpeek.recentFilters';
const MAX_RECENT = 8;

const VARIABLES: [string, string, string][] = [
  ['json', 'dyn', 'Value parsed as JSON (null if not JSON)'],
  ['value', 'string', 'Raw value (base64 if binary)'],
  ['key', 'string', 'Record key ("" if null)'],
  ['headers', 'map', 'Headers, e.g. headers["tenant"]'],
  ['partition', 'int', 'Partition number'],
  ['offset', 'int', 'Record offset'],
  ['timestamp', 'timestamp', 'Record timestamp'],
  ['size', 'int', 'Value size in bytes'],
];

const EXAMPLES: [string, string][] = [
  ['json.status == "ERROR"', 'JSON field equals'],
  ['json.amount > 500 && json.status != "PAID"', 'Numbers and logic'],
  ['has(json.error)', 'Field exists'],
  ['value.contains("timeout")', 'Raw text contains'],
  ['value.matches("(?i)exception|fatal")', 'Regex, case-insensitive'],
  ['key.startsWith("order-")', 'Key prefix'],
  ['headers["source"] == "seed"', 'Header value'],
  ['json.items.exists(i, i.qty > 10)', 'Any item in a list'],
  ['timestamp > timestamp("2026-01-01T00:00:00Z")', 'After a date'],
];

/**
 * Server-side CEL filter input: validates as you type (debounced), shows the
 * error position, applies on Enter, reverts on Esc, remembers recent filters.
 */
@Component({
  selector: 'app-filter-bar',
  imports: [Icon],
  host: { '(document:keydown)': 'onGlobalKey($event)' },
  template: `
    <div class="bar" [attr.data-state]="barState()">
      <span class="badge" aria-hidden="true">CEL</span>
      <label class="sr-only" for="cel-input">Server-side filter expression (CEL)</label>
      <input
        #input
        id="cel-input"
        spellcheck="false"
        autocomplete="off"
        [placeholder]="placeholder()"
        [value]="draft()"
        (input)="draft.set($any($event.target).value)"
        (keydown.enter)="apply()"
        (keydown.escape)="revert($event)"
        [attr.aria-invalid]="current() === 'invalid'"
        aria-describedby="cel-status"
      />
      <span class="state" id="cel-status" role="status">
        @switch (current()) {
          @case ('pending') { <span class="muted">checking…</span> }
          @case ('invalid') { <span class="bad"><app-icon name="alert-circle" [size]="14" /> invalid</span> }
          @case ('valid') {
            @if (dirty()) {
              <span class="muted"><kbd>↵</kbd> to apply</span>
            } @else if (busy() && applied()) {
              <span class="good"><span class="spinner sm" aria-hidden="true"></span> searching…</span>
            } @else {
              <span class="good"><app-icon name="check" [size]="14" /> active</span>
            }
          }
          @default {
            @if (!draft()) {
              <kbd aria-hidden="true">/</kbd>
            }
          }
        }
      </span>
      @if (dirty()) {
        <button class="btn primary sm" (click)="apply()" [disabled]="current() === 'invalid'">Apply</button>
      }
      @if (applied()) {
        <button class="btn ghost sm" (click)="clear()" title="Remove filter">
          <app-icon name="x" [size]="14" /> Clear
        </button>
      }
      <button class="btn ghost icon-only sm" (click)="help.set(!help())" [class.active]="help()" [attr.aria-expanded]="help()" aria-controls="cel-help" aria-label="Filter help and examples">
        <span aria-hidden="true">?</span>
      </button>
    </div>

    @if (error(); as e) {
      <div class="err" role="alert">
        @if (e.column >= 0 && draft().length < 160) {
          <pre class="caret" aria-hidden="true">{{ draft() }}
{{ pad(e.column) }}^</pre>
        }
        <span>{{ e.column >= 0 ? 'Column ' + (e.column + 1) + ': ' : '' }}{{ e.error }}</span>
      </div>
    }

    @if (help()) {
      <div class="help" id="cel-help">
        <div>
          <h4>Examples <span class="muted">— click to use</span></h4>
          <ul class="examples">
            @for (ex of examples; track ex[0]) {
              <li><button (click)="use(ex[0])"><code>{{ ex[0] }}</code><span>{{ ex[1] }}</span></button></li>
            }
          </ul>
        </div>
        <div>
          <h4>Variables</h4>
          <table>
            @for (v of variables; track v[0]) {
              <tr><td><code>{{ v[0] }}</code></td><td class="muted mono">{{ v[1] }}</td><td>{{ v[2] }}</td></tr>
            }
          </table>
          @if (recent().length) {
            <h4>Recent</h4>
            <ul class="examples">
              @for (r of recent(); track r) {
                <li><button (click)="use(r)"><code>{{ r }}</code></button></li>
              }
            </ul>
          }
          <p class="muted note">
            Runs on the server, so only matching messages reach the browser. Messages where the expression errors
            (e.g. a missing JSON field) simply don't match. Operators: <code>&amp;&amp; || ! == != &lt; &gt; in</code>.
          </p>
        </div>
      </div>
    }
  `,
  styles: `
    :host { display: block; margin-bottom: var(--sp-3); }
    .bar {
      display: flex; align-items: center; gap: var(--sp-2);
      height: 40px; padding: 0 var(--sp-1) 0 var(--sp-2);
      background: var(--surface); border: 1px solid var(--border-strong); border-radius: var(--radius);
      transition: border-color var(--t-fast) var(--ease), box-shadow var(--t-fast) var(--ease);
      &:focus-within { border-color: var(--focus); }
      &[data-state='active'] { border-color: var(--accent); box-shadow: 0 0 0 3px var(--accent-soft); }
      &[data-state='invalid'] { border-color: var(--danger); box-shadow: 0 0 0 3px var(--danger-soft); }
    }
    .badge {
      font-family: var(--mono); font-size: 11px; font-weight: 700; letter-spacing: 0.04em;
      padding: 2px 6px; border-radius: 4px; background: var(--surface-3); color: var(--muted);
    }
    [data-state='active'] .badge { background: var(--accent-soft); color: var(--accent); }
    input {
      flex: 1; min-width: 0; height: 100%;
      border: none; outline: none; background: transparent;
      font-family: var(--mono); font-size: var(--fs-sm); color: var(--text);
      &::placeholder { color: var(--faint); }
    }
    .state { font-size: var(--fs-xs); white-space: nowrap; display: inline-flex; align-items: center; }
    .state span { display: inline-flex; align-items: center; gap: 4px; }
    .good { color: var(--accent); }
    .bad { color: var(--danger); }
    .btn.sm { height: 30px; }
    .btn.sm.icon-only { width: 30px; font-weight: 700; }
    .err {
      margin-top: var(--sp-2); padding: var(--sp-2) var(--sp-3);
      border-radius: var(--radius-sm); background: var(--danger-soft);
      font-size: var(--fs-xs); color: var(--text);
    }
    .caret { margin: 0 0 4px; font-family: var(--mono); font-size: var(--fs-xs); color: var(--danger); overflow-x: auto; }
    .help {
      display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1fr); gap: var(--sp-5);
      margin-top: var(--sp-2); padding: var(--sp-4);
      background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius);
      font-size: var(--fs-sm);
    }
    @media (max-width: 1100px) { .help { grid-template-columns: 1fr; } }
    h4 { margin: 0 0 var(--sp-2); font-size: var(--fs-xs); text-transform: uppercase; letter-spacing: 0.05em; color: var(--muted); }
    h4 + * { margin-bottom: var(--sp-3); }
    .examples { list-style: none; margin: 0; padding: 0; display: flex; flex-direction: column; gap: 2px; }
    .examples button {
      all: unset; box-sizing: border-box; width: 100%; cursor: pointer;
      display: flex; justify-content: space-between; gap: var(--sp-3);
      padding: 5px var(--sp-2); border-radius: var(--radius-sm);
      &:hover { background: var(--hover); }
      &:focus-visible { outline: 2px solid var(--focus); }
      span { color: var(--muted); font-size: var(--fs-xs); white-space: nowrap; }
    }
    code { font-family: var(--mono); font-size: 12px; color: var(--syn-key); }
    table td { padding: 3px var(--sp-3) 3px 0; vertical-align: top; }
    .note { font-size: var(--fs-xs); margin: var(--sp-2) 0 0; line-height: 1.6; }
  `,
})
export class FilterBar {
  /** The currently applied expression (source of truth lives in the URL). */
  readonly applied = input('');
  /** True while the page is running the applied filter (search / reconnect). */
  readonly busy = input(false);
  readonly placeholder = input('Filter on the server, e.g. json.status == "ERROR" && headers["tenant"] == "acme"');
  readonly applyFilter = output<string>();

  private readonly api = inject(ApiService);
  private readonly inputEl = viewChild.required<ElementRef<HTMLInputElement>>('input');

  protected readonly examples = EXAMPLES;
  protected readonly variables = VARIABLES;
  protected readonly draft = signal('');
  protected readonly help = signal(false);
  protected readonly recent = signal<string[]>(loadRecent());
  private applyWhenValid = false;

  protected readonly dirty = computed(() => this.draft().trim() !== this.applied().trim());

  protected readonly check = toSignal(
    toObservable(this.draft).pipe(
      map((d) => d.trim()),
      distinctUntilChanged(),
      debounceTime(250),
      switchMap((expr) =>
        !expr
          ? of<Check>({ expr, state: 'empty' })
          : this.api.validateFilter(expr).pipe(
              map((): Check => ({ expr, state: 'valid' })),
              catchError((e: HttpErrorResponse) =>
                of<Check>({ expr, state: 'invalid', error: e.error?.error ?? e.message, column: e.error?.column ?? -1 }),
              ),
              startWith<Check>({ expr, state: 'pending' }),
            ),
      ),
    ),
    { initialValue: { expr: '', state: 'empty' } as Check },
  );

  /** Validation state of the *current* draft ('pending' while a result is stale or in flight). */
  protected readonly current = computed<Check['state']>(() => {
    const c = this.check();
    return c.expr === this.draft().trim() ? c.state : 'pending';
  });

  protected readonly error = computed(() => {
    const c = this.check();
    return c.state === 'invalid' && c.expr === this.draft().trim() ? c : null;
  });

  protected readonly barState = computed(() => {
    const c = this.current();
    if (c === 'invalid') return 'invalid';
    if (this.applied() && !this.dirty()) return 'active';
    return 'idle';
  });

  constructor() {
    // Keep the draft in sync when the applied filter changes externally (URL, back button).
    effect(() => {
      const a = this.applied();
      untracked(() => this.draft.set(a));
    });
    // Enter pressed while validation was in flight: apply once it resolves.
    effect(() => {
      const c = this.current();
      if (!this.applyWhenValid || c === 'pending') return;
      this.applyWhenValid = false;
      if (c !== 'invalid') untracked(() => this.apply());
    });
  }

  protected apply() {
    const expr = this.draft().trim();
    const c = this.current();
    if (c === 'invalid') return;
    if (c === 'pending') {
      this.applyWhenValid = true;
      return;
    }
    if (expr) this.remember(expr);
    this.applyFilter.emit(expr);
  }

  protected clear() {
    this.draft.set('');
    this.applyFilter.emit('');
  }

  protected revert(e: Event) {
    if (this.dirty()) {
      e.stopPropagation();
      this.draft.set(this.applied());
    } else {
      this.inputEl().nativeElement.blur();
    }
  }

  protected use(expr: string) {
    this.draft.set(expr);
    this.help.set(false);
    this.inputEl().nativeElement.focus();
  }

  protected pad(n: number) {
    return ' '.repeat(Math.max(0, n));
  }

  protected onGlobalKey(e: KeyboardEvent) {
    if (e.key === '/' && !isTyping(e)) {
      e.preventDefault();
      this.inputEl().nativeElement.focus();
    }
  }

  private remember(expr: string) {
    const list = [expr, ...this.recent().filter((r) => r !== expr)].slice(0, MAX_RECENT);
    this.recent.set(list);
    try {
      localStorage.setItem(RECENT_KEY, JSON.stringify(list));
    } catch {
      /* private mode / quota: not critical */
    }
  }
}

function loadRecent(): string[] {
  try {
    const v = JSON.parse(localStorage.getItem(RECENT_KEY) ?? '[]');
    return Array.isArray(v) ? v.filter((x) => typeof x === 'string').slice(0, MAX_RECENT) : [];
  } catch {
    return [];
  }
}
