import { Component, computed, input, model } from '@angular/core';
import { Icon } from './icon';

export interface SortState<K extends string> {
  key: K;
  dir: 'asc' | 'desc';
}

export function sortBy<T, K extends string>(rows: T[], s: SortState<K>, get: (row: T, key: K) => string | number): T[] {
  const mul = s.dir === 'asc' ? 1 : -1;
  return [...rows].sort((a, b) => {
    const x = get(a, s.key);
    const y = get(b, s.key);
    if (typeof x === 'number' && typeof y === 'number') return (x - y) * mul;
    return String(x).localeCompare(String(y), undefined, { numeric: true }) * mul;
  });
}

/** Accessible sortable column header button. Put inside a <th>. */
@Component({
  selector: 'app-sort-header',
  imports: [Icon],
  template: `
    <button class="sort" [class.on]="active()" (click)="toggle()" [attr.aria-label]="'Sort by ' + label()">
      {{ label() }}
      <app-icon [name]="!active() ? 'chevrons-up-down' : state().dir === 'asc' ? 'chevron-up' : 'chevron-down'" [size]="12" />
    </button>
  `,
  host: { '[attr.aria-sort]': 'ariaSort()' },
})
export class SortHeader {
  readonly key = input.required<string>();
  readonly label = input.required<string>();
  /** Default direction on first click (numbers usually want desc). */
  readonly first = input<'asc' | 'desc'>('asc');
  readonly state = model.required<SortState<any>>();

  protected readonly active = computed(() => this.state().key === this.key());
  protected readonly ariaSort = computed(() =>
    this.active() ? (this.state().dir === 'asc' ? 'ascending' : 'descending') : null,
  );

  protected toggle() {
    const s = this.state();
    this.state.set(
      s.key === this.key() ? { key: s.key, dir: s.dir === 'asc' ? 'desc' : 'asc' } : { key: this.key(), dir: this.first() },
    );
  }
}
