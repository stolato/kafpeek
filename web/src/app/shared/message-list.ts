import { Component, computed, effect, input, model, output, signal, viewChild } from '@angular/core';
import { DatePipe } from '@angular/common';
import { CdkVirtualScrollViewport, ScrollingModule } from '@angular/cdk/scrolling';
import { KafkaMessage } from '../core/models';
import { Icon } from './icon';
import { msgId, partitionColor, preview } from './format';

export const ROW_H = 34;

/**
 * Virtualized message table shared by Browse and Live.
 * Keyboard: ↑/↓ or j/k move the selection, Home/End jump, Esc clears.
 */
@Component({
  selector: 'app-message-list',
  imports: [ScrollingModule, DatePipe, Icon],
  host: { '[style.--time-w.px]': 'timeWidth()' },
  template: `
    <div class="row head" aria-hidden="true">
      <span>Time</span><span>Partition</span><span class="r">Offset</span><span>Key</span><span>Value</span>
    </div>
    <cdk-virtual-scroll-viewport
      [itemSize]="rowH"
      class="viewport"
      tabindex="0"
      role="listbox"
      [attr.aria-label]="label()"
      [attr.aria-activedescendant]="selected() ? 'm-' + id(selected()!) : null"
      (keydown)="onKey($event)"
    >
      <div
        *cdkVirtualFor="let m of messages(); trackBy: trackBy"
        class="row"
        role="option"
        [id]="'m-' + id(m)"
        [attr.aria-selected]="selected() === m"
        [class.selected]="selected() === m"
        (click)="selected.set(m)"
      >
        <span class="muted num">{{ m.timestamp | date: timeFormat() }}</span>
        <span class="part"><i [style.background]="color(m.partition)"></i>{{ m.partition }}</span>
        <span class="r num mono">{{ m.offset }}</span>
        <span class="mono ellipsis key" [class.null]="m.key === null">{{ preview(m.key, 60) }}</span>
        <span class="mono ellipsis val" [class.null]="m.value === null">{{ preview(m.value) }}</span>
      </div>
    </cdk-virtual-scroll-viewport>
    @if (messages().length === 0) {
      <div class="empty overlay">
        <app-icon [name]="emptyIcon()" [size]="28" />
        <strong>{{ emptyTitle() }}</strong>
        @if (emptyText()) {
          <span>{{ emptyText() }}</span>
        }
      </div>
    }
  `,
  styles: `
    :host {
      position: relative;
      display: flex;
      flex-direction: column;
      min-width: 0;
      min-height: 0;
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      overflow: hidden;
    }
    .viewport { flex: 1; outline: none; }
    .viewport:focus-visible { box-shadow: inset 0 0 0 2px var(--focus); }
    .row {
      display: grid;
      grid-template-columns: var(--time-w, 116px) 80px 84px minmax(80px, 180px) minmax(0, 1fr);
      gap: var(--sp-3);
      align-items: center;
      height: ${ROW_H}px;
      padding: 0 var(--sp-3);
      border-bottom: 1px solid var(--border);
      font-size: var(--fs-sm);
      cursor: pointer;
      transition: background var(--t-fast) var(--ease);
      &:hover { background: var(--hover); }
      &.selected { background: var(--selected); box-shadow: inset 2px 0 0 var(--accent); }
      &.head {
        cursor: default;
        background: var(--surface-2);
        color: var(--muted);
        font-size: var(--fs-xs);
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        &:hover { background: var(--surface-2); }
      }
    }
    .r { text-align: right; }
    .part { display: inline-flex; align-items: center; gap: 6px; font-family: var(--mono); font-size: 12px; }
    .part i { width: 8px; height: 8px; border-radius: 2px; }
    .ellipsis { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .key { color: var(--text-2); }
    .val { color: var(--text); }
    .null { color: var(--faint); font-style: italic; }
    .overlay { position: absolute; inset: ${ROW_H}px 0 0 0; pointer-events: none; }
  `,
})
export class MessageList {
  readonly messages = input.required<KafkaMessage[]>();
  readonly selected = model<KafkaMessage | null>(null);
  readonly label = input('Messages');
  readonly timeFormat = input('HH:mm:ss.SSS');
  /** Width of the time column; widen when the format includes the date. */
  readonly timeWidth = input(116);
  readonly emptyTitle = input('No messages');
  readonly emptyText = input('');
  readonly emptyIcon = input('inbox');
  /** Emits whether the list is scrolled to the very top (newest messages). */
  readonly atTop = output<boolean>();

  protected readonly rowH = ROW_H;
  protected readonly preview = preview;
  protected readonly color = partitionColor;
  protected readonly id = msgId;
  protected readonly trackBy = (_: number, m: KafkaMessage) => msgId(m);

  private readonly viewport = viewChild.required(CdkVirtualScrollViewport);
  private readonly index = computed(() => {
    const s = this.selected();
    return s ? this.messages().indexOf(s) : -1;
  });
  private wasTop = signal(true);

  constructor() {
    effect((onCleanup) => {
      const sub = this.viewport()
        .elementScrolled()
        .subscribe(() => {
          const top = this.viewport().measureScrollOffset('top') < 4;
          if (top !== this.wasTop()) {
            this.wasTop.set(top);
            this.atTop.emit(top);
          }
        });
      onCleanup(() => sub.unsubscribe());
    });
  }

  scrollToTop() {
    this.viewport().scrollToIndex(0, 'smooth');
  }

  protected onKey(e: KeyboardEvent) {
    const list = this.messages();
    if (!list.length) return;
    let i = this.index();
    switch (e.key) {
      case 'ArrowDown':
      case 'j':
        i = Math.min(list.length - 1, i + 1);
        break;
      case 'ArrowUp':
      case 'k':
        i = Math.max(0, i - 1);
        break;
      case 'Home':
        i = 0;
        break;
      case 'End':
        i = list.length - 1;
        break;
      case 'Escape':
        if (this.selected()) {
          this.selected.set(null);
          e.stopPropagation();
        }
        return;
      default:
        return;
    }
    e.preventDefault();
    this.selected.set(list[i]);
    this.ensureVisible(i);
  }

  private ensureVisible(i: number) {
    const vp = this.viewport();
    const top = vp.measureScrollOffset('top');
    const h = vp.getViewportSize();
    const y = i * ROW_H;
    if (y < top) vp.scrollToOffset(y);
    else if (y + ROW_H > top + h) vp.scrollToOffset(y + ROW_H - h);
  }
}
