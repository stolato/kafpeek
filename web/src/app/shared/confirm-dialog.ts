import { Component, ElementRef, computed, signal, viewChild } from '@angular/core';
import { Icon } from './icon';

export interface ConfirmOptions {
  title: string;
  /** Plain text; lines are split on "\n". */
  message: string;
  confirmLabel: string;
  /** If set, the user must type this exact text to enable the button. */
  requireText?: string;
  danger?: boolean;
}

/**
 * Modal confirmation. `await dlg.ask({...})` resolves true on confirm.
 * Used for irreversible or production-affecting actions.
 */
@Component({
  selector: 'app-confirm-dialog',
  imports: [Icon],
  template: `
    <dialog #dlg (close)="settle(false)" (click)="onBackdrop($event)" aria-labelledby="confirm-title">
      @if (opts(); as o) {
        <form class="box" (submit)="$event.preventDefault(); confirm()">
          <header>
            <span class="ico" [class.danger]="o.danger"><app-icon [name]="o.danger ? 'alert' : 'alert-circle'" [size]="18" /></span>
            <h2 id="confirm-title">{{ o.title }}</h2>
          </header>
          <div class="body">
            @for (line of lines(); track $index) {
              <p>{{ line }}</p>
            }
            @if (o.requireText) {
              <label class="lbl">
                <span>Type <code class="mono">{{ o.requireText }}</code> to confirm</span>
                <input
                  #confirmEl
                  class="input mono"
                  [value]="typed()"
                  (input)="typed.set($any($event.target).value)"
                  autocomplete="off"
                  spellcheck="false"
                />
              </label>
            }
          </div>
          <footer>
            <button type="button" class="btn" (click)="close()">Cancel</button>
            <button type="submit" class="btn" [class.danger]="o.danger" [class.primary]="!o.danger" [disabled]="!canConfirm()">
              {{ o.confirmLabel }}
            </button>
          </footer>
        </form>
      }
    </dialog>
  `,
  styles: `
    dialog { padding: 0; border: none; background: transparent; color: var(--text); width: min(460px, calc(100vw - 32px)); }
    dialog::backdrop { background: rgba(2, 6, 23, 0.55); }
    dialog:focus, dialog:focus-visible { outline: none; }
    .box { background: var(--surface); border: 1px solid var(--border-strong); border-radius: var(--radius); box-shadow: var(--shadow-pop); }
    header { display: flex; align-items: center; gap: var(--sp-3); padding: var(--sp-4) var(--sp-5) 0; }
    h2 { font-size: var(--fs-lg); }
    .ico { display: inline-flex; color: var(--warn); }
    .ico.danger { color: var(--danger); }
    .body { padding: var(--sp-3) var(--sp-5) var(--sp-4); display: flex; flex-direction: column; gap: var(--sp-2); font-size: var(--fs-sm); color: var(--text-2); }
    .body p { margin: 0; }
    .lbl { display: flex; flex-direction: column; gap: 6px; margin-top: var(--sp-2); }
    code { color: var(--text); background: var(--surface-3); padding: 0 4px; border-radius: 3px; }
    .input {
      width: 100%;
      background: var(--bg);
      border: 1px solid var(--border-strong);
      border-radius: var(--radius-sm);
      color: var(--text);
      padding: 7px 10px;
      font-size: var(--fs-sm);
      outline: none;
      &:focus { border-color: var(--focus); }
    }
    footer { display: flex; justify-content: flex-end; gap: var(--sp-2); padding: var(--sp-3) var(--sp-5); border-top: 1px solid var(--border); }
  `,
})
export class ConfirmDialog {
  private readonly dlg = viewChild.required<ElementRef<HTMLDialogElement>>('dlg');
  private readonly confirmEl = viewChild<ElementRef<HTMLInputElement>>('confirmEl');

  protected readonly opts = signal<ConfirmOptions | null>(null);
  protected readonly typed = signal('');
  protected readonly lines = computed(() => (this.opts()?.message ?? '').split('\n'));
  protected readonly canConfirm = computed(() => {
    const req = this.opts()?.requireText;
    return !req || this.typed() === req;
  });

  private resolve: ((ok: boolean) => void) | null = null;

  ask(opts: ConfirmOptions): Promise<boolean> {
    this.settle(false); // a previous, unanswered question counts as cancelled
    this.opts.set(opts);
    this.typed.set('');
    const d = this.dlg().nativeElement;
    if (!d.open) d.showModal();
    queueMicrotask(() => this.confirmEl()?.nativeElement.focus());
    return new Promise((r) => (this.resolve = r));
  }

  protected confirm() {
    if (!this.canConfirm()) return;
    this.settle(true);
    this.close();
  }

  protected close() {
    this.dlg().nativeElement.close();
  }

  protected settle(ok: boolean) {
    const r = this.resolve;
    this.resolve = null;
    r?.(ok);
  }

  protected onBackdrop(e: MouseEvent) {
    if (e.target === this.dlg().nativeElement) this.close();
  }
}
