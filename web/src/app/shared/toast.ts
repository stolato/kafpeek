import { Component, Injectable, inject, signal } from '@angular/core';
import { Icon } from './icon';

interface Toast {
  id: number;
  text: string;
}

@Injectable({ providedIn: 'root' })
export class ToastService {
  readonly toasts = signal<Toast[]>([]);
  private seq = 0;

  show(text: string, ms = 1800) {
    const id = ++this.seq;
    this.toasts.update((t) => [...t, { id, text }]);
    setTimeout(() => this.toasts.update((t) => t.filter((x) => x.id !== id)), ms);
  }

  async copy(text: string, label = 'Copied to clipboard') {
    try {
      await navigator.clipboard.writeText(text);
      this.show(label);
    } catch {
      this.show('Clipboard unavailable (needs HTTPS or localhost)');
    }
  }
}

@Component({
  selector: 'app-toast-host',
  imports: [Icon],
  template: `
    <div class="toast-host" role="status" aria-live="polite">
      @for (t of svc.toasts(); track t.id) {
        <div class="toast"><app-icon name="check" />{{ t.text }}</div>
      }
    </div>
  `,
})
export class ToastHost {
  protected readonly svc = inject(ToastService);
}
