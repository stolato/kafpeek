import { Injectable, computed, signal } from '@angular/core';
import { httpResource } from '@angular/common/http';
import { Health, Header } from './models';

export interface ProducePrefill {
  key: string | null;
  value: string | null;
  headers: Header[];
}

/** App-wide state: backend health/flags and cross-component requests. */
@Injectable({ providedIn: 'root' })
export class SessionService {
  readonly health = httpResource<Health>(() => '/api/health');
  readonly cluster = computed(() => this.health.value()?.cluster ?? '');
  /** Unknown (still loading) counts as read-only so write buttons never flash in. */
  readonly readOnly = computed(() => this.health.value()?.readOnly ?? true);
  readonly isProd = computed(() => /prod/i.test(this.cluster()));

  /** Set to open the produce dialog (optionally prefilled, e.g. "Produce copy"). */
  readonly produceRequest = signal<{ prefill: ProducePrefill | null; seq: number } | null>(null);
  private seq = 0;

  openProduce(prefill: ProducePrefill | null = null) {
    if (this.readOnly()) return;
    this.produceRequest.set({ prefill, seq: ++this.seq });
  }
}
