import { DestroyRef, Signal, effect, inject, signal } from '@angular/core';
import { HttpResourceRef } from '@angular/common/http';

export interface RefreshTracker {
  lastUpdated: Signal<Date | null>;
  auto: ReturnType<typeof signal<boolean>>;
  intervalMs: number | null;
  reload: () => void;
  loading: Signal<boolean>;
}

/**
 * Tracks when a resource last loaded and optionally polls it.
 * Polling pauses while the tab is hidden and never overlaps requests.
 * Must be called in an injection context (constructor / field initializer).
 */
export function refreshTracker(res: HttpResourceRef<unknown>, intervalMs: number | null = null): RefreshTracker {
  const lastUpdated = signal<Date | null>(null);
  const auto = signal(intervalMs !== null);

  effect(() => {
    if (res.status() === 'resolved') lastUpdated.set(new Date());
  });

  if (intervalMs !== null) {
    const id = setInterval(() => {
      if (auto() && !document.hidden && !res.isLoading()) res.reload();
    }, intervalMs);
    inject(DestroyRef).onDestroy(() => clearInterval(id));
  }

  return { lastUpdated, auto, intervalMs, reload: () => res.reload(), loading: res.isLoading };
}
