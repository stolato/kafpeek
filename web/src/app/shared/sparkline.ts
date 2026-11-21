import { ChangeDetectionStrategy, Component, computed, input } from '@angular/core';

/** Tiny dependency-free trend line. Decorative: parents must render the value as text too. */
@Component({
  selector: 'app-sparkline',
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <svg [attr.viewBox]="'0 0 ' + width() + ' ' + height()" [attr.width]="width()" [attr.height]="height()" aria-hidden="true">
      @if (paths(); as p) {
        <path [attr.d]="p.area" [attr.fill]="color()" fill-opacity="0.08" />
        <path [attr.d]="p.line" fill="none" [attr.stroke]="color()" stroke-width="1.5" stroke-linejoin="round" stroke-linecap="round" />
      }
    </svg>
  `,
  styles: `:host { display: inline-flex; } svg { overflow: visible; }`,
})
export class Sparkline {
  readonly values = input.required<number[]>();
  readonly width = input(120);
  readonly height = input(28);
  readonly color = input('var(--accent)');
  /** Include 0 in the scale (rates). Turn off for trends where the change matters more. */
  readonly zeroBased = input(true);

  protected readonly paths = computed(() => {
    const v = this.values();
    if (v.length < 2) return null;
    const w = this.width();
    const h = this.height();
    const max = this.zeroBased() ? Math.max(...v, 1) : Math.max(...v);
    const min = this.zeroBased() ? Math.min(...v, 0) : Math.min(...v);
    const span = max - min || 1;
    const pts = v.map((y, i) => [(i / (v.length - 1)) * w, h - 1 - ((y - min) / span) * (h - 2)] as const);
    const line = pts.map(([x, y], i) => `${i ? 'L' : 'M'}${x.toFixed(1)},${y.toFixed(1)}`).join(' ');
    return { line, area: `${line} L${w},${h} L0,${h} Z` };
  });
}
