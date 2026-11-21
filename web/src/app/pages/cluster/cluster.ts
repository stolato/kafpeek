import { Component, computed } from '@angular/core';
import { httpResource } from '@angular/common/http';
import { ClusterInfo } from '../../core/models';
import { Icon } from '../../shared/icon';
import { PageHeader } from '../../shared/page-header';
import { refreshTracker } from '../../shared/refresh';
import { ToastService } from '../../shared/toast';
import { errorMessage } from '../../shared/format';
import { inject } from '@angular/core';

@Component({
  selector: 'app-cluster',
  imports: [Icon, PageHeader],
  template: `
    <app-page-header [title]="cluster.value()?.name ?? 'Cluster'" [tracker]="tracker" />

    @if (cluster.error(); as e) {
      <div class="alert danger" role="alert">
        <app-icon name="alert-circle" />
        <div class="body"><strong>Could not load cluster</strong>{{ errorMessage(e) }}</div>
        <button class="btn" (click)="cluster.reload()">Retry</button>
      </div>
    }

    <section class="stats" aria-label="Summary">
      <div class="stat">
        <div class="label"><app-icon name="server" [size]="13" />Brokers</div>
        <div class="value">{{ cluster.value()?.brokers?.length ?? '—' }}</div>
      </div>
      <div class="stat">
        <div class="label"><app-icon name="crown" [size]="13" />Controller</div>
        <div class="value">{{ controller() ?? '—' }}</div>
      </div>
      <div class="stat">
        <div class="label"><app-icon name="layers" [size]="13" />Topics</div>
        <div class="value">{{ cluster.value()?.topicCount ?? '—' }}</div>
      </div>
      <div class="stat">
        <div class="label"><app-icon name="hash" [size]="13" />Cluster ID</div>
        <div class="value sm id">
          <span class="mono" [title]="cluster.value()?.clusterId ?? ''">{{ cluster.value()?.clusterId || '—' }}</span>
          @if (cluster.value()?.clusterId; as id) {
            <button class="btn ghost icon-only" (click)="toast.copy(id, 'Cluster ID copied')" aria-label="Copy cluster ID">
              <app-icon name="copy" [size]="14" />
            </button>
          }
        </div>
      </div>
    </section>

    <h2 class="section">Brokers</h2>
    <div class="table-wrap">
      <table class="data" [attr.aria-busy]="cluster.isLoading()">
        <thead>
          <tr><th class="num">ID</th><th>Host</th><th class="num">Port</th><th>Rack</th><th>Role</th></tr>
        </thead>
        <tbody>
          @for (b of cluster.value()?.brokers ?? []; track b.id) {
            <tr>
              <td class="num mono">{{ b.id }}</td>
              <td class="mono">{{ b.host }}</td>
              <td class="num mono">{{ b.port }}</td>
              <td>{{ b.rack || '—' }}</td>
              <td>
                @if (b.id === controller()) {
                  <span class="pill info">Controller</span>
                } @else {
                  <span class="pill plain">Broker</span>
                }
              </td>
            </tr>
          } @empty {
            @if (!cluster.hasValue()) {
              @for (i of [1, 2, 3]; track i) {
                <tr>
                  <td><span class="skeleton" style="width: 20px; margin-left: auto"></span></td>
                  <td><span class="skeleton" style="width: 260px"></span></td>
                  <td><span class="skeleton" style="width: 40px; margin-left: auto"></span></td>
                  <td><span class="skeleton" style="width: 60px"></span></td>
                  <td><span class="skeleton" style="width: 70px"></span></td>
                </tr>
              }
            }
          }
        </tbody>
      </table>
    </div>
  `,
  styles: `
    .id { display: flex; align-items: center; gap: var(--sp-1); }
    .id span { overflow: hidden; text-overflow: ellipsis; }
  `,
})
export class ClusterPage {
  protected readonly toast = inject(ToastService);
  protected readonly cluster = httpResource<ClusterInfo>(() => '/api/cluster');
  protected readonly tracker = refreshTracker(this.cluster, 30_000);
  protected readonly controller = computed(() => {
    const c = this.cluster.value();
    return c && c.controllerId >= 0 ? c.controllerId : null;
  });
  protected readonly errorMessage = errorMessage;
}
