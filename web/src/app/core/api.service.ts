import { Injectable } from '@angular/core';
import { HttpClient, HttpParams } from '@angular/common/http';
import { Observable } from 'rxjs';
import { BrowseResult, CreateTopicRequest, CreateTopicResult, PurgeResult, ResetOffsetsRequest, ResetOffsetsResult, TopicConfigChange, LiveEvent, ProduceRequest, ProduceResult, SearchEvent, SearchParams } from './models';

@Injectable({ providedIn: 'root' })
export class ApiService {
  constructor(private http: HttpClient) {}

  browse(topic: string, opts: { limit: number; partition?: number | null; filter?: string; scan?: number }): Observable<BrowseResult> {
    let params = new HttpParams().set('limit', opts.limit);
    if (opts.partition !== null && opts.partition !== undefined) params = params.set('partition', opts.partition);
    if (opts.filter) {
      params = params.set('filter', opts.filter);
      if (opts.scan) params = params.set('scan', opts.scan);
    }
    return this.http.get<BrowseResult>(`/api/topics/${encodeURIComponent(topic)}/messages`, { params });
  }

  createTopic(req: CreateTopicRequest): Observable<CreateTopicResult> {
    return this.http.post<CreateTopicResult>('/api/topics', req);
  }

  deleteTopic(name: string): Observable<void> {
    return this.http.delete<void>(`/api/topics/${encodeURIComponent(name)}`, { body: { confirm: name } });
  }

  setPartitions(name: string, count: number): Observable<{ partitions: number }> {
    return this.http.post<{ partitions: number }>(`/api/topics/${encodeURIComponent(name)}/partitions`, { count });
  }

  alterTopicConfigs(name: string, change: TopicConfigChange): Observable<void> {
    return this.http.patch<void>(`/api/topics/${encodeURIComponent(name)}/configs`, change);
  }

  /** Deletes all records in the given partitions (all when omitted). */
  purgeTopic(name: string, partitions?: number[]): Observable<PurgeResult> {
    return this.http.post<PurgeResult>(`/api/topics/${encodeURIComponent(name)}/purge`, { confirm: name, ...(partitions ? { partitions } : {}) });
  }

  resetOffsets(group: string, req: ResetOffsetsRequest): Observable<ResetOffsetsResult> {
    return this.http.post<ResetOffsetsResult>(`/api/consumer-groups/${encodeURIComponent(group)}/reset-offsets`, req);
  }

  produce(topic: string, req: ProduceRequest): Observable<ProduceResult> {
    return this.http.post<ProduceResult>(`/api/topics/${encodeURIComponent(topic)}/messages`, req);
  }

  /**
   * Streaming search (SSE). Matches arrive while the server scans; unsubscribing
   * closes the connection, which cancels the scan on the server.
   * Unlike live(), it must never auto-reconnect: that would restart the search.
   */
  search(topic: string, p: SearchParams): Observable<SearchEvent> {
    return new Observable<SearchEvent>((sub) => {
      const qs = new URLSearchParams({ mode: p.mode, limit: String(p.limit) });
      if (p.filter) qs.set('filter', p.filter);
      if (p.partition !== null && p.partition !== undefined) qs.set('partition', String(p.partition));
      if (p.mode === 'newest' && p.scan) qs.set('scan', String(p.scan));
      if (p.mode === 'range') {
        if (p.from) qs.set('from', p.from);
        if (p.to) qs.set('to', p.to);
      }
      const es = new EventSource(`/api/topics/${encodeURIComponent(topic)}/search?${qs}`);
      let finished = false;
      const end = () => {
        finished = true;
        es.close();
        sub.complete();
      };
      es.addEventListener('start', (e: MessageEvent) => sub.next({ type: 'start', total: JSON.parse(e.data).total }));
      es.addEventListener('batch', (e: MessageEvent) => {
        const d = JSON.parse(e.data);
        sub.next({ type: 'batch', messages: d.messages ?? [], progress: d.progress });
      });
      es.addEventListener('done', (e: MessageEvent) => {
        const d = JSON.parse(e.data);
        sub.next({ type: 'done', reason: d.reason, progress: d.progress, elapsedMs: d.elapsedMs });
        end();
      });
      es.addEventListener('failure', (e: MessageEvent) => {
        sub.next({ type: 'failure', error: JSON.parse(e.data).error });
        end();
      });
      es.onerror = () => {
        if (finished) return;
        // 400/429 responses or a dropped connection: report, don't reconnect.
        sub.next({ type: 'failure', error: 'The search was interrupted (invalid parameters, too many searches running, or connection lost).' });
        end();
      };
      return () => es.close();
    });
  }

  /** Type-checks a CEL expression on the server. Errors with 400 {error, column}. */
  validateFilter(expr: string): Observable<{ ok: true }> {
    return this.http.get<{ ok: true }>('/api/filter/validate', { params: { expr } });
  }

  /**
   * Live tail over Server-Sent Events. The browser reconnects automatically
   * on network errors; unsubscribing closes the stream (and, if this was the
   * last viewer, the backend stops its Kafka consumer).
   */
  live(topic: string, filter = ''): Observable<LiveEvent> {
    return new Observable<LiveEvent>((sub) => {
      const qs = filter ? `?filter=${encodeURIComponent(filter)}` : '';
      const es = new EventSource(`/api/topics/${encodeURIComponent(topic)}/live${qs}`);

      es.addEventListener('ready', () => sub.next({ type: 'ready' }));
      es.addEventListener('batch', (e: MessageEvent) => {
        const data = JSON.parse(e.data);
        sub.next({ type: 'batch', messages: data.messages ?? [], dropped: data.dropped ?? 0, scanned: data.scanned ?? 0 });
      });
      es.addEventListener('failure', (e: MessageEvent) => {
        sub.next({ type: 'failure', error: JSON.parse(e.data).error });
      });
      es.onerror = () => {
        if (es.readyState === EventSource.CLOSED) {
          // Non-200 response (e.g. 404 topic not found): browser gives up.
          sub.next({ type: 'failure', error: 'stream closed by server' });
          sub.complete();
        } else {
          sub.next({ type: 'reconnecting' });
        }
      };
      return () => es.close();
    });
  }
}
