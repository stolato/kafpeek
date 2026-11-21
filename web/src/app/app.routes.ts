import { Routes } from '@angular/router';

export const routes: Routes = [
  { path: '', pathMatch: 'full', redirectTo: 'topics' },
  { path: 'cluster', title: 'Cluster', loadComponent: () => import('./pages/cluster/cluster').then((m) => m.ClusterPage) },
  { path: 'topics', title: 'Topics', loadComponent: () => import('./pages/topics/topics').then((m) => m.TopicsPage) },
  {
    path: 'topics/:name',
    title: 'Topic',
    loadComponent: () => import('./pages/topic-detail/topic-detail').then((m) => m.TopicDetailPage),
  },
  { path: 'groups', title: 'Consumer groups', loadComponent: () => import('./pages/groups/groups').then((m) => m.GroupsPage) },
  {
    path: 'groups/:name',
    title: 'Consumer group',
    loadComponent: () => import('./pages/group-detail/group-detail').then((m) => m.GroupDetailPage),
  },
  { path: '**', redirectTo: 'topics' },
];
