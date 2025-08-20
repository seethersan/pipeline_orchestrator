
const API_BASE = import.meta.env.VITE_API_BASE || 'http://localhost:8000';

export type RunItem = {
  id: number;
  pipeline_id: number;
  status: string;
  started_at?: string | null;
  finished_at?: string | null;
  correlation_id?: string | null;
};

export type BlockEvent = {
  ts?: string;
  type?: string;
  block_name?: string;
  worker_id?: string;
};

export type Artifact = {
  id: number;
  block_run_id?: number | null;
  kind: string;
  preview?: { filename?: string };
};

export function getHeaders(): HeadersInit {
  const h: HeadersInit = { 'Accept': 'application/json' };
  const key = localStorage.getItem('po_api_key');
  if (key) (h as any)['X-API-Key'] = key;
  return h;
}

export async function apiGet<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, { headers: getHeaders() });
  if (!res.ok) {
    const text = await res.text().catch(()=>'');
    throw new Error(`${res.status} ${res.statusText}${text? ': '+text:''}`);
  }
  const ct = res.headers.get('content-type') || '';
  if (ct.includes('application/json')) return res.json();
  return (await res.text()) as any;
}
