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

export type GraphNode = { id: number; name: string; type: string };
export type GraphEdge = { id: number; from: number; to: number };
export type PipelineGraph = { nodes: GraphNode[]; edges: GraphEdge[] };

export function getHeaders(extra?: HeadersInit): HeadersInit {
  const h: HeadersInit = { 'Accept': 'application/json', ...(extra||{}) };
  const key = localStorage.getItem('po_api_key');
  if (key) (h as any)['X-API-Key'] = key;
  return h;
}

export async function apiGet<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, { headers: getHeaders() });
  if (!res.ok) {
    const text = await res.text().catch(()=> '');
    throw new Error(`${res.status} ${res.statusText}${text? ': '+text:''}`);
  }
  const ct = res.headers.get('content-type') || '';
  if (ct.includes('application/json')) return res.json();
  return (await res.text()) as any;
}

export async function apiPostJson<T>(path: string, body: any): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    method: 'POST',
    headers: getHeaders({'Content-Type':'application/json'}),
    body: JSON.stringify(body)
  });
  if (!res.ok) {
    const text = await res.text().catch(()=> '');
    throw new Error(`${res.status} ${res.statusText}${text? ': '+text:''}`);
  }
  return res.json();
}

export async function apiPostEmpty<T>(path: string, params?: Record<string, any>): Promise<T> {
  const url = new URL(`${API_BASE}${path}`);
  if (params) Object.entries(params).forEach(([k,v]) => url.searchParams.set(k, String(v)));
  const res = await fetch(url.toString(), { method: 'POST', headers: getHeaders() });
  if (!res.ok) {
    const text = await res.text().catch(()=> '');
    throw new Error(`${res.status} ${res.statusText}${text? ': '+text:''}`);
  }
  const ct = res.headers.get('content-type') || '';
  if (ct.includes('application/json')) return res.json();
  return (await res.text()) as any;
}
