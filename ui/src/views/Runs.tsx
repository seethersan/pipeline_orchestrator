import React, { useEffect, useState } from 'react';
import { apiGet, RunItem } from '../api';

export default function Runs({ onOpen }: { onOpen: (id:number)=>void }){
  const [items, setItems] = useState<RunItem[]>([]);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    apiGet<{items:RunItem[]}>('/runs?page=1&page_size=20')
      .then(r => setItems(r.items || []))
      .catch(e => setError(String(e)));
  }, []);
  return (
    <div className="container">
      <div className="card">
        <h2>Runs</h2>
        {error && <div className="small">{error}</div>}
        <table className="table">
          <thead><tr><th>ID</th><th>Pipeline</th><th>Status</th><th>Started</th><th>Finished</th><th></th></tr></thead>
          <tbody>
            {items.length ? items.map(it => (
              <tr key={it.id}>
                <td className="mono">{it.id}</td>
                <td>{it.pipeline_id}</td>
                <td><span className={'badge ' + (it.status==='SUCCEEDED'?'ok':it.status==='FAILED'?'err':'run')}>{it.status}</span></td>
                <td>{it.started_at || '-'}</td>
                <td>{it.finished_at || '-'}</td>
                <td><button className="btn" onClick={() => onOpen(it.id)}>Open</button></td>
              </tr>
            )) : <tr><td colSpan={6} className="small">No data</td></tr>}
          </tbody>
        </table>
      </div>
    </div>
  )
}
