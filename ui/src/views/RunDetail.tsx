import React, { useEffect, useState } from 'react';
import { apiGet, Artifact, BlockEvent } from '../api';
const API_BASE = (import.meta as any).env?.VITE_API_BASE || 'http://localhost:8000';

export default function RunDetail({ runId, goBack }: { runId:number, goBack: ()=>void }){
  const [timeline, setTimeline] = useState<BlockEvent[]>([]);
  const [progress, setProgress] = useState<any>(null);
  const [artifacts, setArtifacts] = useState<Artifact[]>([]);
  useEffect(()=>{
    apiGet<BlockEvent[]>(`/runs/${runId}/timeline`).then(setTimeline).catch(()=>setTimeline([]));
    apiGet(`/runs/${runId}/progress`).then(setProgress).catch(()=>setProgress(null));
    apiGet<Artifact[]>(`/runs/${runId}/artifacts`).then(setArtifacts).catch(()=>setArtifacts([]));
  }, [runId]);

  async function download(id:number){
    try{
  const s = await apiGet<{url:string}>(`/artifacts/${id}/sign`);
  // Ensure absolute URL targeting the API host
  const url = new URL(s.url, API_BASE).toString();
  window.open(url, '_blank');
    }catch{
  const fallback = new URL(`/artifacts/${id}/download`, API_BASE).toString();
  window.open(fallback, '_blank');
    }
  }

  return (
    <div className="container">
      <div className="card">
        <button className="btn" onClick={goBack}>← Back</button>
        <h2>Run <span className="mono">#{runId}</span></h2>
        <div className="grid cols-2">
          <div className="card">
            <h3>Timeline</h3>
            <table className="table">
              <thead><tr><th>Time</th><th>Event</th><th>Block</th><th>Worker</th></tr></thead>
              <tbody>
                {timeline.length ? timeline.map((e, i) => (
                  <tr key={i}>
                    <td>{e.ts || '-'}</td>
                    <td className="mono">{e.type || ''}</td>
                    <td>{e.block_name || ''}</td>
                    <td className="small">{e.worker_id || ''}</td>
                  </tr>
                )) : <tr><td colSpan={4} className="small">No events</td></tr>}
              </tbody>
            </table>
          </div>
          <div className="card">
            <h3>Progress</h3>
            <pre>{progress ? JSON.stringify(progress, null, 2) : 'No data'}</pre>
          </div>
        </div>
      </div>

      <div className="card">
        <h3>Artifacts</h3>
        <table className="table">
          <thead><tr><th>ID</th><th>BlockRun</th><th>Kind</th><th>Name</th><th></th></tr></thead>
          <tbody>
            {artifacts.length ? artifacts.map(a => (
              <tr key={a.id}>
                <td className="mono">{a.id}</td>
                <td>{a.block_run_id ?? '-'}</td>
                <td>{a.kind}</td>
                <td>{a.preview?.filename ?? '-'}</td>
                <td><button className="btn" onClick={() => download(a.id)}>Download</button></td>
              </tr>
            )) : <tr><td colSpan={5} className="small">No artifacts</td></tr>}
          </tbody>
        </table>
      </div>
    </div>
  )
}
