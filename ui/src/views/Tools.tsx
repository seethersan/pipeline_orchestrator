import React, { useState } from 'react';
import { apiGet, apiPostEmpty, apiPostJson } from '../api';

export default function Tools(){
  const [runId, setRunId] = useState<string>('');
  const [queue, setQueue] = useState<any>(null);
  const [days, setDays] = useState<string>('7');
  const [cleanupOut, setCleanupOut] = useState<any>(null);

  // Streams
  const [topic, setTopic] = useState<string>('pipeline_events');
  const [key, setKey] = useState<string>('ui');
  const [payload, setPayload] = useState<string>('{"hello":"world"}');
  const [publishOut, setPublishOut] = useState<any>(null);
  const [consumeOut, setConsumeOut] = useState<any>(null);

  async function getQueue(){
    const r = await apiGet<any>(`/queue/size?run_id=${encodeURIComponent(runId)}`);
    setQueue(r);
  }

  async function cleanup(){
    const out = await apiPostEmpty<any>('/admin/cleanup', { older_than_days: Number(days||'7') });
    setCleanupOut(out);
  }

  async function publish(){
    let body:any;
    try{ body = JSON.parse(payload); }catch(e:any){ alert("Invalid JSON payload"); return; }
    const out = await apiPostJson<any>('/stream/publish', { topic, key, value: body });
    setPublishOut(out);
  }

  async function consume(){
    const out = await apiGet<any>(`/stream/consume?topic=${encodeURIComponent(topic)}&max_messages=10&timeout_ms=500`);
    setConsumeOut(out);
  }

  return (
    <div className="container">
      <div className="card">
        <h2>Queue & Cleanup</h2>
        <div style={{display:'flex', gap:10, alignItems:'center', flexWrap:'wrap'}}>
          <label>Run ID <input className="input" value={runId} onChange={e=>setRunId(e.target.value)} placeholder="e.g. 1" /></label>
          <button className="btn" onClick={getQueue}>Get Queue Size</button>
          {queue && <span className="small">Queue: {JSON.stringify(queue)}</span>}
        </div>
        <div style={{marginTop:12, display:'flex', gap:10, alignItems:'center'}}>
          <label>Older than days <input className="input" value={days} onChange={e=>setDays(e.target.value)} style={{width:80}}/></label>
          <button className="btn" onClick={cleanup}>Cleanup</button>
        </div>
        {cleanupOut && <pre style={{marginTop:10}}>{JSON.stringify(cleanupOut, null, 2)}</pre>}
      </div>

      <div className="card">
        <h2>Streams (Kafka/Redpanda)</h2>
        <div style={{display:'flex', gap:10, flexWrap:'wrap'}}>
          <label>Topic <input className="input" value={topic} onChange={e=>setTopic(e.target.value)} /></label>
          <label>Key <input className="input" value={key} onChange={e=>setKey(e.target.value)} /></label>
        </div>
        <div style={{marginTop:8}}>
          <textarea value={payload} onChange={e=>setPayload(e.target.value)} style={{width:'100%', minHeight:120, background:'#0b132a', color:'white', border:'1px solid #1e293b', borderRadius:8, padding:10}} />
        </div>
        <div style={{display:'flex', gap:8, marginTop:8}}>
          <button className="btn" onClick={publish}>Publish</button>
          <button className="btn" onClick={consume}>Consume</button>
        </div>
        {publishOut && <pre style={{marginTop:10}}>{JSON.stringify(publishOut, null, 2)}</pre>}
        {consumeOut && <pre style={{marginTop:10}}>{JSON.stringify(consumeOut, null, 2)}</pre>}
        <div className="small">Requiere STREAM_BACKEND=kafka y Redpanda corriendo (docker-compose).</div>
      </div>
    </div>
  );
}
