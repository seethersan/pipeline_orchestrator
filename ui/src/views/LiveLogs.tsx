import React, { useEffect, useRef, useState } from 'react';
const API_BASE = (import.meta as any).env?.VITE_API_BASE || 'http://localhost:8000';

export default function LiveLogs(){
  const [lines, setLines] = useState<string[]>([]);
  const [topic, setTopic] = useState<string>('pipeline_events');
  const esRef = useRef<EventSource | null>(null);

  function start(){
    stop();
    const key = localStorage.getItem('po_api_key');
    const url = new URL(`${API_BASE}/logs/stream`);
    url.searchParams.set('topic', topic);
    if (key) url.searchParams.set('api_key', key); // in case your auth reads query param
    const es = new EventSource(url.toString(), { withCredentials: false });
    es.onmessage = (ev) => {
      setLines(prev => {
        const next = [...prev, ev.data];
        return next.slice(-500);
      });
    };
    es.onerror = () => { /* keep open or reconnect manually */ };
    esRef.current = es;
  }

  function stop(){
    if (esRef.current) { esRef.current.close(); esRef.current = null; }
  }

  useEffect(()=>{ start(); return () => stop(); }, [topic]);

  return (
    <div className="container">
      <div className="card">
        <h2>Live Logs (SSE)</h2>
        <div style={{display:'flex', gap:10, alignItems:'center', marginBottom:8}}>
          <label>Topic <input className="input" value={topic} onChange={e=>setTopic(e.target.value)} /></label>
          <button className="btn" onClick={start}>Reconnect</button>
          <button className="btn" onClick={stop}>Stop</button>
        </div>
        <pre style={{minHeight:240, maxHeight:480, overflow:'auto'}}>{lines.join("\n")}</pre>
        <div className="small">Uses <code>EventSource</code> to <code>/logs/stream</code>. Topic defaults to <code>pipeline_events</code>.</div>
      </div>
    </div>
  );
}
