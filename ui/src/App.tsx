
import React, { useEffect, useState } from 'react';
import Runs from './views/Runs';
import RunDetail from './views/RunDetail';
import './index.css';

export default function App(){
  const [apiKey, setApiKey] = useState(localStorage.getItem('po_api_key') || '');
  const [route, setRoute] = useState(window.location.hash || '#/runs');
  useEffect(()=>{
    const onHash = () => setRoute(window.location.hash || '#/runs');
    window.addEventListener('hashchange', onHash);
    return () => window.removeEventListener('hashchange', onHash);
  }, []);

  const parts = (route.startsWith('#/')? route.slice(2): route).split('/').filter(Boolean);
  let view = <Runs onOpen={(id)=> location.hash = `#/runs/${id}`} />;
  if(parts[0]==='runs' && parts[1]) view = <RunDetail runId={parseInt(parts[1])} goBack={()=> location.hash = '#/runs'} />;

  return (
    <>
      <div className="topbar">
        <div className="brand">Pipeline Orchestrator</div>
        <div className="controls">
          <label>API Key <input className="input" type="password" value={apiKey} placeholder="X-API-Key" onChange={e=>setApiKey(e.target.value)} /></label>
          <button onClick={()=>localStorage.setItem('po_api_key', apiKey)}>Save</button>
          <a className="link" href="#/runs">Runs</a>
        </div>
      </div>
      {view}
      <div className="footer">Modern UI • React + Vite</div>
    </>
  )
}
