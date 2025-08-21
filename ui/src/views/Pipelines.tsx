import React, { useState } from 'react';
import * as yaml from 'js-yaml';
import { apiGet, apiPostEmpty, apiPostJson, PipelineGraph } from '../api';

type ImportResult = { pipeline?: any, message?: string };

function TextArea({value, setValue, rows=12}:{value:string,setValue:(v:string)=>void,rows?:number}){
  return <textarea
    style={{width:'100%', minHeight: rows*18, background:'#0b132a', color:'white', border:'1px solid #1e293b', borderRadius:8, padding:10}}
    value={value}
    onChange={e=>setValue(e.target.value)}
    placeholder='Paste JSON or YAML pipeline spec here...'
  />;
}

export default function Pipelines(){
  const [spec, setSpec] = useState<string>('');
  const [importOut, setImportOut] = useState<ImportResult|null>(null);
  const [pipelineId, setPipelineId] = useState<string>('');
  const [runId, setRunId] = useState<number|null>(null);
  const [graph, setGraph] = useState<PipelineGraph|null>(null);
  const [error, setError] = useState<string|null>(null);

  function parseSpec(text:string){
    try {
      const trimmed = text.trim();
      if(!trimmed) throw new Error('Empty spec');
      if(trimmed.startsWith('{')) return JSON.parse(trimmed);
      return yaml.load(trimmed);
    } catch (e:any) {
      throw new Error('Invalid JSON/YAML: ' + e.message);
    }
  }

  async function doImport(){
    setError(null);
    try {
      const obj = parseSpec(spec);
      const res = await apiPostJson<any>('/pipelines/import', obj);
      setImportOut({ pipeline: res });
      setPipelineId(String(res?.pipeline?.id ?? res?.id ?? ''));
    } catch(e:any){
      setImportOut({ message: String(e.message || e) });
    }
  }

  async function startRun(){
    setError(null);
    try{
      if(!pipelineId) throw new Error('Enter a pipeline id');
      const res = await apiPostEmpty<any>(`/pipelines/${pipelineId}/run`);
      const id = res?.run?.id ?? res?.id;
      setRunId(id);
      if(id) window.location.hash = `#/runs/${id}`;
    }catch(e:any){
      setError(String(e.message||e));
    }
  }

  async function loadGraph(){
    setError(null);
    try{
      if(!pipelineId) throw new Error('Enter a pipeline id');
      const g = await apiGet<PipelineGraph>(`/pipelines/${pipelineId}/graph`);
      setGraph(g);
    }catch(e:any){
      setError(String(e.message||e));
    }
  }

  return (
    <div className="container">
      <div className="card">
        <h2>Import Pipeline</h2>
        <TextArea value={spec} setValue={setSpec} />
        <div style={{display:'flex', gap:8, marginTop:8}}>
          <button className="btn" onClick={doImport}>Import</button>
          <button className="btn" onClick={()=>setSpec(JSON.stringify({
            name: "demo-ui",
            replace_if_exists: true,
            blocks: [
              {"name": "csv", "type":"CSV_READER","config":{"input_path":"/app/data/input.csv"}},
              {"name": "sent", "type":"LLM_SENTIMENT"}
            ],
            edges: [{"from":"csv","to":"sent"}]
          }, null, 2))}>Load Demo Spec</button>
        </div>
        {importOut?.pipeline && <pre style={{marginTop:10}}>{JSON.stringify(importOut.pipeline, null, 2)}</pre>}
        {importOut?.message && <div className="small">{importOut.message}</div>}
      </div>

      <div className="card">
        <h2>Manage Pipeline</h2>
        <div style={{display:'flex', gap:10, alignItems:'center'}}>
          <label>Pipeline ID <input className="input" value={pipelineId} onChange={e=>setPipelineId(e.target.value)} placeholder="e.g. 1" /></label>
          <button className="btn" onClick={startRun}>Start Run</button>
          <button className="btn" onClick={loadGraph}>Load Graph</button>
        </div>
        {error && <div className="small" style={{marginTop:8}}>{error}</div>}
        {runId && <div className="small">Started run #{runId}</div>}
        {graph && (
          <div className="grid cols-2" style={{marginTop:12}}>
            <div className="card">
              <h3>Nodes</h3>
              <table className="table"><thead><tr><th>ID</th><th>Name</th><th>Type</th></tr></thead>
                <tbody>
                  {graph.nodes?.length ? graph.nodes.map(n =>
                    <tr key={n.id}><td className="mono">{n.id}</td><td>{n.name}</td><td>{n.type}</td></tr>
                  ): <tr><td colSpan={3} className="small">No nodes</td></tr>}
                </tbody>
              </table>
            </div>
            <div className="card">
              <h3>Edges</h3>
              <table className="table"><thead><tr><th>ID</th><th>From</th><th>To</th></tr></thead>
                <tbody>
                  {graph.edges?.length ? graph.edges.map(e =>
                    <tr key={e.id}><td className="mono">{e.id}</td><td>{e.from}</td><td>{e.to}</td></tr>
                  ): <tr><td colSpan={3} className="small">No edges</td></tr>}
                </tbody>
              </table>
            </div>
          </div>
        )}
        <div className="small" style={{marginTop:12}}>
          Tip: En Docker Compose, monta tu CSV en <code>./data</code>; dentro del contenedor es <code>/app/data/</code> (usa esa ruta en el spec).
        </div>
      </div>
    </div>
  );
}
