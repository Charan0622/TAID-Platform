import { useState, useEffect, useCallback } from 'react'
import {
  ReactFlow,
  Background,
  Controls,
  useNodesState,
  useEdgesState,
  MarkerType,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import axios from 'axios'

const API = 'http://localhost:8000/api'

const typeConfig = {
  source:     { color: '#3b82f6', bg: 'rgba(59,130,246,0.08)', border: 'rgba(59,130,246,0.3)',  label: 'Source',     icon: 'S' },
  processing: { color: '#f97316', bg: 'rgba(249,115,22,0.08)', border: 'rgba(249,115,22,0.3)',  label: 'Processing', icon: 'P' },
  storage:    { color: '#06b6d4', bg: 'rgba(6,182,212,0.08)',  border: 'rgba(6,182,212,0.3)',   label: 'Storage',    icon: 'D' },
  ml:         { color: '#ef4444', bg: 'rgba(239,68,68,0.08)',  border: 'rgba(239,68,68,0.3)',   label: 'ML',         icon: 'M' },
  api:        { color: '#a855f7', bg: 'rgba(168,85,247,0.08)', border: 'rgba(168,85,247,0.3)',  label: 'API',        icon: 'A' },
}

// Manually position nodes in a logical flow layout
const nodePositions = {
  fake_producer:    { x: 0, y: 0 },
  kafka_raw:        { x: 300, y: 0 },
  stream_processor: { x: 600, y: 0 },
  clean_events:     { x: 900, y: -80 },
  dead_letter:      { x: 900, y: 100 },
  quality_checks:   { x: 1200, y: -160 },
  batch_etl:        { x: 1200, y: -40 },
  hourly_agg:       { x: 1500, y: -40 },
  ml_dataset:       { x: 1200, y: 100 },
  autoencoder:      { x: 1500, y: 100 },
  fastapi:          { x: 1800, y: 0 },
}

function toFlowNodes(apiNodes) {
  return apiNodes.map(node => {
    const config = typeConfig[node.type] || typeConfig.source
    return {
      id: node.id,
      data: {
        label: (
          <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
            <div style={{
              width: 28, height: 28, borderRadius: 6,
              background: config.color, color: 'white',
              display: 'flex', alignItems: 'center', justifyContent: 'center',
              fontSize: 11, fontWeight: 700, flexShrink: 0,
            }}>
              {config.icon}
            </div>
            <div style={{ textAlign: 'left' }}>
              <div style={{ fontSize: 11, fontWeight: 600, color: '#f3f4f6', lineHeight: 1.2 }}>{node.label}</div>
              <div style={{ fontSize: 9, color: '#6b7280', marginTop: 2 }}>{config.label}</div>
            </div>
          </div>
        ),
      },
      position: nodePositions[node.id] || { x: 0, y: 0 },
      style: {
        background: config.bg,
        border: `1px solid ${config.border}`,
        borderRadius: 12,
        padding: '12px 16px',
        minWidth: 200,
        backdropFilter: 'blur(8px)',
      },
    }
  })
}

function toFlowEdges(apiEdges) {
  return apiEdges.map((edge, i) => ({
    id: `e${i}`,
    source: edge.source,
    target: edge.target,
    animated: true,
    style: { stroke: '#374151', strokeWidth: 1.5 },
    markerEnd: { type: MarkerType.ArrowClosed, color: '#4b5563', width: 16, height: 16 },
  }))
}

export default function LineageGraph() {
  const [nodes, setNodes, onNodesChange] = useNodesState([])
  const [edges, setEdges, onEdgesChange] = useEdgesState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [selectedNode, setSelectedNode] = useState(null)
  const [apiNodes, setApiNodes] = useState([])

  useEffect(() => {
    axios.get(`${API}/lineage`)
      .then(res => {
        setApiNodes(res.data.nodes)
        setNodes(toFlowNodes(res.data.nodes))
        setEdges(toFlowEdges(res.data.edges))
        setLoading(false)
      })
      .catch(err => { setError(err.message); setLoading(false) })
  }, [])

  const onNodeClick = useCallback((event, node) => {
    const apiNode = apiNodes.find(n => n.id === node.id)
    setSelectedNode(apiNode || null)
  }, [apiNodes])

  if (loading) return (
    <div>
      <PageHeader />
      <div className="h-[600px] rounded-xl shimmer"></div>
    </div>
  )
  if (error) return <div><PageHeader /><p className="text-red-400 text-sm">Error: {error}</p></div>

  return (
    <div className="h-full flex flex-col">
      <PageHeader />

      {/* Legend */}
      <div className="flex items-center gap-5 mb-4">
        {Object.entries(typeConfig).map(([type, config]) => (
          <div key={type} className="flex items-center gap-2">
            <div className="w-2.5 h-2.5 rounded-sm" style={{ background: config.color }} />
            <span className="text-[11px] text-gray-500">{config.label}</span>
          </div>
        ))}
      </div>

      {/* Graph + detail panel */}
      <div className="flex-1 flex gap-4 min-h-[550px]">
        <div className="flex-1 rounded-xl border border-white/5 bg-white/[0.01] overflow-hidden">
          <ReactFlow
            nodes={nodes}
            edges={edges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onNodeClick={onNodeClick}
            fitView
            fitViewOptions={{ padding: 0.3 }}
            colorMode="dark"
            proOptions={{ hideAttribution: true }}
          >
            <Background color="#1f2937" gap={24} size={1} />
            <Controls
              style={{ background: '#1f2937', border: '1px solid rgba(255,255,255,0.05)', borderRadius: 8 }}
            />
          </ReactFlow>
        </div>

        {/* Detail sidebar */}
        {selectedNode && (
          <div className="w-72 rounded-xl border border-white/5 bg-white/[0.02] p-5 animate-in flex-shrink-0">
            <div className="flex items-center gap-3 mb-4">
              <div className="w-8 h-8 rounded-lg flex items-center justify-center text-xs font-bold text-white"
                style={{ background: typeConfig[selectedNode.type]?.color }}>
                {typeConfig[selectedNode.type]?.icon}
              </div>
              <div>
                <h3 className="text-sm font-semibold text-white">{selectedNode.label}</h3>
                <p className="text-[10px] text-gray-500 capitalize">{selectedNode.type}</p>
              </div>
            </div>
            <p className="text-xs text-gray-400 leading-relaxed">{selectedNode.description}</p>
          </div>
        )}
      </div>
    </div>
  )
}

function PageHeader() {
  return (
    <div className="mb-6">
      <h2 className="text-2xl font-bold text-white tracking-tight">Data Lineage</h2>
      <p className="text-sm text-gray-500 mt-1">Interactive pipeline flow — click a node for details</p>
    </div>
  )
}
