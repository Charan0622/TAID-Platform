import { useState, useEffect, useCallback } from 'react'
import {
  ReactFlow,
  Background,
  Controls,
  useNodesState,
  useEdgesState,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import axios from 'axios'

const API = 'http://localhost:8000/api'

// Color map for node types
const typeColors = {
  source: '#3b82f6',      // blue
  processing: '#f97316',   // orange
  storage: '#06b6d4',      // cyan
  ml: '#ef4444',           // red
  api: '#a855f7',          // purple
}

// Convert API lineage data to React Flow format
function toFlowNodes(apiNodes) {
  return apiNodes.map((node, i) => ({
    id: node.id,
    data: {
      label: (
        <div className="text-center">
          <div className="font-bold text-xs">{node.label}</div>
          <div className="text-[10px] opacity-70 mt-1">{node.type}</div>
        </div>
      ),
    },
    position: { x: (i % 4) * 250 + 50, y: Math.floor(i / 4) * 150 + 50 },
    style: {
      background: typeColors[node.type] || '#666',
      color: 'white',
      border: 'none',
      borderRadius: '8px',
      padding: '10px 16px',
      fontSize: '12px',
      width: 180,
    },
  }))
}

function toFlowEdges(apiEdges) {
  return apiEdges.map((edge, i) => ({
    id: `e${i}`,
    source: edge.source,
    target: edge.target,
    label: edge.label,
    animated: true,
    style: { stroke: '#4b5563' },
    labelStyle: { fontSize: 9, fill: '#9ca3af' },
  }))
}

export default function LineageGraph() {
  const [nodes, setNodes, onNodesChange] = useNodesState([])
  const [edges, setEdges, onEdgesChange] = useEdgesState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [selected, setSelected] = useState(null)

  useEffect(() => {
    axios.get(`${API}/lineage`)
      .then(res => {
        setNodes(toFlowNodes(res.data.nodes))
        setEdges(toFlowEdges(res.data.edges))
        setLoading(false)
      })
      .catch(err => { setError(err.message); setLoading(false) })
  }, [])

  const onNodeClick = useCallback((event, node) => {
    setSelected(node)
  }, [])

  if (loading) return <div className="text-gray-400">Loading lineage graph...</div>
  if (error) return <div className="text-red-400">Error: {error}</div>

  return (
    <div className="h-full flex flex-col">
      <h2 className="text-2xl font-bold mb-4">Data Lineage</h2>
      <p className="text-gray-400 mb-4">Interactive pipeline flow — click a node for details</p>

      {/* Legend */}
      <div className="flex gap-4 mb-4 text-xs">
        {Object.entries(typeColors).map(([type, color]) => (
          <div key={type} className="flex items-center gap-1.5">
            <div className="w-3 h-3 rounded" style={{ background: color }} />
            <span className="text-gray-400 capitalize">{type}</span>
          </div>
        ))}
      </div>

      {/* Graph */}
      <div className="flex-1 bg-gray-900 rounded-lg border border-gray-800 min-h-[500px]">
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onNodeClick={onNodeClick}
          fitView
          colorMode="dark"
        >
          <Background color="#374151" gap={20} />
          <Controls />
        </ReactFlow>
      </div>
    </div>
  )
}
