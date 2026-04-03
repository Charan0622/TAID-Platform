import { useState, useEffect } from 'react'
import axios from 'axios'

const API = 'http://localhost:8000/api'

function StatusBadge({ status }) {
  const colors = {
    success: 'bg-green-900 text-green-300',
    healthy: 'bg-green-900 text-green-300',
    failed: 'bg-red-900 text-red-300',
    unhealthy: 'bg-red-900 text-red-300',
    running: 'bg-yellow-900 text-yellow-300',
    degraded: 'bg-yellow-900 text-yellow-300',
  }
  return (
    <span className={`px-2 py-0.5 rounded text-xs font-medium ${colors[status] || 'bg-gray-700 text-gray-300'}`}>
      {status}
    </span>
  )
}

export default function PipelineHealth() {
  const [pipelines, setPipelines] = useState(null)
  const [kafka, setKafka] = useState(null)
  const [storage, setStorage] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  const fetchAll = () => {
    Promise.all([
      axios.get(`${API}/health/pipelines`).catch(() => ({ data: null })),
      axios.get(`${API}/health/kafka`).catch(() => ({ data: null })),
      axios.get(`${API}/health/storage`).catch(() => ({ data: [] })),
    ]).then(([p, k, s]) => {
      setPipelines(p.data)
      setKafka(k.data)
      setStorage(s.data)
      setLoading(false)
    }).catch(err => { setError(err.message); setLoading(false) })
  }

  useEffect(() => {
    fetchAll()
    const interval = setInterval(fetchAll, 30000) // Auto-refresh every 30s
    return () => clearInterval(interval)
  }, [])

  if (loading) return <div className="text-gray-400">Loading health status...</div>
  if (error) return <div className="text-red-400">Error: {error}</div>

  return (
    <div>
      <h2 className="text-2xl font-bold mb-4">Pipeline Health</h2>
      <p className="text-gray-400 mb-6">Real-time status of all platform components (auto-refreshes every 30s)</p>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4 mb-6">
        {/* Kafka health card */}
        <div className="bg-gray-900 rounded-lg border border-gray-800 p-4">
          <div className="flex items-center justify-between mb-3">
            <h3 className="font-bold text-sm">Kafka</h3>
            {kafka && <StatusBadge status={kafka.status} />}
          </div>
          {kafka ? (
            <div className="text-xs text-gray-400 space-y-1">
              <div>Broker: {kafka.broker}</div>
              <div>Topics: {kafka.topics.join(', ') || 'none'}</div>
              <div>Consumer Lag: {kafka.consumer_lag ?? 'N/A'}</div>
            </div>
          ) : (
            <div className="text-xs text-gray-600">Unable to fetch</div>
          )}
        </div>

        {/* Pipeline overall status */}
        <div className="bg-gray-900 rounded-lg border border-gray-800 p-4">
          <div className="flex items-center justify-between mb-3">
            <h3 className="font-bold text-sm">Pipelines</h3>
            {pipelines && <StatusBadge status={pipelines.overall_status} />}
          </div>
          <div className="text-xs text-gray-400">
            {pipelines ? `${pipelines.dags.length} recent DAG runs` : 'Unable to fetch'}
          </div>
        </div>

        {/* Storage freshness */}
        <div className="bg-gray-900 rounded-lg border border-gray-800 p-4">
          <div className="flex items-center justify-between mb-3">
            <h3 className="font-bold text-sm">Storage</h3>
            <StatusBadge status={storage.every(s => !s.is_stale) ? 'healthy' : 'degraded'} />
          </div>
          <div className="text-xs text-gray-400">
            {storage.length} tables tracked
          </div>
        </div>
      </div>

      {/* DAG runs table */}
      {pipelines && pipelines.dags.length > 0 && (
        <div className="bg-gray-900 rounded-lg border border-gray-800 overflow-hidden mb-6">
          <h3 className="font-bold text-sm p-3 border-b border-gray-800">Recent DAG Runs</h3>
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-800 text-gray-400 text-xs">
                <th className="text-left p-3">DAG</th>
                <th className="text-left p-3">Run ID</th>
                <th className="text-right p-3">Duration</th>
                <th className="text-right p-3">Status</th>
              </tr>
            </thead>
            <tbody>
              {pipelines.dags.map((dag, i) => (
                <tr key={i} className="border-b border-gray-800 last:border-0">
                  <td className="p-3 font-medium text-blue-400">{dag.dag_id}</td>
                  <td className="p-3 text-gray-400 text-xs font-mono">{dag.run_id}</td>
                  <td className="p-3 text-right text-gray-400">{dag.duration_seconds}s</td>
                  <td className="p-3 text-right"><StatusBadge status={dag.state} /></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Storage freshness table */}
      <div className="bg-gray-900 rounded-lg border border-gray-800 overflow-hidden">
        <h3 className="font-bold text-sm p-3 border-b border-gray-800">Table Freshness</h3>
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-800 text-gray-400 text-xs">
              <th className="text-left p-3">Table</th>
              <th className="text-right p-3">Rows</th>
              <th className="text-right p-3">Freshness</th>
            </tr>
          </thead>
          <tbody>
            {storage.map(s => (
              <tr key={s.table_name} className="border-b border-gray-800 last:border-0">
                <td className="p-3 font-medium">{s.table_name}</td>
                <td className="p-3 text-right text-gray-400">{s.row_count.toLocaleString()}</td>
                <td className="p-3 text-right">
                  <StatusBadge status={s.is_stale ? 'stale' : 'healthy'} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
