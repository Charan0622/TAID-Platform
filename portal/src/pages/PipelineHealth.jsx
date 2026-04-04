import { useState, useEffect } from 'react'
import axios from 'axios'

const API = 'http://localhost:8000/api'

function StatusDot({ status }) {
  const colors = {
    success: 'bg-emerald-400', healthy: 'bg-emerald-400',
    failed: 'bg-red-400', unhealthy: 'bg-red-400',
    running: 'bg-amber-400', degraded: 'bg-amber-400',
    stale: 'bg-red-400',
  }
  return <span className={`w-2 h-2 rounded-full ${colors[status] || 'bg-gray-500'} ${status === 'healthy' || status === 'success' ? 'pulse-dot' : ''}`} />
}

function StatusBadge({ status }) {
  const styles = {
    success: 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20',
    healthy: 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20',
    failed: 'bg-red-500/10 text-red-400 border-red-500/20',
    unhealthy: 'bg-red-500/10 text-red-400 border-red-500/20',
    running: 'bg-amber-500/10 text-amber-400 border-amber-500/20',
    degraded: 'bg-amber-500/10 text-amber-400 border-amber-500/20',
  }
  return (
    <span className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-[11px] font-medium border ${styles[status] || 'bg-gray-800 text-gray-400 border-gray-700'}`}>
      <StatusDot status={status} />
      {status}
    </span>
  )
}

export default function PipelineHealth() {
  const [pipelines, setPipelines] = useState(null)
  const [kafka, setKafka] = useState(null)
  const [storage, setStorage] = useState([])
  const [loading, setLoading] = useState(true)
  const [lastRefresh, setLastRefresh] = useState(new Date())

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
      setLastRefresh(new Date())
    })
  }

  useEffect(() => {
    fetchAll()
    const interval = setInterval(fetchAll, 30000)
    return () => clearInterval(interval)
  }, [])

  if (loading) return (
    <div>
      <PageHeader lastRefresh={lastRefresh} />
      <div className="grid grid-cols-3 gap-4 mb-6">
        {[1,2,3].map(i => <div key={i} className="h-36 rounded-xl shimmer" />)}
      </div>
    </div>
  )

  return (
    <div>
      <PageHeader lastRefresh={lastRefresh} />

      {/* Top health cards */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4 mb-8">
        {/* Kafka */}
        <div className={`rounded-xl border p-5 ${
          kafka?.status === 'healthy'
            ? 'bg-gradient-to-br from-emerald-500/5 to-emerald-500/0 border-emerald-500/15 glow-green'
            : 'bg-white/[0.02] border-red-500/20 glow-red'
        }`}>
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center gap-2.5">
              <div className="w-9 h-9 rounded-lg bg-emerald-500/10 flex items-center justify-center">
                <svg className="w-4.5 h-4.5 text-emerald-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M8.288 15.038a5.25 5.25 0 017.424 0M5.106 11.856c3.807-3.808 9.98-3.808 13.788 0M1.924 8.674c5.565-5.565 14.587-5.565 20.152 0M12.53 18.22l-.53.53-.53-.53a.75.75 0 011.06 0z" /></svg>
              </div>
              <div>
                <h3 className="text-sm font-semibold text-white">Kafka Broker</h3>
                <p className="text-[10px] text-gray-500">Event Streaming</p>
              </div>
            </div>
            {kafka && <StatusBadge status={kafka.status} />}
          </div>
          {kafka && (
            <div className="space-y-2">
              <InfoRow label="Broker" value={kafka.broker} />
              <InfoRow label="Topics" value={kafka.topics.join(', ') || 'none'} />
              <InfoRow label="Consumer Lag" value={String(kafka.consumer_lag ?? 'N/A')} />
            </div>
          )}
        </div>

        {/* Pipelines */}
        <div className={`rounded-xl border p-5 ${
          pipelines?.overall_status === 'healthy'
            ? 'bg-gradient-to-br from-blue-500/5 to-blue-500/0 border-blue-500/15 glow-blue'
            : 'bg-white/[0.02] border-amber-500/20 glow-orange'
        }`}>
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center gap-2.5">
              <div className="w-9 h-9 rounded-lg bg-blue-500/10 flex items-center justify-center">
                <svg className="w-4.5 h-4.5 text-blue-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M3.75 6A2.25 2.25 0 016 3.75h2.25A2.25 2.25 0 0110.5 6v2.25a2.25 2.25 0 01-2.25 2.25H6a2.25 2.25 0 01-2.25-2.25V6zM3.75 15.75A2.25 2.25 0 016 13.5h2.25a2.25 2.25 0 012.25 2.25V18a2.25 2.25 0 01-2.25 2.25H6A2.25 2.25 0 013.75 18v-2.25zM13.5 6a2.25 2.25 0 012.25-2.25H18A2.25 2.25 0 0120.25 6v2.25A2.25 2.25 0 0118 10.5h-2.25a2.25 2.25 0 01-2.25-2.25V6zM13.5 15.75a2.25 2.25 0 012.25-2.25H18a2.25 2.25 0 012.25 2.25V18A2.25 2.25 0 0118 20.25h-2.25A2.25 2.25 0 0113.5 18v-2.25z" /></svg>
              </div>
              <div>
                <h3 className="text-sm font-semibold text-white">Airflow Pipelines</h3>
                <p className="text-[10px] text-gray-500">Orchestration</p>
              </div>
            </div>
            {pipelines && <StatusBadge status={pipelines.overall_status} />}
          </div>
          <div className="flex items-center gap-2">
            {pipelines?.dags.map((d, i) => (
              <div key={i} className={`h-6 flex-1 rounded-md flex items-center justify-center text-[9px] font-medium ${
                d.state === 'success' ? 'bg-emerald-500/15 text-emerald-400' : 'bg-red-500/15 text-red-400'
              }`}>
                {d.state === 'success' ? 'OK' : 'FAIL'}
              </div>
            ))}
          </div>
          <p className="text-[10px] text-gray-500 mt-2">{pipelines?.dags.length ?? 0} recent runs</p>
        </div>

        {/* Storage */}
        <div className={`rounded-xl border p-5 ${
          storage.every(s => !s.is_stale)
            ? 'bg-gradient-to-br from-violet-500/5 to-violet-500/0 border-violet-500/15 glow-purple'
            : 'bg-white/[0.02] border-red-500/20 glow-red'
        }`}>
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center gap-2.5">
              <div className="w-9 h-9 rounded-lg bg-violet-500/10 flex items-center justify-center">
                <svg className="w-4.5 h-4.5 text-violet-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M20.25 6.375c0 2.278-3.694 4.125-8.25 4.125S3.75 8.653 3.75 6.375m16.5 0c0-2.278-3.694-4.125-8.25-4.125S3.75 4.097 3.75 6.375m16.5 0v11.25c0 2.278-3.694 4.125-8.25 4.125s-8.25-1.847-8.25-4.125V6.375" /></svg>
              </div>
              <div>
                <h3 className="text-sm font-semibold text-white">Iceberg Storage</h3>
                <p className="text-[10px] text-gray-500">Data Lake</p>
              </div>
            </div>
            <StatusBadge status={storage.every(s => !s.is_stale) ? 'healthy' : 'degraded'} />
          </div>
          <div className="space-y-1.5">
            {storage.map(s => (
              <div key={s.table_name} className="flex items-center justify-between text-[11px]">
                <span className="text-gray-400 font-mono">{s.table_name}</span>
                <div className="flex items-center gap-2">
                  <span className="text-gray-500">{s.row_count.toLocaleString()}</span>
                  <StatusDot status={s.is_stale ? 'stale' : 'healthy'} />
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* DAG runs timeline */}
      {pipelines && pipelines.dags.length > 0 && (
        <div className="rounded-xl border border-white/5 bg-white/[0.02] overflow-hidden">
          <div className="flex items-center justify-between px-5 py-4 border-b border-white/5">
            <h3 className="text-sm font-semibold text-white">Recent DAG Runs</h3>
            <span className="text-[10px] text-gray-500">{pipelines.dags.length} runs</span>
          </div>
          <div className="divide-y divide-white/5">
            {pipelines.dags.map((dag, i) => (
              <div key={i} className="flex items-center gap-4 px-5 py-3.5 hover:bg-white/[0.02] transition-colors">
                <StatusDot status={dag.state} />
                <div className="flex-1 min-w-0">
                  <p className="text-[12px] font-medium text-white">{dag.dag_id}</p>
                  <p className="text-[10px] text-gray-500 font-mono truncate">{dag.run_id}</p>
                </div>
                <div className="text-right">
                  <p className="text-[12px] text-gray-300">{dag.duration_seconds}s</p>
                  <p className="text-[10px] text-gray-600">duration</p>
                </div>
                <StatusBadge status={dag.state} />
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

function PageHeader({ lastRefresh }) {
  return (
    <div className="flex items-end justify-between mb-8">
      <div>
        <h2 className="text-2xl font-bold text-white tracking-tight">Pipeline Health</h2>
        <p className="text-sm text-gray-500 mt-1">Real-time status of all platform components</p>
      </div>
      {lastRefresh && (
        <div className="flex items-center gap-2 text-[11px] text-gray-600">
          <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 pulse-dot"></span>
          Live &middot; refreshes every 30s
        </div>
      )}
    </div>
  )
}

function InfoRow({ label, value }) {
  return (
    <div className="flex items-center justify-between text-[11px]">
      <span className="text-gray-500">{label}</span>
      <span className="text-gray-300 font-mono">{value}</span>
    </div>
  )
}
