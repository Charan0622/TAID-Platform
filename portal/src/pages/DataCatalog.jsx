import { useState, useEffect } from 'react'
import axios from 'axios'

const API = 'http://localhost:8000/api'

const tableDescriptions = {
  clean_events: 'Validated telemetry events that passed all quality checks',
  dead_letter: 'Rejected events with rejection reasons for investigation',
  hourly_aggregates: 'Hourly statistics per device per metric',
}

const tableIcons = {
  clean_events: (
    <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" /></svg>
  ),
  dead_letter: (
    <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.964-.833-2.732 0L3.34 16.5c-.77.833.192 2.5 1.732 2.5z" /></svg>
  ),
  hourly_aggregates: (
    <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" /></svg>
  ),
}

function LoadingSkeleton() {
  return (
    <div className="space-y-4">
      {[1, 2, 3].map(i => (
        <div key={i} className="h-28 rounded-xl shimmer"></div>
      ))}
    </div>
  )
}

function QualityRing({ score }) {
  if (score == null) return <span className="text-[11px] text-gray-600">N/A</span>
  const pct = Math.round(score * 100)
  const circumference = 2 * Math.PI * 18
  const offset = circumference - (score * circumference)
  const color = score >= 0.9 ? '#22c55e' : score >= 0.7 ? '#eab308' : '#ef4444'

  return (
    <div className="relative w-12 h-12">
      <svg className="w-12 h-12 -rotate-90" viewBox="0 0 44 44">
        <circle cx="22" cy="22" r="18" fill="none" stroke="#1f2937" strokeWidth="3" />
        <circle cx="22" cy="22" r="18" fill="none" stroke={color} strokeWidth="3"
          strokeDasharray={circumference} strokeDashoffset={offset}
          strokeLinecap="round" className="transition-all duration-700" />
      </svg>
      <span className="absolute inset-0 flex items-center justify-center text-[11px] font-bold" style={{ color }}>
        {pct}
      </span>
    </div>
  )
}

export default function DataCatalog() {
  const [tables, setTables] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [expanded, setExpanded] = useState(null)
  const [detail, setDetail] = useState(null)

  useEffect(() => {
    axios.get(`${API}/datasets`)
      .then(res => { setTables(res.data); setLoading(false) })
      .catch(err => { setError(err.message); setLoading(false) })
  }, [])

  const toggleExpand = async (tableName) => {
    if (expanded === tableName) { setExpanded(null); setDetail(null); return }
    setExpanded(tableName)
    try {
      const res = await axios.get(`${API}/datasets/${tableName}`)
      setDetail(res.data)
    } catch { setDetail(null) }
  }

  if (loading) return <div><PageHeader /><LoadingSkeleton /></div>
  if (error) return <div><PageHeader /><ErrorState message={error} /></div>

  const totalRows = tables.reduce((sum, t) => sum + t.row_count, 0)
  const totalSnapshots = tables.reduce((sum, t) => sum + t.snapshot_count, 0)

  return (
    <div>
      <PageHeader />

      {/* Summary stats */}
      <div className="grid grid-cols-3 gap-4 mb-8">
        <StatCard label="Total Tables" value={tables.length} icon={
          <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4" /></svg>
        } color="blue" />
        <StatCard label="Total Events" value={totalRows.toLocaleString()} icon={
          <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M7 12l3-3 3 3 4-4M8 21l4-4 4 4M3 4h18M4 4h16v12a1 1 0 01-1 1H5a1 1 0 01-1-1V4z" /></svg>
        } color="cyan" />
        <StatCard label="Snapshots" value={totalSnapshots} icon={
          <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" /></svg>
        } color="purple" />
      </div>

      {/* Table cards */}
      <div className="space-y-3">
        {tables.map(t => (
          <div key={t.name} className="group">
            <div
              onClick={() => toggleExpand(t.name)}
              className={`rounded-xl border transition-all duration-200 cursor-pointer ${
                expanded === t.name
                  ? 'bg-white/[0.04] border-blue-500/30 glow-blue'
                  : 'bg-white/[0.02] border-white/5 hover:border-white/10 hover:bg-white/[0.03]'
              }`}
            >
              <div className="flex items-center gap-4 p-5">
                {/* Icon */}
                <div className={`w-10 h-10 rounded-lg flex items-center justify-center ${
                  t.name === 'clean_events' ? 'bg-emerald-500/10 text-emerald-400' :
                  t.name === 'dead_letter' ? 'bg-amber-500/10 text-amber-400' :
                  'bg-blue-500/10 text-blue-400'
                }`}>
                  {tableIcons[t.name]}
                </div>

                {/* Info */}
                <div className="flex-1 min-w-0">
                  <h3 className="text-sm font-semibold text-white font-mono">{t.name}</h3>
                  <p className="text-[11px] text-gray-500 mt-0.5">{tableDescriptions[t.name]}</p>
                </div>

                {/* Stats */}
                <div className="flex items-center gap-8 text-right">
                  <div>
                    <p className="text-sm font-semibold text-white">{t.row_count.toLocaleString()}</p>
                    <p className="text-[10px] text-gray-500 uppercase tracking-wider">rows</p>
                  </div>
                  <div>
                    <p className="text-sm font-semibold text-white">{t.snapshot_count}</p>
                    <p className="text-[10px] text-gray-500 uppercase tracking-wider">snapshots</p>
                  </div>
                  <QualityRing score={t.quality_score} />
                  <svg className={`w-4 h-4 text-gray-600 transition-transform duration-200 ${expanded === t.name ? 'rotate-180' : ''}`} fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                  </svg>
                </div>
              </div>

              {/* Expanded schema */}
              {expanded === t.name && detail && (
                <div className="px-5 pb-5 pt-2 border-t border-white/5 animate-in">
                  <p className="text-[10px] font-semibold text-gray-500 uppercase tracking-wider mb-3">Schema ({detail.columns.length} columns)</p>
                  <div className="grid grid-cols-2 lg:grid-cols-4 gap-2">
                    {detail.columns.map(col => (
                      <div key={col.name} className="bg-white/[0.03] border border-white/5 rounded-lg px-3 py-2.5 group/col hover:border-blue-500/20 transition-colors">
                        <div className="flex items-center justify-between">
                          <span className="text-[12px] font-mono font-medium text-blue-300">{col.name}</span>
                          {col.nullable && (
                            <span className="text-[9px] bg-gray-800 text-gray-500 px-1.5 py-0.5 rounded">nullable</span>
                          )}
                        </div>
                        <span className="text-[10px] text-gray-500 mt-1 block">{col.type}</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

function PageHeader() {
  return (
    <div className="mb-8">
      <h2 className="text-2xl font-bold text-white tracking-tight">Data Catalog</h2>
      <p className="text-sm text-gray-500 mt-1">Browse Iceberg tables, schemas, and quality scores</p>
    </div>
  )
}

function StatCard({ label, value, icon, color }) {
  const colors = {
    blue: 'from-blue-500/10 to-blue-500/5 border-blue-500/10 text-blue-400',
    cyan: 'from-cyan-500/10 to-cyan-500/5 border-cyan-500/10 text-cyan-400',
    purple: 'from-violet-500/10 to-violet-500/5 border-violet-500/10 text-violet-400',
    green: 'from-emerald-500/10 to-emerald-500/5 border-emerald-500/10 text-emerald-400',
  }
  return (
    <div className={`bg-gradient-to-br ${colors[color]} rounded-xl border p-4`}>
      <div className="flex items-center justify-between mb-2">
        <span className="text-[10px] font-semibold text-gray-500 uppercase tracking-wider">{label}</span>
        <span className={colors[color]?.split(' ').pop()}>{icon}</span>
      </div>
      <p className="text-2xl font-bold text-white">{value}</p>
    </div>
  )
}

function ErrorState({ message }) {
  return (
    <div className="rounded-xl border border-red-500/20 bg-red-500/5 p-6 text-center">
      <svg className="w-8 h-8 text-red-400 mx-auto mb-2" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.964-.833-2.732 0L3.34 16.5c-.77.833.192 2.5 1.732 2.5z" /></svg>
      <p className="text-sm text-red-300">Failed to load data</p>
      <p className="text-xs text-red-400/60 mt-1">{message}</p>
    </div>
  )
}
