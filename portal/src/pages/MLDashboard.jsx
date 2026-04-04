import { useState, useEffect } from 'react'
import { AreaChart, Area, LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts'
import axios from 'axios'

const API = 'http://localhost:8000/api'

function MetricCard({ label, value, subtitle, color, icon }) {
  const colorMap = {
    blue:   { bg: 'from-blue-500/10 to-blue-500/5', border: 'border-blue-500/15', text: 'text-blue-400', glow: 'glow-blue' },
    green:  { bg: 'from-emerald-500/10 to-emerald-500/5', border: 'border-emerald-500/15', text: 'text-emerald-400', glow: 'glow-green' },
    amber:  { bg: 'from-amber-500/10 to-amber-500/5', border: 'border-amber-500/15', text: 'text-amber-400', glow: 'glow-orange' },
    purple: { bg: 'from-violet-500/10 to-violet-500/5', border: 'border-violet-500/15', text: 'text-violet-400', glow: 'glow-purple' },
  }
  const c = colorMap[color] || colorMap.blue

  return (
    <div className={`bg-gradient-to-br ${c.bg} ${c.border} ${c.glow} rounded-xl border p-5`}>
      <div className="flex items-center justify-between mb-3">
        <span className="text-[10px] font-semibold text-gray-500 uppercase tracking-wider">{label}</span>
        <span className={c.text}>{icon}</span>
      </div>
      <p className={`text-3xl font-bold ${c.text}`}>
        {typeof value === 'number' ? value.toFixed(value < 1 ? 4 : 2) : value ?? '--'}
      </p>
      {subtitle && <p className="text-[10px] text-gray-500 mt-1">{subtitle}</p>}
    </div>
  )
}

const CustomTooltip = ({ active, payload, label }) => {
  if (!active || !payload?.length) return null
  return (
    <div className="bg-gray-900/95 border border-white/10 rounded-lg px-4 py-3 shadow-xl backdrop-blur-sm">
      <p className="text-[10px] text-gray-500 mb-1.5">Epoch {label}</p>
      {payload.map((p, i) => (
        <div key={i} className="flex items-center gap-2 text-[11px]">
          <span className="w-2 h-2 rounded-full" style={{ background: p.color }} />
          <span className="text-gray-400">{p.name}:</span>
          <span className="text-white font-mono font-medium">{p.value.toFixed(6)}</span>
        </div>
      ))}
    </div>
  )
}

export default function MLDashboard() {
  const [experiments, setExperiments] = useState([])
  const [selected, setSelected] = useState(null)
  const [detail, setDetail] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    axios.get(`${API}/ml/experiments`)
      .then(res => {
        setExperiments(res.data)
        setLoading(false)
        if (res.data.length > 0) selectExperiment(res.data[0].experiment_id)
      })
      .catch(err => { setError(err.message); setLoading(false) })
  }, [])

  const selectExperiment = async (id) => {
    setSelected(id)
    try {
      const res = await axios.get(`${API}/ml/experiments/${id}`)
      setDetail(res.data)
    } catch { setDetail(null) }
  }

  const lossChartData = detail?.losses ? detail.losses.train.map((tl, i) => ({
    epoch: i + 1,
    train: tl,
    validation: detail.losses.val[i],
  })) : []

  if (loading) return (
    <div>
      <PageHeader />
      <div className="grid grid-cols-4 gap-4 mb-6">
        {[1,2,3,4].map(i => <div key={i} className="h-28 rounded-xl shimmer" />)}
      </div>
      <div className="h-80 rounded-xl shimmer" />
    </div>
  )
  if (error) return <div><PageHeader /><p className="text-red-400 text-sm">Error: {error}</p></div>

  const metrics = detail?.evaluation?.metrics
  const results = detail?.results

  return (
    <div>
      <PageHeader />

      {/* Metrics row */}
      {detail && (
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
          <MetricCard
            label="Best Val Loss"
            value={results?.best_val_loss}
            subtitle={`${detail.hyperparameters?.epochs_completed ?? '--'} epochs completed`}
            color="blue"
            icon={<svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M2.25 6L9 12.75l4.286-4.286a11.948 11.948 0 014.306 6.43l.776 2.898m0 0l3.182-5.511m-3.182 5.51l-5.511-3.181" /></svg>}
          />
          <MetricCard
            label="Precision"
            value={metrics?.precision}
            subtitle="True positives / predicted positives"
            color="green"
            icon={<svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 12.75L11.25 15 15 9.75M21 12a9 9 0 11-18 0 9 9 0 0118 0z" /></svg>}
          />
          <MetricCard
            label="Recall"
            value={metrics?.recall}
            subtitle="True positives / actual positives"
            color="amber"
            icon={<svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M7.5 14.25v2.25m3-4.5v4.5m3-6.75v6.75m3-9v9M6 20.25h12A2.25 2.25 0 0020.25 18V6A2.25 2.25 0 0018 3.75H6A2.25 2.25 0 003.75 6v12A2.25 2.25 0 006 20.25z" /></svg>}
          />
          <MetricCard
            label="F1 Score"
            value={metrics?.f1}
            subtitle="Harmonic mean of P & R"
            color="purple"
            icon={<svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M11.48 3.499a.562.562 0 011.04 0l2.125 5.111a.563.563 0 00.475.345l5.518.442c.499.04.701.663.321.988l-4.204 3.602a.563.563 0 00-.182.557l1.285 5.385a.562.562 0 01-.84.61l-4.725-2.885a.563.563 0 00-.586 0L6.982 20.54a.562.562 0 01-.84-.61l1.285-5.386a.562.562 0 00-.182-.557l-4.204-3.602a.563.563 0 01.321-.988l5.518-.442a.563.563 0 00.475-.345L11.48 3.5z" /></svg>}
          />
        </div>
      )}

      {/* Training loss chart */}
      {lossChartData.length > 0 && (
        <div className="rounded-xl border border-white/5 bg-white/[0.02] p-6 mb-8">
          <div className="flex items-center justify-between mb-6">
            <div>
              <h3 className="text-sm font-semibold text-white">Training Loss Curve</h3>
              <p className="text-[11px] text-gray-500 mt-0.5">MSE reconstruction error over training epochs</p>
            </div>
            <div className="flex items-center gap-4 text-[11px]">
              <div className="flex items-center gap-1.5">
                <span className="w-3 h-0.5 rounded bg-blue-500"></span>
                <span className="text-gray-500">Train</span>
              </div>
              <div className="flex items-center gap-1.5">
                <span className="w-3 h-0.5 rounded bg-violet-500"></span>
                <span className="text-gray-500">Validation</span>
              </div>
            </div>
          </div>
          <ResponsiveContainer width="100%" height={320}>
            <AreaChart data={lossChartData}>
              <defs>
                <linearGradient id="trainGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.15}/>
                  <stop offset="95%" stopColor="#3b82f6" stopOpacity={0}/>
                </linearGradient>
                <linearGradient id="valGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#8b5cf6" stopOpacity={0.15}/>
                  <stop offset="95%" stopColor="#8b5cf6" stopOpacity={0}/>
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
              <XAxis dataKey="epoch" stroke="#4b5563" fontSize={10} tickLine={false} axisLine={false} />
              <YAxis stroke="#4b5563" fontSize={10} tickLine={false} axisLine={false} />
              <Tooltip content={<CustomTooltip />} />
              <Area type="monotone" dataKey="train" stroke="#3b82f6" strokeWidth={2} fill="url(#trainGrad)" dot={false} name="Train Loss" />
              <Area type="monotone" dataKey="validation" stroke="#8b5cf6" strokeWidth={2} fill="url(#valGrad)" dot={false} name="Val Loss" />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* Experiments table + Hyperparameters */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Experiments list */}
        <div className="rounded-xl border border-white/5 bg-white/[0.02] overflow-hidden">
          <div className="px-5 py-4 border-b border-white/5">
            <h3 className="text-sm font-semibold text-white">Experiments</h3>
          </div>
          <div className="divide-y divide-white/5">
            {experiments.map(exp => (
              <div
                key={exp.experiment_id}
                onClick={() => selectExperiment(exp.experiment_id)}
                className={`flex items-center gap-4 px-5 py-4 cursor-pointer transition-all duration-200 ${
                  selected === exp.experiment_id
                    ? 'bg-blue-500/5 border-l-2 border-l-blue-500'
                    : 'hover:bg-white/[0.02] border-l-2 border-l-transparent'
                }`}
              >
                <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-blue-500/20 to-violet-500/20 border border-white/5 flex items-center justify-center">
                  <svg className="w-3.5 h-3.5 text-blue-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9.75 3.104v5.714a2.25 2.25 0 01-.659 1.591L5 14.5M9.75 3.104c-.251.023-.501.05-.75.082m.75-.082a24.301 24.301 0 014.5 0m0 0v5.714c0 .597.237 1.17.659 1.591L19.8 15.3M14.25 3.104c.251.023.501.05.75.082M19.8 15.3l-1.57.393A9.065 9.065 0 0112 15a9.065 9.065 0 00-6.23.693L5 14.5m14.8.8l1.402 1.402c1.232 1.232.65 3.318-1.067 3.611A48.309 48.309 0 0112 21c-2.773 0-5.491-.235-8.135-.687-1.718-.293-2.3-2.379-1.067-3.61L5 14.5" /></svg>
                </div>
                <div className="flex-1 min-w-0">
                  <p className="text-[12px] font-mono font-medium text-white truncate">{exp.experiment_id}</p>
                  <p className="text-[10px] text-gray-500">{exp.device} &middot; {exp.epochs_completed} epochs</p>
                </div>
                <div className="text-right">
                  <p className="text-[12px] font-medium text-white">{exp.f1?.toFixed(2) ?? '--'}</p>
                  <p className="text-[9px] text-gray-600 uppercase">F1</p>
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* Hyperparameters */}
        {detail?.hyperparameters && (
          <div className="rounded-xl border border-white/5 bg-white/[0.02] overflow-hidden">
            <div className="px-5 py-4 border-b border-white/5">
              <h3 className="text-sm font-semibold text-white">Configuration</h3>
            </div>
            <div className="p-5 space-y-5">
              <div>
                <p className="text-[10px] font-semibold text-gray-500 uppercase tracking-wider mb-2.5">Hyperparameters</p>
                <div className="space-y-1.5">
                  {Object.entries(detail.hyperparameters).map(([k, v]) => (
                    <div key={k} className="flex items-center justify-between py-1.5 border-b border-white/[0.03] last:border-0">
                      <span className="text-[11px] text-gray-400">{k.replace(/_/g, ' ')}</span>
                      <span className="text-[11px] text-white font-mono">{String(v)}</span>
                    </div>
                  ))}
                </div>
              </div>
              {detail.dataset && (
                <div>
                  <p className="text-[10px] font-semibold text-gray-500 uppercase tracking-wider mb-2.5">Dataset</p>
                  <div className="space-y-1.5">
                    {Object.entries(detail.dataset).map(([k, v]) => (
                      <div key={k} className="flex items-center justify-between py-1.5 border-b border-white/[0.03] last:border-0">
                        <span className="text-[11px] text-gray-400">{k.replace(/_/g, ' ')}</span>
                        <span className="text-[11px] text-white font-mono">{String(v ?? 'N/A')}</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}
              {results && (
                <div>
                  <p className="text-[10px] font-semibold text-gray-500 uppercase tracking-wider mb-2.5">Results</p>
                  <div className="space-y-1.5">
                    {Object.entries(results).map(([k, v]) => (
                      <div key={k} className="flex items-center justify-between py-1.5 border-b border-white/[0.03] last:border-0">
                        <span className="text-[11px] text-gray-400">{k.replace(/_/g, ' ')}</span>
                        <span className="text-[11px] text-white font-mono">
                          {typeof v === 'number' ? v.toFixed(v < 1 ? 6 : 2) : String(v)}
                        </span>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          </div>
        )}
      </div>

      {experiments.length === 0 && (
        <div className="text-center py-16 rounded-xl border border-white/5 bg-white/[0.01]">
          <svg className="w-12 h-12 text-gray-700 mx-auto mb-3" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1} d="M9.75 3.104v5.714a2.25 2.25 0 01-.659 1.591L5 14.5M9.75 3.104c-.251.023-.501.05-.75.082m.75-.082a24.301 24.301 0 014.5 0m0 0v5.714c0 .597.237 1.17.659 1.591L19.8 15.3M14.25 3.104c.251.023.501.05.75.082M19.8 15.3l-1.57.393A9.065 9.065 0 0112 15a9.065 9.065 0 00-6.23.693L5 14.5" /></svg>
          <p className="text-sm text-gray-500">No experiments yet</p>
          <p className="text-xs text-gray-600 mt-1">Run <code className="text-gray-400 bg-white/[0.04] px-1.5 py-0.5 rounded">python ml/train.py</code> to get started</p>
        </div>
      )}
    </div>
  )
}

function PageHeader() {
  return (
    <div className="mb-8">
      <h2 className="text-2xl font-bold text-white tracking-tight">ML Dashboard</h2>
      <p className="text-sm text-gray-500 mt-1">Anomaly detection experiments, training curves, and evaluation metrics</p>
    </div>
  )
}
