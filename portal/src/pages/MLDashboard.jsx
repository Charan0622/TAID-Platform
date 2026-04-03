import { useState, useEffect } from 'react'
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts'
import axios from 'axios'

const API = 'http://localhost:8000/api'

function MetricCard({ label, value, color }) {
  return (
    <div className="bg-gray-900 rounded-lg border border-gray-800 p-4 text-center">
      <div className="text-xs text-gray-400 mb-1">{label}</div>
      <div className={`text-2xl font-bold ${color || 'text-white'}`}>
        {typeof value === 'number' ? value.toFixed(4) : value}
      </div>
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
        // Auto-select the first experiment
        if (res.data.length > 0) selectExperiment(res.data[0].experiment_id)
      })
      .catch(err => { setError(err.message); setLoading(false) })
  }, [])

  const selectExperiment = async (id) => {
    setSelected(id)
    try {
      const res = await axios.get(`${API}/ml/experiments/${id}`)
      setDetail(res.data)
    } catch (err) {
      setDetail(null)
    }
  }

  // Transform loss data for Recharts
  const lossChartData = detail?.losses ? detail.losses.train.map((tl, i) => ({
    epoch: i + 1,
    train: tl,
    validation: detail.losses.val[i],
  })) : []

  if (loading) return <div className="text-gray-400">Loading experiments...</div>
  if (error) return <div className="text-red-400">Error: {error}</div>

  return (
    <div>
      <h2 className="text-2xl font-bold mb-4">ML Dashboard</h2>
      <p className="text-gray-400 mb-6">Anomaly detection experiment results and training curves</p>

      {/* Experiment list */}
      <div className="bg-gray-900 rounded-lg border border-gray-800 overflow-hidden mb-6">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-800 text-gray-400 text-xs">
              <th className="text-left p-3">Experiment</th>
              <th className="text-right p-3">Val Loss</th>
              <th className="text-right p-3">Epochs</th>
              <th className="text-right p-3">Device</th>
              <th className="text-right p-3">Precision</th>
              <th className="text-right p-3">Recall</th>
              <th className="text-right p-3">F1</th>
            </tr>
          </thead>
          <tbody>
            {experiments.map(exp => (
              <tr
                key={exp.experiment_id}
                onClick={() => selectExperiment(exp.experiment_id)}
                className={`border-b border-gray-800 cursor-pointer transition-colors ${
                  selected === exp.experiment_id ? 'bg-blue-900/30' : 'hover:bg-gray-800/50'
                }`}
              >
                <td className="p-3 font-mono text-xs text-blue-400">{exp.experiment_id}</td>
                <td className="p-3 text-right">{exp.best_val_loss?.toFixed(6)}</td>
                <td className="p-3 text-right">{exp.epochs_completed}</td>
                <td className="p-3 text-right text-gray-400">{exp.device}</td>
                <td className="p-3 text-right">{exp.precision?.toFixed(2) ?? '-'}</td>
                <td className="p-3 text-right">{exp.recall?.toFixed(2) ?? '-'}</td>
                <td className="p-3 text-right font-medium">{exp.f1?.toFixed(2) ?? '-'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Experiment detail */}
      {detail && (
        <>
          {/* Metrics cards */}
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
            <MetricCard label="Best Val Loss" value={detail.results?.best_val_loss} color="text-blue-400" />
            <MetricCard
              label="Precision"
              value={detail.evaluation?.metrics?.precision}
              color="text-green-400"
            />
            <MetricCard
              label="Recall"
              value={detail.evaluation?.metrics?.recall}
              color="text-yellow-400"
            />
            <MetricCard
              label="F1 Score"
              value={detail.evaluation?.metrics?.f1}
              color="text-purple-400"
            />
          </div>

          {/* Training loss curve */}
          {lossChartData.length > 0 && (
            <div className="bg-gray-900 rounded-lg border border-gray-800 p-4 mb-6">
              <h3 className="font-bold text-sm mb-4">Training Loss Curve</h3>
              <ResponsiveContainer width="100%" height={300}>
                <LineChart data={lossChartData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                  <XAxis dataKey="epoch" stroke="#9ca3af" fontSize={12} />
                  <YAxis stroke="#9ca3af" fontSize={12} />
                  <Tooltip
                    contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: '8px' }}
                    labelStyle={{ color: '#9ca3af' }}
                  />
                  <Legend />
                  <Line type="monotone" dataKey="train" stroke="#3b82f6" strokeWidth={2} dot={false} name="Train Loss" />
                  <Line type="monotone" dataKey="validation" stroke="#f97316" strokeWidth={2} dot={false} name="Val Loss" />
                </LineChart>
              </ResponsiveContainer>
            </div>
          )}

          {/* Hyperparameters */}
          {detail.hyperparameters && (
            <div className="bg-gray-900 rounded-lg border border-gray-800 p-4">
              <h3 className="font-bold text-sm mb-3">Hyperparameters</h3>
              <div className="grid grid-cols-2 lg:grid-cols-4 gap-2 text-xs">
                {Object.entries(detail.hyperparameters).map(([k, v]) => (
                  <div key={k} className="bg-gray-800 rounded px-3 py-2">
                    <span className="text-gray-400">{k}: </span>
                    <span className="text-white font-mono">{String(v)}</span>
                  </div>
                ))}
              </div>
            </div>
          )}
        </>
      )}

      {experiments.length === 0 && (
        <div className="text-center text-gray-500 py-12">
          No experiments yet. Run <code className="text-gray-400">python ml/train.py</code> first.
        </div>
      )}
    </div>
  )
}
