import { useState, useEffect } from 'react'
import axios from 'axios'

const API = 'http://localhost:8000/api'

export default function DataCatalog() {
  const [tables, setTables] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [expanded, setExpanded] = useState(null) // which table is expanded
  const [detail, setDetail] = useState(null)     // expanded table's detail data

  useEffect(() => {
    axios.get(`${API}/datasets`)
      .then(res => { setTables(res.data); setLoading(false) })
      .catch(err => { setError(err.message); setLoading(false) })
  }, [])

  const toggleExpand = async (tableName) => {
    if (expanded === tableName) {
      setExpanded(null)
      setDetail(null)
      return
    }
    setExpanded(tableName)
    try {
      const res = await axios.get(`${API}/datasets/${tableName}`)
      setDetail(res.data)
    } catch (err) {
      setDetail(null)
    }
  }

  if (loading) return <div className="text-gray-400">Loading datasets...</div>
  if (error) return <div className="text-red-400">Error: {error}</div>

  return (
    <div>
      <h2 className="text-2xl font-bold mb-4">Data Catalog</h2>
      <p className="text-gray-400 mb-6">Browse Iceberg tables, schemas, and quality scores</p>

      <div className="bg-gray-900 rounded-lg border border-gray-800 overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-800 text-gray-400">
              <th className="text-left p-3">Table</th>
              <th className="text-right p-3">Rows</th>
              <th className="text-right p-3">Snapshots</th>
              <th className="text-right p-3">Quality</th>
            </tr>
          </thead>
          <tbody>
            {tables.map(t => (
              <>
                <tr
                  key={t.name}
                  onClick={() => toggleExpand(t.name)}
                  className="border-b border-gray-800 hover:bg-gray-800/50 cursor-pointer"
                >
                  <td className="p-3 font-medium text-blue-400">{t.name}</td>
                  <td className="p-3 text-right">{t.row_count.toLocaleString()}</td>
                  <td className="p-3 text-right">{t.snapshot_count}</td>
                  <td className="p-3 text-right">
                    {t.quality_score != null ? (
                      <span className={`px-2 py-0.5 rounded text-xs font-medium ${
                        t.quality_score >= 0.9 ? 'bg-green-900 text-green-300' :
                        t.quality_score >= 0.7 ? 'bg-yellow-900 text-yellow-300' :
                        'bg-red-900 text-red-300'
                      }`}>
                        {(t.quality_score * 100).toFixed(0)}%
                      </span>
                    ) : (
                      <span className="text-gray-600">N/A</span>
                    )}
                  </td>
                </tr>

                {/* Expanded detail panel */}
                {expanded === t.name && detail && (
                  <tr key={`${t.name}-detail`}>
                    <td colSpan={4} className="p-4 bg-gray-800/30">
                      <h3 className="text-sm font-bold mb-3 text-gray-300">Schema</h3>
                      <div className="grid grid-cols-3 gap-2 mb-4">
                        {detail.columns.map(col => (
                          <div key={col.name} className="bg-gray-800 rounded px-3 py-2 text-xs">
                            <span className="text-blue-300 font-mono">{col.name}</span>
                            <span className="text-gray-500 ml-2">{col.type}</span>
                            {col.nullable && <span className="text-gray-600 ml-1">?</span>}
                          </div>
                        ))}
                      </div>
                    </td>
                  </tr>
                )}
              </>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
