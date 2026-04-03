import { Routes, Route, NavLink } from 'react-router-dom'
import DataCatalog from './pages/DataCatalog'
import LineageGraph from './pages/LineageGraph'
import PipelineHealth from './pages/PipelineHealth'
import MLDashboard from './pages/MLDashboard'

const navItems = [
  { path: '/', label: 'Data Catalog' },
  { path: '/lineage', label: 'Lineage Graph' },
  { path: '/health', label: 'Pipeline Health' },
  { path: '/ml', label: 'ML Dashboard' },
]

export default function App() {
  return (
    <div className="flex h-screen bg-gray-950 text-gray-100">
      {/* Sidebar navigation */}
      <nav className="w-56 bg-gray-900 border-r border-gray-800 flex flex-col p-4 gap-1">
        <h1 className="text-lg font-bold text-white mb-6 px-3">
          Telemetry AI
        </h1>
        {navItems.map(item => (
          <NavLink
            key={item.path}
            to={item.path}
            end={item.path === '/'}
            className={({ isActive }) =>
              `px-3 py-2 rounded text-sm transition-colors ${
                isActive
                  ? 'bg-blue-600 text-white font-medium'
                  : 'text-gray-400 hover:text-white hover:bg-gray-800'
              }`
            }
          >
            {item.label}
          </NavLink>
        ))}
      </nav>

      {/* Main content area */}
      <main className="flex-1 overflow-auto p-6">
        <Routes>
          <Route path="/" element={<DataCatalog />} />
          <Route path="/lineage" element={<LineageGraph />} />
          <Route path="/health" element={<PipelineHealth />} />
          <Route path="/ml" element={<MLDashboard />} />
        </Routes>
      </main>
    </div>
  )
}
