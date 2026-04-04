import { Routes, Route, NavLink, useLocation } from 'react-router-dom'
import DataCatalog from './pages/DataCatalog'
import LineageGraph from './pages/LineageGraph'
import PipelineHealth from './pages/PipelineHealth'
import MLDashboard from './pages/MLDashboard'

const navItems = [
  { path: '/', label: 'Data Catalog', icon: (
    <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M20 7l-8-4-8 4m16 0l-8 4m8-4v10l-8 4m0-10L4 7m8 4v10M4 7v10l8 4" /></svg>
  )},
  { path: '/lineage', label: 'Lineage Graph', icon: (
    <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M13 10V3L4 14h7v7l9-11h-7z" /></svg>
  )},
  { path: '/health', label: 'Pipeline Health', icon: (
    <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z" /></svg>
  )},
  { path: '/ml', label: 'ML Dashboard', icon: (
    <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z" /></svg>
  )},
]

export default function App() {
  const location = useLocation()

  return (
    <div className="flex h-screen bg-[#0a0b0f] text-gray-100 overflow-hidden">
      {/* Sidebar */}
      <nav className="w-60 bg-[#0f1117] border-r border-white/5 flex flex-col">
        {/* Logo area */}
        <div className="p-5 pb-6">
          <div className="flex items-center gap-2.5">
            <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-blue-500 to-violet-600 flex items-center justify-center text-white text-sm font-bold shadow-lg shadow-blue-500/20">
              T
            </div>
            <div>
              <h1 className="text-sm font-semibold text-white tracking-tight">Telemetry AI</h1>
              <p className="text-[10px] text-gray-500 font-medium">Data Platform</p>
            </div>
          </div>
        </div>

        {/* Nav links */}
        <div className="flex-1 px-3 space-y-0.5">
          <p className="text-[10px] font-semibold text-gray-600 uppercase tracking-wider px-3 mb-2">Navigation</p>
          {navItems.map(item => (
            <NavLink
              key={item.path}
              to={item.path}
              end={item.path === '/'}
              className={({ isActive }) =>
                `flex items-center gap-2.5 px-3 py-2 rounded-lg text-[13px] transition-all duration-200 ${
                  isActive
                    ? 'bg-gradient-to-r from-blue-600/20 to-violet-600/10 text-white font-medium border border-blue-500/20 shadow-sm shadow-blue-500/5'
                    : 'text-gray-500 hover:text-gray-300 hover:bg-white/[0.03]'
                }`
              }
            >
              {item.icon}
              {item.label}
            </NavLink>
          ))}
        </div>

        {/* Bottom status */}
        <div className="p-4 mx-3 mb-3 rounded-lg bg-white/[0.02] border border-white/5">
          <div className="flex items-center gap-2 text-[11px]">
            <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 pulse-dot"></span>
            <span className="text-gray-500">All systems operational</span>
          </div>
        </div>
      </nav>

      {/* Main content */}
      <main className="flex-1 overflow-auto">
        <div className="p-8 max-w-[1400px]">
          <div key={location.pathname} className="animate-in">
            <Routes>
              <Route path="/" element={<DataCatalog />} />
              <Route path="/lineage" element={<LineageGraph />} />
              <Route path="/health" element={<PipelineHealth />} />
              <Route path="/ml" element={<MLDashboard />} />
            </Routes>
          </div>
        </div>
      </main>
    </div>
  )
}
