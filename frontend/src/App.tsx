import { useEffect, useState } from 'react'
import { getJSON } from './api'
import Overview from './pages/Overview'
import Sales from './pages/Sales'
import Customers from './pages/Customers'
import Products from './pages/Products'
import Orders from './pages/Orders'
import Ops from './pages/Ops'

type Health = {
  status: string
  warehouse: string
  lake_backend: string
  latency_ms: number
  error: string | null
}

const NAV = [
  { key: 'overview', label: 'Overview', hint: 'Executive KPIs' },
  { key: 'sales', label: 'Sales', hint: 'Revenue & regions' },
  { key: 'customers', label: 'Customers', hint: 'RFM & retention' },
  { key: 'products', label: 'Products', hint: 'Performance & margins' },
  { key: 'orders', label: 'Orders', hint: 'Funnel & fulfilment' },
  { key: 'ops', label: 'Operations', hint: 'Pipeline & data quality' },
] as const

type PageKey = (typeof NAV)[number]['key']

export default function App() {
  const [page, setPage] = useState<PageKey>('overview')
  const [health, setHealth] = useState<Health | null>(null)

  useEffect(() => {
    let alive = true
    const load = () =>
      getJSON<Health>('/api/health')
        .then((h) => alive && setHealth(h))
        .catch(() => alive && setHealth(null))
    load()
    const t = window.setInterval(load, 30000)
    return () => {
      alive = false
      window.clearInterval(t)
    }
  }, [])

  const ok = health?.status === 'ok'

  return (
    <div className="flex min-h-screen">
      <aside className="fixed inset-y-0 left-0 flex w-60 flex-col border-r border-line/70 bg-panel/60 px-4 py-6 backdrop-blur">
        <div className="px-2">
          <p className="text-[10px] font-semibold uppercase tracking-[0.2em] text-accent">
            Data Platform
          </p>
          <h1 className="mt-2 text-[15px] font-semibold leading-tight text-slate-100">
            Divakar Cloud Data Lake
            <span className="block text-slate-400">&amp; Data Warehouse</span>
          </h1>
        </div>

        <nav className="mt-8 space-y-1">
          {NAV.map((n) => (
            <button
              key={n.key}
              onClick={() => setPage(n.key)}
              className={`w-full rounded-lg px-3 py-2 text-left transition ${
                page === n.key
                  ? 'bg-accent/15 text-accent'
                  : 'text-slate-400 hover:bg-white/5 hover:text-slate-200'
              }`}
            >
              <span className="block text-sm font-medium">{n.label}</span>
              <span className="block text-[11px] opacity-70">{n.hint}</span>
            </button>
          ))}
        </nav>

        <div className="mt-auto space-y-2 px-2 text-[11px] text-slate-500">
          <p className="font-semibold uppercase tracking-wider text-slate-400">Stack</p>
          <p>PostgreSQL 17 &middot; Redshift DDL</p>
          <p>S3 lake &middot; Parquet medallion</p>
          <p>dbt 1.9 &middot; FastAPI &middot; React</p>
        </div>
      </aside>

      <main className="ml-60 flex-1 px-8 py-6">
        <header className="mb-6 flex flex-wrap items-end justify-between gap-4">
          <div>
            <h2 className="text-xl font-semibold text-slate-100">
              {NAV.find((n) => n.key === page)?.label}
            </h2>
            <p className="mt-1 text-xs text-slate-400">
              E-commerce analytics &middot; bronze &rarr; silver &rarr; gold &rarr; warehouse
            </p>
          </div>

          <div className="flex items-center gap-3">
            <div className="card flex items-center gap-3 px-4 py-2">
              <span
                className={`h-2.5 w-2.5 rounded-full ${
                  ok ? 'bg-accent' : 'bg-rose-400'
                } ${ok ? 'animate-pulse' : ''}`}
              />
              <div className="leading-tight">
                <p className="text-[10px] font-semibold uppercase tracking-wider text-slate-400">
                  {ok ? 'API healthy' : health ? 'API degraded' : 'API offline'}
                </p>
                <p className="text-[11px] text-slate-500">
                  {health ? `${health.latency_ms} ms · ${health.warehouse}` : 'no response'}
                </p>
              </div>
            </div>
            <a
              className="card px-4 py-3 text-xs text-slate-300 hover:text-accent"
              href="/docs"
              target="_blank"
              rel="noreferrer"
            >
              API docs
            </a>
          </div>
        </header>

        {page === 'overview' && <Overview />}
        {page === 'sales' && <Sales />}
        {page === 'customers' && <Customers />}
        {page === 'products' && <Products />}
        {page === 'orders' && <Orders />}
        {page === 'ops' && <Ops />}
      </main>
    </div>
  )
}
