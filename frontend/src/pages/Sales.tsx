import { useState } from 'react'
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  Line,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { money, num, pct, useApi, type Row } from '../api'
import { Panel, State, Table } from '../ui'

const axis = { stroke: '#64748b', fontSize: 11 }
const grid = '#1e2b45'
const tip = { background: '#0e1728', border: '1px solid #1e2b45', borderRadius: 10 }

export default function Sales() {
  const [days, setDays] = useState(365)
  const daily = useApi<Row[]>(`/api/sales/daily?days=${days}`)
  const monthly = useApi<Row[]>('/api/sales/monthly')
  const regional = useApi<Row[]>('/api/sales/regional')
  const category = useApi<Row[]>('/api/sales/category')

  const regionTotals = Object.values(
    (regional.data ?? []).reduce<Record<string, { region: string; revenue: number; orders: number }>>(
      (acc, r) => {
        const key = String(r.region)
        acc[key] = acc[key] ?? { region: key, revenue: 0, orders: 0 }
        acc[key].revenue += Number(r.revenue)
        acc[key].orders += Number(r.orders)
        return acc
      },
      {},
    ),
  ).sort((a, b) => b.revenue - a.revenue)

  return (
    <div className="space-y-5">
      <Panel
        title="Daily net revenue"
        subtitle="Net of refunds, with 7-day moving average"
        right={
          <select
            value={days}
            onChange={(e) => setDays(Number(e.target.value))}
            className="rounded-md border border-line bg-ink px-2 py-1 text-xs text-slate-200"
          >
            <option value={90}>Last 90 days</option>
            <option value={180}>Last 180 days</option>
            <option value={365}>Last 365 days</option>
            <option value={2000}>All time</option>
          </select>
        }
      >
        <State loading={daily.loading} error={daily.error}>
          <div className="h-72">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={daily.data ?? []}>
                <defs>
                  <linearGradient id="d1" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor="#5b9cf8" stopOpacity={0.5} />
                    <stop offset="100%" stopColor="#5b9cf8" stopOpacity={0.02} />
                  </linearGradient>
                </defs>
                <CartesianGrid stroke={grid} strokeDasharray="3 3" />
                <XAxis dataKey="order_date" {...axis} tickLine={false} minTickGap={40} />
                <YAxis {...axis} tickFormatter={(v) => money(v)} width={70} />
                <Tooltip contentStyle={tip} formatter={(v: any, n: any) => (n === 'revenue_7d_ma' ? money(v) : money(v))} />
                <Legend wrapperStyle={{ fontSize: 12 }} />
                <Area type="monotone" dataKey="net_revenue" name="net revenue" stroke="#5b9cf8" fill="url(#d1)" strokeWidth={2} />
                <Line type="monotone" dataKey="revenue_7d_ma" name="7d avg" stroke="#3ddc97" strokeWidth={2} dot={false} />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </State>
      </Panel>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <Panel title="Monthly revenue growth" subtitle="Month-over-month growth %">
          <State loading={monthly.loading} error={monthly.error}>
            <div className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={monthly.data ?? []}>
                  <CartesianGrid stroke={grid} strokeDasharray="3 3" />
                  <XAxis dataKey="order_month" {...axis} tickLine={false} minTickGap={20} />
                  <YAxis {...axis} tickFormatter={(v) => `${v}%`} />
                  <Tooltip contentStyle={tip} formatter={(v: any) => pct(v, 2)} />
                  <Bar dataKey="revenue_growth_pct" fill="#3ddc97" radius={[4, 4, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </State>
        </Panel>

        <Panel title="Revenue by category" subtitle="Product category contribution">
          <State loading={category.loading} error={category.error}>
            <div className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={category.data ?? []}>
                  <CartesianGrid stroke={grid} strokeDasharray="3 3" />
                  <XAxis dataKey="category" {...axis} tickLine={false} interval={0} angle={-20} height={55} />
                  <YAxis {...axis} tickFormatter={(v) => money(v)} width={70} />
                  <Tooltip contentStyle={tip} formatter={(v: any) => money(v)} />
                  <Bar dataKey="revenue" fill="#a78bfa" radius={[4, 4, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </State>
        </Panel>
      </div>

      <Panel title="Regional performance" subtitle="Revenue by region (state-level detail below)">
        <State loading={regional.loading} error={regional.error}>
          <div className="mb-4 grid grid-cols-2 gap-3 md:grid-cols-5">
            {regionTotals.map((r) => (
              <div key={r.region} className="rounded-lg border border-line bg-ink/40 p-3">
                <p className="text-[11px] uppercase tracking-wider text-slate-400">{r.region}</p>
                <p className="mt-1 text-lg font-semibold text-slate-100">{money(r.revenue)}</p>
                <p className="text-xs text-slate-500">{num(r.orders)} orders</p>
              </div>
            ))}
          </div>
          <Table
            rows={(regional.data ?? []).slice(0, 15)}
            columns={[
              { key: 'state', label: 'State' },
              { key: 'region', label: 'Region' },
              { key: 'orders', label: 'Orders', render: (r) => num(r.orders) },
              { key: 'customers', label: 'Customers', render: (r) => num(r.customers) },
              { key: 'revenue', label: 'Revenue', render: (r) => money(r.revenue) },
              { key: 'aov', label: 'AOV', render: (r) => money(r.aov) },
              { key: 'revenue_share_pct', label: 'Share', render: (r) => pct(r.revenue_share_pct, 2) },
            ]}
          />
        </State>
      </Panel>
    </div>
  )
}
