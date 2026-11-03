import { useMemo, useState } from 'react'
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { money, num, pct, useApi, type Row } from '../api'
import { Kpi, Panel, State, Table } from '../ui'

const axis = { stroke: '#64748b', fontSize: 11 }
const grid = '#1e2b45'
const tip = { background: '#0e1728', border: '1px solid #1e2b45', borderRadius: 10 }
const HEAT = ['#0f2236', '#123a4d', '#155e63', '#1f8f6b', '#3ddc97', '#8ef0c0']

export default function Customers() {
  const summary = useApi<Row>('/api/customers/summary')
  const segments = useApi<Row[]>('/api/customers/segments')
  const cohorts = useApi<Row[]>('/api/customers/cohorts')
  const [segment, setSegment] = useState('')
  const rfm = useApi<Row[]>(`/api/customers/rfm?limit=100${segment ? `&segment=${encodeURIComponent(segment)}` : ''}`)

  const matrix = useMemo(() => {
    const rows = cohorts.data ?? []
    const byCohort = new Map<string, Row[]>()
    for (const r of rows) {
      const key = String(r.cohort_month).slice(0, 7)
      if (!byCohort.has(key)) byCohort.set(key, [])
      byCohort.get(key)!.push(r)
    }
    const maxIdx = rows.reduce((m, r) => Math.max(m, Number(r.month_index)), 0)
    return { byCohort, maxIdx: Math.min(maxIdx, 12) }
  }, [cohorts.data])

  const barData = (segments.data ?? []).map((s) => ({ ...s, avg_ltv: Number(s.avg_ltv) }))

  return (
    <div className="space-y-5">
      <State loading={summary.loading} error={summary.error}>
        <div className="grid grid-cols-2 gap-4 md:grid-cols-5">
          <Kpi label="Customers" value={num(summary.data?.customers)} />
          <Kpi label="Repeat customers" value={num(summary.data?.repeat_customers)} />
          <Kpi
            label="Repeat rate"
            value={pct(summary.data?.repeat_rate_pct)}
            tone={Number(summary.data?.repeat_rate_pct) >= 40 ? 'good' : 'warn'}
          />
          <Kpi label="Avg lifetime value" value={money(summary.data?.avg_ltv)} />
          <Kpi label="Avg orders / customer" value={num(summary.data?.avg_orders)} />
        </div>
      </State>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <Panel title="RFM segments" subtitle="Average monetary value per segment">
          <State loading={segments.loading} error={segments.error}>
            <div className="h-72">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={barData} layout="vertical" margin={{ left: 28 }}>
                  <CartesianGrid stroke={grid} strokeDasharray="3 3" horizontal={false} />
                  <XAxis type="number" {...axis} tickFormatter={(v) => money(v)} />
                  <YAxis type="category" dataKey="segment" {...axis} width={120} tickLine={false} />
                  <Tooltip contentStyle={tip} formatter={(v: any) => money(v)} />
                  <Bar dataKey="avg_ltv" fill="#3ddc97" radius={[0, 6, 6, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </State>
        </Panel>

        <Panel title="Segment economics" subtitle="Customers, revenue and recency by segment">
          <State loading={segments.loading} error={segments.error}>
            <Table
              rows={segments.data ?? []}
              columns={[
                { key: 'segment', label: 'Segment' },
                { key: 'customers', label: 'Customers', render: (r) => num(r.customers) },
                { key: 'revenue', label: 'Revenue', render: (r) => money(r.revenue) },
                { key: 'avg_orders', label: 'Avg orders', render: (r) => num(r.avg_orders) },
                { key: 'avg_recency_days', label: 'Recency (d)', render: (r) => num(r.avg_recency_days) },
              ]}
            />
          </State>
        </Panel>
      </div>

      <Panel title="Cohort retention" subtitle="% of each acquisition cohort ordering again, by month index">
        <State loading={cohorts.loading} error={cohorts.error}>
          <div className="overflow-x-auto">
            <table className="min-w-full text-xs">
              <thead>
                <tr>
                  <th className="th sticky left-0 bg-panel">Cohort</th>
                  <th className="th">Size</th>
                  {Array.from({ length: matrix.maxIdx + 1 }, (_, i) => (
                    <th key={i} className="th">
                      M{i}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {Array.from(matrix.byCohort.entries()).map(([cohort, rows]) => {
                  const size = Number(rows[0]?.cohort_size ?? 0)
                  return (
                    <tr key={cohort}>
                      <td className="td sticky left-0 bg-panel font-medium">{cohort}</td>
                      <td className="td text-slate-400">{num(size)}</td>
                      {Array.from({ length: matrix.maxIdx + 1 }, (_, i) => {
                        const cell = rows.find((r) => Number(r.month_index) === i)
                        const v = cell ? Number(cell.retention_pct) : null
                        const intensity = v === null ? 0 : Math.min(5, Math.floor(v / 20))
                        return (
                          <td key={i} className="px-1 py-1">
                            <div
                              className="rounded px-2 py-1 text-center text-[11px] text-slate-100"
                              style={{ background: v === null ? 'transparent' : HEAT[intensity] }}
                              title={cell ? `${v}% (${cell.customers} customers)` : 'no activity'}
                            >
                              {v === null ? '' : `${v.toFixed(0)}%`}
                            </div>
                          </td>
                        )
                      })}
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        </State>
      </Panel>

      <Panel
        title="Top customers"
        subtitle="Ranked by lifetime monetary value"
        right={
          <select
            value={segment}
            onChange={(e) => setSegment(e.target.value)}
            className="rounded-md border border-line bg-ink px-2 py-1 text-xs text-slate-200"
          >
            <option value="">All segments</option>
            {(segments.data ?? []).map((s) => (
              <option key={s.segment} value={s.segment}>
                {s.segment}
              </option>
            ))}
          </select>
        }
      >
        <State loading={rfm.loading} error={rfm.error}>
          <Table
            rows={(rfm.data ?? []).slice(0, 25)}
            columns={[
              { key: 'customer_name', label: 'Customer' },
              { key: 'region', label: 'Region' },
              { key: 'segment', label: 'Segment' },
              { key: 'orders', label: 'Orders', render: (r) => num(r.orders) },
              { key: 'monetary', label: 'Lifetime value', render: (r) => money(r.monetary) },
              { key: 'avg_order_value', label: 'AOV', render: (r) => money(r.avg_order_value) },
              { key: 'recency_days', label: 'Recency', render: (r) => `${num(r.recency_days)} d` },
              {
                key: 'rfm',
                label: 'R / F / M',
                render: (r) => (
                  <span className="font-mono text-xs text-slate-300">
                    {r.r_score}·{r.f_score}·{r.m_score}
                  </span>
                ),
              },
            ]}
          />
        </State>
      </Panel>
    </div>
  )
}
