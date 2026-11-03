import { useMemo } from 'react'
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ComposedChart,
  Legend,
  Line,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { money, num, pct, useApi, type Row } from '../api'
import { Badge, Kpi, Panel, State, Table } from '../ui'

const PALETTE = ['#3ddc97', '#5b9cf8', '#f6c85f', '#f97066', '#a78bfa', '#38bdf8', '#f472b6']
const axis = { stroke: '#64748b', fontSize: 11 }
const grid = '#1e2b45'
const tip = { background: '#0e1728', border: '1px solid #1e2b45', borderRadius: 10 }

export default function Orders() {
  const funnel = useApi<Row[]>('/api/orders/funnel')
  const status = useApi<Row[]>('/api/orders/status')
  const payments = useApi<Row[]>('/api/payments')
  const returns = useApi<Row[]>('/api/returns')
  const fulfil = useApi<Row[]>('/api/fulfilment')
  const recent = useApi<Row[]>('/api/orders/recent?limit=25')

  const totals = useMemo(() => {
    const rows = funnel.data ?? []
    const sum = (k: string) => rows.reduce((a, r) => a + Number(r[k] ?? 0), 0)
    const orders = sum('orders')
    return {
      orders,
      completed: sum('completed_orders'),
      cancelled: sum('cancelled_orders'),
      returned: sum('returned_orders'),
      pending: sum('pending_orders'),
      attempts: sum('payment_attempts'),
      paid: sum('payment_success_count'),
      shipments: sum('shipments'),
      deliveryDays: rows.length
        ? rows.reduce((a, r) => a + Number(r.avg_delivery_days ?? 0) * Number(r.shipments ?? 0), 0) /
          Math.max(sum('shipments'), 1)
        : 0,
      lateRate: rows.length
        ? rows.reduce((a, r) => a + Number(r.late_shipment_rate_pct ?? 0) * Number(r.shipments ?? 0), 0) /
          Math.max(sum('shipments'), 1)
        : 0,
    }
  }, [funnel.data])

  const reasons = useMemo(() => {
    const byReason = new Map<string, { reason: string; returns: number; refund: number }>()
    for (const r of returns.data ?? []) {
      const key = String(r.reason ?? 'unknown')
      const cur = byReason.get(key) ?? { reason: key, returns: 0, refund: 0 }
      cur.returns += Number(r.returns ?? 0)
      cur.refund += Number(r.refund_amount ?? 0)
      byReason.set(key, cur)
    }
    return [...byReason.values()].sort((a, b) => b.refund - a.refund)
  }, [returns.data])

  return (
    <div className="space-y-5">
      <State loading={funnel.loading} error={funnel.error}>
        <div className="grid grid-cols-2 gap-4 md:grid-cols-3 xl:grid-cols-6">
          <Kpi label="Orders" value={num(totals.orders)} hint="all channels" />
          <Kpi
            label="Completion"
            value={pct((totals.completed / Math.max(totals.orders, 1)) * 100)}
            hint={num(totals.completed) + ' completed'}
            tone="good"
          />
          <Kpi
            label="Cancelled"
            value={num(totals.cancelled)}
            hint={pct((totals.cancelled / Math.max(totals.orders, 1)) * 100)}
            tone="warn"
          />
          <Kpi
            label="Returned"
            value={num(totals.returned)}
            hint={pct((totals.returned / Math.max(totals.orders, 1)) * 100)}
            tone="warn"
          />
          <Kpi
            label="Payment success"
            value={pct((totals.paid / Math.max(totals.attempts, 1)) * 100, 2)}
            hint={num(totals.attempts) + ' attempts'}
            tone={totals.paid / Math.max(totals.attempts, 1) >= 0.9 ? 'good' : 'warn'}
          />
          <Kpi
            label="Avg delivery"
            value={`${totals.deliveryDays.toFixed(1)} d`}
            hint={`${pct(totals.lateRate)} late`}
            tone={totals.lateRate <= 15 ? 'good' : 'warn'}
          />
        </div>

        <Panel
          title="Order status funnel"
          subtitle="Monthly order outcomes with completion rate overlay"
        >
          <div className="h-72">
            <ResponsiveContainer width="100%" height="100%">
              <ComposedChart data={funnel.data ?? []}>
                <CartesianGrid stroke={grid} strokeDasharray="3 3" />
                <XAxis dataKey="year_month" {...axis} tickLine={false} />
                <YAxis {...axis} tickLine={false} width={60} tickFormatter={(v) => num(v)} />
                <YAxis yAxisId="r" orientation="right" {...axis} tickFormatter={(v) => `${v}%`} />
                <Tooltip contentStyle={tip} />
                <Legend wrapperStyle={{ fontSize: 12 }} />
                <Bar dataKey="completed_orders" name="Completed" stackId="s" fill="#3ddc97" />
                <Bar dataKey="returned_orders" name="Returned" stackId="s" fill="#f6c85f" />
                <Bar dataKey="cancelled_orders" name="Cancelled" stackId="s" fill="#f97066" />
                <Bar dataKey="pending_orders" name="Pending" stackId="s" fill="#5b9cf8" radius={[4, 4, 0, 0]} />
                <Line
                  yAxisId="r"
                  type="monotone"
                  dataKey="completion_rate_pct"
                  name="Completion %"
                  stroke="#a78bfa"
                  strokeWidth={2}
                  dot={false}
                />
              </ComposedChart>
            </ResponsiveContainer>
          </div>
        </Panel>

        <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
          <Panel title="Payment attempts by method" subtitle="Success vs failed attempts">
            <State loading={payments.loading} error={payments.error}>
              <div className="h-64">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={payments.data ?? []}>
                    <CartesianGrid stroke={grid} strokeDasharray="3 3" />
                    <XAxis dataKey="payment_method" {...axis} tickLine={false} interval={0} angle={-15} height={50} />
                    <YAxis {...axis} tickFormatter={(v) => num(v)} />
                    <Tooltip contentStyle={tip} formatter={(v: any) => num(v)} />
                    <Legend wrapperStyle={{ fontSize: 12 }} />
                    <Bar dataKey="success_count" name="Success" stackId="a" fill="#3ddc97" />
                    <Bar dataKey="failed_count" name="Failed" stackId="a" fill="#f97066" />
                    <Bar dataKey="refunded_count" name="Refunded" stackId="a" fill="#a78bfa" radius={[4, 4, 0, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </State>
          </Panel>

          <Panel title="Refund exposure by reason" subtitle="Returns grouped across all categories">
            <State loading={returns.loading} error={returns.error}>
              <div className="h-64">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={reasons} layout="vertical" margin={{ left: 20 }}>
                    <CartesianGrid stroke={grid} strokeDasharray="3 3" horizontal={false} />
                    <XAxis type="number" {...axis} tickFormatter={(v) => money(v)} />
                    <YAxis type="category" dataKey="reason" {...axis} width={110} tickLine={false} />
                    <Tooltip contentStyle={tip} formatter={(v: any) => money(v)} />
                    <Bar dataKey="refund" name="Refund" radius={[0, 6, 6, 0]}>
                      {reasons.map((_, i) => (
                        <Cell key={i} fill={PALETTE[i % PALETTE.length]} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </State>
          </Panel>
        </div>

        <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
          <Panel title="Fulfilment performance" subtitle="Carrier delivery speed and lateness">
            <State loading={fulfil.loading} error={fulfil.error}>
              <Table
                rows={fulfil.data ?? []}
                columns={[
                  { key: 'carrier', label: 'Carrier' },
                  { key: 'shipments', label: 'Shipments', render: (r) => num(r.shipments) },
                  { key: 'avg_delivery_days', label: 'Avg days', render: (r) => num(r.avg_delivery_days) },
                  { key: 'late_shipments', label: 'Late', render: (r) => num(r.late_shipments) },
                  {
                    key: 'late_rate_pct',
                    label: 'Late rate',
                    render: (r) => (
                      <span className={Number(r.late_rate_pct) > 20 ? 'text-rose-300' : 'text-slate-200'}>
                        {pct(r.late_rate_pct)}
                      </span>
                    ),
                  },
                ]}
              />
            </State>
          </Panel>

          <Panel title="Recent orders" subtitle="Newest transactions from the fact table">
            <State loading={recent.loading} error={recent.error}>
              <Table
                rows={(recent.data ?? []).slice(0, 12)}
                empty="No orders in warehouse"
                columns={[
                  { key: 'order_id', label: 'Order' },
                  { key: 'order_date', label: 'Date' },
                  { key: 'customer_name', label: 'Customer' },
                  { key: 'status', label: 'Status', render: (r) => <Badge status={String(r.status)} /> },
                  { key: 'region', label: 'Region' },
                  { key: 'order_total', label: 'Total', render: (r) => money(r.order_total) },
                ]}
              />
            </State>
          </Panel>
        </div>

        <Panel title="Order status distribution" subtitle="Share of all orders by terminal state">
          <State loading={status.loading} error={status.error}>
            <Table
              rows={status.data ?? []}
              columns={[
                { key: 'status', label: 'Status', render: (r) => <Badge status={String(r.status)} /> },
                { key: 'orders', label: 'Orders', render: (r) => num(r.orders) },
                { key: 'share_pct', label: 'Share', render: (r) => pct(r.share_pct, 2) },
                { key: 'order_value', label: 'Order value', render: (r) => money(r.order_value) },
              ]}
            />
          </State>
        </Panel>
      </State>
    </div>
  )
}
