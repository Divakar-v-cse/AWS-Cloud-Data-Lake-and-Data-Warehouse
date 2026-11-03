import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  Line,
  LineChart,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { money, num, pct, useApi, type Row } from '../api'
import { Kpi, Panel, State } from '../ui'

const PALETTE = ['#3ddc97', '#5b9cf8', '#f6c85f', '#f97066', '#a78bfa', '#38bdf8', '#f472b6']

const axis = { stroke: '#64748b', fontSize: 11 }
const grid = '#1e2b45'

export default function Overview() {
  const exec = useApi<{ kpis: Row; trend: Row[] }>('/api/executive')
  const segments = useApi<Row[]>('/api/customers/segments')
  const payments = useApi<Row[]>('/api/payments')
  const status = useApi<Row[]>('/api/orders/status')

  const k = exec.data?.kpis ?? {}

  return (
    <div className="space-y-5">
      <State loading={exec.loading} error={exec.error}>
        <div className="grid grid-cols-2 gap-4 md:grid-cols-4">
          <Kpi label="Net revenue" value={money(k.net_revenue)} hint={`${money(k.gross_revenue)} gross`} />
          <Kpi label="Orders" value={num(k.orders)} hint={`${num(k.completed_orders)} completed`} />
          <Kpi label="Avg order value" value={money(k.aov)} hint="net revenue / shipped orders" />
          <Kpi label="Active customers" value={num(k.customers)} hint={`${num(k.products)} products sold`} />
        </div>

        <div className="grid grid-cols-2 gap-4 md:grid-cols-4">
          <Kpi
            label="Revenue growth"
            value={pct(k.revenue_growth_pct)}
            hint="last 30d vs prior 30d"
            tone={Number(k.revenue_growth_pct) >= 0 ? 'good' : 'bad'}
          />
          <Kpi
            label="Order completion"
            value={pct(k.order_completion_rate_pct)}
            hint="of all orders"
            tone="good"
          />
          <Kpi
            label="Payment success"
            value={pct(k.payment_success_rate_pct)}
            hint="all attempts"
            tone={Number(k.payment_success_rate_pct) >= 90 ? 'good' : 'warn'}
          />
          <Kpi
            label="Return rate"
            value={pct(k.return_rate_pct)}
            hint={`${money(k.refunds)} refunded`}
            tone={Number(k.return_rate_pct) <= 8 ? 'good' : 'warn'}
          />
        </div>

        <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
          <Panel
            className="lg:col-span-2"
            title="Net revenue trend"
            subtitle="Monthly net revenue (gross − refunds)"
          >
            <div className="h-72">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={exec.data?.trend ?? []}>
                  <defs>
                    <linearGradient id="rev" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor="#3ddc97" stopOpacity={0.55} />
                      <stop offset="100%" stopColor="#3ddc97" stopOpacity={0.02} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid stroke={grid} strokeDasharray="3 3" />
                  <XAxis dataKey="order_month" {...axis} tickLine={false} />
                  <YAxis {...axis} tickLine={false} tickFormatter={(v) => money(v)} width={70} />
                  <Tooltip
                    contentStyle={{ background: '#0e1728', border: '1px solid #1e2b45', borderRadius: 10 }}
                    formatter={(v: any, n: any) => (n === 'net_revenue' ? money(v) : num(v))}
                  />
                  <Area type="monotone" dataKey="net_revenue" stroke="#3ddc97" fill="url(#rev)" strokeWidth={2} />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          </Panel>

          <Panel title="Order status mix" subtitle="All orders, full history">
            <State loading={status.loading} error={status.error}>
              <div className="h-72">
                <ResponsiveContainer width="100%" height="100%">
                  <PieChart>
                    <Pie
                      data={status.data ?? []}
                      dataKey="orders"
                      nameKey="status"
                      innerRadius={55}
                      outerRadius={90}
                      paddingAngle={3}
                    >
                      {(status.data ?? []).map((_, i) => (
                        <Cell key={i} fill={PALETTE[i % PALETTE.length]} />
                      ))}
                    </Pie>
                    <Legend verticalAlign="bottom" iconType="circle" wrapperStyle={{ fontSize: 12 }} />
                    <Tooltip
                      contentStyle={{ background: '#0e1728', border: '1px solid #1e2b45', borderRadius: 10 }}
                      formatter={(v: any) => num(v)}
                    />
                  </PieChart>
                </ResponsiveContainer>
              </div>
            </State>
          </Panel>
        </div>

        <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
          <Panel title="Customer segments" subtitle="RFM segmentation revenue contribution">
            <State loading={segments.loading} error={segments.error}>
              <div className="h-64">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={segments.data ?? []} layout="vertical" margin={{ left: 24 }}>
                    <CartesianGrid stroke={grid} strokeDasharray="3 3" horizontal={false} />
                    <XAxis type="number" {...axis} tickFormatter={(v) => money(v)} />
                    <YAxis type="category" dataKey="segment" {...axis} width={110} tickLine={false} />
                    <Tooltip
                      contentStyle={{ background: '#0e1728', border: '1px solid #1e2b45', borderRadius: 10 }}
                      formatter={(v: any) => money(v)}
                    />
                    <Bar dataKey="revenue" fill="#5b9cf8" radius={[0, 6, 6, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </State>
          </Panel>

          <Panel title="Payment method success" subtitle="Success rate per method (%)">
            <State loading={payments.loading} error={payments.error}>
              <div className="h-64">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={payments.data ?? []}>
                    <CartesianGrid stroke={grid} strokeDasharray="3 3" />
                    <XAxis dataKey="payment_method" {...axis} tickLine={false} interval={0} angle={-15} height={50} />
                    <YAxis {...axis} domain={[80, 100]} tickFormatter={(v) => `${v}%`} />
                    <Tooltip
                      contentStyle={{ background: '#0e1728', border: '1px solid #1e2b45', borderRadius: 10 }}
                      formatter={(v: any) => pct(v, 2)}
                    />
                    <Line type="monotone" dataKey="success_rate_pct" stroke="#f6c85f" strokeWidth={2} dot={{ r: 3 }} />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            </State>
          </Panel>
        </div>
      </State>
    </div>
  )
}
