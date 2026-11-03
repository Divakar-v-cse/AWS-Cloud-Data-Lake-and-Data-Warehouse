import { useState } from 'react'
import {
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
  ZAxis,
} from 'recharts'
import { money, num, pct, useApi, type Row } from '../api'
import { Panel, State, Table } from '../ui'

const axis = { stroke: '#64748b', fontSize: 11 }
const grid = '#1e2b45'
const tip = { background: '#0e1728', border: '1px solid #1e2b45', borderRadius: 10 }

export default function Products() {
  const categories = useApi<Row[]>('/api/products/categories')
  const [category, setCategory] = useState('')
  const [orderBy, setOrderBy] = useState('revenue')
  const products = useApi<Row[]>(
    `/api/products/performance?limit=100&order_by=${orderBy}${category ? `&category=${encodeURIComponent(category)}` : ''}`,
  )

  const scatter = (products.data ?? []).map((p) => ({
    x: Number(p.return_rate_pct),
    y: Number(p.margin_pct),
    z: Number(p.revenue),
    name: p.product_name,
  }))

  return (
    <div className="space-y-5">
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <Panel title="Category performance" subtitle="Revenue and gross margin by category">
          <State loading={categories.loading} error={categories.error}>
            <div className="h-72">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={categories.data ?? []}>
                  <CartesianGrid stroke={grid} strokeDasharray="3 3" />
                  <XAxis dataKey="category" {...axis} interval={0} angle={-20} height={60} tickLine={false} />
                  <YAxis {...axis} tickFormatter={(v) => money(v)} width={70} />
                  <Tooltip contentStyle={tip} formatter={(v: any) => money(v)} />
                  <Bar dataKey="revenue" name="revenue" fill="#5b9cf8" radius={[4, 4, 0, 0]} />
                  <Bar dataKey="profit" name="profit" fill="#3ddc97" radius={[4, 4, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </State>
        </Panel>

        <Panel title="Margin vs return rate" subtitle="Bubble size = revenue. Watch the top-right (high return, high margin) and bottom-right (low margin)" >
          <State loading={products.loading} error={products.error}>
            <div className="h-72">
              <ResponsiveContainer width="100%" height="100%">
                <ScatterChart margin={{ top: 10, right: 16, bottom: 10, left: 0 }}>
                  <CartesianGrid stroke={grid} strokeDasharray="3 3" />
                  <XAxis type="number" dataKey="x" name="return rate %" {...axis} tickFormatter={(v) => `${v}%`} />
                  <YAxis type="number" dataKey="y" name="margin %" {...axis} tickFormatter={(v) => `${v}%`} width={55} />
                  <ZAxis type="number" dataKey="z" range={[20, 400]} />
                  <Tooltip
                    contentStyle={tip}
                    formatter={(v: any, n: any) => [n === 'margin %' || n === 'return rate %' ? pct(v, 2) : money(v), n]}
                    labelFormatter={() => ''}
                  />
                  <Scatter data={scatter} fill="#a78bfa" fillOpacity={0.65} />
                </ScatterChart>
              </ResponsiveContainer>
            </div>
          </State>
        </Panel>
      </div>

      <Panel
        title="Product performance"
        subtitle="Top 100 products by selected metric"
        right={
          <div className="flex gap-2">
            <select
              value={category}
              onChange={(e) => setCategory(e.target.value)}
              className="rounded-md border border-line bg-ink px-2 py-1 text-xs text-slate-200"
            >
              <option value="">All categories</option>
              {(categories.data ?? []).map((c) => (
                <option key={c.category} value={c.category}>
                  {c.category}
                </option>
              ))}
            </select>
            <select
              value={orderBy}
              onChange={(e) => setOrderBy(e.target.value)}
              className="rounded-md border border-line bg-ink px-2 py-1 text-xs text-slate-200"
            >
              <option value="revenue">Revenue</option>
              <option value="profit">Profit</option>
              <option value="units_sold">Units</option>
              <option value="return_rate">Return rate</option>
              <option value="margin_pct">Margin</option>
            </select>
          </div>
        }
      >
        <State loading={products.loading} error={products.error}>
          <Table
            rows={(products.data ?? []).slice(0, 30)}
            columns={[
              { key: 'revenue_rank', label: '#' },
              { key: 'product_name', label: 'Product' },
              { key: 'category', label: 'Category' },
              { key: 'brand', label: 'Brand' },
              { key: 'units_sold', label: 'Units', render: (r) => num(r.units_sold) },
              { key: 'revenue', label: 'Revenue', render: (r) => money(r.revenue) },
              { key: 'profit', label: 'Profit', render: (r) => money(r.profit) },
              { key: 'margin_pct', label: 'Margin', render: (r) => pct(r.margin_pct) },
              { key: 'return_rate_pct', label: 'Returns', render: (r) => pct(r.return_rate_pct, 2) },
            ]}
          />
        </State>
      </Panel>
    </div>
  )
}
