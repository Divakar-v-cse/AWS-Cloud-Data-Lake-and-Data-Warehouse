import { useMemo } from 'react'
import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { num, useApi, type Row } from '../api'
import { Badge, Kpi, Panel, State, Table } from '../ui'

const axis = { stroke: '#64748b', fontSize: 11 }
const grid = '#1e2b45'
const tip = { background: '#0e1728', border: '1px solid #1e2b45', borderRadius: 10 }

type Catalog = { datasets: Row[]; kpis: Row }
type Meta = {
  project: string
  warehouse_engine: string
  lake_backend: string
  lake_root: string
  buckets: string[]
  counts: Record<string, number | null>
  marts: string[]
  date_range: { min_date?: string; max_date?: string }
}

const ts = (v: unknown): string => {
  if (!v) return '—'
  const d = new Date(String(v))
  return isNaN(d.getTime())
    ? String(v)
    : d.toLocaleString(undefined, {
        month: 'short',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
      })
}

export default function Ops() {
  const runs = useApi<Row[]>('/api/ops/runs?limit=40')
  const quality = useApi<Row[]>('/api/ops/quality?limit=200')
  const summary = useApi<Row[]>('/api/ops/quality/summary')
  const marks = useApi<Row[]>('/api/ops/watermarks')
  const meta = useApi<Meta>('/api/meta')
  const catalog = useApi<Catalog>('/api/catalog')

  const dq = useMemo(() => {
    const rows = summary.data ?? []
    return {
      checks: rows.reduce((a, r) => a + Number(r.checks ?? 0), 0),
      passed: rows.reduce((a, r) => a + Number(r.passed ?? 0), 0),
      failed: rows.reduce((a, r) => a + Number(r.failed ?? 0), 0),
      datasets: rows.length,
    }
  }, [summary.data])

  const byStage = useMemo(() => {
    const map = new Map<string, { stage: string; ok: number; failed: number; rows: number }>()
    for (const r of runs.data ?? []) {
      const key = String(r.stage ?? 'unknown')
      const cur = map.get(key) ?? { stage: key, ok: 0, failed: 0, rows: 0 }
      if (String(r.status) === 'success') cur.ok += 1
      else cur.failed += 1
      cur.rows += Number(r.records_out ?? 0)
      map.set(key, cur)
    }
    return [...map.values()]
  }, [runs.data])

  const catalogRows = catalog.data?.datasets ?? []
  const kpis = catalog.data?.kpis ?? {}

  return (
    <div className="space-y-5">
      <div className="grid grid-cols-2 gap-4 md:grid-cols-3 xl:grid-cols-6">
        <Kpi label="Pipeline runs" value={num((runs.data ?? []).length)} hint="latest 40 executions" />
        <Kpi
          label="Run failures"
          value={num((runs.data ?? []).filter((r) => String(r.status) !== 'success').length)}
          tone={(runs.data ?? []).some((r) => String(r.status) !== 'success') ? 'warn' : 'good'}
          hint="non-success runs"
        />
        <Kpi
          label="DQ checks"
          value={num(dq.checks)}
          hint={`${dq.datasets} datasets tested`}
        />
        <Kpi
          label="DQ pass rate"
          value={dq.checks ? `${((dq.passed / dq.checks) * 100).toFixed(1)}%` : '—'}
          tone={dq.failed === 0 ? 'good' : 'warn'}
          hint={`${num(dq.failed)} failed checks`}
        />
        <Kpi
          label="Gold datasets"
          value={num(catalogRows.length)}
          hint={(meta.data?.buckets ?? []).join(' / ') || 'lake not built'}
        />
        <Kpi
          label="Marts"
          value={num((meta.data?.marts ?? []).length)}
          hint="dbt models in marts schema"
        />
      </div>

      <Panel
        title="Pipeline execution log"
        subtitle="ops.pipeline_runs — every stage writes status, throughput and rejections"
        right={
          <span className="rounded-full bg-white/5 px-3 py-1 text-[11px] text-slate-400">
            CloudWatch analogue
          </span>
        }
      >
        <State loading={runs.loading} error={runs.error}>
          <Table
            rows={runs.data ?? []}
            empty="No runs recorded yet — execute the pipeline"
            columns={[
              { key: 'run_id', label: '#' },
              { key: 'pipeline', label: 'Pipeline' },
              { key: 'stage', label: 'Stage' },
              { key: 'source', label: 'Source' },
              { key: 'status', label: 'Status', render: (r) => <Badge status={String(r.status)} /> },
              { key: 'started_at', label: 'Started', render: (r) => ts(r.started_at) },
              {
                key: 'duration_sec',
                label: 'Duration',
                render: (r) => (r.duration_sec == null ? '—' : `${num(r.duration_sec)} s`),
              },
              { key: 'records_in', label: 'In', render: (r) => num(r.records_in) },
              { key: 'records_out', label: 'Out', render: (r) => num(r.records_out) },
              {
                key: 'records_rejected',
                label: 'Rejected',
                render: (r) => (
                  <span className={Number(r.records_rejected) > 0 ? 'text-amber-300' : 'text-slate-300'}>
                    {num(r.records_rejected)}
                  </span>
                ),
              },
            ]}
          />
        </State>
      </Panel>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <Panel title="Stage throughput" subtitle="Records written per pipeline stage">
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={byStage}>
                <CartesianGrid stroke={grid} strokeDasharray="3 3" />
                <XAxis dataKey="stage" {...axis} tickLine={false} />
                <YAxis {...axis} tickFormatter={(v) => num(v)} />
                <Tooltip contentStyle={tip} formatter={(v: any) => num(v)} />
                <Legend wrapperStyle={{ fontSize: 12 }} />
                <Bar dataKey="ok" name="Successful runs" fill="#3ddc97" radius={[4, 4, 0, 0]} />
                <Bar dataKey="failed" name="Failed runs" fill="#f97066" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </Panel>

        <Panel title="Data quality checks" subtitle="ops.data_quality_results — pass/fail by dataset">
          <State loading={summary.loading} error={summary.error}>
            <Table
              rows={summary.data ?? []}
              empty="No quality results yet"
              columns={[
                { key: 'dataset', label: 'Dataset' },
                { key: 'checks', label: 'Checks', render: (r) => num(r.checks) },
                {
                  key: 'passed',
                  label: 'Passed',
                  render: (r) => <span className="text-emerald-300">{num(r.passed)}</span>,
                },
                {
                  key: 'failed',
                  label: 'Failed',
                  render: (r) => (
                    <span className={Number(r.failed) ? 'text-rose-300' : 'text-slate-400'}>
                      {num(r.failed)}
                    </span>
                  ),
                },
              ]}
            />
          </State>
        </Panel>
      </div>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <Panel title="Incremental watermarks" subtitle="High-water mark per source feed">
          <State loading={marks.loading} error={marks.error}>
            <Table
              rows={marks.data ?? []}
              empty="No watermarks recorded"
              columns={[
                { key: 'dataset', label: 'Feed' },
                { key: 'last_value', label: 'Last value', render: (r) => ts(r.last_value) },
                { key: 'updated_at', label: 'Updated', render: (r) => ts(r.updated_at) },
              ]}
            />
          </State>
        </Panel>

        <Panel title="Warehouse row counts" subtitle="Fact and dimension tables in dwh schema">
          <State loading={meta.loading} error={meta.error}>
            <Table
              rows={Object.entries(meta.data?.counts ?? {}).map(([table, c]) => ({ table, rows: c }))}
              columns={[
                { key: 'table', label: 'Table' },
                { key: 'rows', label: 'Rows', render: (r) => num(r.rows) },
              ]}
            />
            <p className="mt-3 text-[11px] text-slate-500">
              {meta.data?.warehouse_engine} · lake backend {meta.data?.lake_backend} ·{' '}
              {meta.data?.date_range?.min_date} → {meta.data?.date_range?.max_date}
            </p>
          </State>
        </Panel>
      </div>

      <Panel
        title="Gold layer catalog"
        subtitle="Glue Data Catalog analogue — Parquet datasets published for Athena"
      >
        <State loading={catalog.loading} error={catalog.error}>
          <Table
            rows={catalogRows}
            empty="Gold catalog not built"
            columns={[
              { key: 'dataset', label: 'Dataset' },
              { key: 'rows', label: 'Rows', render: (r) => num(r.rows) },
              { key: 'columns', label: 'Cols', render: (r) => num((r.columns ?? []).length) },
              { key: 'format', label: 'Format' },
              { key: 'partition_column', label: 'Partition', render: (r) => r.partition_column ?? '—' },
              {
                key: 'location',
                label: 'Location',
                render: (r) => (
                  <span className="font-mono text-[11px] text-slate-400">{String(r.location)}</span>
                ),
              },
            ]}
          />
          <p className="mt-3 text-[11px] text-slate-500">
            {kpis.total_orders ? `${num(kpis.total_orders)} orders · ` : ''}
            {kpis.date_range ? `${kpis.date_range[0]} → ${kpis.date_range[1]}` : ''}
          </p>
        </State>
      </Panel>

      <Panel title="Recent data quality results" subtitle="Latest executed assertions">
        <State loading={quality.loading} error={quality.error}>
          <Table
            rows={(quality.data ?? []).slice(0, 15)}
            empty="No checks yet"
            columns={[
              { key: 'dataset', label: 'Dataset' },
              { key: 'check_name', label: 'Check' },
              { key: 'status', label: 'Status', render: (r) => <Badge status={String(r.status)} /> },
              { key: 'failed_rows', label: 'Failed', render: (r) => num(r.failed_rows) },
              { key: 'total_rows', label: 'Total', render: (r) => num(r.total_rows) },
              { key: 'checked_at', label: 'Checked', render: (r) => ts(r.checked_at) },
            ]}
          />
        </State>
      </Panel>
    </div>
  )
}
