import type { ReactNode } from 'react'

export function Panel({
  title,
  subtitle,
  right,
  children,
  className = '',
}: {
  title?: string
  subtitle?: string
  right?: ReactNode
  children: ReactNode
  className?: string
}) {
  return (
    <section className={`card ${className}`}>
      {(title || right) && (
        <header className="mb-4 flex items-start justify-between gap-4">
          <div>
            {title && <h2 className="text-base font-semibold text-slate-100">{title}</h2>}
            {subtitle && <p className="mt-1 text-xs text-slate-400">{subtitle}</p>}
          </div>
          {right}
        </header>
      )}
      {children}
    </section>
  )
}

export function Kpi({
  label,
  value,
  hint,
  tone = 'default',
}: {
  label: string
  value: string
  hint?: string
  tone?: 'default' | 'good' | 'warn' | 'bad'
}) {
  const tones: Record<string, string> = {
    default: 'text-slate-100',
    good: 'text-accent',
    warn: 'text-amber-300',
    bad: 'text-rose-400',
  }
  return (
    <div className="card p-4">
      <p className="text-[11px] font-semibold uppercase tracking-wider text-slate-400">{label}</p>
      <p className={`mt-2 text-2xl font-semibold ${tones[tone]}`}>{value}</p>
      {hint && <p className="mt-1 text-xs text-slate-500">{hint}</p>}
    </div>
  )
}

export function Table({
  columns,
  rows,
  empty = 'No rows',
}: {
  columns: { key: string; label: string; render?: (row: Record<string, any>) => ReactNode }[]
  rows: Record<string, any>[]
  empty?: string
}) {
  if (!rows.length) return <p className="py-6 text-center text-sm text-slate-500">{empty}</p>
  return (
    <div className="-mx-2 overflow-x-auto">
      <table className="min-w-full border-separate border-spacing-0">
        <thead>
          <tr>
            {columns.map((c) => (
              <th key={c.key} className="th">
                {c.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i} className="border-t border-line/60 hover:bg-white/[0.03]">
              {columns.map((c) => (
                <td key={c.key} className="td border-t border-line/50">
                  {c.render ? c.render(r) : String(r[c.key] ?? '—')}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

export function State({
  loading,
  error,
  children,
}: {
  loading: boolean
  error: string | null
  children: ReactNode
}) {
  if (loading)
    return (
      <div className="flex items-center gap-3 py-10 text-sm text-slate-400">
        <span className="h-4 w-4 animate-spin rounded-full border-2 border-slate-600 border-t-accent" />
        Loading…
      </div>
    )
  if (error)
    return (
      <div className="rounded-lg border border-rose-500/30 bg-rose-500/10 p-4 text-sm text-rose-200">
        {error}
      </div>
    )
  return <>{children}</>
}

export function Badge({ status }: { status: string }) {
  const map: Record<string, string> = {
    success: 'bg-emerald-500/15 text-emerald-300',
    completed: 'bg-emerald-500/15 text-emerald-300',
    ok: 'bg-emerald-500/15 text-emerald-300',
    passed: 'bg-emerald-500/15 text-emerald-300',
    running: 'bg-sky-500/15 text-sky-300',
    pending: 'bg-amber-500/15 text-amber-300',
    failed: 'bg-rose-500/15 text-rose-300',
    cancelled: 'bg-slate-500/15 text-slate-300',
    returned: 'bg-amber-500/15 text-amber-300',
    refunded: 'bg-violet-500/15 text-violet-300',
  }
  const cls = map[String(status).toLowerCase()] ?? 'bg-slate-500/15 text-slate-300'
  return <span className={`pill ${cls}`}>{status}</span>
}
