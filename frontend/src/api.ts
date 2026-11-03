import { useEffect, useState } from 'react'

export type Row = Record<string, any>

export async function getJSON<T = Row[]>(path: string): Promise<T> {
  const res = await fetch(path, { headers: { Accept: 'application/json' } })
  if (!res.ok) {
    let detail = res.statusText
    try {
      const body = await res.json()
      detail = body.detail ?? detail
    } catch {
      /* ignore */
    }
    throw new Error(`${res.status} ${detail}`)
  }
  return (await res.json()) as T
}

export function useApi<T = Row[]>(path: string, deps: unknown[] = []) {
  const [data, setData] = useState<T | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    let alive = true
    setLoading(true)
    setError(null)
    getJSON<T>(path)
      .then((d) => {
        if (alive) setData(d)
      })
      .catch((e: Error) => {
        if (alive) setError(e.message)
      })
      .finally(() => {
        if (alive) setLoading(false)
      })
    return () => {
      alive = false
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [path, ...deps])

  return { data, error, loading }
}

export const money = (v: unknown): string => {
  const n = Number(v ?? 0)
  if (!isFinite(n)) return '—'
  if (Math.abs(n) >= 1_000_000) return `$${(n / 1_000_000).toFixed(2)}M`
  if (Math.abs(n) >= 1_000) return `$${(n / 1_000).toFixed(1)}K`
  return `$${n.toFixed(2)}`
}

export const num = (v: unknown): string => {
  const n = Number(v ?? 0)
  if (!isFinite(n)) return '—'
  return n.toLocaleString('en-US', { maximumFractionDigits: 2 })
}

export const pct = (v: unknown, digits = 1): string => {
  const n = Number(v ?? 0)
  if (!isFinite(n)) return '—'
  return `${n.toFixed(digits)}%`
}
