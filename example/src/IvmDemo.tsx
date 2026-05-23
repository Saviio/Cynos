import { useState, useEffect, useCallback, useRef, memo } from 'react'
import {
  type Stock,
  getDatabase,
  getStockCount,
  createStockIvmQuery,
  createStockReQueryQuery,
  insertStocks,
  clearAllStocks,
  deleteStocksKeepFirst,
} from './db'
import { col } from '@cynos/core'
import { Button } from '@/components/ui/button'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Loader2, Play, Square, Database, Zap } from 'lucide-react'
import { cn } from '@/lib/utils'

const PRICE_THRESHOLDS = [100, 200, 300, 500, 700, 900]
const DISPLAY_COLS: (keyof Stock)[] = ['id', 'symbol', 'name', 'price', 'change', 'volume', 'high', 'low', 'pe']
const DISPLAY_LIMIT = 100

type Mode = 'ivm' | 'requery'

interface BenchResult {
  mode: Mode
  totalUpdates: number
  elapsed: number
  ups: number
  notifications: number
}

// Memoized table row for better performance
const StockRow = memo(function StockRow({
  stock,
  changedCells
}: {
  stock: Stock
  changedCells: Set<string>
}) {
  return (
    <tr className="border-b border-white/5 hover:bg-white/[0.02]">
      {DISPLAY_COLS.map(c => {
        const key = `${stock.id}-${c}`
        const isChanged = changedCells.has(key)
        return (
          <td
            key={c}
            className={cn(
              "font-mono text-xs whitespace-nowrap py-2 px-4",
              c === 'id' || c === 'symbol' || c === 'name' ? 'text-left' : 'text-right',
              c === 'id' && 'text-white/30',
              c === 'symbol' && 'font-medium',
              c === 'name' && 'text-white/50',
              isChanged && 'animate-cell-flash'
            )}
          >
            {fmtVal(c, stock[c])}
          </td>
        )
      })}
    </tr>
  )
})

const fmtVal = (col: keyof Stock, value: Stock[keyof Stock]) => {
  if (col === 'price' || col === 'high' || col === 'low')
    return `$${(value as number).toFixed(2)}`
  if (col === 'change')
    return (value as number) >= 0 ? `+${(value as number).toFixed(2)}` : (value as number).toFixed(2)
  if (col === 'volume')
    return (value as number).toLocaleString()
  return String(value)
}

export default function IvmDemo() {
  const [priceThreshold, setPriceThreshold] = useState('100')
  const [totalCount, setTotalCount] = useState(0)
  const [loading, setLoading] = useState(true)
  const [inserting, setInserting] = useState(false)
  const [benchRunning, setBenchRunning] = useState(false)
  const [benchDuration, setBenchDuration] = useState('3')
  const [benchDataScale, setBenchDataScale] = useState('10000')
  const [currentMode, setCurrentMode] = useState<Mode | null>(null)
  const [liveUps, setLiveUps] = useState(0)
  const [liveNotifications, setLiveNotifications] = useState(0)
  const [results, setResults] = useState<BenchResult[]>([])
  const [benchMatchCount, setBenchMatchCount] = useState(0)

  const [stocks, setStocks] = useState<Stock[]>([])
  const [changedCells, setChangedCells] = useState<Set<string>>(new Set())
  const stockMapRef = useRef<Map<number, Stock>>(new Map())
  const sortedCacheRef = useRef<Stock[]>([])
  const unsubLiveRef = useRef<(() => void) | null>(null)
  const highlightCounter = useRef(0)
  const pendingChangedKeys = useRef<string[]>([])

  const applyDelta = useCallback((delta: { added: Stock[], removed: Stock[] }) => {
    const map = stockMapRef.current
    const tick = ++highlightCounter.current
    let needResort = false
    const changedKeys: string[] = []

    const removedById = new Map<number, Stock>()
    for (const row of delta.removed) {
      removedById.set(row.id, row)
    }

    for (const row of delta.removed) {
      if (!delta.added.some(a => a.id === row.id)) {
        map.delete(row.id)
        needResort = true
      }
    }

    for (const row of delta.added) {
      const prev = removedById.get(row.id)
      if (prev) {
        for (const c of DISPLAY_COLS) {
          if (prev[c] !== row[c]) {
            changedKeys.push(`${row.id}-${c}`)
          }
        }
      } else {
        if (!map.has(row.id)) {
          needResort = true
          for (const c of DISPLAY_COLS) {
            changedKeys.push(`${row.id}-${c}`)
          }
        }
      }
      map.set(row.id, row)
    }

    if (needResort || sortedCacheRef.current.length === 0) {
      sortedCacheRef.current = Array.from(map.values()).sort((a, b) => a.id - b.id).slice(0, DISPLAY_LIMIT)
    } else {
      const display = sortedCacheRef.current
      for (let i = 0; i < display.length; i++) {
        const fresh = map.get(display[i].id)
        if (fresh) display[i] = fresh
      }
    }

    // Batch state updates
    setStocks(sortedCacheRef.current.slice())

    if (changedKeys.length > 0) {
      pendingChangedKeys.current.push(...changedKeys)
      setChangedCells(new Set(pendingChangedKeys.current))

      setTimeout(() => {
        pendingChangedKeys.current = pendingChangedKeys.current.filter(k => !changedKeys.includes(k))
        setChangedCells(new Set(pendingChangedKeys.current))
      }, 400)
    }
  }, [])

  const subscribeLive = useCallback(async () => {
    unsubLiveRef.current?.()
    unsubLiveRef.current = null
    stockMapRef.current.clear()
    sortedCacheRef.current = []

    const obs = await createStockIvmQuery(Number(priceThreshold))
    const initial = obs.getResult() as Stock[]
    for (const row of initial) stockMapRef.current.set(row.id, row)
    sortedCacheRef.current = Array.from(stockMapRef.current.values()).sort((a, b) => a.id - b.id).slice(0, DISPLAY_LIMIT)
    setStocks(sortedCacheRef.current.slice())

    let pendingDelta: { added: Stock[], removed: Stock[] } | null = null
    let rafId: number | null = null

    const unsub = obs.subscribe((delta: { added: Stock[], removed: Stock[] }) => {
      if (pendingDelta) {
        for (const r of delta.added) pendingDelta.added.push(r)
        for (const r of delta.removed) pendingDelta.removed.push(r)
      } else {
        pendingDelta = { added: delta.added.slice(), removed: delta.removed.slice() }
      }

      if (rafId === null) {
        rafId = requestAnimationFrame(() => {
          if (pendingDelta) {
            applyDelta(pendingDelta)
            pendingDelta = null
          }
          rafId = null
        })
      }
    })
    unsubLiveRef.current = () => {
      unsub()
      if (rafId !== null) cancelAnimationFrame(rafId)
    }
  }, [priceThreshold, applyDelta])

  useEffect(() => {
    getDatabase().then(() => {
      setTotalCount(getStockCount())
      setLoading(false)
      subscribeLive()
    })
    return () => { unsubLiveRef.current?.() }
  }, [])

  useEffect(() => {
    if (!loading) subscribeLive()
  }, [priceThreshold])

  const stopRef = useRef(false)
  const updatingRef = useRef(false)
  const [updating, setUpdating] = useState(false)
  const [updateUps, setUpdateUps] = useState(0)

  const startUpdates = useCallback(async () => {
    if (updatingRef.current) return
    updatingRef.current = true
    setUpdating(true)
    const db = await getDatabase()
    const numericFields: (keyof Stock)[] = ['price', 'change', 'volume', 'high', 'low', 'pe']
    let windowUpdates = 0
    let windowStart = performance.now()

    while (updatingRef.current) {
      const batchStart = performance.now()
      while (updatingRef.current && performance.now() - batchStart < 16) {
        const maxId = getStockCount()
        const id = Math.floor(Math.random() * maxId) + 1
        const field = numericFields[Math.floor(Math.random() * numericFields.length)]
        let val: number
        switch (field) {
          case 'price': val = Math.round((10 + Math.random() * 990) * 100) / 100; break
          case 'change': val = Math.round((Math.random() - 0.5) * 100 * 100) / 100; break
          case 'volume': val = Math.floor(Math.random() * 1e8); break
          case 'high': case 'low': val = Math.round((10 + Math.random() * 990) * 100) / 100; break
          case 'pe': val = Math.round((5 + Math.random() * 95) * 10) / 10; break
          default: val = 0
        }
        await db.update('stocks').set(field, val).where(col('id').eq(id)).exec()
        windowUpdates++
      }
      const now = performance.now()
      const windowElapsed = now - windowStart
      if (windowElapsed >= 500) { // Update UI every 500ms instead of 1000ms
        setUpdateUps(Math.round(windowUpdates / (windowElapsed / 1000)))
        windowUpdates = 0
        windowStart = now
      }
      await new Promise(r => setTimeout(r, 0))
    }
    setUpdating(false)
    setUpdateUps(0)
  }, [])

  const stopUpdates = useCallback(() => {
    updatingRef.current = false
  }, [])

  const runBench = useCallback(async (mode: Mode, durationSec: number): Promise<BenchResult> => {
    const db = await getDatabase()
    const maxId = getStockCount()
    const numericFields: (keyof Stock)[] = ['price', 'change', 'volume', 'high', 'low', 'pe']
    let notifications = 0

    let unsub: (() => void) | null = null
    if (mode === 'ivm') {
      const obs = await createStockIvmQuery(Number(priceThreshold))
      const u = obs.subscribe((_delta: { added: Stock[], removed: Stock[] }) => {
        notifications++
      })
      unsub = () => u()
    } else {
      const obs = await createStockReQueryQuery(Number(priceThreshold))
      const u = obs.subscribe((_data: Stock[]) => {
        notifications++
      })
      unsub = () => u()
    }

    let totalUpdates = 0
    const startTime = performance.now()
    const endTime = startTime + durationSec * 1000
    stopRef.current = false
    let lastUiUpdate = startTime

    while (performance.now() < endTime && !stopRef.current) {
      const batchStart = performance.now()
      while (performance.now() - batchStart < 16 && performance.now() < endTime && !stopRef.current) {
        const id = Math.floor(Math.random() * maxId) + 1
        const field = numericFields[Math.floor(Math.random() * numericFields.length)]
        let val: number
        switch (field) {
          case 'price': val = Math.round((10 + Math.random() * 990) * 100) / 100; break
          case 'change': val = Math.round((Math.random() - 0.5) * 100 * 100) / 100; break
          case 'volume': val = Math.floor(Math.random() * 1e8); break
          case 'high': case 'low': val = Math.round((10 + Math.random() * 990) * 100) / 100; break
          case 'pe': val = Math.round((5 + Math.random() * 95) * 10) / 10; break
          default: val = 0
        }
        await db.update('stocks').set(field, val).where(col('id').eq(id)).exec()
        totalUpdates++
      }

      // Update UI less frequently - every 200ms
      const now = performance.now()
      if (now - lastUiUpdate >= 200) {
        const elapsed = now - startTime
        setLiveUps(Math.round(totalUpdates / (elapsed / 1000)))
        setLiveNotifications(notifications)
        lastUiUpdate = now
      }

      await new Promise(r => setTimeout(r, 0))
    }

    unsub?.()
    const elapsed = performance.now() - startTime
    return { mode, totalUpdates, elapsed, ups: Math.round(totalUpdates / (elapsed / 1000)), notifications }
  }, [priceThreshold])

  const handleRunBench = useCallback(async () => {
    updatingRef.current = false
    setBenchRunning(true)
    setResults([])

    // Step 1: Clear table and prepare 100k rows
    unsubLiveRef.current?.()
    unsubLiveRef.current = null
    stockMapRef.current.clear()
    sortedCacheRef.current = []
    setStocks([])

    setCurrentMode('ivm')
    setLiveUps(0)
    setLiveNotifications(0)

    await clearAllStocks()
    await insertStocks(Number(benchDataScale))
    setTotalCount(getStockCount())

    // Step 2: Prime the table preview, then release the UI subscription so
    // benchmark numbers measure one live query at a time.
    await subscribeLive()
    setBenchMatchCount(stockMapRef.current.size)
    unsubLiveRef.current?.()
    unsubLiveRef.current = null

    // Step 3: Run IVM benchmark
    const ivmResult = await runBench('ivm', Number(benchDuration))
    setResults(r => [...r, ivmResult])

    await new Promise(r => setTimeout(r, 500))

    // Step 4: Run Re-query benchmark
    setCurrentMode('requery')
    setLiveUps(0)
    setLiveNotifications(0)
    const reqResult = await runBench('requery', Number(benchDuration))
    setResults(r => [...r, reqResult])

    // Step 5: Cleanup - keep only 100 rows
    unsubLiveRef.current?.()
    unsubLiveRef.current = null
    await deleteStocksKeepFirst(100)
    setTotalCount(getStockCount())
    stockMapRef.current.clear()
    sortedCacheRef.current = []
    await subscribeLive()

    setCurrentMode(null)
    setBenchRunning(false)
  }, [runBench, benchDuration, benchDataScale, subscribeLive])

  const handleInsert = async (count: number) => {
    setInserting(true)
    unsubLiveRef.current?.()
    unsubLiveRef.current = null
    const t0 = performance.now()
    await insertStocks(count)
    const dt = performance.now() - t0
    setTotalCount(getStockCount())
    await subscribeLive()
    setInserting(false)
    alert(`Inserted ${count.toLocaleString()} rows in ${(dt / 1000).toFixed(2)}s — total: ${getStockCount().toLocaleString()}`)
  }

  if (loading) {
    return (
      <div className="flex flex-col items-center justify-center min-h-[60vh] gap-4">
        <Loader2 className="w-8 h-8 animate-spin" />
        <p className="text-white/40 text-sm tracking-wider uppercase">Initializing Database...</p>
      </div>
    )
  }

  const ivmResult = results.find(r => r.mode === 'ivm')
  const reqResult = results.find(r => r.mode === 'requery')
  const matchCount = stockMapRef.current.size

  return (
    <div className="container mx-auto px-4 py-8 max-w-7xl">
      {/* Header */}
      <div className="mb-8">
        <h1 className="text-2xl font-bold tracking-tight uppercase mb-2">IVM vs Re-query</h1>
        <p className="text-white/40 text-sm">Compare incremental view maintenance with traditional re-query</p>
      </div>

      {/* Two Column Layout */}
      <div className="grid lg:grid-cols-2 gap-6 mb-8">
        {/* Left: Controls & Data */}
        <div className="space-y-6 min-w-0">
          {/* Control Panel */}
          <div className="border border-white/10">
            <div className="border-b border-white/10 px-4 py-3">
              <span className="text-xs tracking-widest uppercase text-white/40">// CONTROLS</span>
            </div>
            <div className="p-4 space-y-4">
              {/* Filter & Updates */}
              <div className="flex flex-wrap gap-3">
                <Select value={priceThreshold} onValueChange={setPriceThreshold} disabled={benchRunning}>
                  <SelectTrigger className="w-[130px] uppercase text-xs tracking-wider">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {PRICE_THRESHOLDS.map(t => (
                      <SelectItem key={t} value={String(t)}>PRICE &gt; {t}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>

                <Button
                  variant={updating ? 'default' : 'outline'}
                  onClick={updating ? stopUpdates : startUpdates}
                  disabled={benchRunning}
                  className={cn(
                    "uppercase tracking-wider text-xs gap-2",
                    updating && "bg-white text-black hover:bg-white/90"
                  )}
                >
                  {updating ? <Square className="w-3 h-3" /> : <Play className="w-3 h-3" />}
                  {updating ? 'STOP' : 'UPDATE'}
                </Button>

                {updating && (
                  <span className="text-xs text-white/50 self-center">{updateUps} UPS</span>
                )}
              </div>

              {/* Data Controls */}
              <div className="flex flex-wrap gap-3">
                <span className="text-xs text-white/40 self-center">
                  {totalCount.toLocaleString()} ROWS
                </span>
                <Button
                  variant="outline"
                  onClick={() => handleInsert(100_000)}
                  disabled={inserting || benchRunning}
                  className="uppercase tracking-wider text-xs gap-2"
                >
                  {inserting ? <Loader2 className="w-3 h-3 animate-spin" /> : <Database className="w-3 h-3" />}
                  +100K
                </Button>
                <Button
                  variant="outline"
                  onClick={() => handleInsert(500_000)}
                  disabled={inserting || benchRunning}
                  className="uppercase tracking-wider text-xs"
                >
                  +500K
                </Button>
              </div>
            </div>
          </div>

          {/* Live Data Table - Using native table for better performance */}
          <div className="border border-white/10">
            <div className="border-b border-white/10 px-4 py-3 flex items-center justify-between">
              <div className="flex items-center gap-3">
                <span className="text-xs tracking-widest uppercase text-white/40">// LIVE DATA</span>
                <div className="flex items-center gap-2 px-2 py-1 border border-white/20 text-xs">
                  <Zap className="w-3 h-3" />
                  <span>IVM trace()</span>
                </div>
              </div>
              <span className="text-xs text-white/30">
                {stocks.length} / {matchCount} ROWS
              </span>
            </div>
            <p className="px-4 py-2 text-[10px] text-white/30 border-b border-white/5">
              Showing {stocks.length} of {matchCount} matching rows. Full dataset will be queried, all {matchCount} rows on every change (no LIMIT).
            </p>
            <div className="max-h-[400px] overflow-auto">
              <table className="w-full">
                <thead className="sticky top-0 bg-background">
                  <tr className="border-b border-white/10">
                    {DISPLAY_COLS.map(c => (
                      <th
                        key={c}
                        className={cn(
                          "text-[10px] uppercase tracking-wider font-normal text-white/40 whitespace-nowrap py-3 px-4",
                          c === 'id' || c === 'symbol' || c === 'name' ? 'text-left' : 'text-right'
                        )}
                      >
                        {c}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {stocks.map(stock => (
                    <StockRow key={stock.id} stock={stock} changedCells={changedCells} />
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>

        {/* Right: Benchmark */}
        <div className="space-y-6">
          {/* Benchmark Panel */}
          <div className="border border-white/10">
            <div className="border-b border-white/10 px-4 py-3">
              <span className="text-xs tracking-widest uppercase text-white/40">// BENCHMARK</span>
            </div>
            <div className="p-4 space-y-4">
              <p className="text-xs text-white/40">
                IVM = O(delta) · Re-query = O(result_set)
              </p>

              <div className="flex flex-wrap gap-3">
                <Select value={benchDuration} onValueChange={setBenchDuration} disabled={benchRunning}>
                  <SelectTrigger className="w-[120px] uppercase text-xs tracking-wider">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="2">2 SEC</SelectItem>
                    <SelectItem value="3">3 SEC</SelectItem>
                    <SelectItem value="5">5 SEC</SelectItem>
                    <SelectItem value="10">10 SEC</SelectItem>
                  </SelectContent>
                </Select>

                <Select value={benchDataScale} onValueChange={setBenchDataScale} disabled={benchRunning}>
                  <SelectTrigger className="w-[120px] uppercase text-xs tracking-wider">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="100">100 ROWS</SelectItem>
                    <SelectItem value="2000">2K ROWS</SelectItem>
                    <SelectItem value="10000">10K ROWS</SelectItem>
                    <SelectItem value="100000">100K ROWS</SelectItem>
                  </SelectContent>
                </Select>

                <Button
                  onClick={handleRunBench}
                  disabled={benchRunning}
                  className={cn(
                    "uppercase tracking-wider text-xs gap-2",
                    !benchRunning && "bg-white text-black hover:bg-white/90"
                  )}
                >
                  {benchRunning ? (
                    <>
                      <Loader2 className="w-3 h-3 animate-spin" />
                      {currentMode === 'ivm' ? 'IVM...' : currentMode === 'requery' ? 'RE-QUERY...' : 'PREPARING...'}
                    </>
                  ) : (
                    <>
                      <Play className="w-3 h-3" />
                      RUN COMPARISON
                    </>
                  )}
                </Button>
              </div>

              {currentMode && (
                <div className="flex items-center gap-3 px-4 py-3 border border-white/20 bg-white/[0.02] text-xs">
                  <span className="w-2 h-2 bg-white animate-pulse" />
                  <span className="text-white/60">
                    {liveUps.toLocaleString()} UPS · {liveNotifications} NOTIFICATIONS
                  </span>
                </div>
              )}
            </div>
          </div>

          {/* Results */}
          {results.length > 0 && (
            <div className="space-y-4">
              {/* Result Cards */}
              <div className="grid grid-cols-2 gap-px bg-white/10">
                {ivmResult && (
                  <div className="bg-background p-4">
                    <div className="flex items-center gap-2 mb-4">
                      <span className="text-xs tracking-widest uppercase">IVM</span>
                      <span className="text-[10px] px-2 py-0.5 border border-white/30">O(δ)</span>
                    </div>
                    <div className="space-y-2 text-xs">
                      <div className="flex justify-between">
                        <span className="text-white/40">UPS</span>
                        <span className="font-mono font-bold">{ivmResult.ups.toLocaleString()}</span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-white/40">TOTAL</span>
                        <span className="font-mono">{ivmResult.totalUpdates.toLocaleString()}</span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-white/40">NOTIFY</span>
                        <span className="font-mono">{ivmResult.notifications}</span>
                      </div>
                    </div>
                  </div>
                )}

                {reqResult && (
                  <div className="bg-background p-4">
                    <div className="flex items-center gap-2 mb-4">
                      <span className="text-xs tracking-widest uppercase">RE-QUERY</span>
                      <span className="text-[10px] px-2 py-0.5 border border-white/20 text-white/50">O(n)</span>
                    </div>
                    <div className="space-y-2 text-xs">
                      <div className="flex justify-between">
                        <span className="text-white/40">UPS</span>
                        <span className="font-mono">{reqResult.ups.toLocaleString()}</span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-white/40">TOTAL</span>
                        <span className="font-mono">{reqResult.totalUpdates.toLocaleString()}</span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-white/40">NOTIFY</span>
                        <span className="font-mono">{reqResult.notifications}</span>
                      </div>
                    </div>
                  </div>
                )}
              </div>

              {/* Comparison */}
              {ivmResult && reqResult && (
                <div className="border border-white/20 bg-white/[0.02] p-6 text-center">
                  <div className="text-4xl font-bold font-mono mb-2">
                    {(ivmResult.ups / reqResult.ups).toFixed(1)}x
                  </div>
                  <div className="text-xs text-white/40 uppercase tracking-wider">
                    IVM FASTER
                  </div>
                  <div className="text-xs text-white/30 mt-2">
                    {ivmResult.ups.toLocaleString()} vs {reqResult.ups.toLocaleString()} UPS
                  </div>
                  <div className="text-[10px] text-white/20 mt-3">
                    Data scale: {benchMatchCount.toLocaleString()} matching rows
                  </div>
                </div>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
