import { useEffect, useState } from 'react'

const API_BASE = import.meta.env.VITE_API_BASE || 'http://127.0.0.1:8000'

const SECTION4_COLUMNS_ORDER = [
  'assetId',
  'clientId',
  'eventType',
  'Key',
  'olc',
  'gpsTimestamp',
  'serverTimestamp',
  'latitude',
  'longitude',
  'satellites',
  'gpsDistance',
  'Description',
]

const GAP_ALERT_SECONDS = 120

function App() {
  const [activeMenu, setActiveMenu] = useState('dashboard')
  const [carreterasCatalog, setCarreterasCatalog] = useState([])
  const [carreterasCatalogLoading, setCarreterasCatalogLoading] = useState(false)
  const [selectedCarretera, setSelectedCarretera] = useState('ALL')
  const [snapshotStatus, setSnapshotStatus] = useState(null)
  const [snapshotLoading, setSnapshotLoading] = useState(false)
  const [rowsData, setRowsData] = useState(null)
  const [rowsLoading, setRowsLoading] = useState(false)
  const [pageSize, setPageSize] = useState(100)
  const [offset, setOffset] = useState(0)
  const [assetIdFilter, setAssetIdFilter] = useState('')
  const [clientIdFilter, setClientIdFilter] = useState('')
  const [eventTypeFilter, setEventTypeFilter] = useState('')
  const [quickSearch, setQuickSearch] = useState('')
  const [sortBy, setSortBy] = useState('')
  const [sortDirection, setSortDirection] = useState('asc')
  const [routeAssetId, setRouteAssetId] = useState('598')
  const [routeClientId, setRouteClientId] = useState('64')
  const [routeDetection, setRouteDetection] = useState(null)
  const [mlPredictionsByRoute, setMlPredictionsByRoute] = useState({})
  const [mlPredictionsLoading, setMlPredictionsLoading] = useState(false)
  const [routeLoading, setRouteLoading] = useState(false)
  const [routePointsFilterMode, setRoutePointsFilterMode] = useState('all')
  const [routePointsEventTypeFilter, setRoutePointsEventTypeFilter] = useState('')
  const [manualMapMode, setManualMapMode] = useState('coords')
  const [manualMapInput, setManualMapInput] = useState('')
  const [error, setError] = useState('')

  const loadRows = async (
    nextOffset = offset,
    nextLimit = pageSize,
    nextAssetIdFilter = assetIdFilter,
    nextClientIdFilter = clientIdFilter,
    nextEventTypeFilter = eventTypeFilter,
  ) => {
    setRowsLoading(true)
    setError('')
    try {
      const query = new URLSearchParams({
        offset: String(nextOffset),
        limit: String(nextLimit),
      })
      if (nextAssetIdFilter.trim() !== '') {
        query.set('asset_id', nextAssetIdFilter.trim())
      }
      if (nextClientIdFilter.trim() !== '') {
        query.set('client_id', nextClientIdFilter.trim())
      }
      if (nextEventTypeFilter.trim() !== '') {
        query.set('event_type', nextEventTypeFilter.trim())
      }
      const response = await fetch(`${API_BASE}/data/merge-eventtype/rows?${query.toString()}`)
      if (!response.ok) {
        const detail = await response.json()
        throw new Error(detail.detail || 'No se pudieron cargar las filas')
      }
      const data = await response.json()
      setRowsData(data)
      setOffset(data.offset)
    } catch (err) {
      setError(err.message)
    } finally {
      setRowsLoading(false)
    }
  }

  const loadSnapshotStatus = async () => {
    setSnapshotLoading(true)
    setError('')
    try {
      const response = await fetch(`${API_BASE}/data/snapshot/status`)
      if (!response.ok) {
        const detail = await response.json()
        throw new Error(detail.detail || 'No se pudo obtener el estado del snapshot')
      }
      const data = await response.json()
      setSnapshotStatus(data)
    } catch (err) {
      setError(err.message)
    } finally {
      setSnapshotLoading(false)
    }
  }

  const loadCarreterasCatalog = async () => {
    setCarreterasCatalogLoading(true)
    try {
      const response = await fetch(`${API_BASE}/data/carreteras/rows?limit=2000&offset=0`)
      if (!response.ok) {
        const detail = await response.json()
        throw new Error(detail.detail || 'No se pudo obtener el catálogo de carreteras')
      }

      const data = await response.json()
      const rows = Array.isArray(data.rows) ? data.rows : []
      const optionsSet = new Set()

      rows.forEach((row) => {
        const name = String(row.name || '').trim()
        const route = String(row.p_route || '').trim()
        const label = name || route
        if (label) {
          optionsSet.add(label)
        }
      })

      const options = Array.from(optionsSet).sort((left, right) => left.localeCompare(right, 'es'))
      setCarreterasCatalog(options)
    } catch (err) {
      setError(err.message)
      setCarreterasCatalog([])
    } finally {
      setCarreterasCatalogLoading(false)
    }
  }

  const detectRoutesByValues = async (assetValue, clientValue, clearPrevious = true) => {
    setRouteLoading(true)
    setError('')
    if (clearPrevious) {
      setRouteDetection(null)
    }

    if (!assetValue || !clientValue) {
      setRouteLoading(false)
      setError('Debes indicar assetId y clientId para detectar rutas')
      return
    }

    try {
      const query = new URLSearchParams({
        asset_id: assetValue,
        client_id: clientValue,
      })
      const response = await fetch(`${API_BASE}/routes/detect?${query.toString()}`)
      if (!response.ok) {
        const detail = await response.json()
        throw new Error(detail.detail || 'No se pudieron detectar rutas')
      }
      const data = await response.json()
      setRouteDetection(data)
      setRoutePointsFilterMode('all')
      setRoutePointsEventTypeFilter('')
    } catch (err) {
      setError(err.message)
    } finally {
      setRouteLoading(false)
    }
  }

  const detectRoutes = async () => {
    const assetValue = routeAssetId.trim()
    const clientValue = routeClientId.trim()
    await detectRoutesByValues(assetValue, clientValue, true)
  }

  const handlePrevPage = () => {
    const nextOffset = Math.max(0, offset - pageSize)
    loadRows(nextOffset, pageSize, assetIdFilter, clientIdFilter, eventTypeFilter)
  }

  const handleNextPage = () => {
    const totalRows = rowsData?.total_rows || 0
    const nextOffset = offset + pageSize
    if (nextOffset < totalRows) {
      loadRows(nextOffset, pageSize, assetIdFilter, clientIdFilter, eventTypeFilter)
    }
  }

  const handlePageSizeChange = (event) => {
    const nextLimit = Number(event.target.value)
    setPageSize(nextLimit)
    loadRows(0, nextLimit, assetIdFilter, clientIdFilter, eventTypeFilter)
  }

  const handleApplyFilters = () => {
    setRouteAssetId(assetIdFilter.trim())
    setRouteClientId(clientIdFilter.trim())
    loadRows(0, pageSize, assetIdFilter, clientIdFilter, eventTypeFilter)
  }

  const handleClearFilters = () => {
    setAssetIdFilter('')
    setClientIdFilter('')
    setEventTypeFilter('')
    setRouteAssetId('')
    setRouteClientId('')
    loadRows(0, pageSize, '', '', '')
  }

  const handleSort = (columnName) => {
    if (sortBy === columnName) {
      setSortDirection((prev) => {
        if (prev === 'asc') {
          return 'desc'
        }

        return 'asc'
      })
    } else {
      setSortBy(columnName)
      setSortDirection('asc')
    }
  }

  const getSortIndicator = (columnName) => {
    if (sortBy !== columnName) {
      return ''
    }

    if (sortDirection === 'asc') {
      return ' ▲'
    }

    return ' ▼'
  }

  useEffect(() => {
    loadSnapshotStatus()
    loadCarreterasCatalog()
    loadRows(0, pageSize)
  }, [])

  useEffect(() => {
    const timer = setInterval(() => {
      loadSnapshotStatus()
    }, 30000)

    return () => clearInterval(timer)
  }, [])

  const sourceLabel = (() => {
    if (!snapshotStatus) {
      return 'Fuente: desconocida'
    }

    const hasSnapshotData = snapshotStatus.exists && snapshotStatus.merged_rows != null
    if (hasSnapshotData) {
      return 'Fuente activa: DuckDB snapshot'
    }

    return 'Fuente activa: CSV fallback'
  })()

  const getRouteKey = (route) => [
    String(route?.start_index ?? ''),
    String(route?.end_index ?? ''),
    String(route?.start_timestamp || ''),
  ].join('|')

  const clampRange = (value, min, max) => {
    if (!Number.isFinite(value)) {
      return min
    }
    return Math.min(max, Math.max(min, value))
  }

  const buildMlFeaturesFromRoute = (route) => {
    const startDate = new Date(route?.start_timestamp || '')
    const durationHours = Number(route?.duration_hours)
    const totalEvents = Number(route?.total_events)

    const eventsPerHour = Number.isFinite(durationHours) && durationHours > 0
      ? totalEvents / durationHours
      : 0

    const trafficLevel = clampRange(eventsPerHour / 20, 0, 10)

    return {
      distance_km: clampRange(Number(route?.distance_km), 0, Number.MAX_SAFE_INTEGER),
      traffic_level: trafficLevel,
      weather_index: 3,
      hour_of_day: Number.isNaN(startDate.getTime()) ? 9 : startDate.getHours(),
      day_of_week: Number.isNaN(startDate.getTime()) ? 1 : startDate.getDay(),
      vehicle_load_kg: 1000,
    }
  }

  const requestMlPredictionForRoute = async (route) => {
    const response = await fetch(`${API_BASE}/ml/predict`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(buildMlFeaturesFromRoute(route)),
    })

    if (response.ok) {
      const data = await response.json()
      return {
        predicted_route: data.predicted_route,
        confidence: Number(data.confidence),
      }
    }

    let message = 'No se pudo obtener predicción ML'
    try {
      const detail = await response.json()
      if (detail?.detail) {
        message = detail.detail
      }
    } catch {
      // Keep default message if response body is not JSON.
    }

    return { error: message }
  }

  useEffect(() => {
    setRouteAssetId(assetIdFilter)
    setRouteClientId(clientIdFilter)
  }, [assetIdFilter, clientIdFilter])

  useEffect(() => {
    const assetValue = routeAssetId.trim()
    const clientValue = routeClientId.trim()

    if (!assetValue || !clientValue) {
      setRouteDetection(null)
      setRouteLoading(false)
      return
    }

    const timer = setTimeout(() => {
      detectRoutesByValues(assetValue, clientValue, false)
    }, 600)

    return () => clearTimeout(timer)
  }, [routeAssetId, routeClientId])

  useEffect(() => {
    const completeRoutes = Array.isArray(routeDetection?.complete_routes)
      ? routeDetection.complete_routes
      : []

    if (completeRoutes.length === 0) {
      setMlPredictionsByRoute({})
      setMlPredictionsLoading(false)
      return
    }

    let cancelled = false

    const loadMlPredictions = async () => {
      setMlPredictionsLoading(true)

      const entries = await Promise.all(
        completeRoutes.map(async (route) => {
          const routeKey = getRouteKey(route)

          try {
            const prediction = await requestMlPredictionForRoute(route)
            return [routeKey, prediction]
          } catch {
            return [routeKey, { error: 'Error de conexión al predecir ruta ML' }]
          }
        }),
      )

      if (cancelled) {
        return
      }

      setMlPredictionsByRoute(Object.fromEntries(entries))
      setMlPredictionsLoading(false)
    }

    loadMlPredictions()

    return () => {
      cancelled = true
    }
  }, [routeDetection])

  const displayedRows = (() => {
    if (!rowsData) return []

    const searchTerm = quickSearch.trim().toLowerCase()
    let result = rowsData.rows

    if (searchTerm) {
      result = result.filter((row) =>
        rowsData.columns.some((columnName) => String(row[columnName] ?? '').toLowerCase().includes(searchTerm)),
      )
    }

    if (sortBy) {
      let direction = -1
      if (sortDirection === 'asc') {
        direction = 1
      }
      result = [...result].sort((leftRow, rightRow) => {
        const leftValue = leftRow[sortBy]
        const rightValue = rightRow[sortBy]

        const leftNumber = Number(leftValue)
        const rightNumber = Number(rightValue)
        const bothNumeric = Number.isFinite(leftNumber) && Number.isFinite(rightNumber)

        if (bothNumeric) {
          return (leftNumber - rightNumber) * direction
        }

        return String(leftValue ?? '').localeCompare(String(rightValue ?? '')) * direction
      })
    }

    return result
  })()

  const formatTimestamp = (value) => {
    if (!value) {
      return ''
    }

    const numericValue = Number(value)
    let date

    if (Number.isFinite(numericValue)) {
      const absoluteValue = Math.abs(numericValue)
      let milliseconds

      if (absoluteValue >= 1e17) {
        milliseconds = numericValue / 1e6
      } else if (absoluteValue >= 1e14) {
        milliseconds = numericValue / 1e3
      } else if (absoluteValue >= 1e11) {
        milliseconds = numericValue
      } else {
        milliseconds = numericValue * 1e3
      }

      date = new Date(milliseconds)
    } else {
      date = new Date(value)
    }

    if (Number.isNaN(date.getTime())) {
      return String(value)
    }

    return date.toLocaleString('es-MX', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false,
    })
  }

  const formatIsoDateTime = (value) => {
    if (!value) {
      return 'N/A'
    }

    const date = new Date(value)
    if (Number.isNaN(date.getTime())) {
      return String(value)
    }

    return date.toLocaleString('es-MX', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false,
      timeZoneName: 'short',
    })
  }

  const formatRelativeAge = (value) => {
    if (!value) {
      return 'N/A'
    }

    const date = new Date(value)
    if (Number.isNaN(date.getTime())) {
      return 'N/A'
    }

    const diffMs = Date.now() - date.getTime()
    if (diffMs < 0) {
      return 'en el futuro'
    }

    const diffSeconds = Math.floor(diffMs / 1000)
    if (diffSeconds < 60) {
      return `hace ${diffSeconds} s`
    }

    const diffMinutes = Math.floor(diffSeconds / 60)
    if (diffMinutes < 60) {
      return `hace ${diffMinutes} min`
    }

    const diffHours = Math.floor(diffMinutes / 60)
    if (diffHours < 24) {
      return `hace ${diffHours} h`
    }

    const diffDays = Math.floor(diffHours / 24)
    return `hace ${diffDays} días`
  }

  const displayedColumns = (() => {
    if (!rowsData) {
      return []
    }

    const orderedAvailable = SECTION4_COLUMNS_ORDER.filter((columnName) => rowsData.columns.includes(columnName))
    if (orderedAvailable.length > 0) {
      return orderedAvailable
    }

    return rowsData.columns
  })()

  const getRouteIntervalRecords = (route) => {
    const allRecords = routeDetection?.all_records || []
    if (allRecords.length === 0) {
      return []
    }

    const startIndex = Number(route?.start_index)
    const endIndex = Number(route?.end_index)
    if (Number.isInteger(startIndex) && Number.isInteger(endIndex) && startIndex >= 0 && endIndex >= startIndex) {
      return allRecords.slice(startIndex, endIndex + 1)
    }

    if (!route?.start_timestamp || !route?.end_timestamp) {
      return []
    }

    const startMs = new Date(route.start_timestamp).getTime()
    const endMs = new Date(route.end_timestamp).getTime()
    if (!Number.isFinite(startMs) || !Number.isFinite(endMs)) {
      return []
    }

    return allRecords
      .filter((record) => {
        const orderMs = new Date(record.order_timestamp || '').getTime()
        if (!Number.isFinite(orderMs)) {
          return false
        }
        return orderMs >= startMs && orderMs <= endMs
      })
      .sort((left, right) => String(left.order_timestamp || '').localeCompare(String(right.order_timestamp || '')))
  }

  const getIntervalRecordRole = (route, record) => {
    const orderTimestamp = String(record?.order_timestamp || '')
    const eventType = Number(record?.event_type)

    const isStart = orderTimestamp === String(route?.start_timestamp || '') && eventType === Number(routeDetection?.event_start)
    if (isStart) {
      return 'start'
    }

    const isEnd = orderTimestamp === String(route?.end_timestamp || '') && eventType === Number(routeDetection?.event_end)
    if (isEnd) {
      return 'end'
    }

    return 'normal'
  }

  const getIntervalRowClassName = (role) => {
    if (role === 'start') {
      return 'route-point-row-start'
    }
    if (role === 'end') {
      return 'route-point-row-end'
    }
    return ''
  }

  const getIntervalRoleLabel = (role) => {
    if (role === 'start') {
      return 'INICIO'
    }
    if (role === 'end') {
      return 'FIN'
    }
    return ''
  }

  const renderIntervalRecordRow = (route, record, recordIndex) => {
    const role = getIntervalRecordRole(route, record)
    const rowClassName = getIntervalRowClassName(role)
    const roleLabel = getIntervalRoleLabel(role)

    return (
      <tr
        key={`route-interval-${route.start_index}-${route.end_index}-${recordIndex}-${record.order_timestamp || 'na'}`}
        className={rowClassName}
      >
        <td>{recordIndex + 1}</td>
        <td>
          {roleLabel ? <span className={`event-badge ${role}`}>{roleLabel}</span> : ''}
        </td>
        <td>{formatTimestamp(record.order_timestamp)}</td>
        <td>{formatTimestamp(record.gps_timestamp)}</td>
        <td>{formatTimestamp(record.server_timestamp)}</td>
        <td>{record.event_type ?? ''}</td>
        <td>{record.event_key || ''}</td>
        <td>{record.event_description || ''}</td>
        <td>{record.matched_road_label || ''}</td>
        <td>
          {record.matched_road_distance_m == null
            ? ''
            : Number(record.matched_road_distance_m).toFixed(1)}
        </td>
        <td>{record.latitude ?? ''}</td>
        <td>{record.longitude ?? ''}</td>
      </tr>
    )
  }

  const filterIntervalRecords = (route, records) => {
    if (routePointsFilterMode === 'boundaries') {
      return records.filter((record) => {
        const role = getIntervalRecordRole(route, record)
        return role === 'start' || role === 'end'
      })
    }

    if (routePointsFilterMode === 'eventType') {
      const targetEventType = Number(routePointsEventTypeFilter)
      if (!Number.isFinite(targetEventType)) {
        return records
      }
      return records.filter((record) => Number(record.event_type) === targetEventType)
    }

    return records
  }

  const parseRecordTimeMs = (record) => {
    const rawValue = record?.order_timestamp || record?.gps_timestamp || record?.server_timestamp || ''
    if (!rawValue) {
      return null
    }

    const parsed = new Date(rawValue).getTime()
    if (!Number.isFinite(parsed)) {
      return null
    }

    return parsed
  }

  const hasValidGpsPoint = (record) => {
    const latitude = Number(record?.latitude)
    const longitude = Number(record?.longitude)
    return Number.isFinite(latitude) && Number.isFinite(longitude)
  }

  const formatGapDuration = (totalSeconds) => {
    if (!Number.isFinite(totalSeconds) || totalSeconds < 0) {
      return ''
    }

    if (totalSeconds < 60) {
      return `${totalSeconds.toFixed(0)} s`
    }

    const hours = Math.floor(totalSeconds / 3600)
    const minutes = Math.floor((totalSeconds % 3600) / 60)
    const seconds = Math.floor(totalSeconds % 60)
    if (hours > 0) {
      return `${hours}h ${minutes}m ${seconds}s`
    }
    return `${minutes}m ${seconds}s`
  }

  const buildRouteGapAnalysis = (records) => {
    if (!Array.isArray(records) || records.length < 2) {
      return {
        intervals: [],
        totalGapSeconds: 0,
        avgGapSeconds: 0,
        minGapSeconds: 0,
        maxGapSeconds: 0,
        missingGpsIntervals: 0,
        alertIntervals: 0,
      }
    }

    const intervals = []
    for (let index = 1; index < records.length; index += 1) {
      const previous = records[index - 1]
      const current = records[index]

      const previousMs = parseRecordTimeMs(previous)
      const currentMs = parseRecordTimeMs(current)
      if (!Number.isFinite(previousMs) || !Number.isFinite(currentMs)) {
        continue
      }

      const rawGapSeconds = (currentMs - previousMs) / 1000
      if (!Number.isFinite(rawGapSeconds) || rawGapSeconds < 0) {
        continue
      }

      const previousHasGps = hasValidGpsPoint(previous)
      const currentHasGps = hasValidGpsPoint(current)
      const missingGps = !previousHasGps || !currentHasGps

      intervals.push({
        sequence: intervals.length + 1,
        fromOrderTimestamp: previous.order_timestamp || previous.gps_timestamp || previous.server_timestamp || '',
        toOrderTimestamp: current.order_timestamp || current.gps_timestamp || current.server_timestamp || '',
        fromEventType: previous.event_type,
        toEventType: current.event_type,
        gapSeconds: rawGapSeconds,
        gapMinutes: rawGapSeconds / 60,
        alertGap: rawGapSeconds >= GAP_ALERT_SECONDS,
        missingGps,
        fromHasGps: previousHasGps,
        toHasGps: currentHasGps,
        fromRoad: previous.matched_road_label || '',
        toRoad: current.matched_road_label || '',
      })
    }

    const totalGapSeconds = intervals.reduce((sum, item) => sum + item.gapSeconds, 0)
    const minGapSeconds = intervals.reduce((minValue, item) => Math.min(minValue, item.gapSeconds), Number.POSITIVE_INFINITY)
    const maxGapSeconds = intervals.reduce((maxValue, item) => Math.max(maxValue, item.gapSeconds), 0)
    const missingGpsIntervals = intervals.filter((item) => item.missingGps).length
    const alertIntervals = intervals.filter((item) => item.alertGap).length
    const avgGapSeconds = intervals.length > 0 ? totalGapSeconds / intervals.length : 0

    return {
      intervals,
      totalGapSeconds,
      avgGapSeconds,
      minGapSeconds: Number.isFinite(minGapSeconds) ? minGapSeconds : 0,
      maxGapSeconds,
      missingGpsIntervals,
      alertIntervals,
    }
  }

  const openManualMapWindow = () => {
    const lines = manualMapInput
      .split('\n')
      .map((line) => line.trim())
      .filter((line) => line.length > 0)

    if (lines.length < 2) {
      setError('Necesitas al menos 2 puntos/OLC para pintar una ruta manual')
      return
    }

    const manualPoints = []
    const invalidEntries = []

    if (manualMapMode === 'coords') {
      lines.forEach((line, index) => {
        const parts = line.split(/[\s,;]+/).filter(Boolean)
        if (parts.length < 2) {
          invalidEntries.push(line)
          return
        }

        const latitude = Number(parts[0])
        const longitude = Number(parts[1])
        const validLatitude = Number.isFinite(latitude) && latitude >= -90 && latitude <= 90
        const validLongitude = Number.isFinite(longitude) && longitude >= -180 && longitude <= 180

        if (!validLatitude || !validLongitude) {
          invalidEntries.push(line)
          return
        }

        manualPoints.push({
          sequence: index + 1,
          latitude,
          longitude,
          olc: '',
          gpsTimestamp: '',
          eventType: 'manual',
        })
      })
    } else {
      if (!routeDetection || !Array.isArray(routeDetection.all_records)) {
        setError('Para usar OLC primero debes detectar rutas (para tener referencia de OLC y coordenadas)')
        return
      }

      const olcLookup = new Map()
      routeDetection.all_records.forEach((record) => {
        const code = String(record?.olc || '').trim().toUpperCase()
        const latitude = Number(record?.latitude)
        const longitude = Number(record?.longitude)
        if (!code) {
          return
        }
        if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
          return
        }
        if (!olcLookup.has(code)) {
          olcLookup.set(code, record)
        }
      })

      lines.forEach((line, index) => {
        const code = line.toUpperCase()
        const match = olcLookup.get(code)
        if (!match) {
          invalidEntries.push(line)
          return
        }

        manualPoints.push({
          sequence: index + 1,
          latitude: Number(match.latitude),
          longitude: Number(match.longitude),
          olc: code,
          gpsTimestamp: match.gps_timestamp || '',
          eventType: match.event_type ?? 'manual',
        })
      })
    }

    if (manualPoints.length < 2) {
      setError('No se pudieron construir suficientes puntos válidos para pintar la ruta manual')
      return
    }

    if (invalidEntries.length > 0) {
      setError(`Se omitieron ${invalidEntries.length} entradas inválidas al pintar la ruta manual`)
    } else {
      setError('')
    }

    const mapWindow = window.open('', '_blank', 'width=1200,height=800')
    if (!mapWindow) {
      setError('No se pudo abrir la ventana del mapa. Revisa el bloqueador de popups del navegador.')
      return
    }

    const safePointsJson = JSON.stringify(manualPoints).replaceAll('</', String.raw`<\/`)
    const title = `Ruta manual assetId=${routeAssetId || '-'} clientId=${routeClientId || '-'}`
    const subtitle = manualMapMode === 'coords'
      ? 'entrada manual por coordenadas (lat,lon)'
      : 'entrada manual por OLC (resuelto con registros cargados)'

    const htmlContent = `
      <!doctype html>
      <html lang="es">
        <head>
          <meta charset="utf-8" />
          <meta name="viewport" content="width=device-width, initial-scale=1" />
          <title>${title}</title>
          <link
            rel="stylesheet"
            href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
            integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY="
            crossorigin=""
          />
          <style>
            html, body { margin: 0; padding: 0; height: 100%; font-family: Inter, system-ui, sans-serif; }
            #map { width: 100%; height: calc(100% - 56px); }
            .header {
              height: 56px;
              display: flex;
              align-items: center;
              padding: 0 16px;
              border-bottom: 1px solid #e5e7eb;
              background: #f9fafb;
              font-size: 14px;
              color: #111827;
            }
          </style>
        </head>
        <body>
          <div class="header">${title} · puntos: ${manualPoints.length} · ${subtitle}</div>
          <div id="map"></div>

          <script
            src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
            integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo="
            crossorigin=""
          ></script>
          <script>
            const points = ${safePointsJson};
            const map = L.map('map');

            L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
              maxZoom: 19,
              attribution: '&copy; OpenStreetMap contributors',
            }).addTo(map);

            const latlngs = points.map((point) => [point.latitude, point.longitude]);
            const routeLine = L.polyline(latlngs, {
              color: '#2563eb',
              weight: 4,
              opacity: 0.9,
            }).addTo(map);

            const escapeHtml = (value) =>
              String(value ?? '')
                .replace(/&/g, '&amp;')
                .replace(/</g, '&lt;')
                .replace(/>/g, '&gt;')
                .replace(/"/g, '&quot;')
                .replace(/'/g, '&#39;');

            points.forEach((point, index) => {
              const isStart = index === 0;
              const isEnd = index === points.length - 1;
              const marker = L.circleMarker([point.latitude, point.longitude], {
                radius: isStart || isEnd ? 8 : 5,
                color: '#111827',
                fillColor: isStart ? '#16a34a' : (isEnd ? '#dc2626' : '#60a5fa'),
                fillOpacity: 0.95,
                weight: 1,
              }).addTo(map);

              marker.bindPopup(
                '<b>' + (isStart ? 'Inicio' : (isEnd ? 'Fin' : 'Punto')) + '</b><br/>' +
                '#: ' + point.sequence + '<br/>' +
                'gpsTimestamp: ' + escapeHtml(point.gpsTimestamp) + '<br/>' +
                'eventType: ' + escapeHtml(point.eventType) + '<br/>' +
                'olc: ' + escapeHtml(point.olc) + '<br/>' +
                'lat: ' + escapeHtml(point.latitude) + '<br/>' +
                'lon: ' + escapeHtml(point.longitude)
              );
            });

            map.fitBounds(routeLine.getBounds(), { padding: [24, 24] });
          </script>
        </body>
      </html>
    `

    const blob = new Blob([htmlContent], { type: 'text/html' })
    const blobUrl = URL.createObjectURL(blob)
    mapWindow.location.replace(blobUrl)
    mapWindow.addEventListener('beforeunload', () => URL.revokeObjectURL(blobUrl), { once: true })
  }

  const openRouteMapWindow = () => {
    if (!routeDetection || !Array.isArray(routeDetection.all_records)) {
      setError('Primero detecta rutas para cargar los registros del asset/client')
      return
    }

    const orderedPoints = [...routeDetection.all_records]
      .filter((record) => record?.gps_timestamp)
      .sort((left, right) => String(left.gps_timestamp).localeCompare(String(right.gps_timestamp)))
      .filter((record) => Number.isFinite(Number(record.latitude)) && Number.isFinite(Number(record.longitude)))
      .map((record, index) => ({
        sequence: index + 1,
        gpsTimestamp: record.gps_timestamp,
        olc: record.olc || '',
        eventType: record.event_type,
        latitude: Number(record.latitude),
        longitude: Number(record.longitude),
      }))

    if (orderedPoints.length < 2) {
      setError('No hay suficientes puntos GPS válidos para pintar la ruta en el mapa')
      return
    }

    const mapWindow = window.open('', '_blank', 'width=1200,height=800')
    if (!mapWindow) {
      setError('No se pudo abrir la ventana del mapa. Revisa el bloqueador de popups del navegador.')
      return
    }

    const safePointsJson = JSON.stringify(orderedPoints).replaceAll('</', String.raw`<\/`)
    const title = `Ruta assetId=${routeDetection.asset_id} clientId=${routeDetection.client_id}`

    const htmlContent = `
      <!doctype html>
      <html lang="es">
        <head>
          <meta charset="utf-8" />
          <meta name="viewport" content="width=device-width, initial-scale=1" />
          <title>${title}</title>
          <link
            rel="stylesheet"
            href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
            integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY="
            crossorigin=""
          />
          <style>
            html, body { margin: 0; padding: 0; height: 100%; font-family: Inter, system-ui, sans-serif; }
            #map { width: 100%; height: calc(100% - 56px); }
            .header {
              height: 56px;
              display: flex;
              align-items: center;
              padding: 0 16px;
              border-bottom: 1px solid #e5e7eb;
              background: #f9fafb;
              font-size: 14px;
              color: #111827;
            }
          </style>
        </head>
        <body>
          <div class="header">${title} · puntos: ${orderedPoints.length} · criterio: primer 519 → primer evento de fin</div>
          <div id="map"></div>

          <script
            src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
            integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo="
            crossorigin=""
          ></script>
          <script>
            const points = ${safePointsJson};
            const map = L.map('map');
            const routeColors = ['#2563eb', '#dc2626', '#16a34a', '#7c3aed', '#ea580c', '#0891b2'];
            const candidateStartColors = ['#f59e0b', '#8b5cf6', '#ec4899', '#14b8a6', '#f97316', '#22c55e'];
            const endRouteEventTypes = [520, 321, 334, 518, 541, 549, 726];
            const endRouteLabels = {
              520: 'IGNITION_OFF',
              321: 'GEOFENCE_EXIT',
              334: 'WORK_DAY_FINISHED',
              518: 'IGNITION_OFF',
              541: 'LOGISTICS_DESTINATION_REACHED',
              549: 'TRIP_FINISHED',
              726: 'PASSENGER_COUNT_RESET',
            };
            const startIcon = L.divIcon({
              className: '',
              html: '<div style="background:#16a34a;color:#fff;border:2px solid #064e3b;border-radius:16px;padding:2px 8px;font-size:11px;font-weight:700;line-height:1;">INICIO</div>',
              iconSize: [62, 22],
              iconAnchor: [31, 11],
            });
            const endIcon = L.divIcon({
              className: '',
              html: '<div style="background:#dc2626;color:#fff;border:2px solid #7f1d1d;border-radius:16px;padding:2px 10px;font-size:11px;font-weight:700;line-height:1;">FIN</div>',
              iconSize: [46, 22],
              iconAnchor: [23, 11],
            });

            L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
              maxZoom: 19,
              attribution: '&copy; OpenStreetMap contributors',
            }).addTo(map);

            const escapeHtml = (value) =>
              String(value ?? '')
                .replace(/&/g, '&amp;')
                .replace(/</g, '&lt;')
                .replace(/>/g, '&gt;')
                .replace(/"/g, '&quot;')
                .replace(/'/g, '&#39;');


            const segments = [];
            let openStartIndex = null;

            points.forEach((point, index) => {
              if (point.eventType === 519 && openStartIndex === null) {
                openStartIndex = index;
                return;
              }

              if (openStartIndex !== null && endRouteEventTypes.includes(point.eventType)) {
                const segmentPoints = points.slice(openStartIndex, index + 1);
                if (segmentPoints.length >= 2) {
                  segments.push({
                    points: segmentPoints,
                    startIndex: openStartIndex,
                    endIndex: index,
                  });
                }
                openStartIndex = null;
              }
            });

            if (segments.length === 0) {
              L.marker([points[0].latitude, points[0].longitude]).addTo(map)
                .bindPopup('No se detectaron segmentos completos 519→evento_fin con el criterio solicitado.');
              map.setView([points[0].latitude, points[0].longitude], 12);
            } else {
              const allLatLngs = [];

              segments.forEach((segment, segmentIndex) => {
                const segmentColor = routeColors[segmentIndex % routeColors.length];
                const segmentPoints = segment.points;
                const latlngs = segmentPoints.map((point) => [point.latitude, point.longitude]);
                allLatLngs.push(...latlngs);

                L.polyline(latlngs, {
                  color: segmentColor,
                  weight: 4,
                  opacity: 0.9,
                }).addTo(map);

                const startPoint = segmentPoints[0];
                const endPoint = segmentPoints[segmentPoints.length - 1];
                const endLabel = endRouteLabels[endPoint.eventType] || 'FIN_EVENT';

                L.marker([startPoint.latitude, startPoint.longitude], { icon: startIcon })
                  .addTo(map)
                  .bindPopup(
                    '<b>Inicio de ruta #' + (segmentIndex + 1) + ' (519)</b><br/>' +
                    'gpsTimestamp: ' + escapeHtml(startPoint.gpsTimestamp) + '<br/>' +
                    'eventType: ' + escapeHtml(startPoint.eventType) + '<br/>' +
                    'olc: ' + escapeHtml(startPoint.olc) + '<br/>' +
                    'lat: ' + escapeHtml(startPoint.latitude) + '<br/>' +
                    'lon: ' + escapeHtml(startPoint.longitude)
                  );

                L.marker([endPoint.latitude, endPoint.longitude], { icon: endIcon })
                  .addTo(map)
                  .bindPopup(
                    '<b>Fin de ruta #' + (segmentIndex + 1) + ' (' + escapeHtml(endLabel) + ')</b><br/>' +
                    'gpsTimestamp: ' + escapeHtml(endPoint.gpsTimestamp) + '<br/>' +
                    'eventType: ' + escapeHtml(endPoint.eventType) + '<br/>' +
                    'olc: ' + escapeHtml(endPoint.olc) + '<br/>' +
                    'lat: ' + escapeHtml(endPoint.latitude) + '<br/>' +
                    'lon: ' + escapeHtml(endPoint.longitude)
                  );

                let candidateStartCounter = 0;
                segmentPoints.forEach((point, pointIndex) => {
                  const isStart = pointIndex === 0;
                  const isEnd = pointIndex === segmentPoints.length - 1;
                  const isStartCandidate = point.eventType === 519;

                  let fillColor = '#60a5fa';
                  let radius = 4;
                  let popupTitle = 'Punto de ruta';

                  if (isStart) {
                    fillColor = '#16a34a';
                    radius = 8;
                    popupTitle = 'Inicio de ruta';
                  } else if (isEnd) {
                    fillColor = '#dc2626';
                    radius = 8;
                    popupTitle = 'Fin de ruta';
                  } else if (isStartCandidate) {
                    const candidateColor = candidateStartColors[candidateStartCounter % candidateStartColors.length];
                    fillColor = candidateColor;
                    radius = 7;
                    popupTitle = 'Posible inicio alterno (' + (candidateStartCounter + 1) + ')';
                    candidateStartCounter += 1;
                  }

                  const marker = L.circleMarker([point.latitude, point.longitude], {
                    radius,
                    color: '#111827',
                    fillColor,
                    fillOpacity: 0.95,
                    weight: 1,
                  }).addTo(map);

                  marker.bindPopup(
                    '<b>' + popupTitle + ' · Ruta ' + (segmentIndex + 1) + '</b><br/>' +
                    '#: ' + point.sequence + '<br/>' +
                    'gpsTimestamp: ' + escapeHtml(point.gpsTimestamp) + '<br/>' +
                    'eventType: ' + escapeHtml(point.eventType) + '<br/>' +
                    'olc: ' + escapeHtml(point.olc) + '<br/>' +
                    'lat: ' + escapeHtml(point.latitude) + '<br/>' +
                    'lon: ' + escapeHtml(point.longitude)
                  );
                });
              });

              const bounds = L.latLngBounds(allLatLngs);
              map.fitBounds(bounds, { padding: [24, 24] });
            }
          </script>
        </body>
      </html>
    `

    const blob = new Blob([htmlContent], { type: 'text/html' })
    const blobUrl = URL.createObjectURL(blob)
    mapWindow.location.replace(blobUrl)
    mapWindow.addEventListener('beforeunload', () => URL.revokeObjectURL(blobUrl), { once: true })
  }

  const openCompleteRouteIntervalMap = async (route, routeIndex) => {
    const intervalRecords = getRouteIntervalRecords(route)
    const intervalPoints = intervalRecords
      .filter((record) => Number.isFinite(Number(record.latitude)) && Number.isFinite(Number(record.longitude)))
      .map((record, index) => ({
        sequence: index + 1,
        orderTimestamp: record.order_timestamp,
        gpsTimestamp: record.gps_timestamp,
        serverTimestamp: record.server_timestamp,
        eventType: record.event_type,
        eventKey: record.event_key,
        eventDescription: record.event_description,
        latitude: Number(record.latitude),
        longitude: Number(record.longitude),
        role: getIntervalRecordRole(route, record),
      }))

    if (intervalPoints.length < 2) {
      setError('No hay suficientes puntos válidos para pintar el mapa de esta ruta completa.')
      return
    }

    const matchedRoadLabels = Array.isArray(route?.matched_roads)
      ? route.matched_roads.map((value) => String(value || '').trim()).filter(Boolean)
      : []

    let matchedRoadSegments = []
    if (matchedRoadLabels.length > 0) {
      try {
        const response = await fetch(`${API_BASE}/data/carreteras/rows?limit=2000&offset=0`)
        if (response.ok) {
          const data = await response.json()
          const rows = Array.isArray(data.rows) ? data.rows : []
          const roadSet = new Set(matchedRoadLabels)

          matchedRoadSegments = rows
            .filter((row) => {
              const roadName = String(row.name || '').trim()
              const routeName = String(row.p_route || '').trim()
              return roadSet.has(roadName) || roadSet.has(routeName)
            })
            .map((row) => {
              const latlngs = parseLinestringWkt(row.geometry)
              if (latlngs.length < 2) {
                return null
              }

              return {
                label: String(row.name || row.p_route || '').trim(),
                route: String(row.p_route || '').trim(),
                no: String(row.p_no || '').trim(),
                latlngs,
              }
            })
            .filter(Boolean)
        }
      } catch {
        // If road overlay fetch fails, keep rendering the route map itself.
      }
    }

    const mapWindow = window.open('', '_blank', 'width=1200,height=800')
    if (!mapWindow) {
      setError('No se pudo abrir la ventana del mapa. Revisa el bloqueador de popups del navegador.')
      return
    }

    const safePointsJson = JSON.stringify(intervalPoints).replaceAll('</', String.raw`<\/`)
    const safeRoadsJson = JSON.stringify(matchedRoadSegments).replaceAll('</', String.raw`<\/`)
    const title = `Ruta completa #${routeIndex + 1} assetId=${routeDetection?.asset_id || '-'} clientId=${routeDetection?.client_id || '-'}`

    const htmlContent = `
      <!doctype html>
      <html lang="es">
        <head>
          <meta charset="utf-8" />
          <meta name="viewport" content="width=device-width, initial-scale=1" />
          <title>${title}</title>
          <link
            rel="stylesheet"
            href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
            integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY="
            crossorigin=""
          />
          <style>
            html, body { margin: 0; padding: 0; height: 100%; font-family: Inter, system-ui, sans-serif; }
            #map { width: 100%; height: calc(100% - 56px); }
            .header {
              height: 56px;
              display: flex;
              align-items: center;
              justify-content: space-between;
              padding: 0 16px;
              border-bottom: 1px solid #e5e7eb;
              background: #f9fafb;
              font-size: 14px;
              color: #111827;
            }
            .controls {
              display: flex;
              gap: 8px;
              flex-wrap: wrap;
            }
            .controls button {
              border: 1px solid #d1d5db;
              background: #ffffff;
              color: #111827;
              border-radius: 6px;
              padding: 6px 10px;
              font-size: 12px;
              cursor: pointer;
            }
          </style>
        </head>
        <body>
          <div class="header">
            <span>${title} · puntos: ${intervalPoints.length} · carreteras esperadas: ${matchedRoadLabels.length}</span>
            <div class="controls">
              <button id="toggle-route" type="button">Ocultar ruta</button>
              <button id="toggle-roads" type="button">Ocultar carreteras</button>
            </div>
          </div>
          <div id="map"></div>

          <script
            src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
            integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo="
            crossorigin=""
          ></script>
          <script>
            const points = ${safePointsJson};
            const expectedRoads = ${safeRoadsJson};
            const map = L.map('map');
            const routeLayer = L.layerGroup().addTo(map);
            const roadsLayer = L.layerGroup().addTo(map);

            const escapeHtml = (value) =>
              String(value ?? '')
                .replace(/&/g, '&amp;')
                .replace(/</g, '&lt;')
                .replace(/>/g, '&gt;')
                .replace(/"/g, '&quot;')
                .replace(/'/g, '&#39;');

            L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
              maxZoom: 19,
              attribution: '&copy; OpenStreetMap contributors',
            }).addTo(map);

            const latlngs = points.map((point) => [point.latitude, point.longitude]);

            const allBoundsPoints = [...latlngs];

            expectedRoads.forEach((road) => {
              const roadLine = L.polyline(road.latlngs, {
                color: '#f97316',
                weight: 3,
                opacity: 0.6,
                dashArray: '6,4',
              }).addTo(roadsLayer);

              allBoundsPoints.push(...road.latlngs);

              roadLine.bindPopup(
                '<b>Carretera esperada</b><br/>' +
                'Nombre: ' + escapeHtml(road.label || 'N/A') + '<br/>' +
                'Ruta: ' + escapeHtml(road.route || 'N/A') + '<br/>' +
                'No: ' + escapeHtml(road.no || 'N/A')
              );
            });

            const routeLine = L.polyline(latlngs, {
              color: '#2563eb',
              weight: 4,
              opacity: 0.95,
            }).addTo(routeLayer);

            const startIcon = L.divIcon({
              className: '',
              html: '<div style="background:#16a34a;color:#fff;border:2px solid #064e3b;border-radius:16px;padding:2px 8px;font-size:11px;font-weight:700;line-height:1;">INICIO</div>',
              iconSize: [62, 22],
              iconAnchor: [31, 11],
            });
            const endIcon = L.divIcon({
              className: '',
              html: '<div style="background:#dc2626;color:#fff;border:2px solid #7f1d1d;border-radius:16px;padding:2px 10px;font-size:11px;font-weight:700;line-height:1;">FIN</div>',
              iconSize: [46, 22],
              iconAnchor: [23, 11],
            });

            points.forEach((point) => {
              const isStart = point.role === 'start';
              const isEnd = point.role === 'end';

              if (isStart) {
                L.marker([point.latitude, point.longitude], { icon: startIcon }).addTo(routeLayer);
              }
              if (isEnd) {
                L.marker([point.latitude, point.longitude], { icon: endIcon }).addTo(routeLayer);
              }

              const marker = L.circleMarker([point.latitude, point.longitude], {
                radius: isStart || isEnd ? 7 : 4,
                color: '#111827',
                fillColor: isStart ? '#16a34a' : (isEnd ? '#dc2626' : '#60a5fa'),
                fillOpacity: 0.95,
                weight: 1,
              }).addTo(routeLayer);

              marker.bindPopup(
                '<b>' + (isStart ? 'INICIO' : (isEnd ? 'FIN' : 'Punto')) + '</b><br/>' +
                '#: ' + point.sequence + '<br/>' +
                'order: ' + escapeHtml(point.orderTimestamp) + '<br/>' +
                'gps: ' + escapeHtml(point.gpsTimestamp) + '<br/>' +
                'server: ' + escapeHtml(point.serverTimestamp) + '<br/>' +
                'eventType: ' + escapeHtml(point.eventType) + '<br/>' +
                'key: ' + escapeHtml(point.eventKey) + '<br/>' +
                'desc: ' + escapeHtml(point.eventDescription) + '<br/>' +
                'lat: ' + escapeHtml(point.latitude) + '<br/>' +
                'lon: ' + escapeHtml(point.longitude)
              );
            });

            map.fitBounds(L.latLngBounds(allBoundsPoints), { padding: [24, 24] });

            const routeToggleButton = document.getElementById('toggle-route');
            const roadsToggleButton = document.getElementById('toggle-roads');
            let routeVisible = true;
            let roadsVisible = true;

            routeToggleButton?.addEventListener('click', () => {
              if (routeVisible) {
                map.removeLayer(routeLayer);
                routeToggleButton.textContent = 'Mostrar ruta';
              } else {
                map.addLayer(routeLayer);
                routeToggleButton.textContent = 'Ocultar ruta';
              }
              routeVisible = !routeVisible;
            });

            roadsToggleButton?.addEventListener('click', () => {
              if (roadsVisible) {
                map.removeLayer(roadsLayer);
                roadsToggleButton.textContent = 'Mostrar carreteras';
              } else {
                map.addLayer(roadsLayer);
                roadsToggleButton.textContent = 'Ocultar carreteras';
              }
              roadsVisible = !roadsVisible;
            });
          </script>
        </body>
      </html>
    `

    const blob = new Blob([htmlContent], { type: 'text/html' })
    const blobUrl = URL.createObjectURL(blob)
    mapWindow.location.replace(blobUrl)
    mapWindow.addEventListener('beforeunload', () => URL.revokeObjectURL(blobUrl), { once: true })
  }

  const openGapAnalysisWindow = (route, routeIndex) => {
    const intervalRecords = getRouteIntervalRecords(route)
    const gapAnalysis = buildRouteGapAnalysis(intervalRecords)

    if (!Array.isArray(gapAnalysis.intervals) || gapAnalysis.intervals.length === 0) {
      setError('No hay suficientes puntos con timestamp para graficar brechas en una ventana aparte.')
      return
    }

    const popup = window.open('', '_blank', 'width=1280,height=860')
    if (!popup) {
      setError('No se pudo abrir la ventana. Revisa el bloqueador de popups del navegador.')
      return
    }

    const safeIntervals = JSON.stringify(gapAnalysis.intervals).replaceAll('</', String.raw`<\/`)
    const title = `Brechas de tiempo - Ruta #${routeIndex + 1} assetId=${routeDetection?.asset_id || '-'} clientId=${routeDetection?.client_id || '-'}`
    const summary = {
      total: gapAnalysis.intervals.length,
      totalGap: formatGapDuration(gapAnalysis.totalGapSeconds),
      avgGap: formatGapDuration(gapAnalysis.avgGapSeconds),
      minGap: formatGapDuration(gapAnalysis.minGapSeconds),
      maxGap: formatGapDuration(gapAnalysis.maxGapSeconds),
      missingGps: gapAnalysis.missingGpsIntervals,
      alertThreshold: GAP_ALERT_SECONDS,
      alertIntervals: gapAnalysis.alertIntervals,
    }

    const safeSummary = JSON.stringify(summary).replaceAll('</', String.raw`<\/`)

    const html = `
      <!doctype html>
      <html lang="es">
        <head>
          <meta charset="utf-8" />
          <meta name="viewport" content="width=device-width, initial-scale=1" />
          <title>${title}</title>
          <style>
            :root { color-scheme: light; }
            body {
              margin: 0;
              font-family: Inter, system-ui, sans-serif;
              background: #f8fafc;
              color: #0f172a;
            }
            .container {
              max-width: 1200px;
              margin: 0 auto;
              padding: 16px;
            }
            .card {
              background: #ffffff;
              border: 1px solid #e2e8f0;
              border-radius: 10px;
              padding: 14px;
              margin-bottom: 14px;
            }
            .title {
              margin: 0 0 8px;
              font-size: 18px;
            }
            .summary {
              display: grid;
              grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
              gap: 8px;
              font-size: 13px;
            }
            .summary .pill {
              background: #f1f5f9;
              border: 1px solid #cbd5e1;
              border-radius: 8px;
              padding: 8px;
            }
            .chart-wrap {
              border: 1px solid #e2e8f0;
              border-radius: 10px;
              padding: 10px;
              background: linear-gradient(180deg, #ffffff 0%, #f8fafc 100%);
            }
            .legend {
              display: flex;
              gap: 12px;
              flex-wrap: wrap;
              font-size: 12px;
              margin-bottom: 8px;
            }
            .dot { width: 10px; height: 10px; border-radius: 50%; display: inline-block; margin-right: 6px; }
            .dot.normal { background: #2563eb; }
            .dot.missing { background: #f59e0b; }
            .dot.alert { background: #dc2626; }
            .chart {
              position: relative;
              height: 260px;
              display: flex;
              align-items: flex-end;
              gap: 2px;
              overflow-x: auto;
              padding: 0 0 20px;
              border-bottom: 1px solid #94a3b8;
            }
            .grid-line {
              position: absolute;
              left: 0;
              right: 0;
              border-top: 1px dashed #cbd5e1;
              pointer-events: none;
            }
            .grid-label {
              position: absolute;
              right: 0;
              transform: translateY(-50%);
              background: #ffffffcc;
              font-size: 11px;
              padding: 1px 4px;
              color: #475569;
            }
            .threshold {
              position: absolute;
              left: 0;
              right: 0;
              border-top: 2px solid #ef4444;
              pointer-events: none;
            }
            .bar {
              width: 8px;
              min-width: 8px;
              border-radius: 3px 3px 0 0;
              background: #2563eb;
            }
            .bar.missing { background: #f59e0b; }
            .bar.alert { background: #dc2626; }
            table { width: 100%; border-collapse: collapse; font-size: 12px; }
            th, td { border-bottom: 1px solid #e2e8f0; padding: 6px; text-align: left; white-space: nowrap; }
            th { background: #f8fafc; position: sticky; top: 0; }
            .table-wrap { max-height: 320px; overflow: auto; border: 1px solid #e2e8f0; border-radius: 8px; }
          </style>
        </head>
        <body>
          <div class="container">
            <div class="card">
              <h1 class="title">${title}</h1>
              <div id="summary" class="summary"></div>
            </div>

            <div class="card">
              <div class="legend">
                <span><i class="dot normal"></i>Normal</span>
                <span><i class="dot missing"></i>Sin GPS</span>
                <span><i class="dot alert"></i>Brecha >= umbral</span>
              </div>
              <div id="chart" class="chart-wrap"></div>
            </div>

            <div class="card">
              <div class="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>#</th>
                      <th>Desde</th>
                      <th>Hasta</th>
                      <th>Brecha (s)</th>
                      <th>Sin GPS</th>
                      <th>eventType</th>
                    </tr>
                  </thead>
                  <tbody id="rows"></tbody>
                </table>
              </div>
            </div>
          </div>

          <script>
            const intervals = ${safeIntervals};
            const summary = ${safeSummary};

            const formatTimestamp = (value) => {
              if (!value) return '';
              const date = new Date(value);
              if (Number.isNaN(date.getTime())) return String(value);
              return date.toLocaleString('es-MX', {
                year: 'numeric', month: '2-digit', day: '2-digit',
                hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false,
              });
            };

            const summaryNode = document.getElementById('summary');
            summaryNode.innerHTML = [
              ['Intervalos', String(summary.total)],
              ['Tiempo acumulado', summary.totalGap],
              ['Brecha promedio', summary.avgGap],
              ['Brecha mínima', summary.minGap],
              ['Brecha máxima', summary.maxGap],
              ['Intervalos sin GPS', String(summary.missingGps)],
              ['Brechas >= ' + String(summary.alertThreshold) + 's', String(summary.alertIntervals)],
            ].map(([label, value]) =>
              '<div class="pill"><strong>' + label + ':</strong> ' + value + '</div>'
            ).join('');

            const chartOuter = document.getElementById('chart');
            const chart = document.createElement('div');
            chart.className = 'chart';
            chartOuter.appendChild(chart);

            const maxGap = Math.max(...intervals.map((item) => Number(item.gapSeconds) || 0), 1);
            [0, 25, 50, 75, 100].forEach((pct) => {
              const line = document.createElement('div');
              line.className = 'grid-line';
              line.style.bottom = pct + '%';
              chart.appendChild(line);

              const label = document.createElement('div');
              label.className = 'grid-label';
              label.style.bottom = pct + '%';
              label.textContent = Math.round((maxGap * pct) / 100) + ' s';
              chart.appendChild(label);
            });

            const thresholdPct = Math.min(100, (summary.alertThreshold / maxGap) * 100);
            const threshold = document.createElement('div');
            threshold.className = 'threshold';
            threshold.style.bottom = thresholdPct + '%';
            chart.appendChild(threshold);

            intervals.forEach((item) => {
              const bar = document.createElement('div');
              const barHeight = Math.max(6, Math.round((Number(item.gapSeconds) / maxGap) * 220));
              bar.className = 'bar' +
                (item.missingGps ? ' missing' : '') +
                (Number(item.gapSeconds) >= Number(summary.alertThreshold) ? ' alert' : '');
              bar.style.height = barHeight + 'px';
              bar.title = [
                'Intervalo #' + String(item.sequence),
                'Brecha: ' + String(Number(item.gapSeconds).toFixed(1)) + ' s',
                'Sin GPS: ' + (item.missingGps ? 'sí' : 'no'),
                'Desde: ' + formatTimestamp(item.fromOrderTimestamp),
                'Hasta: ' + formatTimestamp(item.toOrderTimestamp),
              ].join(' | ');
              chart.appendChild(bar);
            });

            const rowsNode = document.getElementById('rows');
            rowsNode.innerHTML = intervals.map((item) =>
              '<tr>' +
              '<td>' + String(item.sequence) + '</td>' +
              '<td>' + formatTimestamp(item.fromOrderTimestamp) + '</td>' +
              '<td>' + formatTimestamp(item.toOrderTimestamp) + '</td>' +
              '<td>' + String(Number(item.gapSeconds).toFixed(1)) + '</td>' +
              '<td>' + (item.missingGps ? 'Sí' : 'No') + '</td>' +
              '<td>' + String(item.fromEventType ?? '') + ' → ' + String(item.toEventType ?? '') + '</td>' +
              '</tr>'
            ).join('');
          </script>
        </body>
      </html>
    `

    const blob = new Blob([html], { type: 'text/html' })
    const blobUrl = URL.createObjectURL(blob)
    popup.location.replace(blobUrl)
    popup.addEventListener('beforeunload', () => URL.revokeObjectURL(blobUrl), { once: true })
  }

  const buildMlClassRoadsIndex = () => {
    const index = {}
    const completeRoutes = Array.isArray(routeDetection?.complete_routes)
      ? routeDetection.complete_routes
      : []

    completeRoutes.forEach((candidateRoute) => {
      const prediction = mlPredictionsByRoute[getRouteKey(candidateRoute)]
      const className = String(prediction?.predicted_route || '').trim()
      if (!className) {
        return
      }

      if (!index[className]) {
        index[className] = {}
      }

      const roads = Array.isArray(candidateRoute?.matched_roads)
        ? candidateRoute.matched_roads.map((value) => String(value || '').trim()).filter(Boolean)
        : []

      roads.forEach((roadLabel) => {
        index[className][roadLabel] = (index[className][roadLabel] || 0) + 1
      })
    })

    const sortedIndex = {}
    Object.entries(index).forEach(([className, counts]) => {
      sortedIndex[className] = Object.entries(counts)
        .sort((left, right) => right[1] - left[1])
        .map(([label]) => label)
    })

    return sortedIndex
  }

  const openMlPredictedRouteMapWindow = async (route, routeIndex) => {
    const prediction = mlPredictionsByRoute[getRouteKey(route)]
    const predictedClass = String(prediction?.predicted_route || '').trim()
    if (!predictedClass) {
      setError('Esta ruta aún no tiene predicción ML disponible.')
      return
    }

    const classRoadsIndex = buildMlClassRoadsIndex()
    const predictedRoadLabels = (classRoadsIndex[predictedClass] || []).slice(0, 12)
    if (predictedRoadLabels.length === 0) {
      setError(`No hay mapeo de carreteras disponible para ${predictedClass}.`)
      return
    }

    let predictedRoadSegments = []
    try {
      const response = await fetch(`${API_BASE}/data/carreteras/rows?limit=3000&offset=0`)
      if (!response.ok) {
        const detail = await response.json()
        throw new Error(detail.detail || 'No se pudieron cargar las carreteras para mapa ML')
      }

      const data = await response.json()
      const rows = Array.isArray(data.rows) ? data.rows : []
      const roadSet = new Set(predictedRoadLabels)

      predictedRoadSegments = rows
        .filter((row) => {
          const name = String(row.name || '').trim()
          const routeName = String(row.p_route || '').trim()
          return roadSet.has(name) || roadSet.has(routeName)
        })
        .map((row, index) => {
          const latlngs = parseLinestringWkt(row.geometry)
          if (latlngs.length < 2) {
            return null
          }

          return {
            id: index + 1,
            label: String(row.name || row.p_route || '').trim(),
            route: String(row.p_route || '').trim(),
            no: String(row.p_no || '').trim(),
            latlngs,
          }
        })
        .filter(Boolean)
    } catch (err) {
      setError(err.message)
      return
    }

    if (predictedRoadSegments.length === 0) {
      setError(`No se encontraron geometrías para la predicción ML ${predictedClass}.`)
      return
    }

    const intervalRecords = getRouteIntervalRecords(route)
    const actualPoints = intervalRecords
      .filter((record) => Number.isFinite(Number(record.latitude)) && Number.isFinite(Number(record.longitude)))
      .map((record) => [Number(record.latitude), Number(record.longitude)])

    const mapWindow = window.open('', '_blank', 'width=1200,height=800')
    if (!mapWindow) {
      setError('No se pudo abrir la ventana del mapa. Revisa el bloqueador de popups del navegador.')
      return
    }

    const safeRoadsJson = JSON.stringify(predictedRoadSegments).replaceAll('</', String.raw`<\/`)
    const safeActualPointsJson = JSON.stringify(actualPoints).replaceAll('</', String.raw`<\/`)
    const title = `Ruta ML #${routeIndex + 1} class=${predictedClass} assetId=${routeDetection?.asset_id || '-'} clientId=${routeDetection?.client_id || '-'}`

    const htmlContent = `
      <!doctype html>
      <html lang="es">
        <head>
          <meta charset="utf-8" />
          <meta name="viewport" content="width=device-width, initial-scale=1" />
          <title>${title}</title>
          <link
            rel="stylesheet"
            href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
            integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY="
            crossorigin=""
          />
          <style>
            html, body { margin: 0; padding: 0; height: 100%; font-family: Inter, system-ui, sans-serif; }
            #map { width: 100%; height: calc(100% - 56px); }
            .header {
              height: 56px;
              display: flex;
              align-items: center;
              padding: 0 16px;
              border-bottom: 1px solid #e5e7eb;
              background: #f9fafb;
              font-size: 14px;
              color: #111827;
            }
          </style>
        </head>
        <body>
          <div class="header">${title} · carreteras inferidas: ${predictedRoadSegments.length}</div>
          <div id="map"></div>

          <script
            src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
            integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo="
            crossorigin=""
          ></script>
          <script>
            const roads = ${safeRoadsJson};
            const actualPoints = ${safeActualPointsJson};
            const map = L.map('map');

            L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
              maxZoom: 19,
              attribution: '&copy; OpenStreetMap contributors',
            }).addTo(map);

            const allPoints = [];
            roads.forEach((road) => {
              const line = L.polyline(road.latlngs, {
                color: '#7c3aed',
                weight: 4,
                opacity: 0.9,
              }).addTo(map);
              allPoints.push(...road.latlngs);

              line.bindPopup(
                '<b>Ruta ML</b><br/>' +
                'Carretera: ' + String(road.label || 'N/A') + '<br/>' +
                'Ruta: ' + String(road.route || 'N/A') + '<br/>' +
                'No: ' + String(road.no || 'N/A')
              );
            });

            if (Array.isArray(actualPoints) && actualPoints.length >= 2) {
              const actualLine = L.polyline(actualPoints, {
                color: '#2563eb',
                weight: 3,
                opacity: 0.8,
                dashArray: '6,4',
              }).addTo(map);
              allPoints.push(...actualPoints);
              actualLine.bindPopup('<b>Ruta real (referencia)</b>');
            }

            if (allPoints.length > 0) {
              map.fitBounds(L.latLngBounds(allPoints), { padding: [24, 24] });
            } else {
              map.setView([23.6345, -102.5528], 5);
            }
          </script>
        </body>
      </html>
    `

    const blob = new Blob([htmlContent], { type: 'text/html' })
    const blobUrl = URL.createObjectURL(blob)
    mapWindow.location.replace(blobUrl)
    mapWindow.addEventListener('beforeunload', () => URL.revokeObjectURL(blobUrl), { once: true })
  }

  const parseLinestringWkt = (wkt) => {
    if (!wkt) {
      return []
    }

    const text = String(wkt).trim()
    const regex = /^LINESTRING\s*\((.*)\)$/i
    const match = regex.exec(text)
    if (!match?.[1]) {
      return []
    }

    return match[1]
      .split(',')
      .map((pointText) => pointText.trim().split(/\s+/).filter(Boolean))
      .filter((parts) => parts.length >= 2)
      .map(([lonText, latText]) => {
        const longitude = Number(lonText)
        const latitude = Number(latText)

        if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
          return null
        }

        return [latitude, longitude]
      })
      .filter(Boolean)
  }


  const openCarreterasMapWindow = async () => {
    setError('')

    try {
      const response = await fetch(`${API_BASE}/data/carreteras/rows?limit=2000&offset=0`)
      if (!response.ok) {
        const detail = await response.json()
        throw new Error(detail.detail || 'No se pudieron cargar las carreteras')
      }

      const data = await response.json()
      const roadRows = Array.isArray(data.rows) ? data.rows : []

      const filteredRoadRows = roadRows.filter((row) => {
        if (selectedCarretera === 'ALL') {
          return true
        }

        const name = String(row.name || '').trim()
        const route = String(row.p_route || '').trim()
        return name === selectedCarretera || route === selectedCarretera
      })

      const segments = filteredRoadRows
        .map((row, index) => {
          const latlngs = parseLinestringWkt(row.geometry)
          if (latlngs.length < 2) {
            return null
          }

          return {
            id: index + 1,
            name: row.name || row.p_route || `Carretera ${index + 1}`,
            route: row.p_route || '',
            no: row.p_no || '',
            latlngs,
          }
        })
        .filter(Boolean)

      if (segments.length === 0) {
        setError('No se encontraron geometrías LINESTRING válidas para carreteras')
        return
      }

      const mapWindow = window.open('', '_blank', 'width=1280,height=860')
      if (!mapWindow) {
        setError('No se pudo abrir la ventana del mapa. Revisa el bloqueador de popups del navegador.')
        return
      }

      const safeSegmentsJson = JSON.stringify(segments).replaceAll('</', String.raw`<\/`)
      const htmlContent = `
        <!doctype html>
        <html lang="es">
          <head>
            <meta charset="utf-8" />
            <meta name="viewport" content="width=device-width, initial-scale=1" />
            <title>Mapa de carreteras</title>
            <link
              rel="stylesheet"
              href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
              integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY="
              crossorigin=""
            />
            <style>
              html, body { margin: 0; padding: 0; height: 100%; font-family: Inter, system-ui, sans-serif; }
              #map { width: 100%; height: calc(100% - 56px); }
              .header {
                height: 56px;
                display: flex;
                align-items: center;
                padding: 0 16px;
                border-bottom: 1px solid #e5e7eb;
                background: #f9fafb;
                font-size: 14px;
                color: #111827;
              }
            </style>
          </head>
          <body>
            <div class="header">Carreteras cargadas desde DuckDB · filtro: ${selectedCarretera === 'ALL' ? 'Todas' : selectedCarretera} · segmentos: ${segments.length}</div>
            <div id="map"></div>

            <script
              src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
              integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo="
              crossorigin=""
            ></script>
            <script>
              const roads = ${safeSegmentsJson};
              const map = L.map('map');

              L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
                maxZoom: 19,
                attribution: '&copy; OpenStreetMap contributors',
              }).addTo(map);

              const allPoints = [];
              roads.forEach((road, index) => {
                const colorHue = (index * 29) % 360;
                const color = 'hsl(' + colorHue + ' 72% 45%)';
                const polyline = L.polyline(road.latlngs, {
                  color,
                  weight: 3,
                  opacity: 0.72,
                }).addTo(map);

                polyline.bindPopup(
                  '<b>' + String(road.name) + '</b><br/>' +
                  'Ruta: ' + String(road.route || 'N/A') + '<br/>' +
                  'No: ' + String(road.no || 'N/A') + '<br/>' +
                  'Puntos: ' + String(road.latlngs.length)
                );

                allPoints.push(...road.latlngs);
              });

              if (allPoints.length > 0) {
                map.fitBounds(L.latLngBounds(allPoints), { padding: [20, 20] });
              } else {
                map.setView([23.6345, -102.5528], 5);
              }
            </script>
          </body>
        </html>
      `

      const blob = new Blob([htmlContent], { type: 'text/html' })
      const blobUrl = URL.createObjectURL(blob)
      mapWindow.location.replace(blobUrl)
      mapWindow.addEventListener('beforeunload', () => URL.revokeObjectURL(blobUrl), { once: true })
    } catch (err) {
      setError(err.message)
    }
  }

  return (
    <main className="page">
      <h1>Sistema de análisis de rutas con ML</h1>
      <p className="subtitle">Backend Python + TensorFlow | Frontend React</p>

      <div className="menu-tabs" role="tablist" aria-label="Navegación principal">
        <button
          type="button"
          role="tab"
          aria-selected={activeMenu === 'dashboard'}
          className={`menu-tab ${activeMenu === 'dashboard' ? 'active' : ''}`}
          onClick={() => setActiveMenu('dashboard')}
        >
          Dashboard
        </button>
        <button
          type="button"
          role="tab"
          aria-selected={activeMenu === 'carreteras'}
          className={`menu-tab ${activeMenu === 'carreteras' ? 'active' : ''}`}
          onClick={() => setActiveMenu('carreteras')}
        >
          Carreteras
        </button>
        <button
          type="button"
          role="tab"
          aria-selected={activeMenu === 'administracion'}
          className={`menu-tab ${activeMenu === 'administracion' ? 'active' : ''}`}
          onClick={() => setActiveMenu('administracion')}
        >
          Administracion
        </button>
      </div>

      {activeMenu === 'dashboard' && (
        <>

      <section className="card">
        <h2>1) Estado backend / snapshot</h2>
        <div className="toolbar">
          <button onClick={loadSnapshotStatus} disabled={snapshotLoading}>
            {snapshotLoading ? 'Consultando...' : 'Ver estado snapshot'}
          </button>
        </div>

        <p className="subtitle source-chip">{sourceLabel}</p>

        {snapshotStatus && (
          <>
            <p className="subtitle">Snapshot existe: {snapshotStatus.exists ? 'sí' : 'no'}</p>
            <p className="subtitle">DuckDB: {snapshotStatus.db_path}</p>
            <p className="subtitle">
              Última actualización: {formatIsoDateTime(snapshotStatus.last_updated_iso)} ({formatRelativeAge(snapshotStatus.last_updated_iso)})
            </p>
            {snapshotStatus.merged_rows != null && (
              <p className="subtitle">
                Filas en snapshot: {snapshotStatus.merged_rows.toLocaleString()} · Assets: {(snapshotStatus.distinct_assets || 0).toLocaleString()}
              </p>
            )}
          </>
        )}

      </section>

      <section className="card">
        <h2>2) Datos actuales (POS + EventType)</h2>
        <p className="subtitle source-chip">{sourceLabel}</p>
        <div className="toolbar">
          <button onClick={() => loadRows(0, pageSize, assetIdFilter, clientIdFilter, eventTypeFilter)} disabled={rowsLoading}>
            {rowsLoading ? 'Cargando...' : 'Cargar datos'}
          </button>
          <label className="inline-label">
            <span>assetId</span>
            <input
              type="number"
              value={assetIdFilter}
              onChange={(event) => setAssetIdFilter(event.target.value)}
              onKeyDown={(event) => event.key === 'Enter' && handleApplyFilters()}
              placeholder="Ej. 598"
            />
          </label>
          <label className="inline-label">
            <span>clientId</span>
            <input
              type="number"
              value={clientIdFilter}
              onChange={(event) => setClientIdFilter(event.target.value)}
              onKeyDown={(event) => event.key === 'Enter' && handleApplyFilters()}
              placeholder="Ej. 64"
            />
          </label>
          <label className="inline-label">
            <span>eventType</span>
            <input
              type="number"
              value={eventTypeFilter}
              onChange={(event) => setEventTypeFilter(event.target.value)}
              onKeyDown={(event) => event.key === 'Enter' && handleApplyFilters()}
              placeholder="Ej. 519"
            />
          </label>
          <button onClick={handleApplyFilters} disabled={rowsLoading}>
            Aplicar filtros
          </button>
          <button onClick={handleClearFilters} disabled={rowsLoading}>
            Limpiar filtros
          </button>
          <label className="inline-label">
            <span>Buscar en página</span>
            <input
              type="text"
              value={quickSearch}
              onChange={(event) => setQuickSearch(event.target.value)}
              placeholder="Texto..."
            />
          </label>
          <label className="inline-label">
            <span>Filas por página</span>
            <select value={pageSize} onChange={handlePageSizeChange}>
              <option value={50}>50</option>
              <option value={100}>100</option>
              <option value={250}>250</option>
              <option value={500}>500</option>
            </select>
          </label>
        </div>

        {rowsData && (
          <>
            <p className="subtitle">
              Total filas (backend): {rowsData.total_rows.toLocaleString()} · Mostrando en página: {displayedRows.length} · offset: {rowsData.offset}
            </p>

            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    {displayedColumns.map((columnName) => (
                      <th
                        key={columnName}
                        className="sortable"
                        onClick={() => handleSort(columnName)}
                        title="Click para ordenar"
                      >
                        {columnName}
                        {getSortIndicator(columnName)}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {displayedRows.map((row, index) => (
                    <tr key={`${rowsData.offset}-${index}`}>
                      {displayedColumns.map((columnName) => (
                        <td key={columnName}>
                          {columnName === 'gpsTimestamp' || columnName === 'serverTimestamp'
                            ? formatTimestamp(row[columnName])
                            : row[columnName] ?? ''}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            <div className="pager">
              <button onClick={handlePrevPage} disabled={rowsLoading || offset === 0}>
                Anterior
              </button>
              <button
                onClick={handleNextPage}
                disabled={rowsLoading || offset + pageSize >= rowsData.total_rows}
              >
                Siguiente
              </button>
            </div>
          </>
        )}
      </section>

      <section className="card">
        <h2>3) Detección de rutas por assetId/clientId (519/520)</h2>
        <p className="subtitle source-chip">{sourceLabel}</p>
        <p className="subtitle">
          Se usa <strong>519</strong> como inicio y <strong>520</strong> como fin. El cálculo ordena por
          gpsTimestamp/serverTimestamp legibles, empareja eventos y reporta rutas incompletas.
        </p>

        <div className="toolbar">
          <label className="inline-label">
            <span>assetId</span>
            <input
              type="number"
              value={routeAssetId}
              onChange={(event) => setRouteAssetId(event.target.value)}
              placeholder="Ej. 598"
            />
          </label>
          <label className="inline-label">
            <span>clientId</span>
            <input
              type="number"
              value={routeClientId}
              onChange={(event) => setRouteClientId(event.target.value)}
              placeholder="Ej. 64"
            />
          </label>
          <button onClick={detectRoutes} disabled={routeLoading}>
            {routeLoading ? 'Analizando...' : 'Detectar rutas'}
          </button>
          <button onClick={openRouteMapWindow} disabled={routeLoading || !routeDetection}>
            Ver mapa ruta
          </button>
        </div>

        {routeDetection && (
          <>
            <p className="subtitle">
              Registros analizados: {routeDetection.total_records_analyzed.toLocaleString()} · Rutas completas:{' '}
              {routeDetection.complete_routes_count.toLocaleString()} · Rutas incompletas:{' '}
              <strong className={routeDetection.incomplete_routes_count > 0 ? 'warning' : 'success'}>
                {routeDetection.incomplete_routes_count.toLocaleString()}
              </strong>
            </p>

            <h3>Rutas completas</h3>
            {mlPredictionsLoading && (
              <p className="subtitle">Calculando predicciones ML para rutas completas...</p>
            )}
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Inicio</th>
                    <th>Fin</th>
                    <th>Duración (h)</th>
                    <th>Distancia (km)</th>
                    <th>Total eventos</th>
                    <th>Tipos únicos</th>
                    <th>Puntos GPS</th>
                    <th>Brecha mínima (s)</th>
                    <th>Match carreteras</th>
                    <th>Carreteras detectadas</th>
                    <th>Ruta ML</th>
                    <th>Confianza ML</th>
                  </tr>
                </thead>
                <tbody>
                  {routeDetection.complete_routes.map((route) => {
                    const mlPrediction = mlPredictionsByRoute[getRouteKey(route)]
                    const routeGapAnalysis = buildRouteGapAnalysis(getRouteIntervalRecords(route))
                    const mlConfidenceLabel = Number.isFinite(Number(mlPrediction?.confidence))
                      ? `${(Number(mlPrediction.confidence) * 100).toFixed(1)}%`
                      : ''
                    return (
                    <tr key={`route-${route.start_index}-${route.end_index}-${route.start_timestamp || 'na'}`}>
                      <td>{route.start_timestamp || ''}</td>
                      <td>{route.end_timestamp || ''}</td>
                      <td>{route.duration_hours == null ? '' : route.duration_hours.toFixed(2)}</td>
                      <td>{route.distance_km.toFixed(2)}</td>
                      <td>{route.total_events}</td>
                      <td>{route.unique_event_types}</td>
                      <td>{route.total_gps_points}</td>
                      <td>{routeGapAnalysis.intervals.length > 0 ? routeGapAnalysis.minGapSeconds.toFixed(1) : ''}</td>
                      <td>
                        {route.road_match_ratio == null
                          ? '0.0%'
                          : `${(Number(route.road_match_ratio) * 100).toFixed(1)}%`}
                      </td>
                      <td>{Array.isArray(route.matched_roads) ? route.matched_roads.join(' | ') : ''}</td>
                      <td>{mlPrediction?.predicted_route || (mlPrediction?.error ? 'N/A' : '')}</td>
                      <td>{mlConfidenceLabel || (mlPrediction?.error ? 'N/A' : '')}</td>
                    </tr>
                    )
                  })}
                  {routeDetection.complete_routes.length === 0 && (
                    <tr>
                      <td colSpan={12}>No se detectaron rutas completas 519→520 para este asset/client.</td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>

            <h3>Puntos de cada ruta completa (GPS/Server Timestamp)</h3>
            <div className="toolbar">
              <label className="inline-label">
                <span>Filtro rápido</span>
                <select value={routePointsFilterMode} onChange={(event) => setRoutePointsFilterMode(event.target.value)}>
                  <option value="all">Todos</option>
                  <option value="boundaries">Solo INICIO/FIN</option>
                  <option value="eventType">Por eventType</option>
                </select>
              </label>
              {routePointsFilterMode === 'eventType' && (
                <label className="inline-label">
                  <span>eventType</span>
                  <input
                    type="number"
                    value={routePointsEventTypeFilter}
                    onChange={(event) => setRoutePointsEventTypeFilter(event.target.value)}
                    placeholder="Ej. 519"
                  />
                </label>
              )}
            </div>
            {routeDetection.complete_routes.map((route, routeIndex) => (
              <div
                className="route-points-block"
                key={`route-points-${route.start_index}-${route.end_index}-${route.start_timestamp || 'na'}`}
              >
                {(() => {
                  const intervalRecords = getRouteIntervalRecords(route)
                  const visibleIntervalRecords = filterIntervalRecords(route, intervalRecords)
                  const mlPrediction = mlPredictionsByRoute[getRouteKey(route)]
                  const mlClassRoadsIndex = buildMlClassRoadsIndex()
                  const mlPredictedRoads = mlPrediction?.predicted_route
                    ? (mlClassRoadsIndex[mlPrediction.predicted_route] || [])
                    : []
                  const gapAnalysis = buildRouteGapAnalysis(intervalRecords)
                  const chartIntervals = gapAnalysis.intervals.slice(0, 240)
                  return (
                    <>
                      <p className="subtitle">
                        Ruta #{routeIndex + 1} · Inicio: {formatTimestamp(route.start_timestamp)} · Fin: {formatTimestamp(route.end_timestamp)} ·
                        Registros entre eventos inicio/fin: {intervalRecords.length} ·
                        Mostrando: {visibleIntervalRecords.length}
                      </p>
                      <p className="subtitle">
                        Carreteras involucradas: {Array.isArray(route.matched_roads) && route.matched_roads.length > 0
                          ? route.matched_roads.join(' | ')
                          : 'sin match de carretera'}
                      </p>
                      <p className="subtitle">
                        Predicción ML:{' '}
                        {mlPrediction?.predicted_route
                          ? `${mlPrediction.predicted_route} (${(Number(mlPrediction.confidence) * 100).toFixed(1)}%)`
                          : (mlPrediction?.error || 'pendiente')}
                      </p>
                      <p className="subtitle">
                        Carreteras inferidas para la clase ML:{' '}
                        {mlPredictedRoads.length > 0 ? mlPredictedRoads.slice(0, 8).join(' | ') : 'sin mapeo todavía'}
                      </p>
                      <div className="toolbar">
                        <button onClick={() => openCompleteRouteIntervalMap(route, routeIndex)}>
                          Ver mapa de esta ruta completa
                        </button>
                        <button onClick={() => openMlPredictedRouteMapWindow(route, routeIndex)}>
                          Ver mapa ruta ML
                        </button>
                        <button onClick={() => openGapAnalysisWindow(route, routeIndex)}>
                          Ver gráfico en ventana aparte
                        </button>
                      </div>
                      <div className="table-wrap">
                        <table>
                          <thead>
                            <tr>
                              <th>#</th>
                              <th>Marca</th>
                              <th>Orden</th>
                              <th>GPS</th>
                              <th>Server</th>
                              <th>eventType</th>
                              <th>Key</th>
                              <th>Description</th>
                              <th>Carretera</th>
                              <th>Dist. a carretera (m)</th>
                              <th>Lat</th>
                              <th>Lon</th>
                            </tr>
                          </thead>
                          <tbody>
                            {visibleIntervalRecords.map((record, recordIndex) => renderIntervalRecordRow(route, record, recordIndex))}
                            {visibleIntervalRecords.length === 0 && (
                              <tr>
                                <td colSpan={12}>No hay registros para el filtro seleccionado en esta ruta.</td>
                              </tr>
                            )}
                          </tbody>
                        </table>
                      </div>

                      <p className="subtitle">
                        Análisis de brechas temporales (ruta original): intervalos {gapAnalysis.intervals.length} ·
                        tiempo acumulado {formatGapDuration(gapAnalysis.totalGapSeconds)} ·
                        brecha promedio {formatGapDuration(gapAnalysis.avgGapSeconds)} ·
                        brecha mínima {formatGapDuration(gapAnalysis.minGapSeconds)} ·
                        brecha máxima {formatGapDuration(gapAnalysis.maxGapSeconds)} ·
                        intervalos sin GPS {gapAnalysis.missingGpsIntervals} ·
                        brechas &gt;= {GAP_ALERT_SECONDS}s {gapAnalysis.alertIntervals}
                      </p>

                      {chartIntervals.length > 0 && (
                        <>
                          <div className="gap-chart" role="img" aria-label="Gráfico de brechas de tiempo entre puntos consecutivos">
                            {chartIntervals.map((item) => {
                              const heightRatio = gapAnalysis.maxGapSeconds > 0 ? (item.gapSeconds / gapAnalysis.maxGapSeconds) : 0
                              const barHeight = Math.max(8, Math.round(heightRatio * 120))
                              const className = `gap-bar${item.missingGps ? ' missing-gps' : ''}${item.gapSeconds >= GAP_ALERT_SECONDS ? ' alert-gap' : ''}`
                              const tooltip = [
                                `Intervalo #${item.sequence}`,
                                `Duración: ${formatGapDuration(item.gapSeconds)}`,
                                `Sin GPS: ${item.missingGps ? 'sí' : 'no'}`,
                                `Desde: ${formatTimestamp(item.fromOrderTimestamp)}`,
                                `Hasta: ${formatTimestamp(item.toOrderTimestamp)}`,
                              ].join(' | ')

                              return (
                                <div
                                  key={`gap-${route.start_index}-${route.end_index}-${item.sequence}`}
                                  className={className}
                                  style={{ height: `${barHeight}px` }}
                                  title={tooltip}
                                />
                              )
                            })}
                          </div>
                          <p className="subtitle">
                            Azul: intervalo normal · Naranja: intervalo sin GPS en alguno de los extremos · Rojo: brecha mayor a {GAP_ALERT_SECONDS} s.
                            {gapAnalysis.intervals.length > chartIntervals.length
                              ? ` Mostrando ${chartIntervals.length} barras de ${gapAnalysis.intervals.length} intervalos.`
                              : ''}
                          </p>
                        </>
                      )}

                      <div className="table-wrap">
                        <table>
                          <thead>
                            <tr>
                              <th>#</th>
                              <th>Desde</th>
                              <th>Hasta</th>
                              <th>Brecha</th>
                              <th>Brecha (min)</th>
                              <th>Sin GPS</th>
                              <th>eventType (desde→hasta)</th>
                              <th>Carretera (desde→hasta)</th>
                            </tr>
                          </thead>
                          <tbody>
                            {gapAnalysis.intervals.map((item) => (
                              <tr key={`gap-row-${route.start_index}-${route.end_index}-${item.sequence}`}>
                                <td>{item.sequence}</td>
                                <td>{formatTimestamp(item.fromOrderTimestamp)}</td>
                                <td>{formatTimestamp(item.toOrderTimestamp)}</td>
                                <td>{formatGapDuration(item.gapSeconds)}</td>
                                <td>{item.gapMinutes.toFixed(2)}</td>
                                <td>{item.missingGps ? 'Sí' : 'No'}</td>
                                <td>{`${item.fromEventType ?? ''} → ${item.toEventType ?? ''}`}</td>
                                <td>{`${item.fromRoad || '-'} → ${item.toRoad || '-'}`}</td>
                              </tr>
                            ))}
                            {gapAnalysis.intervals.length === 0 && (
                              <tr>
                                <td colSpan={8}>No hay suficientes puntos con timestamp para calcular brechas.</td>
                              </tr>
                            )}
                          </tbody>
                        </table>
                      </div>

                    </>
                  )
                })()}
              </div>
            ))}
            {routeDetection.complete_routes.length === 0 && (
              <p className="subtitle">No hay puntos de rutas completas para mostrar.</p>
            )}

            <h3>Rutas incompletas / inconsistencias</h3>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Tipo</th>
                    <th>Inicio</th>
                    <th>Fin</th>
                    <th>Detalle</th>
                  </tr>
                </thead>
                <tbody>
                  {routeDetection.incomplete_routes.map((issue) => (
                    <tr key={`incomplete-${issue.type}-${issue.start_index ?? 'na'}-${issue.end_index ?? 'na'}-${issue.start_timestamp || 'na'}`}>
                      <td>{issue.type}</td>
                      <td>{issue.start_timestamp || ''}</td>
                      <td>{issue.end_timestamp || ''}</td>
                      <td>{issue.detail}</td>
                    </tr>
                  ))}
                  {routeDetection.incomplete_routes.length === 0 && (
                    <tr>
                      <td colSpan={4}>No hay rutas incompletas para este asset/client.</td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </>
        )}
      </section>

        </>
      )}

      {activeMenu === 'carreteras' && (
        <section className="card">
          <h2>Carreteras</h2>
          <p className="subtitle source-chip">{sourceLabel}</p>
          <div className="toolbar">
            <label className="inline-label">
              <span>Carretera</span>
              <select
                value={selectedCarretera}
                onChange={(event) => setSelectedCarretera(event.target.value)}
                disabled={carreterasCatalogLoading}
              >
                <option value="ALL">Todas</option>
                {carreterasCatalog.map((option) => (
                  <option key={option} value={option}>{option}</option>
                ))}
              </select>
            </label>
            <button onClick={loadCarreterasCatalog} disabled={carreterasCatalogLoading}>
              {carreterasCatalogLoading ? 'Actualizando carreteras...' : 'Actualizar carreteras'}
            </button>
            <button onClick={openCarreterasMapWindow}>
              Ver carreteras (ventana aparte)
            </button>
          </div>
          <p className="subtitle">Carreteras disponibles en catálogo: {carreterasCatalog.length}</p>
          <p className="subtitle">
            Usa el selector para filtrar una carretera específica o elige Todas para visualizar toda la red cargada.
          </p>
        </section>
      )}

      {activeMenu === 'administracion' && (
        <section className="card">
          <h2>Administracion</h2>
          <div className="manual-map-panel">
            <h3>Pintar ruta manual (ventana nueva)</h3>
            <div className="toolbar">
              <label className="inline-label">
                <span>Modo</span>
                <select value={manualMapMode} onChange={(event) => setManualMapMode(event.target.value)}>
                  <option value="coords">Coordenadas (lat,lon)</option>
                  <option value="olc">OLC</option>
                </select>
              </label>
              <button onClick={openManualMapWindow}>Abrir mapa manual</button>
            </div>
            <label>
              <span>
                {manualMapMode === 'coords'
                  ? 'Puntos en orden (lat,lon), uno por línea'
                  : 'OLC en orden, uno por línea'}
              </span>
              <textarea
                rows={6}
                value={manualMapInput}
                onChange={(event) => setManualMapInput(event.target.value)}
                placeholder={
                  manualMapMode === 'coords'
                    ? '31.002765,-110.247512\n31.003100,-110.248000'
                    : '853G7CV9+FR5\n853G7CV8+XQ'
                }
              />
            </label>
            <p className="subtitle">
              {manualMapMode === 'coords'
                ? 'Usa latitud y longitud separadas por coma o espacio.'
                : 'El modo OLC usa los OLC ya cargados para este asset/client y los pinta en el orden capturado.'}
            </p>
          </div>
        </section>
      )}

      {error && <p className="error">Error: {error}</p>}
    </main>
  )
}

export default App
