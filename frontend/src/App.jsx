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
  const [eventTypeFilter2, setEventTypeFilter2] = useState('')
  const [quickSearch, setQuickSearch] = useState('')
  const [sortBy, setSortBy] = useState('')
  const [sortDirection, setSortDirection] = useState('asc')
  const [routeAssetId, setRouteAssetId] = useState('598')
  const [routeClientId, setRouteClientId] = useState('64')
  const [routeDetection, setRouteDetection] = useState(null)
  const [mlPredictionsByRoute, setMlPredictionsByRoute] = useState({})
  const [mlPredictionsLoading, setMlPredictionsLoading] = useState(false)
  const [routeLoading, setRouteLoading] = useState(false)
  const [signalLossData, setSignalLossData] = useState(null)
  const [signalLossLoading, setSignalLossLoading] = useState(false)
  const [signalLossAnomalies, setSignalLossAnomalies] = useState(null)
  const [stopsData, setStopsData] = useState(null)
  const [stopsLoading, setStopsLoading] = useState(false)
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
    nextEventTypeFilter2 = eventTypeFilter2,
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
      if (nextEventTypeFilter2.trim() !== '') {
        query.set('event_type_2', nextEventTypeFilter2.trim())
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
    setSignalLossLoading(true)
    setError('')
    if (clearPrevious) {
      setRouteDetection(null)
      setSignalLossData(null)
      setSignalLossAnomalies(null)
      setStopsData(null)
    }

    if (!assetValue || !clientValue) {
      setRouteLoading(false)
      setSignalLossLoading(false)
      setError('Debes indicar assetId y clientId para detectar rutas')
      return
    }

    const query = new URLSearchParams({ asset_id: assetValue, client_id: clientValue })

    setStopsLoading(true)
    const [routeResult, signalResult, anomalyResult, stopsResult] = await Promise.allSettled([
      fetch(`${API_BASE}/routes/detect?${query.toString()}`),
      fetch(`${API_BASE}/routes/signal-loss?${query.toString()}`),
      fetch(`${API_BASE}/routes/signal-loss/anomalies?${query.toString()}`),
      fetch(`${API_BASE}/routes/stops?${query.toString()}`),
    ])

    // --- rutas ---
    try {
      if (routeResult.status === 'rejected') throw new Error(routeResult.reason?.message || 'Error de red')
      const resp = routeResult.value
      if (!resp.ok) {
        const detail = await resp.json()
        throw new Error(detail.detail || 'No se pudieron detectar rutas')
      }
      const data = await resp.json()
      setRouteDetection(data)
      setRoutePointsFilterMode('all')
      setRoutePointsEventTypeFilter('')
    } catch (err) {
      setError(err.message)
    } finally {
      setRouteLoading(false)
    }

    // --- señal ---
    try {
      if (signalResult.status === 'fulfilled' && signalResult.value.ok) {
        const data = await signalResult.value.json()
        setSignalLossData(data)
      }
    } finally {
      setSignalLossLoading(false)
    }

    // --- anomalías de señal ---
    if (anomalyResult.status === 'fulfilled' && anomalyResult.value.ok) {
      const data = await anomalyResult.value.json()
      setSignalLossAnomalies(data)
    }

    // --- paradas ---
    try {
      if (stopsResult.status === 'fulfilled' && stopsResult.value.ok) {
        const data = await stopsResult.value.json()
        setStopsData(data)
      }
    } finally {
      setStopsLoading(false)
    }
  }

  const detectRoutes = async () => {
    const assetValue = routeAssetId.trim()
    const clientValue = routeClientId.trim()
    await detectRoutesByValues(assetValue, clientValue, true)
  }

  const handlePrevPage = () => {
    const nextOffset = Math.max(0, offset - pageSize)
    loadRows(nextOffset, pageSize, assetIdFilter, clientIdFilter, eventTypeFilter, eventTypeFilter2)
  }

  const handleNextPage = () => {
    const totalRows = rowsData?.total_rows || 0
    const nextOffset = offset + pageSize
    if (nextOffset < totalRows) {
      loadRows(nextOffset, pageSize, assetIdFilter, clientIdFilter, eventTypeFilter, eventTypeFilter2)
    }
  }

  const handlePageSizeChange = (event) => {
    const nextLimit = Number(event.target.value)
    setPageSize(nextLimit)
    loadRows(0, nextLimit, assetIdFilter, clientIdFilter, eventTypeFilter, eventTypeFilter2)
  }

  const handleApplyFilters = () => {
    setRouteAssetId(assetIdFilter.trim())
    setRouteClientId(clientIdFilter.trim())
    loadRows(0, pageSize, assetIdFilter, clientIdFilter, eventTypeFilter, eventTypeFilter2)
  }

  const handleClearFilters = () => {
    setAssetIdFilter('')
    setClientIdFilter('')
    setEventTypeFilter('')
    setEventTypeFilter2('')
    setRouteAssetId('')
    setRouteClientId('')
    loadRows(0, pageSize, '', '', '', '')
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
    const hasValidStartDate = !Number.isNaN(startDate.getTime())
    const pandasDayOfWeek = hasValidStartDate ? ((startDate.getUTCDay() + 6) % 7) : 1

    const firstComp = Array.isArray(route?.components) ? route.components.find(
      (c) => c.latitude != null && c.longitude != null
    ) : null

    return {
      start_lat: firstComp?.latitude ?? 0,
      start_lon: firstComp?.longitude ?? 0,
      distance_km: clampRange(Number(route?.distance_km), 0, Number.MAX_SAFE_INTEGER),
      hour_of_day: hasValidStartDate ? startDate.getUTCHours() : 9,
      day_of_week: pandasDayOfWeek,
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
      setSignalLossData(null)
      setSignalLossAnomalies(null)
      setStopsData(null)
      setRouteLoading(false)
      setSignalLossLoading(false)
      setStopsLoading(false)
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

  const getRouteRoadLabels = (route) => {
    const inferred = Array.isArray(route?.inferred_roads)
      ? route.inferred_roads.map((value) => String(value || '').trim()).filter(Boolean)
      : []
    if (inferred.length > 0) {
      return inferred
    }

    const matched = Array.isArray(route?.matched_roads)
      ? route.matched_roads.map((value) => String(value || '').trim()).filter(Boolean)
      : []
    return matched
  }

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

  const renderIntervalRecordRow = (route, record, recordIndex, estimatedGpsMetadataBySourceIndex = {}) => {
    const role = getIntervalRecordRole(route, record)
    const estimatedMetadata = estimatedGpsMetadataBySourceIndex[recordIndex + 1] || null
    const baseRowClassName = getIntervalRowClassName(role)
    const rowClassName = [baseRowClassName, estimatedMetadata?.isEstimated ? 'route-point-row-estimated' : '']
      .filter(Boolean)
      .join(' ')
    const roleLabel = getIntervalRoleLabel(role)

    return (
      <tr
        key={`route-interval-${route.start_index}-${route.end_index}-${recordIndex}-${record.order_timestamp || 'na'}`}
        className={rowClassName}
      >
        <td>{recordIndex + 1}</td>
        <td>
          {roleLabel ? <span className={`event-badge ${role}`}>{roleLabel}</span> : ''}
          {estimatedMetadata?.isEstimated ? <span className="event-badge estimated">EST</span> : ''}
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
        <td>{estimatedMetadata?.isEstimated ? 'Si' : 'No'}</td>
        <td>{estimatedMetadata?.pointDescription || ''}</td>
        <td>{record.latitude ?? ''}</td>
        <td>{record.longitude ?? ''}</td>
      </tr>
    )
  }

  const filterIntervalRecords = (route, records, estimatedGpsMetadataBySourceIndex = {}) => {
    if (routePointsFilterMode === 'boundaries') {
      return records.filter((record) => {
        const role = getIntervalRecordRole(route, record)
        return role === 'start' || role === 'end'
      })
    }

    if (routePointsFilterMode === 'estimated') {
      return records.filter((_, recordIndex) => estimatedGpsMetadataBySourceIndex[recordIndex + 1]?.isEstimated)
    }

    if (routePointsFilterMode === 'original') {
      return records.filter((_, recordIndex) => !estimatedGpsMetadataBySourceIndex[recordIndex + 1]?.isEstimated)
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

  const findPreviousValidGpsRecord = (records, startIndex) => {
    for (let index = startIndex - 1; index >= 0; index -= 1) {
      const candidate = records[index]
      if (hasValidGpsPoint(candidate)) {
        return { index, record: candidate }
      }
    }
    return null
  }

  const findNextValidGpsRecord = (records, startIndex) => {
    for (let index = startIndex + 1; index < records.length; index += 1) {
      const candidate = records[index]
      if (hasValidGpsPoint(candidate)) {
        return { index, record: candidate }
      }
    }
    return null
  }

  const buildGpsPointsForRouteMap = (route) => {
    const records = getRouteIntervalRecords(route)
    if (!Array.isArray(records) || records.length === 0) {
      return []
    }

    const points = []
    records.forEach((record, index) => {
      const recordTimeMs = parseRecordTimeMs(record)
      const timestampText = record?.order_timestamp || record?.gps_timestamp || record?.server_timestamp || ''

      if (hasValidGpsPoint(record)) {
        points.push({
          source_index: index + 1,
          latitude: Number(record.latitude),
          longitude: Number(record.longitude),
          order_timestamp: timestampText,
          event_type: record?.event_type ?? null,
          is_estimated: false,
          point_description: 'Punto GPS original.',
        })
        return
      }

      const previous = findPreviousValidGpsRecord(records, index)
      const next = findNextValidGpsRecord(records, index)
      if (!previous || !next) {
        return
      }

      const prevTimeMs = parseRecordTimeMs(previous.record)
      const nextTimeMs = parseRecordTimeMs(next.record)
      if (!Number.isFinite(recordTimeMs) || !Number.isFinite(prevTimeMs) || !Number.isFinite(nextTimeMs)) {
        return
      }
      if (nextTimeMs <= prevTimeMs) {
        return
      }

      const ratioRaw = (recordTimeMs - prevTimeMs) / (nextTimeMs - prevTimeMs)
      const ratio = Math.min(1, Math.max(0, ratioRaw))

      const prevLat = Number(previous.record.latitude)
      const prevLon = Number(previous.record.longitude)
      const nextLat = Number(next.record.latitude)
      const nextLon = Number(next.record.longitude)
      if (![prevLat, prevLon, nextLat, nextLon].every(Number.isFinite)) {
        return
      }

      const estimatedLat = prevLat + ((nextLat - prevLat) * ratio)
      const estimatedLon = prevLon + ((nextLon - prevLon) * ratio)

      points.push({
        source_index: index + 1,
        latitude: estimatedLat,
        longitude: estimatedLon,
        order_timestamp: timestampText,
        event_type: record?.event_type ?? null,
        is_estimated: true,
        interpolation_ratio: ratio,
        previous_source_index: previous.index + 1,
        next_source_index: next.index + 1,
        previous_timestamp: previous.record?.order_timestamp || previous.record?.gps_timestamp || previous.record?.server_timestamp || '',
        next_timestamp: next.record?.order_timestamp || next.record?.gps_timestamp || next.record?.server_timestamp || '',
        point_description:
          `Punto calculado por interpolación temporal (pérdida de GPS). ` +
          `Se estimó usando fecha/hora entre el punto previo #${previous.index + 1} y el siguiente #${next.index + 1}.`,
      })
    })

    return points
  }

  const buildEstimatedGpsMetadataBySourceIndex = (route) => {
    const metadata = {}
    const gpsPoints = buildGpsPointsForRouteMap(route)

    gpsPoints.forEach((point) => {
      const sourceIndex = Number(point?.source_index)
      if (!Number.isInteger(sourceIndex) || sourceIndex <= 0) {
        return
      }

      metadata[sourceIndex] = {
        isEstimated: Boolean(point?.is_estimated),
        pointDescription: String(point?.point_description || ''),
      }
    })

    return metadata
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
    const startRouteEventType = Number(routeDetection.event_start ?? 519)
    const endRouteEventType = Number(routeDetection.event_end ?? 520)

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
          <div class="header">${title} · puntos: ${orderedPoints.length} · criterio: primer ${startRouteEventType} → primer ${endRouteEventType}</div>
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
            const startRouteEventType = ${startRouteEventType};
            const endRouteEventType = ${endRouteEventType};
            const endRouteEventTypes = [endRouteEventType];
            const endRouteLabels = {
              [endRouteEventType]: 'FIN',
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
              if (point.eventType === startRouteEventType && openStartIndex === null) {
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
                .bindPopup('No se detectaron segmentos completos ${startRouteEventType}→${endRouteEventType} con el criterio solicitado.');
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
                    '<b>Inicio de ruta #' + (segmentIndex + 1) + ' (' + escapeHtml(startRouteEventType) + ')</b><br/>' +
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
                  const isStartCandidate = point.eventType === startRouteEventType;

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
            <span id="routing-status" style="font-size:11px;color:#6b7280;margin-left:8px;"></span>
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

            let routeLine = L.polyline(latlngs, {
              color: '#2563eb',
              weight: 4,
              opacity: 0.95,
            }).addTo(routeLayer);

            const _RAD = Math.PI / 180;
            function haversinKm(lat1, lon1, lat2, lon2) {
              const dLat = (lat2 - lat1) * _RAD;
              const dLon = (lon2 - lon1) * _RAD;
              const a = Math.sin(dLat / 2) ** 2 + Math.cos(lat1 * _RAD) * Math.cos(lat2 * _RAD) * Math.sin(dLon / 2) ** 2;
              return 6371 * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
            }

            async function osrmSegment(lat1, lon1, lat2, lon2) {
              const url = 'https://router.project-osrm.org/route/v1/driving/' +
                lon1 + ',' + lat1 + ';' + lon2 + ',' + lat2 +
                '?overview=full&geometries=geojson';
              try {
                const resp = await fetch(url, { signal: AbortSignal.timeout(10000) });
                if (!resp.ok) return null;
                const data = await resp.json();
                if (data.code !== 'Ok' || !data.routes?.[0]) return null;
                return data.routes[0].geometry.coordinates.map((c) => [c[1], c[0]]);
              } catch (e) {
                return null;
              }
            }

            async function routeLargeGaps() {
              const GAP_KM = 20;
              const gapSegments = [];
              for (let i = 0; i < points.length - 1; i++) {
                const dist = haversinKm(points[i].latitude, points[i].longitude, points[i + 1].latitude, points[i + 1].longitude);
                if (dist > GAP_KM) gapSegments.push({ i, dist: dist.toFixed(1) });
              }
              if (gapSegments.length === 0) return;

              const statusEl = document.getElementById('routing-status');
              statusEl.textContent = 'Enrutando ' + gapSegments.length + ' tramo(s) via OSRM...';

              const routedLatlngs = [[points[0].latitude, points[0].longitude]];
              for (let i = 0; i < points.length - 1; i++) {
                const p1 = points[i];
                const p2 = points[i + 1];
                const dist = haversinKm(p1.latitude, p1.longitude, p2.latitude, p2.longitude);
                if (dist > GAP_KM) {
                  const routed = await osrmSegment(p1.latitude, p1.longitude, p2.latitude, p2.longitude);
                  if (routed && routed.length > 1) {
                    routedLatlngs.push(...routed.slice(1));
                  } else {
                    routedLatlngs.push([p2.latitude, p2.longitude]);
                  }
                } else {
                  routedLatlngs.push([p2.latitude, p2.longitude]);
                }
              }

              routeLayer.removeLayer(routeLine);
              routeLine = L.polyline(routedLatlngs, { color: '#2563eb', weight: 4, opacity: 0.95 }).addTo(routeLayer);
              statusEl.textContent = '✓ OSRM: ' + gapSegments.length + ' tramo(s) enrutado(s) por carretera';
            }

            routeLargeGaps();

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

  const openSignalLossChartWindow = () => {
    if (!signalLossData) return

    const pts = (signalLossData.all_points || []).filter(
      (p) => typeof p.diff_seconds === 'number' && Number.isFinite(p.diff_seconds) && p.diff_seconds >= 0
    )

    if (pts.length < 2) {
      setError('No hay suficientes puntos con timestamp para graficar en una ventana aparte.')
      return
    }

    const popup = window.open('', '_blank', 'width=1280,height=860')
    if (!popup) {
      setError('No se pudo abrir la ventana. Revisa el bloqueador de popups del navegador.')
      return
    }

    const safePts = JSON.stringify(pts).replaceAll('</', String.raw`<\/`)
    const safeSegments = JSON.stringify(signalLossData.segments || []).replaceAll('</', String.raw`<\/`)
    const assetLabel = `assetId=${signalLossData.asset_id} clientId=${signalLossData.client_id}`
    const title = `Señal GPS vs Servidor — ${assetLabel}`
    const safeSummary = JSON.stringify({
      total_records: signalLossData.total_records,
      segments_count: signalLossData.signal_loss_segments_count,
      total_minutes: signalLossData.total_signal_loss_minutes,
    }).replaceAll('</', String.raw`<\/`)

    const html = `
      <!doctype html>
      <html lang="es">
        <head>
          <meta charset="utf-8" />
          <meta name="viewport" content="width=device-width, initial-scale=1" />
          <title>${title}</title>
          <style>
            :root { color-scheme: light; }
            body { margin: 0; font-family: Inter, system-ui, sans-serif; background: #f8fafc; color: #0f172a; }
            .container { max-width: 1240px; margin: 0 auto; padding: 16px; }
            .card { background: #fff; border: 1px solid #e2e8f0; border-radius: 10px; padding: 14px; margin-bottom: 14px; }
            h1 { margin: 0 0 10px; font-size: 18px; }
            h2 { margin: 0 0 10px; font-size: 15px; }
            .summary { display: flex; flex-wrap: wrap; gap: 8px; font-size: 13px; margin-bottom: 4px; }
            .pill { background: #f1f5f9; border: 1px solid #cbd5e1; border-radius: 8px; padding: 8px 12px; }
            .pill.warn { border-color: #f59e0b; background: #fffbeb; color: #92400e; }
            .pill.ok { border-color: #22c55e; background: #f0fdf4; color: #166534; }
            .subtitle { font-size: 12px; color: #64748b; margin: 0 0 10px; }
            svg { display: block; width: 100%; }
            table { width: 100%; border-collapse: collapse; font-size: 12px; }
            th, td { border-bottom: 1px solid #e2e8f0; padding: 6px 8px; text-align: left; white-space: nowrap; }
            th { background: #f8fafc; position: sticky; top: 0; z-index: 1; }
            .table-wrap { max-height: 260px; overflow: auto; border: 1px solid #e2e8f0; border-radius: 8px; }
          </style>
        </head>
        <body>
          <div class="container">
            <div class="card">
              <h1>${title}</h1>
              <div class="summary" id="summary"></div>
            </div>
            <div class="card">
              <h2>Diferencia GPS → Servidor por registro</h2>
              <p class="subtitle">Eje Y: segundos entre GPSTimeStamp y ServerTimeStamp. Picos = transmisión en lote (sin señal previa).</p>
              <svg id="chart" viewBox="0 0 1200 220" aria-label="Gráfico diferencia GPS vs Servidor"></svg>
            </div>
            <div class="card">
              <h2>Segmentos sin señal detectados</h2>
              <div class="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>#</th>
                      <th>GPS inicio</th>
                      <th>GPS fin</th>
                      <th>Servidor recibió en</th>
                      <th>Duración</th>
                      <th>Registros</th>
                    </tr>
                  </thead>
                  <tbody id="seg-rows"></tbody>
                </table>
              </div>
            </div>
          </div>

          <script>
            const pts = ${safePts};
            const segments = ${safeSegments};
            const summary = ${safeSummary};

            const fmtTs = (v) => {
              if (!v) return '—';
              const d = new Date(v);
              if (isNaN(d.getTime())) return String(v);
              return d.toLocaleString('es-MX', { year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });
            };

            // Summary pills
            document.getElementById('summary').innerHTML = [
              ['Registros analizados', String(summary.total_records), ''],
              ['Segmentos sin señal', String(summary.segments_count), summary.segments_count > 0 ? 'warn' : 'ok'],
              ['Tiempo total sin señal', summary.total_minutes.toFixed(1) + ' min', summary.total_minutes > 0 ? 'warn' : 'ok'],
            ].map(([l, v, cls]) => '<div class="pill ' + cls + '"><strong>' + l + ':</strong> ' + v + '</div>').join('');

            // Chart
            const W = 1200, H = 220;
            const PL = 58, PR = 16, PT = 12, PB = 30;
            const iW = W - PL - PR, iH = H - PT - PB;
            const maxDiff = Math.max(...pts.map((p) => p.diff_seconds), 60);
            const xOf = (i) => PL + (i / Math.max(pts.length - 1, 1)) * iW;
            const yOf = (v) => PT + iH - Math.min(v / maxDiff, 1) * iH;

            const NS = 'http://www.w3.org/2000/svg';
            const svg = document.getElementById('chart');

            const mk = (tag, attrs) => {
              const el = document.createElementNS(NS, tag);
              Object.entries(attrs).forEach(([k, v]) => el.setAttribute(k, v));
              return el;
            };

            // Signal loss background bands
            segments.forEach((seg) => {
              const lossIdx = pts.reduce((acc, p, i) => { if (p.is_signal_loss) acc.push(i); return acc; }, []);
              if (lossIdx.length === 0) return;
              const x0 = xOf(lossIdx[0]);
              const x1 = xOf(lossIdx[lossIdx.length - 1]);
              svg.appendChild(mk('rect', { x: x0, y: PT, width: Math.max(x1 - x0, 4), height: iH, fill: 'rgba(239,68,68,0.08)' }));
            });

            // Grid lines
            [0, 25, 50, 75, 100].forEach((pct) => {
              const yy = PT + iH - (pct / 100) * iH;
              svg.appendChild(mk('line', { x1: PL, y1: yy, x2: W - PR, y2: yy, stroke: '#e2e8f0', 'stroke-width': 1 }));
              const label = document.createElementNS(NS, 'text');
              label.setAttribute('x', PL - 4);
              label.setAttribute('y', yy + 3);
              label.setAttribute('text-anchor', 'end');
              label.setAttribute('fill', '#94a3b8');
              label.setAttribute('font-size', '11');
              label.textContent = Math.round(maxDiff * pct / 100) + 's';
              svg.appendChild(label);
            });

            // Threshold line (30 s)
            const thY = yOf(30);
            svg.appendChild(mk('line', { x1: PL, y1: thY, x2: W - PR, y2: thY, stroke: '#f59e0b', 'stroke-width': 1.5, 'stroke-dasharray': '5 4' }));

            // Axes
            svg.appendChild(mk('line', { x1: PL, y1: PT, x2: PL, y2: PT + iH, stroke: '#475569', 'stroke-width': 1 }));
            svg.appendChild(mk('line', { x1: PL, y1: PT + iH, x2: W - PR, y2: PT + iH, stroke: '#475569', 'stroke-width': 1 }));

            // Polyline
            const polyPts = pts.map((p, i) => xOf(i).toFixed(1) + ',' + yOf(p.diff_seconds).toFixed(1)).join(' ');
            svg.appendChild(mk('polyline', { points: polyPts, fill: 'none', stroke: '#3b82f6', 'stroke-width': 1.5 }));

            // Dots
            const r = pts.length > 300 ? 1.5 : 3;
            pts.forEach((p, i) => {
              const c = mk('circle', { cx: xOf(i).toFixed(1), cy: yOf(p.diff_seconds).toFixed(1), r, fill: p.is_signal_loss ? '#ef4444' : '#3b82f6' });
              c.innerHTML = '<title>Reg #' + (i+1) + ' | diff: ' + p.diff_seconds.toFixed(1) + 's | GPS: ' + fmtTs(p.gps_timestamp) + ' | Srv: ' + fmtTs(p.server_timestamp) + (p.is_signal_loss ? ' | SIN SEÑAL' : '') + '</title>';
              svg.appendChild(c);
            });

            // Legend
            const legY = H - 8;
            [{cx: PL+10, fill:'#3b82f6', text:'Normal', tx: PL+18},
             {cx: PL+80, fill:'#ef4444', text:'Sin señal', tx: PL+88}].forEach(({cx, fill, text, tx}) => {
              svg.appendChild(mk('circle', { cx, cy: legY, r: 4, fill }));
              const t = document.createElementNS(NS, 'text');
              t.setAttribute('x', tx); t.setAttribute('y', legY + 4);
              t.setAttribute('fill', '#64748b'); t.setAttribute('font-size', '11');
              t.textContent = text;
              svg.appendChild(t);
            });
            // threshold legend
            svg.appendChild(mk('line', { x1: PL+150, y1: legY, x2: PL+168, y2: legY, stroke:'#f59e0b','stroke-width':1.5,'stroke-dasharray':'5 4'}));
            const tl = document.createElementNS(NS, 'text');
            tl.setAttribute('x', PL+172); tl.setAttribute('y', legY+4);
            tl.setAttribute('fill', '#64748b'); tl.setAttribute('font-size', '11');
            tl.textContent = 'Umbral 30 s'; svg.appendChild(tl);

            // Segment table
            document.getElementById('seg-rows').innerHTML = segments.length === 0
              ? '<tr><td colspan="6">No se detectaron segmentos sin señal.</td></tr>'
              : segments.map((s, i) =>
                '<tr><td>' + (i+1) + '</td><td>' + fmtTs(s.gps_start) + '</td><td>' + fmtTs(s.gps_end) + '</td><td>' + fmtTs(s.server_received_at) + '</td><td><strong>' + s.duration_minutes.toFixed(1) + ' min</strong></td><td>' + s.num_records + '</td></tr>'
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

  const openStopsMapWindow = () => {
    if (!stopsData?.stops?.length) return

    const stops = stopsData.stops.filter((s) => s.latitude != null && s.longitude != null)
    if (stops.length === 0) {
      setError('No hay paradas con coordenadas para mostrar en el mapa.')
      return
    }

    const popup = window.open('', '_blank', 'width=1280,height=860')
    if (!popup) {
      setError('No se pudo abrir la ventana. Revisa el bloqueador de popups del navegador.')
      return
    }

    const safeStops = JSON.stringify(stops).replaceAll('</', String.raw`<\/`)
    const title = `Paradas — assetId=${stopsData.asset_id} clientId=${stopsData.client_id}`

    const html = `
      <!doctype html>
      <html lang="es">
        <head>
          <meta charset="utf-8" />
          <meta name="viewport" content="width=device-width, initial-scale=1" />
          <title>${title}</title>
          <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
            integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY=" crossorigin="" />
          <style>
            html, body { margin: 0; padding: 0; height: 100%; font-family: Inter, system-ui, sans-serif; background: #f8fafc; }
            #map { width: 100%; height: calc(100% - 52px); }
            .header {
              height: 52px; display: flex; align-items: center; justify-content: space-between;
              padding: 0 16px; border-bottom: 1px solid #e2e8f0; background: #fff; font-size: 13px; color: #0f172a;
            }
            .legend {
              position: absolute; bottom: 24px; right: 10px; z-index: 1000;
              background: #ffffffee; border: 1px solid #e2e8f0; border-radius: 8px;
              padding: 10px 14px; font-size: 12px; line-height: 1.8;
            }
            .legend-dot { display: inline-block; width: 12px; height: 12px; border-radius: 50%; margin-right: 6px; vertical-align: middle; }
          </style>
        </head>
        <body>
          <div class="header">
            <strong>${title}</strong>
            <span>${stops.length} paradas · ${stopsData.total_stop_minutes.toFixed(1)} min detenido</span>
          </div>
          <div id="map"></div>

          <script
            src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
            integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo="
            crossorigin=""
          ></script>
          <script>
            const stops = ${safeStops};

            const CAT_COLORS = {
              gasolinera:               '#f59e0b',
              hotel:                    '#8b5cf6',
              restaurante:              '#22c55e',
              tienda_conveniencia:      '#3b82f6',
              motor_apagado:            '#64748b',
              ralenti:                  '#94a3b8',
              parada_no_autorizada:     '#ef4444',
              estacionamiento_prolongado:'#f97316',
              pernocta_fuera_de_base:   '#dc2626',
              parada_en_geocerca:       '#0ea5e9',
              punto_de_control:         '#10b981',
              entrega:                  '#10b981',
              parada_en_carretera:      '#94a3b8',
            };

            const fmtTs = (v) => {
              if (!v) return '—';
              const d = new Date(v);
              return isNaN(d) ? v : d.toLocaleString('es-MX', { year:'numeric', month:'2-digit', day:'2-digit', hour:'2-digit', minute:'2-digit', hour12:false });
            };

            const map = L.map('map');
            L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
              attribution: '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
              maxZoom: 19,
            }).addTo(map);

            const markers = [];
            stops.forEach((s, i) => {
              const color = CAT_COLORS[s.final_category] || '#94a3b8';
              const radius = Math.max(8, Math.min(30, Math.sqrt(s.duration_minutes) * 1.5));

              const marker = L.circleMarker([s.latitude, s.longitude], {
                radius,
                color: '#fff',
                weight: 2,
                fillColor: color,
                fillOpacity: 0.85,
              }).addTo(map);

              const label = s.final_category.replace(/_/g, ' ');
              const place = s.nearby_place ? '<br><b>Lugar:</b> ' + s.nearby_place : '';
              const evCat = s.event_type_category ? '<br><b>Evento:</b> ' + s.event_type_category.replace(/_/g, ' ') : '';

              marker.bindPopup(
                '<b>Parada #' + (i + 1) + '</b><br>' +
                '<b>Categoría:</b> <span style="color:' + color + ';font-weight:600">' + label + '</span><br>' +
                '<b>Duración:</b> ' + s.duration_minutes.toFixed(1) + ' min<br>' +
                '<b>Inicio:</b> ' + fmtTs(s.start_timestamp) + '<br>' +
                '<b>Fin:</b> ' + fmtTs(s.end_timestamp) + '<br>' +
                '<b>Registros:</b> ' + s.num_records +
                evCat + place + '<br>' +
                '<b>Confianza:</b> ' + s.confidence + '<br>' +
                '<b>Coords:</b> ' + s.latitude.toFixed(5) + ', ' + s.longitude.toFixed(5),
                { maxWidth: 280 }
              );

              marker.bindTooltip('#' + (i + 1) + ' · ' + s.duration_minutes.toFixed(0) + ' min · ' + label, { direction: 'top' });
              markers.push(marker);
            });

            if (markers.length > 0) {
              const group = L.featureGroup(markers);
              map.fitBounds(group.getBounds().pad(0.15));
            }

            // Leyenda
            const usedCats = [...new Set(stops.map((s) => s.final_category))];
            const legDiv = L.DomUtil.create('div', 'legend');
            legDiv.innerHTML = '<b>Categoría</b><br>' +
              usedCats.map((cat) =>
                '<span class="legend-dot" style="background:' + (CAT_COLORS[cat] || '#94a3b8') + '"></span>' +
                cat.replace(/_/g, ' ')
              ).join('<br>');
            document.getElementById('map').appendChild(legDiv);
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

      const roads = getRouteRoadLabels(candidateRoute)

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
    const predictedRoutePoints = Array.isArray(route?.predicted_route_points)
      ? route.predicted_route_points
        .filter((point) => Array.isArray(point) && point.length >= 2)
        .map((point) => [Number(point[0]), Number(point[1])])
        .filter((point) => Number.isFinite(point[0]) && Number.isFinite(point[1]))
      : []
    const predictedRouteRoads = Array.isArray(route?.predicted_route_roads)
      ? route.predicted_route_roads.map((value) => String(value || '').trim()).filter(Boolean)
      : []
    const routeRoadLabels = getRouteRoadLabels(route)
    let selectedRoadLabelsSource = classRoadsIndex[predictedClass] || []
    if (routeRoadLabels.length > 0) {
      selectedRoadLabelsSource = routeRoadLabels
    }
    if (predictedRouteRoads.length > 0) {
      selectedRoadLabelsSource = predictedRouteRoads
    }
    const predictedRoadLabels = selectedRoadLabelsSource
    if (predictedRoadLabels.length === 0) {
      setError(`No hay mapeo de carreteras disponible para ${predictedClass}.`)
      return
    }

    const gpsPointsForMap = buildGpsPointsForRouteMap(route)
    const actualPoints = gpsPointsForMap.map((point) => [Number(point.latitude), Number(point.longitude)])
    const estimatedPointsCount = gpsPointsForMap.filter((point) => point.is_estimated).length

    const normalizeRoadLabel = (value) => String(value || '')
      .normalize('NFD')
      .replaceAll(/[\u0300-\u036f]/g, '')
      .replaceAll(/\s+/g, ' ')
      .trim()
      .toUpperCase()

    const haversineMeters = (lat1, lon1, lat2, lon2) => {
      const radians = Math.PI / 180
      const dLat = (lat2 - lat1) * radians
      const dLon = (lon2 - lon1) * radians
      const sinLat = Math.sin(dLat / 2)
      const sinLon = Math.sin(dLon / 2)
      const value = sinLat * sinLat + Math.cos(lat1 * radians) * Math.cos(lat2 * radians) * sinLon * sinLon
      return 6371000 * (2 * Math.asin(Math.sqrt(value)))
    }

    const minDistanceToRouteMeters = (polyline, routePoints) => {
      if (!Array.isArray(polyline) || polyline.length === 0 || !Array.isArray(routePoints) || routePoints.length === 0) {
        return Number.POSITIVE_INFINITY
      }

      let minDistance = Number.POSITIVE_INFINITY
      const vertexStride = Math.max(1, Math.floor(polyline.length / 120))

      for (let vertexIndex = 0; vertexIndex < polyline.length; vertexIndex += vertexStride) {
        const vertex = polyline[vertexIndex]
        for (const routePoint of routePoints) {
          const distance = haversineMeters(vertex[0], vertex[1], routePoint[0], routePoint[1])
          if (distance < minDistance) {
            minDistance = distance
          }
        }
      }

      return minDistance
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
      const roadSetNormalized = new Set(predictedRoadLabels.map((label) => normalizeRoadLabel(label)))
      const corridorDistanceThresholdM = 5000
      const corridorCandidates = []

      const preparedRows = rows
        .map((row, index) => {
          const latlngs = parseLinestringWkt(row.geometry)
          if (latlngs.length < 2) {
            return null
          }

          const name = String(row.name || '').trim()
          const routeName = String(row.p_route || '').trim()
          const label = String(row.name || row.p_route || '').trim()
          const rowLabelsNormalized = [name, routeName, label]
            .map((value) => normalizeRoadLabel(value))
            .filter(Boolean)

          const isPredicted = rowLabelsNormalized.some((value) => roadSetNormalized.has(value))

          let minDistanceToRoute = Number.POSITIVE_INFINITY
          if (actualPoints.length > 0) {
            minDistanceToRoute = minDistanceToRouteMeters(latlngs, actualPoints)
          }

          if (!isPredicted && Number.isFinite(minDistanceToRoute) && minDistanceToRoute <= corridorDistanceThresholdM) {
            corridorCandidates.push({ label, distance: minDistanceToRoute })
          }

          return {
            id: index + 1,
            label,
            route: routeName,
            no: String(row.p_no || '').trim(),
            latlngs,
            isPredicted,
            minDistanceToRoute,
          }
        })
        .filter(Boolean)

      const sortedCorridorCandidates = corridorCandidates.toSorted(
        (left, right) => left.distance - right.distance,
      )

      const selectedCorridorLabels = new Set(
        sortedCorridorCandidates
          .slice(0, 10)
          .map((item) => normalizeRoadLabel(item.label))
      )

      predictedRoadSegments = preparedRows.filter((road) => {
        if (road.isPredicted) {
          return true
        }
        return selectedCorridorLabels.has(normalizeRoadLabel(road.label))
      })
    } catch (err) {
      setError(err.message)
      return
    }

    if (predictedRoadSegments.length === 0) {
      setError(`No se encontraron geometrías para la predicción ML ${predictedClass}.`)
      return
    }

    const mapWindow = window.open('', '_blank', 'width=1200,height=800')
    if (!mapWindow) {
      setError('No se pudo abrir la ventana del mapa. Revisa el bloqueador de popups del navegador.')
      return
    }

    const safeRoadsJson = JSON.stringify(predictedRoadSegments).replaceAll('</', String.raw`<\/`)
    const safeActualPointsJson = JSON.stringify(gpsPointsForMap).replaceAll('</', String.raw`<\/`)
    const safePredictedRoutePointsJson = JSON.stringify(predictedRoutePoints).replaceAll('</', String.raw`<\/`)
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
            #map { width: 100%; height: calc(100% - 74px); }
            .header {
              min-height: 74px;
              display: flex;
              flex-direction: column;
              align-items: flex-start;
              justify-content: center;
              gap: 6px;
              padding: 8px 16px;
              border-bottom: 1px solid #e5e7eb;
              background: #f9fafb;
              font-size: 14px;
              color: #111827;
            }
            .header-top {
              width: 100%;
              display: flex;
              align-items: center;
              justify-content: space-between;
              gap: 12px;
              flex-wrap: wrap;
            }
            .legend {
              display: flex;
              flex-wrap: wrap;
              gap: 10px;
              font-size: 12px;
              color: #374151;
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
            .legend-item {
              display: inline-flex;
              align-items: center;
              gap: 6px;
            }
            .legend-swatch {
              width: 18px;
              height: 4px;
              border-radius: 999px;
              display: inline-block;
            }
            .legend-swatch-point {
              width: 10px;
              height: 10px;
              border-radius: 999px;
              display: inline-block;
            }
          </style>
        </head>
        <body>
          <div class="header">
            <div class="header-top">
              <div>${title} · carreteras inferidas: ${predictedRoadSegments.length} · puntos GPS: ${actualPoints.length} · calculados: ${estimatedPointsCount}</div>
              <div class="controls">
                <button id="toggle-ml-routes" type="button">Ocultar ruta ML</button>
                <button id="toggle-real-route" type="button">Ocultar ruta real</button>
                <button id="toggle-gps-points" type="button">Ocultar puntos GPS</button>
              </div>
            </div>
            <div class="legend">
              <span class="legend-item"><span class="legend-swatch" style="background:#f97316"></span>Ruta ML inferida</span>
              <span class="legend-item"><span class="legend-swatch" style="background:#1d4ed8"></span>Ruta real (línea)</span>
              <span class="legend-item"><span class="legend-swatch-point" style="background:#06b6d4"></span>Puntos GPS</span>
              <span class="legend-item"><span class="legend-swatch-point" style="background:#f59e0b"></span>Puntos GPS calculados</span>
            </div>
          </div>
          <div id="map"></div>

          <script
            src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
            integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo="
            crossorigin=""
          ></script>
          <script>
            const roads = ${safeRoadsJson};
            const gpsPoints = ${safeActualPointsJson};
            const actualPoints = Array.isArray(gpsPoints)
              ? gpsPoints
                  .map((point) => [Number(point.latitude), Number(point.longitude)])
                  .filter((point) => Number.isFinite(point[0]) && Number.isFinite(point[1]))
              : [];
            const predictedRoutePoints = ${safePredictedRoutePointsJson};
            const map = L.map('map');
            const mlRoutesLayer = L.layerGroup().addTo(map);
            const realRouteLayer = L.layerGroup().addTo(map);
            const gpsPointsLayer = L.layerGroup().addTo(map);

            L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
              maxZoom: 19,
              attribution: '&copy; OpenStreetMap contributors',
            }).addTo(map);

            const ROAD_SEGMENT_BUFFER_M = 1200;
            const ROAD_VERTEX_NEAR_GPS_MAX_M = 1200;

            const pointDistanceM = (a, b) => {
              const latA = Number(a?.[0]);
              const lonA = Number(a?.[1]);
              const latB = Number(b?.[0]);
              const lonB = Number(b?.[1]);
              if (![latA, lonA, latB, lonB].every(Number.isFinite)) {
                return Number.POSITIVE_INFINITY;
              }

              const radians = Math.PI / 180;
              const dLat = (latB - latA) * radians;
              const dLon = (lonB - lonA) * radians;
              const sinLat = Math.sin(dLat / 2);
              const sinLon = Math.sin(dLon / 2);
              const value = sinLat * sinLat + Math.cos(latA * radians) * Math.cos(latB * radians) * sinLon * sinLon;
              return 6371000 * (2 * Math.asin(Math.sqrt(value)));
            };

            const findNearestRoadVertexIndex = (gpsPoint, roadLatLngs) => {
              let bestIndex = -1;
              let bestDistance = Number.POSITIVE_INFINITY;

              roadLatLngs.forEach((vertex, index) => {
                const distance = pointDistanceM(gpsPoint, vertex);
                if (distance < bestDistance) {
                  bestDistance = distance;
                  bestIndex = index;
                }
              });

              return { index: bestIndex, distance: bestDistance };
            };

            const extractClosestRoadSegment = (roadLatLngs, gpsPoints, bufferM) => {
              if (!Array.isArray(roadLatLngs) || roadLatLngs.length < 2) {
                return [];
              }
              if (!Array.isArray(gpsPoints) || gpsPoints.length < 2) {
                return roadLatLngs;
              }

              const nearestIndexes = gpsPoints
                .map((gpsPoint) => findNearestRoadVertexIndex(gpsPoint, roadLatLngs))
                .filter((item) => item.index >= 0 && Number.isFinite(item.distance) && item.distance <= ROAD_VERTEX_NEAR_GPS_MAX_M)
                .map((item) => item.index)
                .sort((left, right) => left - right);

              if (nearestIndexes.length >= 2) {
                const segmentStart = nearestIndexes[0];
                const segmentEnd = nearestIndexes[nearestIndexes.length - 1];
                const paddedStart = Math.max(0, segmentStart - 2);
                const paddedEnd = Math.min(roadLatLngs.length - 1, segmentEnd + 2);
                if (paddedEnd - paddedStart >= 1) {
                  return roadLatLngs.slice(paddedStart, paddedEnd + 1);
                }
              }

              // Fallback: use contiguous vertices that are still close to the GPS route.
              const closeIndexes = [];
              roadLatLngs.forEach((vertex, index) => {
                const isCloseToAnyGps = gpsPoints.some((gpsPoint) => pointDistanceM(vertex, gpsPoint) <= bufferM);
                if (isCloseToAnyGps) {
                  closeIndexes.push(index);
                }
              });

              if (closeIndexes.length === 0) {
                return [];
              }

              let bestStart = closeIndexes[0];
              let bestEnd = closeIndexes[0];
              let currentStart = closeIndexes[0];
              let currentEnd = closeIndexes[0];

              for (let i = 1; i < closeIndexes.length; i += 1) {
                const currentIndex = closeIndexes[i];
                if (currentIndex - currentEnd <= 2) {
                  currentEnd = currentIndex;
                  continue;
                }

                if (currentEnd - currentStart > bestEnd - bestStart) {
                  bestStart = currentStart;
                  bestEnd = currentEnd;
                }

                currentStart = currentIndex;
                currentEnd = currentIndex;
              }

              if (currentEnd - currentStart > bestEnd - bestStart) {
                bestStart = currentStart;
                bestEnd = currentEnd;
              }

              const paddedStart = Math.max(0, bestStart - 1);
              const paddedEnd = Math.min(roadLatLngs.length - 1, bestEnd + 1);
              return roadLatLngs.slice(paddedStart, paddedEnd + 1);
            };

            const allPoints = [];
            if (Array.isArray(predictedRoutePoints) && predictedRoutePoints.length >= 2) {
              const predictedLine = L.polyline(predictedRoutePoints, {
                color: '#f97316',
                weight: 5,
                opacity: 0.95,
              }).addTo(mlRoutesLayer);
              allPoints.push(...predictedRoutePoints);
              predictedLine.bindPopup('<b>Ruta ML (inicio→fin por red de carreteras)</b>');
            }

            roads.forEach((road) => {
              const segmentLatLngs = extractClosestRoadSegment(road.latlngs, actualPoints, ROAD_SEGMENT_BUFFER_M);
              if (!Array.isArray(segmentLatLngs) || segmentLatLngs.length < 2) {
                return;
              }

              const line = L.polyline(segmentLatLngs, {
                color: '#f97316',
                weight: 4,
                opacity: 0.9,
              }).addTo(mlRoutesLayer);
              allPoints.push(...segmentLatLngs);

              line.bindPopup(
                '<b>Ruta ML</b><br/>' +
                'Carretera: ' + String(road.label || 'N/A') + '<br/>' +
                'Ruta: ' + String(road.route || 'N/A') + '<br/>' +
                'No: ' + String(road.no || 'N/A') + '<br/>' +
                'Render: segmento cercano a GPS'
              );
            });

            if (Array.isArray(actualPoints) && actualPoints.length >= 2) {
              const actualLine = L.polyline(actualPoints, {
                color: '#1d4ed8',
                weight: 3,
                opacity: 0.95,
                dashArray: '7,5',
              }).addTo(realRouteLayer);
              allPoints.push(...actualPoints);
              actualLine.bindPopup('<b>Ruta real (referencia)</b>');
            }

            if (Array.isArray(gpsPoints) && gpsPoints.length > 0) {
              gpsPoints.forEach((point, index) => {
                const latitude = Number(point?.latitude);
                const longitude = Number(point?.longitude);
                if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
                  return;
                }

                const isEstimated = Boolean(point?.is_estimated);

                const marker = L.circleMarker([latitude, longitude], {
                  radius: isEstimated ? 4 : 3,
                  color: isEstimated ? '#92400e' : '#0f766e',
                  weight: 1,
                  fillColor: isEstimated ? '#f59e0b' : '#06b6d4',
                  fillOpacity: 0.9,
                }).addTo(gpsPointsLayer);

                marker.bindPopup(
                  '<b>' + (isEstimated ? 'Punto GPS calculado' : 'Punto GPS') + '</b><br/>' +
                  'Índice visual: ' + String(index + 1) + '<br/>' +
                  'Índice del registro: ' + String(point?.source_index ?? '') + '<br/>' +
                  'Timestamp: ' + String(point?.order_timestamp || '') + '<br/>' +
                  'eventType: ' + String(point?.event_type ?? '') + '<br/>' +
                  String(point?.point_description || '')
                );
              });

              const startPoint = actualPoints[0];
              const endPoint = actualPoints[actualPoints.length - 1];

              if (Array.isArray(startPoint) && startPoint.length >= 2) {
                L.circleMarker(startPoint, {
                  radius: 6,
                  color: '#14532d',
                  weight: 2,
                  fillColor: '#22c55e',
                  fillOpacity: 0.95,
                }).addTo(gpsPointsLayer).bindPopup('<b>Inicio de ruta real</b>');
              }

              if (Array.isArray(endPoint) && endPoint.length >= 2) {
                L.circleMarker(endPoint, {
                  radius: 6,
                  color: '#7f1d1d',
                  weight: 2,
                  fillColor: '#ef4444',
                  fillOpacity: 0.95,
                }).addTo(gpsPointsLayer).bindPopup('<b>Fin de ruta real</b>');
              }
            }

            if (allPoints.length > 0) {
              map.fitBounds(L.latLngBounds(allPoints), { padding: [24, 24] });
            } else {
              map.setView([23.6345, -102.5528], 5);
            }

            const mlRoutesToggleButton = document.getElementById('toggle-ml-routes');
            const realRouteToggleButton = document.getElementById('toggle-real-route');
            const gpsPointsToggleButton = document.getElementById('toggle-gps-points');

            let mlRoutesVisible = true;
            let realRouteVisible = true;
            let gpsPointsVisible = true;

            mlRoutesToggleButton?.addEventListener('click', () => {
              if (mlRoutesVisible) {
                map.removeLayer(mlRoutesLayer);
                mlRoutesToggleButton.textContent = 'Mostrar ruta ML';
              } else {
                map.addLayer(mlRoutesLayer);
                mlRoutesToggleButton.textContent = 'Ocultar ruta ML';
              }
              mlRoutesVisible = !mlRoutesVisible;
            });

            realRouteToggleButton?.addEventListener('click', () => {
              if (realRouteVisible) {
                map.removeLayer(realRouteLayer);
                realRouteToggleButton.textContent = 'Mostrar ruta real';
              } else {
                map.addLayer(realRouteLayer);
                realRouteToggleButton.textContent = 'Ocultar ruta real';
              }
              realRouteVisible = !realRouteVisible;
            });

            gpsPointsToggleButton?.addEventListener('click', () => {
              if (gpsPointsVisible) {
                map.removeLayer(gpsPointsLayer);
                gpsPointsToggleButton.textContent = 'Mostrar puntos GPS';
              } else {
                map.addLayer(gpsPointsLayer);
                gpsPointsToggleButton.textContent = 'Ocultar puntos GPS';
              }
              gpsPointsVisible = !gpsPointsVisible;
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
          <button onClick={() => loadRows(0, pageSize, assetIdFilter, clientIdFilter, eventTypeFilter, eventTypeFilter2)} disabled={rowsLoading}>
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
            <span>eventType 1</span>
            <input
              type="number"
              value={eventTypeFilter}
              onChange={(event) => setEventTypeFilter(event.target.value)}
              onKeyDown={(event) => event.key === 'Enter' && handleApplyFilters()}
              placeholder="Ej. 519"
            />
          </label>
          <label className="inline-label">
            <span>eventType 2</span>
            <input
              type="number"
              value={eventTypeFilter2}
              onChange={(event) => setEventTypeFilter2(event.target.value)}
              onKeyDown={(event) => event.key === 'Enter' && handleApplyFilters()}
              placeholder="Ej. 520"
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
                    <th>Principal</th>
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
                    const routeRoadLabels = getRouteRoadLabels(route)
                    const mlConfidenceLabel = Number.isFinite(Number(mlPrediction?.confidence))
                      ? `${(Number(mlPrediction.confidence) * 100).toFixed(1)}%`
                      : ''
                    return (
                    <tr key={`route-${route.start_index}-${route.end_index}-${route.start_timestamp || 'na'}`}>
                      <td>{route.is_primary ? 'SI' : ''}</td>
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
                      <td>{routeRoadLabels.join(' | ')}</td>
                      <td>{mlPrediction?.predicted_route || (mlPrediction?.error ? 'N/A' : '')}</td>
                      <td>{mlConfidenceLabel || (mlPrediction?.error ? 'N/A' : '')}</td>
                    </tr>
                    )
                  })}
                  {routeDetection.complete_routes.length === 0 && (
                    <tr>
                      <td colSpan={13}>No se detectaron rutas completas 519→520 para este asset/client.</td>
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
                  <option value="estimated">Solo estimados</option>
                  <option value="original">Solo originales</option>
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
                  const estimatedGpsMetadataBySourceIndex = buildEstimatedGpsMetadataBySourceIndex(route)
                  const visibleIntervalRecords = filterIntervalRecords(route, intervalRecords, estimatedGpsMetadataBySourceIndex)
                  const mlPrediction = mlPredictionsByRoute[getRouteKey(route)]
                  const mlClassRoadsIndex = buildMlClassRoadsIndex()
                  const routeRoadLabels = getRouteRoadLabels(route)
                  const mlPredictedRoads = mlPrediction?.predicted_route
                    ? (mlClassRoadsIndex[mlPrediction.predicted_route] || [])
                    : []
                  const gapAnalysis = buildRouteGapAnalysis(intervalRecords)
                  const chartIntervals = gapAnalysis.intervals.slice(0, 240)
                  return (
                    <>
                      <p className="subtitle">
                        Ruta #{routeIndex + 1}{route.is_primary ? ' · PRINCIPAL (mejor match de carreteras)' : ''} · Inicio: {formatTimestamp(route.start_timestamp)} · Fin: {formatTimestamp(route.end_timestamp)} ·
                        Registros entre eventos inicio/fin: {intervalRecords.length} ·
                        Mostrando: {visibleIntervalRecords.length}
                      </p>
                      <p className="subtitle">
                        Carreteras involucradas: {routeRoadLabels.length > 0
                          ? routeRoadLabels.join(' | ')
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
                              <th>GPS estimado</th>
                              <th>Detalle estimacion</th>
                              <th>Lat</th>
                              <th>Lon</th>
                            </tr>
                          </thead>
                          <tbody>
                            {visibleIntervalRecords.map((record, recordIndex) => renderIntervalRecordRow(
                              route,
                              record,
                              recordIndex,
                              estimatedGpsMetadataBySourceIndex,
                            ))}
                            {visibleIntervalRecords.length === 0 && (
                              <tr>
                                <td colSpan={14}>No hay registros para el filtro seleccionado en esta ruta.</td>
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

      <section className="card">
        <h2>4) Detección de desplazamientos sin señal (GPS vs Server timestamp)</h2>
        <p className="subtitle">
          Identifica periodos donde el dispositivo perdió conectividad y transmitió registros en lote.
          Señal: varios registros con <strong>GPSTimeStamp distintos</strong> llegan con el{' '}
          <strong>mismo ServerTimeStamp</strong> (o muy cercanos).
        </p>

        {signalLossLoading && <p className="subtitle">Analizando timestamps...</p>}

        {!signalLossLoading && signalLossData && (
          <>
            <p className="subtitle">
              Registros analizados:{' '}
              <strong>{signalLossData.total_records.toLocaleString()}</strong>
              {' · '}
              Segmentos sin señal:{' '}
              <strong className={signalLossData.signal_loss_segments_count > 0 ? 'warning' : 'success'}>
                {signalLossData.signal_loss_segments_count}
              </strong>
              {' · '}
              Tiempo total sin señal:{' '}
              <strong className={signalLossData.total_signal_loss_minutes > 0 ? 'warning' : 'success'}>
                {signalLossData.total_signal_loss_minutes.toFixed(1)} min
              </strong>
            </p>

            {signalLossData.error && (
              <p className="subtitle warning">{signalLossData.error}</p>
            )}

            {signalLossData.signal_loss_segments_count === 0 && (
              <p className="subtitle success">
                No se detectaron periodos sin señal para este asset/client.
              </p>
            )}

            {signalLossData.segments.length > 0 && (
              <>
                <h3>Segmentos sin señal detectados</h3>
                <div className="table-scroll">
                  <table>
                    <thead>
                      <tr>
                        <th>#</th>
                        <th>GPS inicio (dispositivo)</th>
                        <th>GPS fin (dispositivo)</th>
                        <th>Servidor recibió en</th>
                        <th>Duración sin señal</th>
                        <th>Registros acumulados</th>
                      </tr>
                    </thead>
                    <tbody>
                      {signalLossData.segments.map((seg, i) => (
                        <tr key={i}>
                          <td>{i + 1}</td>
                          <td>{seg.gps_start || '—'}</td>
                          <td>{seg.gps_end || '—'}</td>
                          <td>{seg.server_received_at || '—'}</td>
                          <td>
                            <strong className="warning">
                              {seg.duration_minutes.toFixed(1)} min
                            </strong>
                          </td>
                          <td>{seg.num_records}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>

                {signalLossAnomalies?.anomalies?.length > 0 && (
                  <>
                    <h3>Clasificación de anomalías por segmento</h3>
                    <p className="subtitle">
                      Velocidad implícita = distancia entre último punto antes del gap y primer punto después ÷ duración.
                      Umbral: {signalLossAnomalies.max_speed_kmh_threshold} km/h.
                    </p>
                    <div className="table-scroll">
                      <table>
                        <thead>
                          <tr>
                            <th>#</th>
                            <th>Categoría</th>
                            <th>Duración</th>
                            <th>Distancia GPS</th>
                            <th>Vel. implícita</th>
                            <th>Punto anterior</th>
                            <th>Punto posterior</th>
                          </tr>
                        </thead>
                        <tbody>
                          {signalLossAnomalies.anomalies.map((a, i) => {
                            const categoryLabel = {
                              salto_sospechoso: '⚠ Salto sospechoso',
                              apagado_en_reposo: '⚠ Apagado en reposo',
                              sin_cobertura: 'Sin cobertura',
                            }[a.category] ?? a.category
                            const isAlert = a.category !== 'sin_cobertura'
                            const fmtCoord = (p) =>
                              p?.latitude != null
                                ? `${p.latitude.toFixed(4)}, ${p.longitude.toFixed(4)}`
                                : '—'
                            return (
                              <tr key={i}>
                                <td>{i + 1}</td>
                                <td>
                                  <strong className={isAlert ? 'warning' : 'success'}>
                                    {categoryLabel}
                                  </strong>
                                </td>
                                <td>{a.duration_minutes.toFixed(1)} min</td>
                                <td>{a.distance_km != null ? `${a.distance_km.toFixed(2)} km` : '—'}</td>
                                <td className={isAlert ? 'warning' : ''}>
                                  {a.implied_speed_kmh != null ? `${a.implied_speed_kmh.toFixed(1)} km/h` : '—'}
                                </td>
                                <td style={{ fontSize: 11 }}>{fmtCoord(a.point_before)}</td>
                                <td style={{ fontSize: 11 }}>{fmtCoord(a.point_after)}</td>
                              </tr>
                            )
                          })}
                        </tbody>
                      </table>
                    </div>
                  </>
                )}

                <h3 style={{ display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap' }}>
                  Gráfico temporal: diferencia GPS → Servidor
                  <button style={{ fontSize: 12, padding: '3px 10px' }} onClick={openSignalLossChartWindow}>
                    Ver en ventana aparte ↗
                  </button>
                </h3>
                <p className="subtitle">
                  Eje Y: segundos entre GPSTimeStamp y ServerTimeStamp del mismo registro.
                  En condiciones normales la diferencia es de pocos segundos.
                  Picos indican transmisión en lote (sin señal previa).
                </p>
                {(() => {
                  const pts = signalLossData.all_points.filter(
                    (p) => typeof p.diff_seconds === 'number' && Number.isFinite(p.diff_seconds) && p.diff_seconds >= 0
                  )
                  if (pts.length < 2) return <p className="subtitle">Insuficientes puntos para graficar.</p>

                  const W = 720
                  const H = 140
                  const PL = 52, PR = 12, PT = 10, PB = 24
                  const iW = W - PL - PR
                  const iH = H - PT - PB
                  const maxDiff = Math.max(...pts.map((p) => p.diff_seconds), 60)
                  const xOf = (i) => PL + (i / (pts.length - 1)) * iW
                  const yOf = (v) => PT + iH - Math.min(v / maxDiff, 1) * iH
                  const thresholdY = yOf(30)

                  const polyline = pts.map((p, i) => `${xOf(i).toFixed(1)},${yOf(p.diff_seconds).toFixed(1)}`).join(' ')

                  const yLabels = [0, Math.round(maxDiff / 2), Math.round(maxDiff)]

                  return (
                    <svg
                      viewBox={`0 0 ${W} ${H}`}
                      style={{ width: '100%', maxWidth: W, display: 'block', overflow: 'visible', fontFamily: 'monospace', fontSize: 10 }}
                      aria-label="Grafico de diferencia GPS vs Servidor"
                    >
                      {/* Área de fondo para segmentos sin señal */}
                      {signalLossData.segments.map((seg, si) => {
                        const segPts = pts.filter((p) => p.is_signal_loss)
                        if (segPts.length === 0) return null
                        const firstIdx = pts.findIndex((p) => p.gps_timestamp === seg.gps_start || p.is_signal_loss)
                        const lastIdx = pts.findLastIndex((p) => p.gps_timestamp === seg.gps_end || p.is_signal_loss)
                        if (firstIdx < 0 || lastIdx < 0) return null
                        return (
                          <rect
                            key={si}
                            x={xOf(firstIdx)}
                            y={PT}
                            width={xOf(lastIdx) - xOf(firstIdx)}
                            height={iH}
                            fill="rgba(239,68,68,0.08)"
                          />
                        )
                      })}

                      {/* Línea de umbral normal (30 s) */}
                      <line x1={PL} y1={thresholdY} x2={W - PR} y2={thresholdY} stroke="#f59e0b" strokeWidth={1} strokeDasharray="4 3" />
                      <text x={PL - 2} y={thresholdY + 3} textAnchor="end" fill="#f59e0b">30s</text>

                      {/* Eje Y etiquetas */}
                      {yLabels.map((v) => (
                        <g key={v}>
                          <line x1={PL - 4} y1={yOf(v)} x2={PL} y2={yOf(v)} stroke="#888" strokeWidth={1} />
                          <text x={PL - 6} y={yOf(v) + 3} textAnchor="end" fill="#888">{v}s</text>
                        </g>
                      ))}

                      {/* Eje X e Y base */}
                      <line x1={PL} y1={PT} x2={PL} y2={PT + iH} stroke="#555" strokeWidth={1} />
                      <line x1={PL} y1={PT + iH} x2={W - PR} y2={PT + iH} stroke="#555" strokeWidth={1} />

                      {/* Línea de datos */}
                      <polyline points={polyline} fill="none" stroke="#3b82f6" strokeWidth={1.5} />

                      {/* Puntos coloreados */}
                      {pts.map((p, i) => (
                        <circle
                          key={i}
                          cx={xOf(i)}
                          cy={yOf(p.diff_seconds)}
                          r={pts.length > 200 ? 1.5 : 3}
                          fill={p.is_signal_loss ? '#ef4444' : '#3b82f6'}
                        />
                      ))}

                      {/* Leyenda */}
                      <circle cx={PL + 10} cy={H - 8} r={4} fill="#3b82f6" />
                      <text x={PL + 18} y={H - 5} fill="#888">Normal</text>
                      <circle cx={PL + 75} cy={H - 8} r={4} fill="#ef4444" />
                      <text x={PL + 83} y={H - 5} fill="#888">Sin señal</text>
                      <line x1={PL + 145} y1={H - 8} x2={PL + 160} y2={H - 8} stroke="#f59e0b" strokeDasharray="4 3" strokeWidth={1.5} />
                      <text x={PL + 164} y={H - 5} fill="#888">Umbral 30 s</text>
                    </svg>
                  )
                })()}
              </>
            )}
          </>
        )}

        {!signalLossLoading && !signalLossData && routeDetection && (
          <p className="subtitle">No se pudo obtener el análisis de señal.</p>
        )}

        {!signalLossLoading && !signalLossData && !routeDetection && (
          <p className="subtitle">Ingresa assetId y clientId en la sección de rutas para analizar la señal.</p>
        )}
      </section>

      <section className="card">
        <h2>5) Detección y clasificación de paradas</h2>
        <p className="subtitle">
          Parada detectada cuando el vehículo permanece con desplazamiento {'<'} 50 m durante ≥ 5 min.
          Clasificación: A) EventType Motum · B) POI cercano (OpenStreetMap).
        </p>

        {stopsLoading && <p className="subtitle">Detectando paradas...</p>}

        {!stopsLoading && stopsData && (
          <>
            <p className="subtitle">
              Registros analizados: <strong>{stopsData.total_records.toLocaleString()}</strong>
              {' · '}
              Paradas detectadas:{' '}
              <strong className={stopsData.total_stops > 0 ? 'warning' : 'success'}>
                {stopsData.total_stops}
              </strong>
              {' · '}
              Tiempo total detenido:{' '}
              <strong>{stopsData.total_stop_minutes.toFixed(1)} min</strong>
              {' · '}
              <span className="subtitle">
                Geo OSM: {stopsData.use_geo ? 'activo' : 'desactivado'}
                {!stopsData.use_geo && (
                  <button
                    style={{ marginLeft: 8, fontSize: 11, padding: '2px 8px' }}
                    onClick={() => {
                      const query = new URLSearchParams({
                        asset_id: routeAssetId.trim(),
                        client_id: routeClientId.trim(),
                        use_geo: 'true',
                      })
                      setStopsLoading(true)
                      fetch(`${API_BASE}/routes/stops?${query.toString()}`)
                        .then((r) => r.json())
                        .then((d) => setStopsData(d))
                        .finally(() => setStopsLoading(false))
                    }}
                  >
                    Activar clasificación OSM ↗
                  </button>
                )}
              </span>
            </p>

            {stopsData.total_stops > 0 && (
              <button onClick={openStopsMapWindow} style={{ marginBottom: 10 }}>
                Ver mapa de paradas ↗
              </button>
            )}

            {stopsData.total_stops === 0 && (
              <p className="subtitle success">No se detectaron paradas significativas para este asset/client.</p>
            )}

            {stopsData.stops.length > 0 && (
              <div className="table-scroll">
                <table>
                  <thead>
                    <tr>
                      <th>#</th>
                      <th>Inicio</th>
                      <th>Fin</th>
                      <th>Duración</th>
                      <th>Registros</th>
                      <th>Categoría final</th>
                      <th>Evento Motum</th>
                      <th>Lugar OSM</th>
                      <th>Confianza</th>
                      <th>Coordenadas</th>
                    </tr>
                  </thead>
                  <tbody>
                    {stopsData.stops.map((s) => {
                      const catColors = {
                        gasolinera: '#f59e0b',
                        hotel: '#8b5cf6',
                        restaurante: '#22c55e',
                        tienda_conveniencia: '#3b82f6',
                        motor_apagado: '#64748b',
                        ralenti: '#94a3b8',
                        parada_no_autorizada: '#ef4444',
                        estacionamiento_prolongado: '#f97316',
                        pernocta_fuera_de_base: '#dc2626',
                        parada_en_geocerca: '#0ea5e9',
                        punto_de_control: '#10b981',
                        entrega: '#10b981',
                        parada_en_carretera: '#94a3b8',
                      }
                      const color = catColors[s.final_category] ?? '#94a3b8'
                      return (
                        <tr key={s.stop_index}>
                          <td>{s.stop_index + 1}</td>
                          <td style={{ fontSize: 11 }}>{s.start_timestamp ? new Date(s.start_timestamp).toLocaleString('es-MX') : '—'}</td>
                          <td style={{ fontSize: 11 }}>{s.end_timestamp ? new Date(s.end_timestamp).toLocaleString('es-MX') : '—'}</td>
                          <td><strong>{s.duration_minutes.toFixed(1)} min</strong></td>
                          <td>{s.num_records}</td>
                          <td>
                            <span style={{ background: color + '22', color, border: `1px solid ${color}`, borderRadius: 6, padding: '2px 8px', fontSize: 12, fontWeight: 600 }}>
                              {s.final_category.replace(/_/g, ' ')}
                            </span>
                          </td>
                          <td style={{ fontSize: 11 }}>{s.event_type_category?.replace(/_/g, ' ') ?? '—'}</td>
                          <td style={{ fontSize: 11 }}>{s.nearby_place ?? '—'}</td>
                          <td style={{ fontSize: 11, color: s.confidence === 'alta' ? '#22c55e' : s.confidence === 'media' ? '#f59e0b' : '#94a3b8' }}>
                            {s.confidence}
                          </td>
                          <td style={{ fontSize: 11 }}>
                            {s.latitude != null ? `${s.latitude.toFixed(4)}, ${s.longitude.toFixed(4)}` : '—'}
                          </td>
                        </tr>
                      )
                    })}
                  </tbody>
                </table>
              </div>
            )}
          </>
        )}

        {!stopsLoading && !stopsData && routeDetection && (
          <p className="subtitle">No se pudo obtener el análisis de paradas.</p>
        )}
        {!stopsLoading && !stopsData && !routeDetection && (
          <p className="subtitle">Ingresa assetId y clientId en la sección de rutas para analizar las paradas.</p>
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
