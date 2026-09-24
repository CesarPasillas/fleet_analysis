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
  const [routeDateFrom, setRouteDateFrom] = useState('')
  const [routeDateTo, setRouteDateTo] = useState('')
  const [signalLossData, setSignalLossData] = useState(null)
  const [signalLossLoading, setSignalLossLoading] = useState(false)
  const [signalLossProgress, setSignalLossProgress] = useState(0)
  const [signalLossAnomalies, setSignalLossAnomalies] = useState(null)
  const [stopsData, setStopsData] = useState(null)
  const [stopsLoading, setStopsLoading] = useState(false)
  const [stopsProgress, setStopsProgress] = useState(0)
  const [stopsWithGeo, setStopsWithGeo] = useState(false)
  const [stopAnomaliesData, setStopAnomaliesData] = useState(null)
  const [stopAnomaliesLoading, setStopAnomaliesLoading] = useState(false)
  const [learnedRoutesData, setLearnedRoutesData] = useState(null)
  const [learnedRoutesLoading, setLearnedRoutesLoading] = useState(false)
  const [routeAnomaliesData, setRouteAnomaliesData] = useState(null)
  const [routeAnomaliesLoading, setRouteAnomaliesLoading] = useState(false)
  const [durationAnomaliesData, setDurationAnomaliesData] = useState(null)
  const [durationAnomaliesLoading, setDurationAnomaliesLoading] = useState(false)
  const [selectedMapRouteIndex, setSelectedMapRouteIndex] = useState(null)
  const [expandedRouteIndex, setExpandedRouteIndex] = useState(null)
  const [routePointsFilterMode, setRoutePointsFilterMode] = useState('all')
  const [routePointsEventTypeFilter, setRoutePointsEventTypeFilter] = useState('')
  const [manualMapMode, setManualMapMode] = useState('coords')
  const [manualMapInput, setManualMapInput] = useState('')
  const [error, setError] = useState('')

  // --- Resumen ejecutivo de rutas ---
  const [summaryTab, setSummaryTab] = useState('fleet')
  const [fleetSummary, setFleetSummary] = useState(null)
  const [fleetSummaryLoading, setFleetSummaryLoading] = useState(false)
  const [clientSummary, setClientSummary] = useState(null)
  const [clientSummaryLoading, setClientSummaryLoading] = useState(false)
  const [summaryClientId, setSummaryClientId] = useState('')
  const [clientAssets, setClientAssets] = useState([])
  const [clientAssetsTotal, setClientAssetsTotal] = useState(0)
  const [selectedAssets, setSelectedAssets] = useState(new Set())
  const [assetSearch, setAssetSearch] = useState('')
  const [globalCovData, setGlobalCovData] = useState(null)
  const [histStops, setHistStops] = useState(null)
  const [histStopsLoading, setHistStopsLoading] = useState(false)
  const [batchStops, setBatchStops] = useState(null)
  const [batchStopsJob, setBatchStopsJob] = useState(null)
  const [batchStopsPolling, setBatchStopsPolling] = useState(false)
  const [globalCovJob, setGlobalCovJob] = useState(null)   // {status, progress, message, done}
  const [globalCovPolling, setGlobalCovPolling] = useState(false)
  const [roadCovMatch, setRoadCovMatch] = useState(null)
  const [roadCovLoading, setRoadCovLoading] = useState(false)
  const [roadCovFilter, setRoadCovFilter] = useState('all')  // 'all' | 'alta' | 'media' | 'baja' | 'aislada'

  const loadFleetSummary = async () => {
    setFleetSummaryLoading(true)
    try {
      const res = await fetch(`${API_BASE}/routes/fleet-summary?limit=20`)
      if (res.ok) setFleetSummary(await res.json())
    } finally {
      setFleetSummaryLoading(false)
    }
  }

  const loadClientSummary = async (cid, assetFilter) => {
    const id = (cid ?? summaryClientId).toString().trim()
    if (!id) return
    setClientSummaryLoading(true)
    try {
      const active = assetFilter ?? selectedAssets
      const assetParam = active.size > 0 ? `&asset_ids=${[...active].join(',')}` : ''
      const res = await fetch(`${API_BASE}/routes/client-summary?client_id=${id}&limit=20${assetParam}`)
      if (res.ok) setClientSummary(await res.json())
    } finally {
      setClientSummaryLoading(false)
    }
  }

  const loadClientAssets = async (cid) => {
    const id = (cid ?? summaryClientId).toString().trim()
    if (!id) return
    const res = await fetch(`${API_BASE}/routes/client-assets?client_id=${id}`)
    if (res.ok) {
      const data = await res.json()
      setClientAssets(data.assets ?? [])
      setClientAssetsTotal(data.total_assets ?? data.assets?.length ?? 0)
      setSelectedAssets(new Set())
      setAssetSearch('')
    }
  }

  const loadGlobalCovCache = async () => {
    try {
      const res = await fetch(`${API_BASE}/routes/signal-loss/global-coverage`)
      if (res.ok) {
        const d = await res.json()
        if (d.available) setGlobalCovData(d)
      }
    } catch (_) {}
  }

  const loadRoadCovMatch = async () => {
    setRoadCovLoading(true)
    setError('')
    try {
      const res = await fetch(`${API_BASE}/routes/signal-loss/road-coverage-match`)
      if (!res.ok) { const e = await res.json(); throw new Error(e.detail || 'Error en el análisis'); }
      const d = await res.json()
      if (d.error) throw new Error(d.error)
      setRoadCovMatch(d)
    } catch (e) { setError(e.message) }
    finally { setRoadCovLoading(false) }
  }

  const startGlobalCovJob = async () => {
    setError('')
    setGlobalCovData(null)   // clear stale results while new job runs
    try {
      const res = await fetch(`${API_BASE}/routes/signal-loss/global-coverage/compute`, { method: 'POST' })
      if (!res.ok) { setError('No se pudo iniciar el análisis global.'); return }
      const d = await res.json()
      setGlobalCovJob(d.status)
      setGlobalCovPolling(true)
    } catch (e) { setError(e.message) }
  }


  const loadBatchStopsCache = async () => {
    try {
      const res = await fetch(`${API_BASE}/routes/stops/batch-analysis`)
      if (res.ok) { const d = await res.json(); if (d.available) setBatchStops(d) }
    } catch (_) {}
  }

  const startBatchStopsJob = async (force = false) => {
    setError('')
    if (force) setBatchStops(null)
    try {
      const res = await fetch(`${API_BASE}/routes/stops/batch-analysis/compute?force=${force}`, { method: 'POST' })
      if (!res.ok) { setError('No se pudo iniciar el análisis batch.'); return }
      const d = await res.json()
      setBatchStopsJob(d.status)
      setBatchStopsPolling(true)
    } catch (e) { setError(e.message) }
  }

  const loadHistStops = async () => {
    setHistStopsLoading(true)
    setError('')
    try {
      const res = await fetch(`${API_BASE}/routes/stops/historical-analysis?top_locations=30`)
      if (!res.ok) throw new Error('Error al cargar análisis histórico de paradas')
      const d = await res.json()
      if (d.error) throw new Error(d.error)
      setHistStops(d)
    } catch (e) { setError(e.message) }
    finally { setHistStopsLoading(false) }
  }

  const openHistStopsMapWindow = () => {
    if (!histStops?.top_locations?.length) return
    const locs = histStops.top_locations
    const mapWindow = window.open('', '_blank', 'width=1200,height=800')
    if (!mapWindow) { setError('El navegador bloqueó el popup.'); return }
    const safeLocs = JSON.stringify(locs).replaceAll('</', String.raw`<\/`)
    const CAT_COLORS = {ralenti:'#94a3b8',motor_apagado:'#64748b',entrega:'#10b981',
      punto_de_control:'#10b981',parada_en_geocerca:'#0ea5e9',estacionamiento_prolongado:'#f97316',
      pernocta_fuera_de_base:'#dc2626',parada_no_autorizada:'#ef4444'}
    const safeColors = JSON.stringify(CAT_COLORS).replaceAll('</', String.raw`<\/`)
    mapWindow.document.write(`<!doctype html><html lang="es"><head><meta charset="utf-8"/>
<title>Top paradas históricas</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
  integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY=" crossorigin=""/>
<style>html,body{margin:0;height:100%;font-family:Inter,system-ui,sans-serif;}
#map{width:100%;height:calc(100% - 48px);}
.hdr{height:48px;display:flex;align-items:center;padding:0 16px;border-bottom:1px solid #e5e7eb;background:#f9fafb;font-size:13px;font-weight:700;}
</style></head><body>
<div class="hdr">Top ${locs.length} ubicaciones de paradas — histórico de flota completa</div>
<div id="map"></div>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
  integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo=" crossorigin=""></script>
<script>
const locs = ${safeLocs};
const CAT_COLORS = ${safeColors};
const map = L.map('map');
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',{maxZoom:19,attribution:'© OpenStreetMap'}).addTo(map);
const bounds = [];
locs.forEach((l,i) => {
  const color = CAT_COLORS[l.dominant_cat] || '#94a3b8';
  const r = Math.max(10, Math.min(40, Math.sqrt(l.unique_assets) * 1.5));
  const pos = [l.lat, l.lon]; bounds.push(pos);
  L.circleMarker(pos,{radius:r,color:'#fff',weight:2,fillColor:color,fillOpacity:0.85}).addTo(map)
   .bindPopup('<b>'+l.icon+' #'+(i+1)+' '+l.dominant_cat.replace(/_/g,' ')+'</b><br/>' +
              'Eventos: <b>'+l.total_events.toLocaleString()+'</b><br/>' +
              'Assets únicos: <b>'+l.unique_assets+'</b><br/>' +
              'Clientes: <b>'+l.unique_clients+'</b><br/>' +
              'Lat: '+l.lat.toFixed(4)+' Lon: '+l.lon.toFixed(4))
   .bindTooltip(l.icon+' #'+(i+1)+' · '+l.total_events.toLocaleString()+' eventos',{direction:'top'});
});
if(bounds.length) map.fitBounds(L.latLngBounds(bounds),{padding:[30,30]});
</script></body></html>`)
    mapWindow.document.close()
  }

  const openGlobalCovMapWindow = () => {
    const allHotspots = [
      ...(globalCovData?.structural_hotspots ?? []),
      ...(globalCovData?.isolated_hotspots ?? []),
    ]
    if (!globalCovData || allHotspots.length === 0) {
      setError('No hay datos de cobertura global para mostrar en el mapa.')
      return
    }
    const hotspots = allHotspots
    const mapWindow = window.open('', '_blank', 'width=1400,height=900')
    if (!mapWindow) { setError('El navegador bloqueó la ventana. Permite pop-ups para este sitio.'); return }

    const safeHotspots = JSON.stringify(hotspots).replaceAll('</', String.raw`<\/`)
    const structural = globalCovData.structural_zones ?? 0
    const isolated = globalCovData.isolated_events ?? 0
    const totalClients = globalCovData.total_clients ?? 0

    mapWindow.document.write(`<!doctype html>
<html lang="es"><head><meta charset="utf-8"/>
<title>Cobertura GPS Global — ${totalClients} clientes</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
  integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY=" crossorigin=""/>
<style>
html,body{margin:0;padding:0;height:100%;font-family:Inter,system-ui,sans-serif;}
#map{width:100%;height:calc(100% - 56px);}
.hdr{height:56px;display:flex;align-items:center;justify-content:space-between;
  padding:0 16px;border-bottom:1px solid #e5e7eb;background:#f9fafb;font-size:13px;gap:10px;}
.badge{border-radius:12px;padding:2px 10px;font-size:12px;font-weight:600;}
</style></head><body>
<div class="hdr">
  <b>Cobertura GPS Global — ${totalClients} clientes analizados</b>
  <span>
    <span class="badge" style="background:#fee2e2;color:#991b1b;">⛔ Zonas estructurales: ${structural}</span>
    &nbsp;
    <span class="badge" style="background:#fef9c3;color:#854d0e;">⚡ Eventos aislados: ${isolated}</span>
  </span>
</div>
<div id="map"></div>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
  integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo=" crossorigin=""></script>
<script>
const hotspots = ${safeHotspots};
const map = L.map('map');
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',{maxZoom:19,attribution:'© OpenStreetMap'}).addTo(map);
const structLayer = L.layerGroup().addTo(map);
const isolLayer = L.layerGroup().addTo(map);
const bounds = [];
hotspots.forEach(h => {
  const pos = [h.lat, h.lon];
  bounds.push(pos);
  const isStruct = h.category === 'zona_sin_cobertura';
  const r = Math.min(20, 4 + h.asset_count * 2);
  L.circleMarker(pos, {
    radius: r, color: isStruct ? '#dc2626' : '#f59e0b',
    fillColor: isStruct ? '#fca5a5' : '#fde68a', fillOpacity: 0.75, weight: isStruct ? 2 : 1
  }).addTo(isStruct ? structLayer : isolLayer).bindPopup(
    '<b>' + (isStruct ? '⛔ Zona sin cobertura' : '⚡ Evento aislado') + '</b><br/>' +
    'Assets: <b>' + h.asset_count + '</b> · Clientes: <b>' + h.client_count + '</b><br/>' +
    'Eventos: ' + h.event_count + '<br/>' +
    'Lat: ' + h.lat.toFixed(4) + ' Lon: ' + h.lon.toFixed(4)
  );
});
L.control.layers(null,{'Zonas estructurales (≥2 assets)':structLayer,'Eventos aislados':isolLayer},{collapsed:false}).addTo(map);
if(bounds.length>0) map.fitBounds(L.latLngBounds(bounds),{padding:[30,30]});
</script></body></html>`)
    mapWindow.document.close()
  }

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

  const buildRouteQuery = (assetValue, clientValue) => {
    const query = new URLSearchParams({ asset_id: assetValue, client_id: clientValue })
    if (routeDateFrom.trim()) query.set('date_from', routeDateFrom.trim())
    if (routeDateTo.trim()) query.set('date_to', routeDateTo.trim())
    return query
  }

  const detectRoutesByValues = async (assetValue, clientValue, clearPrevious = true) => {
    setRouteLoading(true)
    setError('')
    if (clearPrevious) {
      setRouteDetection(null)
      setSignalLossData(null)
      setSignalLossAnomalies(null)
      setStopsData(null)
      setStopAnomaliesData(null)
      setLearnedRoutesData(null)
      setRouteAnomaliesData(null)
      setDurationAnomaliesData(null)
      setSelectedMapRouteIndex(null)
      setExpandedRouteIndex(null)
    }

    if (!assetValue || !clientValue) {
      setRouteLoading(false)
      setError('Debes indicar assetId y clientId para detectar rutas')
      return
    }

    const query = buildRouteQuery(assetValue, clientValue)

    try {
      const resp = await fetch(`${API_BASE}/routes/detect?${query.toString()}`)
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
  }

  const loadSignalLoss = async () => {
    const assetValue = routeAssetId.trim()
    const clientValue = routeClientId.trim()
    if (!assetValue || !clientValue) return
    setSignalLossProgress(0)
    setSignalLossLoading(true)
    setSignalLossData(null)
    setSignalLossAnomalies(null)
    const query = buildRouteQuery(assetValue, clientValue)
    try {
      const [signalResp, anomalyResp] = await Promise.all([
        fetch(`${API_BASE}/routes/signal-loss?${query.toString()}`),
        fetch(`${API_BASE}/routes/signal-loss/anomalies?${query.toString()}`),
      ])
      if (signalResp.ok) setSignalLossData(await signalResp.json())
      if (anomalyResp.ok) setSignalLossAnomalies(await anomalyResp.json())
    } finally {
      setSignalLossLoading(false)
    }
  }

  const loadStops = async (useGeo = false) => {
    const assetValue = routeAssetId.trim()
    const clientValue = routeClientId.trim()
    if (!assetValue || !clientValue) return
    setStopsProgress(0)
    setStopsWithGeo(useGeo)
    setStopsLoading(true)
    setStopsData(null)
    const query = buildRouteQuery(assetValue, clientValue)
    query.set('use_geo', String(useGeo))
    // Filter to the selected route's time window
    const selectedRoute = expandedRouteIndex !== null
      ? routeDetection?.complete_routes?.[expandedRouteIndex]
      : null
    if (selectedRoute?.start_timestamp) query.set('date_from', selectedRoute.start_timestamp)
    if (selectedRoute?.end_timestamp)   query.set('date_to',   selectedRoute.end_timestamp)
    try {
      const resp = await fetch(`${API_BASE}/routes/stops?${query.toString()}`)
      if (resp.ok) setStopsData(await resp.json())
    } finally {
      setStopsLoading(false)
    }
  }

  const loadStopAnomalies = async () => {
    const assetValue = routeAssetId.trim()
    const clientValue = routeClientId.trim()
    if (!assetValue || !clientValue) return
    setStopAnomaliesLoading(true)
    setStopAnomaliesData(null)
    const query = buildRouteQuery(assetValue, clientValue)
    query.set('use_geo', String(stopsWithGeo))
    try {
      const resp = await fetch(`${API_BASE}/routes/stops/asset-anomalies?${query.toString()}`)
      if (resp.ok) setStopAnomaliesData(await resp.json())
    } finally {
      setStopAnomaliesLoading(false)
    }
  }

  const loadRouteAnomalies = async () => {
    const assetValue = routeAssetId.trim()
    const clientValue = routeClientId.trim()
    if (!assetValue || !clientValue) return
    setRouteAnomaliesLoading(true)
    setRouteAnomaliesData(null)
    const query = buildRouteQuery(assetValue, clientValue)
    query.set('use_geo', String(stopsWithGeo))
    try {
      const resp = await fetch(`${API_BASE}/routes/stops/route-anomalies?${query.toString()}`)
      if (resp.ok) setRouteAnomaliesData(await resp.json())
    } finally {
      setRouteAnomaliesLoading(false)
    }
  }

  const loadDurationAnomalies = async () => {
    const assetValue = routeAssetId.trim()
    const clientValue = routeClientId.trim()
    if (!assetValue || !clientValue) return
    setDurationAnomaliesLoading(true)
    setDurationAnomaliesData(null)
    const query = buildRouteQuery(assetValue, clientValue)
    try {
      const resp = await fetch(`${API_BASE}/routes/predict/duration-anomalies?${query.toString()}`)
      if (resp.ok) setDurationAnomaliesData(await resp.json())
    } finally {
      setDurationAnomaliesLoading(false)
    }
  }

  const loadLearnedRoutes = async () => {
    const assetValue = routeAssetId.trim()
    const clientValue = routeClientId.trim()
    if (!assetValue || !clientValue) return
    setLearnedRoutesLoading(true)
    setLearnedRoutesData(null)
    const query = buildRouteQuery(assetValue, clientValue)
    try {
      const resp = await fetch(`${API_BASE}/routes/predict/learned-routes?${query.toString()}`)
      if (resp.ok) setLearnedRoutesData(await resp.json())
    } finally {
      setLearnedRoutesLoading(false)
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
    loadGlobalCovCache()
  }, [])

  useEffect(() => {
    if (!globalCovPolling) return
    const id = setInterval(async () => {
      try {
        const res = await fetch(`${API_BASE}/routes/signal-loss/global-coverage/status`)
        if (!res.ok) return
        const s = await res.json()
        setGlobalCovJob(s)
        if (s.done) {
          setGlobalCovPolling(false)
          loadGlobalCovCache()
        }
      } catch (_) {}
    }, 5000)
    return () => clearInterval(id)
  }, [globalCovPolling])

  useEffect(() => {
    if (!batchStopsPolling) return
    const id = setInterval(async () => {
      try {
        const res = await fetch(`${API_BASE}/routes/stops/batch-analysis/status`)
        if (!res.ok) return
        const s = await res.json()
        setBatchStopsJob(s)
        if (s.done) { setBatchStopsPolling(false); loadBatchStopsCache() }
      } catch (_) {}
    }, 6000)
    return () => clearInterval(id)
  }, [batchStopsPolling])

  // Load batch stops cache on startup
  useEffect(() => { loadBatchStopsCache() }, [])

  // Simulated progress for section 4 (signal loss ~10-30s)
  useEffect(() => {
    if (!signalLossLoading) { setSignalLossProgress(100); return }
    setSignalLossProgress(0)
    let p = 0
    const id = setInterval(() => {
      p = Math.min(90, p + Math.max(0.4, (90 - p) * 0.04))
      setSignalLossProgress(Math.round(p))
    }, 400)
    return () => clearInterval(id)
  }, [signalLossLoading])

  // Simulated progress for section 5 (stops: fast without geo, slow with geo)
  useEffect(() => {
    if (!stopsLoading) { setStopsProgress(100); return }
    setStopsProgress(0)
    let p = 0
    // With geo (OSM calls): ~minutes → slower increment; without: ~15s → faster
    const tick = stopsWithGeo ? 600 : 350
    const rate = stopsWithGeo ? 0.02 : 0.05
    const id = setInterval(() => {
      p = Math.min(90, p + Math.max(0.2, (90 - p) * rate))
      setStopsProgress(Math.round(p))
    }, tick)
    return () => clearInterval(id)
  }, [stopsLoading, stopsWithGeo])

  // Clear section 5 (stops) when the selected route changes — stops are route-specific
  useEffect(() => {
    setStopsData(null)
    setStopAnomaliesData(null)
  }, [expandedRouteIndex])


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
      setStopAnomaliesData(null)
      setLearnedRoutesData(null)
      setRouteAnomaliesData(null)
      setDurationAnomaliesData(null)
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

  const ROUTE_REASON_LABELS = {
    ignition_event: 'evento 519/520',
    resumed_after_stop: 'reanudó tras parada',
    stop_detected: 'parada real detectada',
    data_start: 'inicio de los datos',
    data_end: 'fin de los datos',
  }

  const describeRouteBoundary = (reason) => ROUTE_REASON_LABELS[reason] || reason || '—'

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
    // El inicio/fin de un recorrido ahora se determina por movimiento real (reposo
    // GPS), no siempre coincide con un evento 519/520 exacto — comparar por
    // record_index (posición absoluta), que route_detection.py siempre reporta.
    const recordIndex = Number(record?.record_index)

    const isStart = Number.isFinite(recordIndex) && recordIndex === Number(route?.start_index)
    if (isStart) {
      return 'start'
    }

    const isEnd = Number.isFinite(recordIndex) && recordIndex === Number(route?.end_index)
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
    if (record?.latitude == null || record?.longitude == null) return false
    const latitude = Number(record.latitude)
    const longitude = Number(record.longitude)
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

    const sourceRecords = selectedMapRouteIndex !== null
      ? (() => {
          const route = routeDetection.complete_routes[selectedMapRouteIndex]
          if (!route) return routeDetection.all_records
          return routeDetection.all_records.filter((_, i) => i >= route.start_index && i <= route.end_index)
        })()
      : routeDetection.all_records

    const orderedPoints = [...sourceRecords]
      .filter((record) => record?.gps_timestamp)
      .sort((left, right) => String(left.gps_timestamp).localeCompare(String(right.gps_timestamp)))
      .filter((record) => record?.latitude != null && record?.longitude != null && Number.isFinite(Number(record.latitude)) && Number.isFinite(Number(record.longitude)))
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

    // Build all points preserving lost-GPS records as gap markers
    const allPoints = intervalRecords.map((record, index) => {
      const hasGps = hasValidGpsPoint(record)
      return {
        sequence: index + 1,
        orderTimestamp: record.order_timestamp,
        gpsTimestamp: record.gps_timestamp,
        serverTimestamp: record.server_timestamp,
        eventType: record.event_type,
        eventKey: record.event_key,
        eventDescription: record.event_description,
        latitude: hasGps ? Number(record.latitude) : null,
        longitude: hasGps ? Number(record.longitude) : null,
        hasGps,
        role: getIntervalRecordRole(route, record),
      }
    })

    const gpsPoints = allPoints.filter((p) => p.hasGps)
    const lostCount = allPoints.length - gpsPoints.length

    if (gpsPoints.length < 2) {
      setError('No hay suficientes puntos GPS válidos para pintar el mapa de esta ruta completa.')
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

    const windowName = `route-map-complete-${routeIndex}`
    const mapWindow = window.open('', windowName, 'width=1200,height=800')
    if (!mapWindow) {
      setError('No se pudo abrir la ventana del mapa. Revisa el bloqueador de popups del navegador.')
      return
    }

    const safePointsJson = JSON.stringify(allPoints).replaceAll('</', String.raw`<\/`)
    const safeRoadsJson = JSON.stringify(matchedRoadSegments).replaceAll('</', String.raw`<\/`)
    const title = `Ruta completa #${routeIndex + 1} assetId=${routeDetection?.asset_id || '-'} clientId=${routeDetection?.client_id || '-'}`
    const lostLabel = lostCount > 0 ? ` · ⚡ ${lostCount} sin GPS` : ''

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
            <span>${title} · GPS: ${gpsPoints.length}${lostLabel} · carreteras: ${matchedRoadLabels.length}</span>
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

            // Separate GPS and lost-signal points
            const gpsPoints = points.filter((p) => p.hasGps);
            const lostCount = points.length - gpsPoints.length;

            const allBoundsPoints = gpsPoints.map((p) => [p.latitude, p.longitude]);

            expectedRoads.forEach((road) => {
              const roadLine = L.polyline(road.latlngs, {
                color: '#22c55e',
                weight: 3,
                opacity: 0.7,
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

            // Build GPS segments (consecutive runs of hasGps points)
            // and gap bridges (dashed lines between last GPS before loss and first GPS after)
            const gpsSections = [];
            let currentSection = [];
            points.forEach((p) => {
              if (p.hasGps) {
                currentSection.push(p);
              } else {
                if (currentSection.length > 0) {
                  gpsSections.push(currentSection);
                  currentSection = [];
                }
              }
            });
            if (currentSection.length > 0) gpsSections.push(currentSection);

            const routeLines = [];
            gpsSections.forEach((section, idx) => {
              const latlngs = section.map((p) => [p.latitude, p.longitude]);
              const line = L.polyline(latlngs, { color: '#8b5cf6', weight: 4, opacity: 0.95 }).addTo(routeLayer);
              routeLines.push(line);

              // Draw dashed bridge to next section
              if (idx < gpsSections.length - 1) {
                const bridgeEnd = gpsSections[idx + 1][0];
                const bridgeLatlngs = [
                  [section[section.length - 1].latitude, section[section.length - 1].longitude],
                  [bridgeEnd.latitude, bridgeEnd.longitude],
                ];
                L.polyline(bridgeLatlngs, {
                  color: '#8b5cf6',
                  weight: 2,
                  opacity: 0.7,
                  dashArray: '8,6',
                }).addTo(routeLayer).bindPopup('<b>⚡ Brecha sin GPS</b><br/>Tramo sin conectividad');
              }
            });

            // Interpolate lost-point positions for gap markers
            function parseTimeMs(ts) {
              if (!ts) return null;
              const t = new Date(ts).getTime();
              return Number.isFinite(t) ? t : null;
            }

            // Build time-indexed list of GPS points for interpolation
            const gpsTimeline = gpsPoints
              .map((p) => ({ tMs: parseTimeMs(p.orderTimestamp || p.gpsTimestamp || p.serverTimestamp), lat: p.latitude, lon: p.longitude }))
              .filter((p) => p.tMs != null);

            function interpolatePosition(tMs) {
              if (gpsTimeline.length < 2) return null;
              let prev = null, next = null;
              for (const g of gpsTimeline) {
                if (g.tMs <= tMs) prev = g;
                else if (next == null) next = g;
              }
              if (!prev || !next || next.tMs <= prev.tMs) return null;
              const ratio = (tMs - prev.tMs) / (next.tMs - prev.tMs);
              return [prev.lat + ratio * (next.lat - prev.lat), prev.lon + ratio * (next.lon - prev.lon)];
            }

            // Draw lost-point gap markers
            const lostIcon = L.divIcon({
              className: '',
              html: '<div style="background:#8b5cf6;color:#fff;border:2px solid #4c1d95;border-radius:50%;width:14px;height:14px;display:flex;align-items:center;justify-content:center;font-size:9px;font-weight:700;">⚡</div>',
              iconSize: [14, 14],
              iconAnchor: [7, 7],
            });

            points.filter((p) => !p.hasGps).forEach((p) => {
              const tMs = parseTimeMs(p.orderTimestamp || p.gpsTimestamp || p.serverTimestamp);
              const pos = tMs != null ? interpolatePosition(tMs) : null;
              if (!pos) return;
              L.marker(pos, { icon: lostIcon }).addTo(routeLayer).bindPopup(
                '<b>⚡ Sin señal GPS</b><br/>' +
                '#: ' + p.sequence + '<br/>' +
                'eventType: ' + escapeHtml(p.eventType) + '<br/>' +
                'desc: ' + escapeHtml(p.eventDescription) + '<br/>' +
                'order: ' + escapeHtml(p.orderTimestamp) + '<br/>' +
                'key: ' + escapeHtml(p.eventKey)
              );
            });

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
              // Only route large gaps within each GPS section (ignore lost-point bridges)
              for (const section of gpsSections) {
                const gapIdxs = [];
                for (let i = 0; i < section.length - 1; i++) {
                  const dist = haversinKm(section[i].latitude, section[i].longitude, section[i+1].latitude, section[i+1].longitude);
                  if (dist > GAP_KM) gapIdxs.push({ i, dist: dist.toFixed(1) });
                }
                if (gapIdxs.length === 0) continue;
                const statusEl = document.getElementById('routing-status');
                statusEl.textContent = 'Enrutando ' + gapIdxs.length + ' tramo(s) via OSRM...';
                const routedLatlngs = [[section[0].latitude, section[0].longitude]];
                for (let i = 0; i < section.length - 1; i++) {
                  const p1 = section[i], p2 = section[i + 1];
                  const dist = haversinKm(p1.latitude, p1.longitude, p2.latitude, p2.longitude);
                  if (dist > GAP_KM) {
                    const routed = await osrmSegment(p1.latitude, p1.longitude, p2.latitude, p2.longitude);
                    routedLatlngs.push(...(routed && routed.length > 1 ? routed.slice(1) : [[p2.latitude, p2.longitude]]));
                  } else {
                    routedLatlngs.push([p2.latitude, p2.longitude]);
                  }
                }
                // replace the section's polyline with the routed version
                routeLines.forEach((l) => routeLayer.removeLayer(l));
                L.polyline(routedLatlngs, { color: '#8b5cf6', weight: 4, opacity: 0.95 }).addTo(routeLayer);
                statusEl.textContent = '✓ OSRM: tramos enrutados por carretera';
              }
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

            gpsPoints.forEach((point) => {
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
                fillColor: isStart ? '#16a34a' : (isEnd ? '#dc2626' : '#06b6d4'),
                fillOpacity: 0.95,
                weight: 1,
              }).addTo(routeLayer);

              marker.bindPopup(
                '<b>' + (isStart ? 'INICIO' : (isEnd ? 'FIN' : 'Punto GPS')) + '</b><br/>' +
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

            if (lostCount > 0) {
              const legend = L.control({ position: 'bottomleft' });
              legend.onAdd = () => {
                const div = L.DomUtil.create('div');
                div.style.cssText = 'background:#fff;border:1px solid #d1d5db;border-radius:6px;padding:8px 12px;font-size:12px;line-height:1.6;';
                div.innerHTML =
                  '<b>Leyenda</b><br/>' +
                  '<span style="color:#8b5cf6">━━</span> Ruta con GPS<br/>' +
                  '<span style="color:#8b5cf6">┄┄</span> Brecha sin conectividad<br/>' +
                  '<span style="color:#06b6d4">●</span> Punto GPS<br/>' +
                  '<span style="color:#22c55e">━━</span> Carretera esperada<br/>' +
                  '⚡ Punto perdido (posición estimada)<br/>' +
                  'Total sin GPS: <b>' + lostCount + '</b>';
                return div;
              };
              legend.addTo(map);
            }

            if (allBoundsPoints.length > 0) {
              map.fitBounds(L.latLngBounds(allBoundsPoints), { padding: [24, 24] });
            }

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
    mapWindow.focus()
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

  const openSignalLossCoverageMap = () => {
    if (!signalLossData) return

    const allPts = signalLossData.all_points || []
    const gpsPts = allPts.filter((p) => p.latitude != null && p.longitude != null)
    if (gpsPts.length < 2) {
      setError('No hay suficientes puntos GPS para mostrar el mapa de cobertura.')
      return
    }

    const anomalies = signalLossAnomalies?.anomalies || []
    const mapWindow = window.open('', '_blank', 'width=1200,height=800')
    if (!mapWindow) {
      setError('No se pudo abrir la ventana del mapa. Revisa el bloqueador de popups.')
      return
    }

    const title = `Mapa de cobertura — assetId=${signalLossData.asset_id || '-'} clientId=${signalLossData.client_id || '-'}`
    const lostCount = allPts.filter((p) => p.latitude == null).length
    const estCount = allPts.filter((p) => p.latitude == null && p.estimated_latitude != null).length

    const safeGpsJson = JSON.stringify(allPts).replaceAll('</', String.raw`<\/`)
    const safeAnomalyJson = JSON.stringify(anomalies).replaceAll('</', String.raw`<\/`)

    const htmlContent = `
      <!doctype html>
      <html lang="es">
        <head>
          <meta charset="utf-8"/>
          <meta name="viewport" content="width=device-width, initial-scale=1"/>
          <title>${title}</title>
          <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
            integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY=" crossorigin=""/>
          <style>
            html,body{margin:0;padding:0;height:100%;font-family:Inter,system-ui,sans-serif;}
            #map{width:100%;height:calc(100% - 56px);}
            .header{height:56px;display:flex;align-items:center;justify-content:space-between;
              padding:0 16px;border-bottom:1px solid #e5e7eb;background:#f9fafb;font-size:13px;color:#111827;}
          </style>
        </head>
        <body>
          <div class="header">
            <span>${title} · GPS reales: ${gpsPts.length} · sin GPS: ${lostCount} · estimados: ${estCount}</span>
          </div>
          <div id="map"></div>
          <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
            integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo=" crossorigin=""></script>
          <script>
            const allPts = ${safeGpsJson};
            const anomalies = ${safeAnomalyJson};

            const map = L.map('map');
            L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
              maxZoom: 19, attribution: '&copy; OpenStreetMap contributors'
            }).addTo(map);

            const escapeHtml = (v) => String(v ?? '')
              .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');

            // --- 1. Tramos con GPS real (azul sólido) ---
            const gpsLayer = L.layerGroup().addTo(map);
            const lossLayer = L.layerGroup().addTo(map);
            const estLayer  = L.layerGroup().addTo(map);

            const allBounds = [];

            // Build GPS segments (consecutive valid-GPS points)
            const sections = [];
            let cur = [];
            allPts.forEach((p) => {
              if (p.latitude != null) {
                cur.push(p);
              } else {
                if (cur.length > 0) { sections.push(cur); cur = []; }
              }
            });
            if (cur.length > 0) sections.push(cur);

            sections.forEach((sec) => {
              const ll = sec.map((p) => [p.latitude, p.longitude]);
              allBounds.push(...ll);
              L.polyline(ll, { color: '#8b5cf6', weight: 3, opacity: 0.85 }).addTo(gpsLayer);
            });

            // GPS real markers (cyan dots)
            allPts.filter((p) => p.latitude != null && !p.is_signal_loss).forEach((p) => {
              L.circleMarker([p.latitude, p.longitude], {
                radius: 3, color: '#0f766e', fillColor: '#06b6d4', fillOpacity: 0.9, weight: 1
              }).addTo(gpsLayer).bindPopup(
                '<b>GPS real</b><br/>gps: ' + escapeHtml(p.gps_timestamp) +
                '<br/>diff: ' + p.diff_seconds + 's' +
                '<br/>lat: ' + p.latitude + ' lon: ' + p.longitude
              );
            });

            // --- 2. Puntos dentro de segmento sin señal con GPS real (⚡ morado) ---
            const lostIconSmall = L.divIcon({
              className: '',
              html: '<div style="background:#8b5cf6;color:#fff;border:2px solid #4c1d95;border-radius:50%;width:14px;height:14px;display:flex;align-items:center;justify-content:center;font-size:9px;font-weight:700;">⚡</div>',
              iconSize: [14, 14], iconAnchor: [7, 7],
            });
            allPts.filter((p) => p.latitude != null && p.is_signal_loss).forEach((p) => {
              L.marker([p.latitude, p.longitude], { icon: lostIconSmall })
                .addTo(lossLayer).bindPopup(
                  '<b>⚡ GPS dentro de pérdida</b><br/>gps: ' + escapeHtml(p.gps_timestamp) +
                  '<br/>diff: ' + p.diff_seconds + 's' +
                  '<br/>lat: ' + p.latitude + ' lon: ' + p.longitude
                );
            });

            // --- 3. Posiciones estimadas para puntos sin GPS (⚡ rosa) ---
            const estIcon = L.divIcon({
              className: '',
              html: '<div style="background:#ec4899;color:#9d174d;border:2px solid #9d174d;border-radius:50%;width:14px;height:14px;display:flex;align-items:center;justify-content:center;font-size:9px;font-weight:700;">⚡</div>',
              iconSize: [14, 14], iconAnchor: [7, 7],
            });
            allPts.filter((p) => p.latitude == null && p.estimated_latitude != null).forEach((p) => {
              const pos = [p.estimated_latitude, p.estimated_longitude];
              L.marker(pos, { icon: estIcon })
                .addTo(estLayer).bindPopup(
                  '<b>⚡ Sin GPS (posición estimada)</b><br/>' +
                  'gps: ' + escapeHtml(p.gps_timestamp) + '<br/>' +
                  'diff: ' + p.diff_seconds + 's<br/>' +
                  'est lat: ' + p.estimated_latitude + '<br/>' +
                  'est lon: ' + p.estimated_longitude
                );
            });

            // --- 4. Puentes de gaps por anomalías (línea punteada roja) ---
            anomalies.forEach((a, idx) => {
              const pb = a.point_before;
              const pa = a.point_after;
              if (!pb || !pa) return;
              const bridgeLl = [
                [pb.latitude, pb.longitude],
                [pa.latitude, pa.longitude],
              ];
              allBounds.push(...bridgeLl);
              const cat = { salto_sospechoso: '⚠ Salto sospechoso', apagado_en_reposo: '⚠ Apagado en reposo', sin_cobertura: 'Sin cobertura' }[a.category] || a.category;
              const color = a.category === 'sin_cobertura' ? '#8b5cf6'
                          : a.category === 'apagado_en_reposo' ? '#991b1b'
                          : '#dc2626';
              L.polyline(bridgeLl, { color, weight: 2.5, opacity: 0.8, dashArray: '10,6' })
                .addTo(lossLayer)
                .bindPopup(
                  '<b>Zona sin cobertura #' + (idx + 1) + '</b><br/>' +
                  'Categoría: <b>' + escapeHtml(cat) + '</b><br/>' +
                  'Duración: ' + a.duration_minutes.toFixed(1) + ' min<br/>' +
                  'Distancia GPS: ' + (a.distance_km != null ? a.distance_km.toFixed(2) + ' km' : '—') + '<br/>' +
                  'Vel. implícita: ' + (a.implied_speed_kmh != null ? a.implied_speed_kmh.toFixed(1) + ' km/h' : '—')
                );

              // Midpoint marker
              const midLat = (pb.latitude + pa.latitude) / 2;
              const midLon = (pb.longitude + pa.longitude) / 2;
              const textColor   = a.category === 'sin_cobertura' ? '#2e1065' : '#fff';
              const borderColor = a.category === 'sin_cobertura' ? '#4c1d95' : '#7f1d1d';
              const lossIcon = L.divIcon({
                className: '',
                html: '<div style="background:' + color + ';color:' + textColor + ';border:2px solid ' + borderColor + ';border-radius:12px;padding:1px 7px;font-size:10px;font-weight:700;white-space:nowrap;">⚡ ' + a.duration_minutes.toFixed(0) + 'min</div>',
                iconAnchor: [30, 10],
              });
              L.marker([midLat, midLon], { icon: lossIcon }).addTo(lossLayer);
            });

            // --- Leyenda ---
            const legend = L.control({ position: 'bottomleft' });
            legend.onAdd = () => {
              const d = L.DomUtil.create('div');
              d.style.cssText = 'background:#fff;border:1px solid #d1d5db;border-radius:6px;padding:8px 12px;font-size:12px;line-height:1.8;min-width:200px;';
              d.innerHTML =
                '<b>Leyenda cobertura GPS</b><br/>' +
                '<span style="color:#8b5cf6">━━</span> Tramo con GPS real<br/>' +
                '<span style="color:#8b5cf6">┄┄</span> Brecha sin cobertura<br/>' +
                '<span style="color:#dc2626">┄┄</span> Salto sospechoso<br/>' +
                '<span style="color:#991b1b">┄┄</span> Apagado en reposo<br/>' +
                '<span style="color:#06b6d4">●</span> Punto GPS real<br/>' +
                '<span style="color:#8b5cf6">⚡</span> GPS dentro de pérdida<br/>' +
                '<span style="color:#ec4899">⚡</span> Posición estimada (sin GPS)';
              return d;
            };
            legend.addTo(map);

            // --- Layer control ---
            L.control.layers(null, {
              'Tramos GPS': gpsLayer,
              'Zonas sin cobertura': lossLayer,
              'Estimados sin GPS': estLayer,
            }, { collapsed: false }).addTo(map);

            if (allBounds.length > 0) {
              map.fitBounds(L.latLngBounds(allBounds), { padding: [24, 24] });
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

    // Build route GPS polyline from selected route records
    const selectedRoute = expandedRouteIndex !== null
      ? routeDetection?.complete_routes?.[expandedRouteIndex]
      : null
    const routePoints = selectedRoute
      ? getRouteIntervalRecords(selectedRoute)
          .filter((r) => r.latitude != null && r.longitude != null &&
                         Number.isFinite(Number(r.latitude)) && Number.isFinite(Number(r.longitude)))
          .map((r) => [Number(r.latitude), Number(r.longitude)])
      : []

    const safeStops = JSON.stringify(stops).replaceAll('</', String.raw`<\/`)
    const safeRoute = JSON.stringify(routePoints).replaceAll('</', String.raw`<\/`)
    const routeLabel = selectedRoute
      ? ` · Ruta #${expandedRouteIndex + 1} (${selectedRoute.start_timestamp?.slice(0, 10) ?? ''})`
      : ''
    const title = `Paradas — assetId=${stopsData.asset_id} clientId=${stopsData.client_id}${routeLabel}`

    const html = `<!doctype html>
<html lang="es"><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>${title}</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
  integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY=" crossorigin=""/>
<style>
*{box-sizing:border-box;margin:0;padding:0;}
html,body{height:100%;font-family:Inter,system-ui,sans-serif;display:flex;flex-direction:column;}
#app{display:flex;flex:1;overflow:hidden;}
#sidebar{width:300px;min-width:200px;display:flex;flex-direction:column;border-right:1px solid #e2e8f0;background:#fff;}
#map{flex:1;}
.hdr{padding:10px 12px;border-bottom:1px solid #e2e8f0;background:#f8fafc;}
.hdr h1{font-size:13px;font-weight:700;color:#0f172a;margin-bottom:2px;}
.hdr p{font-size:11px;color:#64748b;}
.controls{padding:8px 10px;border-bottom:1px solid #e2e8f0;display:flex;flex-direction:column;gap:5px;}
.controls label{font-size:11px;color:#475569;display:flex;align-items:center;gap:5px;cursor:pointer;}
#stop-list{flex:1;overflow-y:auto;}
.stop-item{padding:7px 10px;border-bottom:1px solid #f1f5f9;cursor:pointer;display:flex;align-items:flex-start;gap:8px;}
.stop-item:hover{background:#f0f9ff;}
.stop-item.active{background:#eff6ff;border-left:3px solid #2563eb;}
.stop-item.far{opacity:0.45;}
.dist-badge{font-size:10px;padding:1px 5px;border-radius:8px;font-weight:600;white-space:nowrap;flex-shrink:0;}
.dist-ok{background:#dcfce7;color:#15803d;}
.dist-warn{background:#fef9c3;color:#92400e;}
.dist-far{background:#fee2e2;color:#991b1b;}
.stop-name{font-size:12px;color:#0f172a;line-height:1.3;}
.stop-meta{font-size:10px;color:#94a3b8;margin-top:2px;}
</style>
</head><body>
<div id="app">
  <div id="sidebar">
    <div class="hdr">
      <h1>${title}</h1>
      <p>${stops.length} paradas · ${stopsData.total_stop_minutes.toFixed(1)} min detenido</p>
    </div>
    <div class="controls">
      <label><input type="checkbox" id="chk-far" checked onchange="toggleFar()"/> Mostrar paradas lejanas (>500 m)</label>
      <label><input type="checkbox" id="chk-lines" checked onchange="toggleLines()"/> Mostrar líneas conectoras</label>
    </div>
    <div id="stop-list"></div>
  </div>
  <div id="map"></div>
</div>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
  integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo=" crossorigin=""></script>
<script>
const stops    = ${safeStops};
const routePts = ${safeRoute};

const CAT_COLORS = {
  gasolinera:'#f59e0b',hotel:'#8b5cf6',restaurante:'#22c55e',
  tienda_conveniencia:'#3b82f6',farmacia:'#06b6d4',hospital:'#0891b2',
  taller_mecanico:'#78716c',terminal_carga:'#6366f1',motor_apagado:'#64748b',
  ralenti:'#94a3b8',parada_no_autorizada:'#ef4444',estacionamiento_prolongado:'#f97316',
  pernocta_fuera_de_base:'#dc2626',parada_larga:'#b45309',parada_en_geocerca:'#0ea5e9',
  punto_de_control:'#10b981',entrega:'#10b981',en_carretera:'#78716c',parada_en_carretera:'#94a3b8',
};
const CAT_ICONS = {
  gasolinera:'⛽',hotel:'🏨',restaurante:'🍽',tienda_conveniencia:'🏪',farmacia:'💊',
  hospital:'🏥',taller_mecanico:'🔧',terminal_carga:'🏭',motor_apagado:'🔌',ralenti:'⏳',
  parada_no_autorizada:'⛔',estacionamiento_prolongado:'🅿',pernocta_fuera_de_base:'🌙',
  parada_larga:'⏸',parada_en_geocerca:'📍',punto_de_control:'✅',entrega:'📦',
  en_carretera:'🛣',parada_en_carretera:'🛣',
};

const fmtTs = (v) => {
  if (!v) return '—';
  const d = new Date(v);
  return isNaN(d)?v:d.toLocaleString('es-MX',{year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',hour12:false});
};

const haversinM = (a, b) => {
  const R=6371000, dLat=(b[0]-a[0])*Math.PI/180, dLon=(b[1]-a[1])*Math.PI/180;
  const s=Math.sin(dLat/2)**2+Math.cos(a[0]*Math.PI/180)*Math.cos(b[0]*Math.PI/180)*Math.sin(dLon/2)**2;
  return R*2*Math.atan2(Math.sqrt(s),Math.sqrt(1-s));
};

const nearestRoutePoint = (lat, lon) => {
  if (!routePts.length) return null;
  let min=Infinity, best=null;
  for (const p of routePts) { const d=haversinM([lat,lon],p); if(d<min){min=d;best=p;} }
  return {dist:min, point:best};
};

const map = L.map('map');
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',{maxZoom:19,attribution:'© OpenStreetMap'}).addTo(map);

if (routePts.length >= 2) {
  L.polyline(routePts,{color:'#8b5cf6',weight:3,opacity:0.7}).addTo(map).bindTooltip('Ruta GPS',{sticky:true});
  L.circleMarker(routePts[0],{radius:7,color:'#14532d',weight:2,fillColor:'#22c55e',fillOpacity:.95}).addTo(map).bindPopup('<b>Inicio de ruta</b>');
  L.circleMarker(routePts[routePts.length-1],{radius:7,color:'#7f1d1d',weight:2,fillColor:'#ef4444',fillOpacity:.95}).addTo(map).bindPopup('<b>Fin de ruta</b>');
}

// Build stop data with distance
const stopData = stops.map((s,i) => {
  const nr = nearestRoutePoint(s.latitude, s.longitude);
  return { s, i, nr, distM: nr ? nr.dist : null };
});

// Connector lines layer
const linesLayer = L.layerGroup().addTo(map);
const markersLayer = L.layerGroup().addTo(map);
let activeStop = null;

const borderColor = (d) => d===null?'#fff':d<100?'#16a34a':d<500?'#f59e0b':'#dc2626';
const distClass   = (d) => d===null?'dist-ok':d<100?'dist-ok':d<500?'dist-warn':'dist-far';
const distLabel   = (d) => d===null?'—':d<100?Math.round(d)+'m ✓':d<500?Math.round(d)+'m':(d/1000).toFixed(2)+'km ⚠';

const markerRefs = [];
const lineRefs   = [];

stopData.forEach(({s, i, nr, distM}) => {
  const color = CAT_COLORS[s.final_category] || '#94a3b8';
  const icon  = CAT_ICONS[s.final_category]  || '📌';
  const size  = Math.max(28, Math.min(52, Math.sqrt(s.duration_minutes)*3));
  const fs    = Math.max(11, Math.min(20, size*0.45));
  const bc    = borderColor(distM);

  const divIcon = L.divIcon({
    className:'',
    html: '<div style="background:'+color+';border:3px solid '+bc+';border-radius:50%;'+
          'width:'+size+'px;height:'+size+'px;display:flex;align-items:center;justify-content:center;'+
          'font-size:'+fs+'px;box-shadow:0 2px 8px rgba(0,0,0,0.35);cursor:pointer;">'+icon+'</div>',
    iconSize:[size,size],iconAnchor:[size/2,size/2],popupAnchor:[0,-(size/2+4)],
  });

  const marker = L.marker([s.latitude,s.longitude],{icon:divIcon}).addTo(markersLayer);

  const label  = s.final_category.replace(/_/g,' ');
  const place  = s.nearby_place?'<br><b>Lugar OSM:</b> '+s.nearby_place:'';
  const evCat  = s.event_type_category?'<br><b>Evento:</b> '+s.event_type_category.replace(/_/g,' '):'';
  const distTx = distM===null?'':
    distM<100 ?'<br><b>Dist. a ruta:</b> <span style="color:#16a34a">'+Math.round(distM)+' m ✓</span>':
    distM<500 ?'<br><b>Dist. a ruta:</b> <span style="color:#d97706">'+Math.round(distM)+' m</span>':
               '<br><b>Dist. a ruta:</b> <span style="color:#dc2626">'+(distM/1000).toFixed(2)+' km ⚠</span>';

  marker.bindPopup(
    '<b>'+icon+' Parada #'+(i+1)+'</b><br>'+
    '<b>Categoría:</b> <span style="color:'+color+';font-weight:700">'+label+'</span><br>'+
    '<b>Duración:</b> '+s.duration_minutes.toFixed(1)+' min<br>'+
    '<b>Inicio:</b> '+fmtTs(s.start_timestamp)+'<br>'+
    '<b>Fin:</b> '+fmtTs(s.end_timestamp)+'<br>'+
    '<b>Registros:</b> '+s.num_records+evCat+place+distTx+'<br>'+
    '<b>Confianza:</b> '+s.confidence+'<br>'+
    '<b>Coords:</b> '+s.latitude.toFixed(5)+', '+s.longitude.toFixed(5),
    {maxWidth:280}
  );
  marker.bindTooltip(icon+' #'+(i+1)+' · '+s.duration_minutes.toFixed(0)+' min · '+label+(distM&&distM>=500?' · '+(distM/1000).toFixed(1)+'km ⚠':''),{direction:'top'});

  // Connector line to nearest route point
  let line = null;
  if (nr && nr.point) {
    const lineColor = distM<100?'#16a34a':distM<500?'#f59e0b':'#dc2626';
    line = L.polyline([[s.latitude,s.longitude],nr.point],{
      color:lineColor, weight:1.5, opacity:0.7, dashArray:'5,4'
    }).addTo(linesLayer);
  }

  markerRefs.push({marker, distM, el:null});
  lineRefs.push(line);

  marker.on('click', () => selectStop(i));
});

// Sidebar list
function buildList() {
  const list = document.getElementById('stop-list');
  list.innerHTML = '';
  const showFar = document.getElementById('chk-far').checked;
  stopData.forEach(({s,i,distM}) => {
    if (!showFar && distM !== null && distM >= 500) return;
    const icon = CAT_ICONS[s.final_category]||'📌';
    const color = CAT_COLORS[s.final_category]||'#94a3b8';
    const el = document.createElement('div');
    el.className = 'stop-item'+(i===activeStop?' active':'')+(distM&&distM>=500?' far':'');
    el.dataset.idx = i;
    el.innerHTML =
      '<span style="font-size:18px;flex-shrink:0">'+icon+'</span>'+
      '<div style="flex:1;min-width:0">'+
        '<div class="stop-name" style="color:'+color+'">#'+(i+1)+' '+s.final_category.replace(/_/g,' ')+'</div>'+
        '<div class="stop-meta">'+s.duration_minutes.toFixed(0)+' min</div>'+
      '</div>'+
      '<span class="dist-badge '+distClass(distM)+'">'+distLabel(distM)+'</span>';
    el.onclick = () => selectStop(i);
    list.appendChild(el);
    markerRefs[i].el = el;
  });
}

function selectStop(idx) {
  activeStop = idx;
  const {s, nr, distM} = stopData[idx];
  map.setView([s.latitude,s.longitude],15);
  markerRefs[idx].marker.openPopup();
  document.querySelectorAll('.stop-item').forEach(el => el.classList.toggle('active', Number(el.dataset.idx)===idx));
}

function toggleFar() {
  const showFar = document.getElementById('chk-far').checked;
  stopData.forEach(({distM},i) => {
    const isFar = distM !== null && distM >= 500;
    if (isFar) {
      if (showFar) { markerRefs[i].marker.addTo(markersLayer); if(lineRefs[i]) lineRefs[i].addTo(linesLayer); }
      else         { markersLayer.removeLayer(markerRefs[i].marker); if(lineRefs[i]) linesLayer.removeLayer(lineRefs[i]); }
    }
  });
  buildList();
}

function toggleLines() {
  if (document.getElementById('chk-lines').checked) map.addLayer(linesLayer);
  else map.removeLayer(linesLayer);
}

buildList();

const allPts = stopData.map(d=>[d.s.latitude,d.s.longitude]);
if (allPts.length) map.fitBounds(L.latLngBounds([...allPts,...routePts.slice(0,50)]),{padding:[20,20]});
else if (routePts.length) map.fitBounds(L.latLngBounds(routePts),{padding:[20,20]});
else map.setView([23.6,-102.5],5);
</script></body></html>`

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
              <span class="legend-item"><span class="legend-swatch" style="background:#22c55e"></span>Ruta ML inferida</span>
              <span class="legend-item"><span class="legend-swatch" style="background:#8b5cf6"></span>Ruta real GPS</span>
              <span class="legend-item"><span class="legend-swatch-point" style="background:#06b6d4"></span>Puntos GPS</span>
              <span class="legend-item"><span class="legend-swatch-point" style="background:#8b5cf6"></span>Puntos perdidos</span>
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
                color: '#22c55e',
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
                color: '#22c55e',
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
                color: '#8b5cf6',
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

                const lostIcon = L.divIcon({
                  className: '',
                  html: '<div style="background:#8b5cf6;color:#fff;border:2px solid #4c1d95;border-radius:50%;width:14px;height:14px;display:flex;align-items:center;justify-content:center;font-size:9px;font-weight:700;">⚡</div>',
                  iconSize: [14, 14],
                  iconAnchor: [7, 7],
                });

                const marker = isEstimated
                  ? L.marker([latitude, longitude], { icon: lostIcon }).addTo(gpsPointsLayer)
                  : L.circleMarker([latitude, longitude], {
                      radius: 3,
                      color: '#0f766e',
                      weight: 1,
                      fillColor: '#06b6d4',
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

  const openRoadCovMapWindow = async () => {
    if (!roadCovMatch) { setError('Primero calcula la cobertura por carretera.'); return }
    setError('')

    const roadIndex = {}
    for (const r of roadCovMatch.roads) roadIndex[r.name] = r

    let roadRows = []
    try {
      const res = await fetch(`${API_BASE}/data/carreteras/rows?limit=2000&offset=0`)
      if (!res.ok) throw new Error('No se pudieron cargar las geometrías')
      const d = await res.json()
      roadRows = Array.isArray(d.rows) ? d.rows : []
    } catch (e) { setError(e.message); return }

    const segments = roadRows.flatMap((row, idx) => {
      const latlngs = parseLinestringWkt(row.geometry)
      if (latlngs.length < 2) return []
      const stats = roadIndex[row.name] || null
      return [{ id: idx, name: row.name || '', stats, latlngs }]
    })

    if (segments.length === 0) { setError('Sin geometrías válidas.'); return }

    const mapWindow = window.open('', '_blank', 'width=1500,height=920')
    if (!mapWindow) { setError('El navegador bloqueó el popup. Permite pop-ups para este sitio.'); return }

    const structural = globalCovData?.structural_hotspots ?? []
    const isolated   = globalCovData?.isolated_hotspots   ?? []

    const safeSegs  = JSON.stringify(segments).replaceAll('</', String.raw`<\/`)
    const safeStruc = JSON.stringify(structural).replaceAll('</', String.raw`<\/`)
    const safeIsol  = JSON.stringify(isolated).replaceAll('</', String.raw`<\/`)
    const totalAffected = roadCovMatch.roads_with_issues
    const totalRoads    = roadCovMatch.total_roads_analyzed

    const html = `<!doctype html>
<html lang="es"><head><meta charset="utf-8"/>
<title>Cobertura GPS — carreteras</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
  integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY=" crossorigin=""/>
<style>
*{box-sizing:border-box;margin:0;padding:0;}
html,body{height:100%;font-family:Inter,system-ui,sans-serif;display:flex;flex-direction:column;}
#app{display:flex;flex:1;overflow:hidden;}
#sidebar{width:320px;min-width:220px;display:flex;flex-direction:column;border-right:1px solid #e5e7eb;background:#fff;}
#map{flex:1;}
.hdr{padding:10px 12px;border-bottom:1px solid #e5e7eb;background:#f9fafb;font-size:13px;}
.hdr h1{font-size:13px;font-weight:700;color:#111827;margin-bottom:4px;}
.hdr p{font-size:11px;color:#6b7280;}
.controls{padding:8px 10px;border-bottom:1px solid #e5e7eb;display:flex;flex-direction:column;gap:6px;}
.controls select,.controls input{width:100%;padding:5px 8px;border:1px solid #d1d5db;border-radius:6px;font-size:12px;}
.btn-all{padding:5px 10px;background:#2563eb;color:#fff;border:none;border-radius:6px;font-size:12px;cursor:pointer;font-weight:600;}
.btn-all:hover{background:#1d4ed8;}
#road-list{flex:1;overflow-y:auto;}
.road-item{padding:8px 10px;border-bottom:1px solid #f3f4f6;cursor:pointer;display:flex;align-items:flex-start;gap:8px;}
.road-item:hover{background:#f0f9ff;}
.road-item.active{background:#eff6ff;border-left:3px solid #2563eb;}
.road-name{font-size:12px;color:#111827;line-height:1.3;flex:1;}
.road-meta{font-size:10px;color:#9ca3af;margin-top:2px;}
.sev-dot{width:10px;height:10px;border-radius:50%;flex-shrink:0;margin-top:2px;}
.badge{display:inline-block;border-radius:8px;padding:1px 7px;font-size:10px;font-weight:600;}
</style></head><body>
<div id="app">
  <div id="sidebar">
    <div class="hdr">
      <h1>Carreteras — cobertura GPS</h1>
      <p>${totalAffected} afectadas · ${totalRoads} analizadas</p>
    </div>
    <div class="controls">
      <button class="btn-all" onclick="showAll()">Ver todas las carreteras</button>
      <select id="sev-filter" onchange="applyFilter()">
        <option value="all">Todas las severidades</option>
        <option value="alta">🔴 Alta (≥10 zonas)</option>
        <option value="media">🟠 Media (3–9 zonas)</option>
        <option value="baja">🔵 Baja (1–2 zonas)</option>
        <option value="aislada">⚫ Solo eventos aislados</option>
        <option value="ok">✅ Sin problemas</option>
      </select>
      <input id="search" type="text" placeholder="Buscar carretera…" oninput="applyFilter()"/>
    </div>
    <div id="road-list"></div>
  </div>
  <div id="map"></div>
</div>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
  integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo=" crossorigin=""></script>
<script>
const segments   = ${safeSegs};
const structural = ${safeStruc};
const isolated   = ${safeIsol};

const SEV_COLOR  = { alta:'#dc2626', media:'#d97706', baja:'#2563eb', aislada:'#9ca3af', ok:'#22c55e' };
const SEV_WEIGHT = { alta:4.5, media:3.5, baja:3, aislada:2, ok:1.5 };
const SEV_DOT_COLOR = { alta:'#dc2626', media:'#f97316', baja:'#2563eb', aislada:'#9ca3af', ok:'#22c55e' };

const map = L.map('map');
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',{maxZoom:19,attribution:'© OpenStreetMap'}).addTo(map);

// Build road → polylines index (one road can have multiple segments)
const roadPolylines = {};   // roadName → [polyline, ...]
const allPolylines  = [];
const allBounds     = [];

segments.forEach(seg => {
  const sev = seg.stats ? seg.stats.severity : 'ok';
  const color  = SEV_COLOR[sev]  || '#d1d5db';
  const weight = SEV_WEIGHT[sev] || 1.5;

  const popup = seg.stats
    ? '<b>' + seg.name + '</b><br/>'
      + 'Severidad: <b style="color:' + color + '">' + sev + '</b><br/>'
      + '⛔ Zonas sin cobertura: <b>' + seg.stats.structural_zones + '</b><br/>'
      + '⚡ Eventos aislados: <b>' + seg.stats.isolated_events + '</b><br/>'
      + 'Eventos GPS perdidos: <b>' + seg.stats.total_events.toLocaleString() + '</b><br/>'
      + 'Tipo: ' + (seg.stats.p_type || seg.stats.tipo || '—')
    : '<b>' + seg.name + '</b><br/><span style="color:#16a34a">✅ Sin problemas de cobertura</span>';

  const line = L.polyline(seg.latlngs, { color, weight, opacity: sev === 'ok' ? 0.4 : 0.85 })
    .bindPopup(popup);
  line.addTo(map);
  line._roadName = seg.name;
  line._sev = sev;

  if (!roadPolylines[seg.name]) roadPolylines[seg.name] = [];
  roadPolylines[seg.name].push(line);
  allPolylines.push(line);
  seg.latlngs.forEach(p => allBounds.push(p));
});

// ── Hotspot markers — stored with lat/lon for road filtering ─────────────────
const MATCH_DEG = 0.02; // ~2 km match radius
const layerStruc = L.layerGroup().addTo(map);
const layerIsol  = L.layerGroup().addTo(map);
const strucMarkers = []; // {marker, lat, lon}
const isolMarkers  = [];

structural.forEach(h => {
  const r = Math.min(18, 4 + h.asset_count * 1.2);
  const m = L.circleMarker([h.lat,h.lon],{radius:r,color:'#dc2626',fillColor:'#fca5a5',fillOpacity:0.7,weight:1.5})
    .bindPopup('<b>⛔ Zona sin cobertura</b><br/>Assets: <b>'+h.asset_count+'</b><br/>Clientes: '+h.client_count+'<br/>Eventos: '+h.event_count);
  m.addTo(layerStruc);
  strucMarkers.push({marker:m, lat:h.lat, lon:h.lon});
});
isolated.forEach(h => {
  const m = L.circleMarker([h.lat,h.lon],{radius:5,color:'#d97706',fillColor:'#fde68a',fillOpacity:0.7,weight:1})
    .bindPopup('<b>⚡ Evento aislado</b><br/>Asset: '+(h.asset_ids?.[0]||'—')+'<br/>Eventos: '+h.event_count);
  m.addTo(layerIsol);
  isolMarkers.push({marker:m, lat:h.lat, lon:h.lon});
});
L.control.layers(null,{'⛔ Hotspots sin cobertura':layerStruc,'⚡ Eventos aislados':layerIsol},{collapsed:false}).addTo(map);

if (allBounds.length) map.fitBounds(L.latLngBounds(allBounds),{padding:[20,20]});
else map.setView([23.6,-102.5],5);

// Check if a hotspot is near any point of the selected road (within MATCH_DEG)
function hotspotNearRoad(hLat, hLon, roadPoints) {
  return roadPoints.some(p => Math.abs(p.lat - hLat) < MATCH_DEG && Math.abs(p.lng - hLon) < MATCH_DEG);
}

// ── Sidebar road list ───────────────────────────────────────────────────────
const roadMap = {};
segments.forEach(seg => {
  if (!roadMap[seg.name]) roadMap[seg.name] = { name: seg.name, stats: seg.stats, sev: seg.stats ? seg.stats.severity : 'ok' };
});
const SEV_ORDER = { alta:0, media:1, baja:2, aislada:3, ok:4 };
const allRoads = Object.values(roadMap).sort((a,b) => (SEV_ORDER[a.sev]??5) - (SEV_ORDER[b.sev]??5) || a.name.localeCompare(b.name));

let activeRoad = null;

function renderList(roads) {
  const list = document.getElementById('road-list');
  list.innerHTML = '';
  roads.forEach(road => {
    const el = document.createElement('div');
    el.className = 'road-item' + (road.name === activeRoad ? ' active' : '');
    el.dataset.name = road.name;
    const dotColor = SEV_DOT_COLOR[road.sev] || '#d1d5db';
    const meta = road.stats
      ? road.stats.structural_zones + ' zonas · ' + road.stats.isolated_events + ' aislados · ' + road.stats.total_events.toLocaleString() + ' eventos'
      : 'Sin problemas de cobertura';
    el.innerHTML =
      '<div class="sev-dot" style="background:' + dotColor + '"></div>'
      + '<div><div class="road-name">' + road.name + '</div><div class="road-meta">' + meta + '</div></div>';
    el.onclick = () => selectRoad(road.name);
    list.appendChild(el);
  });
}

function applyFilter() {
  const sev   = document.getElementById('sev-filter').value;
  const query = document.getElementById('search').value.toLowerCase();
  const filtered = allRoads.filter(r =>
    (sev === 'all' || r.sev === sev) &&
    (!query || r.name.toLowerCase().includes(query))
  );
  renderList(filtered);
}

function selectRoad(name) {
  activeRoad = name;

  // Dim all roads, highlight selected
  allPolylines.forEach(line => {
    const isMatch = line._roadName === name;
    line.setStyle({ opacity: isMatch ? 1 : 0.07, weight: isMatch ? (SEV_WEIGHT[line._sev]||2) + 1 : 1 });
    if (isMatch) line.bringToFront();
  });

  // Collect all points of the selected road for distance checks
  const polys = roadPolylines[name] || [];
  const roadPoints = polys.flatMap(p => p.getLatLngs());

  // Filter hotspot markers: show only those near this road
  layerStruc.clearLayers();
  layerIsol.clearLayers();
  let hasHotspots = false;
  strucMarkers.forEach(({marker, lat, lon}) => {
    if (hotspotNearRoad(lat, lon, roadPoints)) {
      marker.addTo(layerStruc);
      hasHotspots = true;
    }
  });
  isolMarkers.forEach(({marker, lat, lon}) => {
    if (hotspotNearRoad(lat, lon, roadPoints)) {
      marker.addTo(layerIsol);
      hasHotspots = true;
    }
  });

  // Zoom to road bounds
  if (roadPoints.length) {
    map.fitBounds(L.latLngBounds(roadPoints), { padding: [40, 40], maxZoom: 14 });
  }

  // Sidebar highlight
  document.querySelectorAll('.road-item').forEach(el => {
    el.classList.toggle('active', el.dataset.name === name);
  });
}

function showAll() {
  activeRoad = null;

  // Restore all road styles
  allPolylines.forEach(line => {
    const sev = line._sev;
    line.setStyle({ opacity: sev === 'ok' ? 0.4 : 0.85, weight: SEV_WEIGHT[sev] || 1.5 });
  });

  // Restore all hotspot markers
  layerStruc.clearLayers();
  layerIsol.clearLayers();
  strucMarkers.forEach(({marker}) => marker.addTo(layerStruc));
  isolMarkers.forEach(({marker}) => marker.addTo(layerIsol));

  if (allBounds.length) map.fitBounds(L.latLngBounds(allBounds), { padding: [20, 20] });
  document.querySelectorAll('.road-item').forEach(el => el.classList.remove('active'));
}

renderList(allRoads);
</script></body></html>`

    mapWindow.document.write(html)
    mapWindow.document.close()
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
          aria-selected={activeMenu === 'resumen'}
          className={`menu-tab ${activeMenu === 'resumen' ? 'active' : ''}`}
          onClick={() => setActiveMenu('resumen')}
        >
          Resumen de Rutas
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

      {activeMenu === 'resumen' && (
        <section className="card">
          <h2>Resumen Ejecutivo de Rutas</h2>
        <p className="subtitle">
          Análisis agregado de rutas detectadas en la flota. Fuente: dataset histórico de entrenamiento ML.
        </p>

        {/* Tab selector */}
        <div className="toolbar" style={{ marginBottom: 0, borderBottom: '2px solid #e5e7eb', paddingBottom: 0 }}>
          {[
            { key: 'fleet', label: 'Flota completa' },
            { key: 'client', label: 'Por clientId' },
          ].map(({ key, label }) => (
            <button
              key={key}
              onClick={() => setSummaryTab(key)}
              style={{
                borderRadius: '6px 6px 0 0',
                fontWeight: summaryTab === key ? 700 : 400,
                color: summaryTab === key ? '#2563eb' : '#6b7280',
                background: 'none',
                border: 'none',
                borderBottom: summaryTab === key ? '3px solid #2563eb' : '3px solid transparent',
                padding: '8px 18px',
                cursor: 'pointer',
                fontSize: 14,
              }}
            >
              {label}
            </button>
          ))}
        </div>

        {/* ── TAB: FLOTA ── */}
        {summaryTab === 'fleet' && (
          <div style={{ marginTop: 16 }}>
            {!fleetSummary && !fleetSummaryLoading && (
              <div className="toolbar">
                <button onClick={() => { loadFleetSummary() }}>
                  Cargar resumen de flota
                </button>
              </div>
            )}
            {fleetSummaryLoading && <p className="subtitle">Cargando datos de flota...</p>}
            {fleetSummary?.error && <p className="subtitle warning">{fleetSummary.error}</p>}

            {fleetSummary && !fleetSummary.error && (
              <>
                {/* Stats cards */}
                <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', marginBottom: 20 }}>
                  {[
                    { label: 'Viajes totales', value: fleetSummary.stats.total_trips?.toLocaleString() },
                    { label: 'Rutas únicas', value: fleetSummary.stats.unique_routes?.toLocaleString() },
                    { label: 'Unidades (assets)', value: fleetSummary.stats.unique_assets?.toLocaleString() },
                    { label: 'Clientes', value: fleetSummary.stats.unique_clients?.toLocaleString() },
                  ].map(({ label, value }) => (
                    <div key={label} style={{
                      background: '#f0f9ff',
                      border: '1px solid #bae6fd',
                      borderRadius: 8,
                      padding: '12px 20px',
                      minWidth: 130,
                      textAlign: 'center',
                    }}>
                      <div style={{ fontSize: 26, fontWeight: 700, color: '#0369a1' }}>{value ?? '—'}</div>
                      <div style={{ fontSize: 11, color: '#6b7280', marginTop: 2 }}>{label}</div>
                    </div>
                  ))}
                </div>

                {/* Gráfico de barras horizontal - Top rutas */}
                {fleetSummary.top_routes.length > 0 && (() => {
                  const maxCount = fleetSummary.top_routes[0].trip_count
                  const BAR_H = 22
                  const GAP = 6
                  const LABEL_W = 260
                  const BAR_MAX_W = 300
                  const COUNT_W = 50
                  const W = LABEL_W + BAR_MAX_W + COUNT_W + 24
                  const H = fleetSummary.top_routes.length * (BAR_H + GAP) + 30
                  return (
                    <>
                      <h3 style={{ marginBottom: 8 }}>Top rutas por frecuencia (flota completa)</h3>
                      <div style={{ overflowX: 'auto' }}>
                        <svg viewBox={`0 0 ${W} ${H}`} style={{ width: '100%', maxWidth: W, display: 'block', fontFamily: 'system-ui, sans-serif', fontSize: 11 }}>
                          {fleetSummary.top_routes.map((r, i) => {
                            const barW = maxCount > 0 ? (r.trip_count / maxCount) * BAR_MAX_W : 0
                            const y = i * (BAR_H + GAP) + 20
                            const shortLabel = r.route_label.length > 38 ? r.route_label.slice(0, 37) + '…' : r.route_label
                            return (
                              <g key={r.route_label}>
                                <text x={LABEL_W - 6} y={y + BAR_H / 2 + 4} textAnchor="end" fill="#374151" fontSize={11}>{shortLabel}</text>
                                <rect x={LABEL_W} y={y} width={Math.max(barW, 2)} height={BAR_H} rx={3} fill={i === 0 ? '#2563eb' : i < 3 ? '#3b82f6' : '#93c5fd'} />
                                <text x={LABEL_W + barW + 6} y={y + BAR_H / 2 + 4} fill="#374151" fontSize={11}>{r.trip_count}</text>
                              </g>
                            )
                          })}
                          <line x1={LABEL_W} y1={10} x2={LABEL_W} y2={H} stroke="#d1d5db" strokeWidth={1} />
                        </svg>
                      </div>
                    </>
                  )
                })()}

                {/* Timeline mensual */}
                {fleetSummary.monthly_counts.length > 1 && (() => {
                  const pts = fleetSummary.monthly_counts
                  const maxV = Math.max(...pts.map(p => p.trip_count))
                  const W = 680, H = 120
                  const PL = 40, PR = 16, PT = 10, PB = 30
                  const iW = W - PL - PR
                  const iH = H - PT - PB
                  const xOf = i => PL + (i / (pts.length - 1)) * iW
                  const yOf = v => PT + iH - (v / maxV) * iH
                  const polyline = pts.map((p, i) => `${xOf(i).toFixed(1)},${yOf(p.trip_count).toFixed(1)}`).join(' ')
                  const labelStep = Math.ceil(pts.length / 8)
                  return (
                    <>
                      <h3 style={{ marginBottom: 4, marginTop: 20 }}>Viajes por mes (flota completa)</h3>
                      <div style={{ overflowX: 'auto' }}>
                        <svg viewBox={`0 0 ${W} ${H}`} style={{ width: '100%', maxWidth: W, display: 'block', fontFamily: 'monospace', fontSize: 10 }}>
                          <line x1={PL} y1={PT} x2={PL} y2={PT + iH} stroke="#555" strokeWidth={1} />
                          <line x1={PL} y1={PT + iH} x2={W - PR} y2={PT + iH} stroke="#555" strokeWidth={1} />
                          {[0, Math.round(maxV / 2), maxV].map(v => (
                            <g key={v}>
                              <line x1={PL - 4} y1={yOf(v)} x2={PL} y2={yOf(v)} stroke="#888" strokeWidth={1} />
                              <text x={PL - 6} y={yOf(v) + 3} textAnchor="end" fill="#888">{v}</text>
                            </g>
                          ))}
                          <polyline points={polyline} fill="none" stroke="#2563eb" strokeWidth={2} />
                          {pts.map((p, i) => (
                            <g key={p.month}>
                              <circle cx={xOf(i)} cy={yOf(p.trip_count)} r={3} fill="#2563eb" />
                              {i % labelStep === 0 && (
                                <text x={xOf(i)} y={PT + iH + 14} textAnchor="middle" fill="#6b7280">{p.month.slice(2)}</text>
                              )}
                            </g>
                          ))}
                        </svg>
                      </div>
                    </>
                  )
                })()}


                {/* Tabla de métricas */}
                <h3 style={{ marginTop: 20 }}>Detalle de rutas</h3>
                <div className="table-wrap">
                  <table>
                    <thead>
                      <tr>
                        <th>#</th>
                        <th>Ruta</th>
                        <th>Viajes</th>
                        <th>Dist. prom. (km)</th>
                        <th>Duración prom. (h)</th>
                        <th>Unidades</th>
                        <th>Clientes</th>
                      </tr>
                    </thead>
                    <tbody>
                      {fleetSummary.top_routes.map((r, i) => (
                        <tr key={r.route_label}>
                          <td>{i + 1}</td>
                          <td>{r.route_label}</td>
                          <td><strong>{r.trip_count}</strong></td>
                          <td>{r.avg_distance_km ?? '—'}</td>
                          <td>{r.avg_duration_hours ?? '—'}</td>
                          <td>{r.unique_assets ?? '—'}</td>
                          <td>{r.unique_clients ?? '—'}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </>
            )}
          </div>
        )}

        {/* ── TAB: CLIENTE ── */}
        {summaryTab === 'client' && (
          <div style={{ marginTop: 16 }}>
            <div className="toolbar">
              <label className="inline-label">
                <span>clientId</span>
                <input
                  type="number"
                  value={summaryClientId}
                  onChange={(e) => setSummaryClientId(e.target.value)}
                  placeholder="Ej. 1663"
                />
              </label>
              <button
                onClick={() => {
                  setClientSummary(null)
                  setClientAssets([])
                  setSelectedAssets(new Set())
                  setAssetSearch('')
                  loadClientAssets(summaryClientId)
                  loadClientSummary(summaryClientId, new Set())
                }}
                disabled={clientSummaryLoading || !summaryClientId.trim()}
              >
                {clientSummaryLoading ? 'Cargando...' : 'Cargar'}
              </button>
            </div>

            {/* Selector de assets */}
            {clientAssets.length > 0 && (
              <div style={{ marginBottom: 16 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 8, flexWrap: 'wrap' }}>
                  <p className="subtitle" style={{ margin: 0 }}>
                    <strong>Filtrar por unidad (asset):</strong>{' '}
                    {selectedAssets.size === 0
                      ? `Top ${clientAssets.length} de ${clientAssetsTotal} totales`
                      : `${selectedAssets.size} seleccionado${selectedAssets.size > 1 ? 's' : ''} · ${clientAssetsTotal} totales`}
                  </p>
                  <input
                    type="text"
                    placeholder="Buscar asset ID…"
                    value={assetSearch}
                    onChange={(e) => setAssetSearch(e.target.value)}
                    style={{
                      padding: '4px 10px',
                      borderRadius: 6,
                      border: '1px solid #d1d5db',
                      fontSize: 12,
                      width: 160,
                      outline: 'none',
                    }}
                  />
                  {assetSearch && (
                    <button
                      onClick={() => setAssetSearch('')}
                      style={{ fontSize: 11, background: 'none', border: 'none', cursor: 'pointer', color: '#6b7280' }}
                    >
                      ✕ Limpiar
                    </button>
                  )}
                </div>
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                  <button
                    onClick={() => {
                      setSelectedAssets(new Set())
                      loadClientSummary(summaryClientId, new Set())
                    }}
                    style={{
                      padding: '4px 12px',
                      borderRadius: 20,
                      border: '1.5px solid #d1d5db',
                      background: selectedAssets.size === 0 ? '#1d4ed8' : '#f3f4f6',
                      color: selectedAssets.size === 0 ? '#fff' : '#374151',
                      fontWeight: selectedAssets.size === 0 ? 700 : 400,
                      cursor: 'pointer',
                      fontSize: 12,
                    }}
                  >
                    Todos ({clientAssets.length})
                  </button>
                  {clientAssets
                    .filter((a) => !assetSearch || String(a.asset_id).includes(assetSearch.trim()))
                    .map((a) => {
                      const active = selectedAssets.has(a.asset_id)
                      return (
                        <button
                          key={a.asset_id}
                          onClick={() => {
                            const next = new Set(selectedAssets)
                            if (next.has(a.asset_id)) next.delete(a.asset_id)
                            else next.add(a.asset_id)
                            setSelectedAssets(next)
                            loadClientSummary(summaryClientId, next)
                          }}
                          title={`${a.trip_count} registros`}
                          style={{
                            padding: '4px 12px',
                            borderRadius: 20,
                            border: `1.5px solid ${active ? '#2563eb' : '#d1d5db'}`,
                            background: active ? '#dbeafe' : '#f9fafb',
                            color: active ? '#1d4ed8' : '#374151',
                            fontWeight: active ? 700 : 400,
                            cursor: 'pointer',
                            fontSize: 12,
                          }}
                        >
                          {a.asset_id}
                          <span style={{ marginLeft: 4, fontSize: 10, color: active ? '#1d4ed8' : '#9ca3af' }}>
                            ({a.trip_count.toLocaleString()})
                          </span>
                        </button>
                      )
                    })}
                  {assetSearch && clientAssets.filter((a) => String(a.asset_id).includes(assetSearch.trim())).length === 0 && (
                    <span style={{ fontSize: 12, color: '#9ca3af', alignSelf: 'center' }}>Sin coincidencias</span>
                  )}
                </div>
              </div>
            )}

            {clientSummary?.error && <p className="subtitle warning">{clientSummary.error}</p>}

            {clientSummary && !clientSummary.error && (
              <>
                {/* Stats cards */}
                <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', marginBottom: 20 }}>
                  {[
                    { label: 'Viajes totales', value: clientSummary.stats.total_trips?.toLocaleString() },
                    { label: 'Rutas únicas', value: clientSummary.stats.unique_routes?.toLocaleString() },
                    { label: 'Unidades activas', value: clientSummary.stats.unique_assets?.toLocaleString() },
                  ].map(({ label, value }) => (
                    <div key={label} style={{
                      background: '#f0fdf4',
                      border: '1px solid #bbf7d0',
                      borderRadius: 8,
                      padding: '12px 20px',
                      minWidth: 130,
                      textAlign: 'center',
                    }}>
                      <div style={{ fontSize: 26, fontWeight: 700, color: '#15803d' }}>{value ?? '—'}</div>
                      <div style={{ fontSize: 11, color: '#6b7280', marginTop: 2 }}>{label}</div>
                    </div>
                  ))}
                </div>

                {/* Gráfico de barras horizontal */}
                {clientSummary.top_routes.length > 0 && (() => {
                  const maxCount = clientSummary.top_routes[0].trip_count
                  const BAR_H = 22, GAP = 6, LABEL_W = 260, BAR_MAX_W = 300, COUNT_W = 50
                  const W = LABEL_W + BAR_MAX_W + COUNT_W + 24
                  const H = clientSummary.top_routes.length * (BAR_H + GAP) + 30
                  return (
                    <>
                      <h3 style={{ marginBottom: 8 }}>
                        Top rutas — clientId {clientSummary.client_id}
                        {selectedAssets.size > 0 && (
                          <span style={{ fontWeight: 400, fontSize: 13, color: '#6b7280', marginLeft: 8 }}>
                            · assets: {[...selectedAssets].join(', ')}
                          </span>
                        )}
                      </h3>
                      <div style={{ overflowX: 'auto' }}>
                        <svg viewBox={`0 0 ${W} ${H}`} style={{ width: '100%', maxWidth: W, display: 'block', fontFamily: 'system-ui, sans-serif', fontSize: 11 }}>
                          {clientSummary.top_routes.map((r, i) => {
                            const barW = maxCount > 0 ? (r.trip_count / maxCount) * BAR_MAX_W : 0
                            const y = i * (BAR_H + GAP) + 20
                            const shortLabel = r.route_label.length > 38 ? r.route_label.slice(0, 37) + '…' : r.route_label
                            return (
                              <g key={r.route_label}>
                                <text x={LABEL_W - 6} y={y + BAR_H / 2 + 4} textAnchor="end" fill="#374151" fontSize={11}>{shortLabel}</text>
                                <rect x={LABEL_W} y={y} width={Math.max(barW, 2)} height={BAR_H} rx={3} fill={i === 0 ? '#16a34a' : i < 3 ? '#22c55e' : '#86efac'} />
                                <text x={LABEL_W + barW + 6} y={y + BAR_H / 2 + 4} fill="#374151" fontSize={11}>{r.trip_count}</text>
                              </g>
                            )
                          })}
                          <line x1={LABEL_W} y1={10} x2={LABEL_W} y2={H} stroke="#d1d5db" strokeWidth={1} />
                        </svg>
                      </div>
                    </>
                  )
                })()}

                {/* Timeline mensual */}
                {clientSummary.monthly_counts.length > 1 && (() => {
                  const pts = clientSummary.monthly_counts
                  const maxV = Math.max(...pts.map(p => p.trip_count))
                  const W = 680, H = 120
                  const PL = 40, PR = 16, PT = 10, PB = 30
                  const iW = W - PL - PR, iH = H - PT - PB
                  const xOf = i => PL + (i / (pts.length - 1)) * iW
                  const yOf = v => PT + iH - (v / maxV) * iH
                  const polyline = pts.map((p, i) => `${xOf(i).toFixed(1)},${yOf(p.trip_count).toFixed(1)}`).join(' ')
                  const labelStep = Math.ceil(pts.length / 8)
                  return (
                    <>
                      <h3 style={{ marginBottom: 4, marginTop: 20 }}>
                        Viajes por mes
                        {selectedAssets.size > 0 && (
                          <span style={{ fontWeight: 400, fontSize: 13, color: '#6b7280', marginLeft: 8 }}>
                            · {selectedAssets.size} asset{selectedAssets.size > 1 ? 's' : ''}
                          </span>
                        )}
                      </h3>
                      <div style={{ overflowX: 'auto' }}>
                        <svg viewBox={`0 0 ${W} ${H}`} style={{ width: '100%', maxWidth: W, display: 'block', fontFamily: 'monospace', fontSize: 10 }}>
                          <line x1={PL} y1={PT} x2={PL} y2={PT + iH} stroke="#555" strokeWidth={1} />
                          <line x1={PL} y1={PT + iH} x2={W - PR} y2={PT + iH} stroke="#555" strokeWidth={1} />
                          {[0, Math.round(maxV / 2), maxV].map(v => (
                            <g key={v}>
                              <line x1={PL - 4} y1={yOf(v)} x2={PL} y2={yOf(v)} stroke="#888" strokeWidth={1} />
                              <text x={PL - 6} y={yOf(v) + 3} textAnchor="end" fill="#888">{v}</text>
                            </g>
                          ))}
                          <polyline points={polyline} fill="none" stroke="#16a34a" strokeWidth={2} />
                          {pts.map((p, i) => (
                            <g key={p.month}>
                              <circle cx={xOf(i)} cy={yOf(p.trip_count)} r={3} fill="#16a34a" />
                              {i % labelStep === 0 && (
                                <text x={xOf(i)} y={PT + iH + 14} textAnchor="middle" fill="#6b7280">{p.month.slice(2)}</text>
                              )}
                            </g>
                          ))}
                        </svg>
                      </div>
                    </>
                  )
                })()}


                {/* Tabla */}
                <h3 style={{ marginTop: 20 }}>Detalle de rutas</h3>
                <div className="table-wrap">
                  <table>
                    <thead>
                      <tr>
                        <th>#</th>
                        <th>Ruta</th>
                        <th>Viajes</th>
                        <th>Dist. prom. (km)</th>
                        <th>Duración prom. (h)</th>
                        <th>Unidades</th>
                      </tr>
                    </thead>
                    <tbody>
                      {clientSummary.top_routes.map((r, i) => (
                        <tr key={r.route_label}>
                          <td>{i + 1}</td>
                          <td>{r.route_label}</td>
                          <td><strong>{r.trip_count}</strong></td>
                          <td>{r.avg_distance_km ?? '—'}</td>
                          <td>{r.avg_duration_hours ?? '—'}</td>
                          <td>{r.unique_assets ?? '—'}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </>
            )}
          </div>
        )}
        </section>
      )}

      {activeMenu === 'resumen' && (
        <section className="card">
          <h2>Análisis histórico de paradas — flota completa</h2>
          <p className="subtitle">
            Detecta paradas en <strong>todos los clientes y assets</strong> del dataset usando DuckDB,
            clasifica con OpenStreetMap (gasolinera, hotel, restaurante…) y agrega por categoría y ubicación.
            Tiempo estimado: ~30-40 minutos. Se ejecuta una vez y queda cacheado.
          </p>

          <div className="toolbar" style={{ marginBottom: 12 }}>
            <button onClick={() => startBatchStopsJob(false)} disabled={batchStopsJob?.phase === 'running'}>
              {batchStopsJob?.phase === 'running' ? 'Calculando…' : 'Calcular análisis completo'}
            </button>
            {batchStops && (
              <button onClick={() => startBatchStopsJob(true)} disabled={batchStopsJob?.phase === 'running'}
                style={{ fontSize: 13 }}>
                ↺ Recalcular todo
              </button>
            )}
            {batchStops && (
              <button onClick={() => {
                const locs = batchStops.top_hotspots?.slice(0, 100) || []
                if (!locs.length) return
                const mw = window.open('', '_blank', 'width=1280,height=860')
                if (!mw) { setError('El navegador bloqueó el popup.'); return }
                const CAT_COLORS_MAP = {gasolinera:'#f59e0b',hotel:'#8b5cf6',restaurante:'#22c55e',
                  tienda_conveniencia:'#3b82f6',farmacia:'#06b6d4',hospital:'#0891b2',
                  taller_mecanico:'#78716c',terminal_carga:'#6366f1',motor_apagado:'#64748b',
                  ralenti:'#94a3b8',parada_no_autorizada:'#ef4444',estacionamiento_prolongado:'#f97316',
                  pernocta_fuera_de_base:'#dc2626',parada_larga:'#b45309',parada_en_geocerca:'#0ea5e9',
                  punto_de_control:'#10b981',entrega:'#10b981',parada_en_carretera:'#94a3b8'}
                const safe = JSON.stringify(locs).replaceAll('</', String.raw`<\/`)
                const safeC = JSON.stringify(CAT_COLORS_MAP).replaceAll('</', String.raw`<\/`)
                mw.document.write(`<!doctype html><html lang="es"><head><meta charset="utf-8"/>
<title>Top paradas históricas</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
  integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY=" crossorigin=""/>
<style>html,body{margin:0;height:100%;font-family:Inter,system-ui,sans-serif;}
#map{width:100%;height:calc(100% - 48px);}
.hdr{height:48px;display:flex;align-items:center;padding:0 16px;border-bottom:1px solid #e5e7eb;background:#f9fafb;font-size:13px;font-weight:700;gap:12px;}
</style></head><body>
<div class="hdr">Top ${Math.min(locs.length, 100)} paradas históricas — flota completa
  <span style="font-size:11px;color:#6b7280;font-weight:400">Tamaño = assets únicos · Color = categoría</span>
</div>
<div id="map"></div>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
  integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo=" crossorigin=""></script>
<script>
const locs=\${safe};const CAT=\${safeC};
const map=L.map('map');
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',{maxZoom:19,attribution:'© OpenStreetMap'}).addTo(map);
const bounds=[];
locs.forEach((l,i)=>{
  const color=CAT[l.category]||'#94a3b8';
  const r=Math.max(8,Math.min(40,Math.sqrt(l.unique_assets)*1.8));
  const pos=[l.lat,l.lon];bounds.push(pos);
  L.circleMarker(pos,{radius:r,color:'#fff',weight:2,fillColor:color,fillOpacity:0.85}).addTo(map)
   .bindPopup('<b>'+l.icon+' #'+(i+1)+' '+(l.place||l.category.replace(/_/g,' '))+'</b><br/>'+
              'Categoría: <b>'+l.category.replace(/_/g,' ')+'</b><br/>'+
              'Paradas detectadas: <b>'+l.stop_count.toLocaleString()+'</b><br/>'+
              'Assets únicos: <b>'+l.unique_assets+'</b><br/>'+
              'Clientes: <b>'+l.unique_clients+'</b><br/>'+
              'Duración prom.: <b>'+l.avg_dur_min.toFixed(0)+' min</b>')
   .bindTooltip(l.icon+' '+(l.place||l.category.replace(/_/g,' '))+' · '+l.stop_count.toLocaleString()+' paradas',{direction:'top'});
});
if(bounds.length)map.fitBounds(L.latLngBounds(bounds),{padding:[30,30]});
</script></body></html>`)
                mw.document.close()
              }} style={{ fontSize: 13 }}>
                Ver mapa top 100 ↗
              </button>
            )}
          </div>

          {/* Progreso */}
          {batchStopsJob?.phase === 'running' && (
            <div style={{ marginBottom: 14 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 12, color: '#6b7280', marginBottom: 4 }}>
                <span>{batchStopsJob.message || 'Procesando…'}</span>
                <span>{Math.round((batchStopsJob.progress ?? 0) * 100)}%</span>
              </div>
              <div style={{ background: '#e5e7eb', borderRadius: 6, height: 8 }}>
                <div style={{
                  background: '#8b5cf6', borderRadius: 6, height: 8,
                  width: `${Math.round((batchStopsJob.progress ?? 0) * 100)}%`,
                  transition: 'width 0.6s ease',
                }} />
              </div>
              <p style={{ fontSize: 11, color: '#9ca3af', marginTop: 4 }}>
                Fase 1: detección de paradas en DuckDB · Fase 2: clasificación OSM.
                Puedes seguir usando la app mientras corre.
              </p>
            </div>
          )}

          {batchStopsJob?.phase === 'error' && (
            <p style={{ color: '#dc2626', fontSize: 13, marginBottom: 10 }}>{batchStopsJob.message}</p>
          )}

          {/* Resultados */}
          {batchStops && (
            <>
              <p style={{ fontSize: 11, color: '#9ca3af', marginBottom: 14 }}>
                Calculado el {new Date(batchStops.computed_at * 1000).toLocaleString('es-MX')} ·
                {' '}{batchStops.total_clients?.toLocaleString()} clientes ·
                {' '}{batchStops.total_stop_cells?.toLocaleString()} zonas de parada ·
                {' '}{batchStops.total_stops?.toLocaleString()} paradas detectadas
              </p>

              {/* Barras de categoría */}
              <h4 style={{ marginBottom: 10 }}>Distribución por tipo de parada (más → menos frecuente)</h4>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 7, marginBottom: 24 }}>
                {batchStops.categories?.map((c, i) => (
                  <div key={c.category} style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                    <span style={{ width: 26, textAlign: 'center', fontSize: 18, flexShrink: 0 }}>{c.icon}</span>
                    <span style={{ width: 220, fontSize: 13, color: '#374151', flexShrink: 0 }}>
                      {c.category.replace(/_/g, ' ')}
                    </span>
                    <div style={{ flex: 1, background: '#f1f5f9', borderRadius: 4, height: 20, overflow: 'hidden' }}>
                      <div style={{
                        height: '100%', borderRadius: 4,
                        background: i === 0 ? '#8b5cf6' : i === batchStops.categories.length - 1 ? '#d1d5db' : '#6366f1',
                        width: `${Math.max(c.pct, 0.5)}%`,
                        transition: 'width 0.5s ease',
                      }} />
                    </div>
                    <span style={{ width: 55, textAlign: 'right', fontSize: 12, color: '#6b7280', flexShrink: 0 }}>{c.pct}%</span>
                    <span style={{ width: 110, textAlign: 'right', fontSize: 12, color: '#374151', flexShrink: 0 }}>
                      {c.stop_count?.toLocaleString()} paradas
                    </span>
                  </div>
                ))}
              </div>

              {/* Top 30 tabla */}
              <h4 style={{ marginBottom: 8 }}>Top 30 ubicaciones con más paradas</h4>
              <div className="table-wrap">
                <table style={{ fontSize: 13 }}>
                  <thead>
                    <tr>
                      <th>#</th><th>Lugar / Categoría</th><th>Latitud</th><th>Longitud</th>
                      <th>Paradas</th><th>Assets</th><th>Clientes</th><th>Dur. prom.</th>
                    </tr>
                  </thead>
                  <tbody>
                    {batchStops.top_hotspots?.slice(0, 30).map((h, i) => (
                      <tr key={i}>
                        <td>{i + 1}</td>
                        <td>
                          <span style={{ fontSize: 15, marginRight: 6 }}>{h.icon}</span>
                          <span style={{ color: '#374151' }}>{h.place || h.category.replace(/_/g, ' ')}</span>
                          {h.place && <span style={{ color: '#9ca3af', fontSize: 11, marginLeft: 4 }}>({h.category.replace(/_/g, ' ')})</span>}
                        </td>
                        <td>{h.lat?.toFixed(4)}</td>
                        <td>{h.lon?.toFixed(4)}</td>
                        <td><strong>{h.stop_count?.toLocaleString()}</strong></td>
                        <td>{h.unique_assets?.toLocaleString()}</td>
                        <td>{h.unique_clients?.toLocaleString()}</td>
                        <td>{h.avg_dur_min?.toFixed(0)} min</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </>
          )}

          {!batchStops && batchStopsJob?.phase !== 'running' && (
            <p className="subtitle">
              Presiona "Calcular análisis completo" para procesar todos los clientes, detectar paradas
              con DuckDB y clasificarlas con OpenStreetMap (gasolinera, hotel, restaurante, etc.).
            </p>
          )}
        </section>
      )}

      {activeMenu === 'dashboard' && (
        <>
      <section className="card">
        <h2>1) Estado backend / snapshot</h2>
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
          <button onClick={() => { loadSnapshotStatus(); loadRows(0, pageSize, assetIdFilter, clientIdFilter, eventTypeFilter, eventTypeFilter2) }} disabled={rowsLoading}>
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
        <h2>3) Detección de rutas por assetId/clientId (movimiento + 519/520)</h2>
        <p className="subtitle source-chip">{sourceLabel}</p>
        <p className="subtitle">
          El inicio/fin de cada recorrido se determina por movimiento GPS real (paradas de
          ≥5 min sin desplazamiento marcan el corte); un evento <strong>519</strong> (inicio) o{' '}
          <strong>520</strong> (fin) cercano se usa para afinar el límite exacto cuando el
          dispositivo lo reporta. Esto evita que assets cuyo dispositivo nunca emite 520
          (frecuente en el dataset) colapsen en un solo recorrido abierto de semanas.
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
          <label className="inline-label">
            <span>Desde</span>
            <input
              type="date"
              value={routeDateFrom}
              onChange={(event) => setRouteDateFrom(event.target.value)}
            />
          </label>
          <label className="inline-label">
            <span>Hasta</span>
            <input
              type="date"
              value={routeDateTo}
              onChange={(event) => setRouteDateTo(event.target.value)}
            />
          </label>
          <button onClick={detectRoutes} disabled={routeLoading}>
            {routeLoading ? 'Analizando...' : 'Detectar rutas'}
          </button>
          {routeDetection?.complete_routes?.length > 0 && (
            <label className="inline-label">
              <span>Ver ruta</span>
              <select
                value={selectedMapRouteIndex ?? ''}
                onChange={(event) => setSelectedMapRouteIndex(event.target.value === '' ? null : Number(event.target.value))}
              >
                <option value="">Todas ({routeDetection.complete_routes.length})</option>
                {routeDetection.complete_routes.map((route, i) => (
                  <option key={i} value={i}>
                    #{i + 1} · {formatTimestamp(route.start_timestamp)?.slice(0, 10) ?? '—'} · {Number(route.distance_km).toFixed(1)} km
                  </option>
                ))}
              </select>
            </label>
          )}
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
                    <th title="Cómo se determinó el límite del recorrido: evento 519/520 confirmado, parada real detectada por GPS, o borde de los datos disponibles">Confirmación inicio/fin</th>
                    <th>Duración (h)</th>
                    <th>Distancia (km)</th>
                    <th>Total eventos</th>
                    <th>Tipos únicos</th>
                    <th>Puntos GPS</th>
                    <th title="Eventos sin coordenadas GPS (sin conectividad)">Sin GPS ⚡</th>
                    <th>Match carreteras</th>
                    <th>Carreteras detectadas</th>
                    <th>Ruta ML</th>
                    <th>Confianza ML</th>
                    <th>Detalle</th>
                  </tr>
                </thead>
                <tbody>
                  {routeDetection.complete_routes.map((route, routeIndex) => {
                    const mlPrediction = mlPredictionsByRoute[getRouteKey(route)]
                    const routeRoadLabels = getRouteRoadLabels(route)
                    const mlConfidenceLabel = Number.isFinite(Number(mlPrediction?.confidence))
                      ? `${(Number(mlPrediction.confidence) * 100).toFixed(1)}%`
                      : ''
                    const isExpanded = expandedRouteIndex === routeIndex
                    return (
                    <tr
                      key={`route-${route.start_index}-${route.end_index}-${route.start_timestamp || 'na'}`}
                      onClick={() => setExpandedRouteIndex(isExpanded ? null : routeIndex)}
                      style={{ cursor: 'pointer', background: isExpanded ? '#eff6ff' : undefined }}
                      title="Clic para ver puntos de esta ruta"
                    >
                      <td>{route.is_primary ? 'SI' : ''}</td>
                      <td>{route.start_timestamp || ''}</td>
                      <td>{route.end_timestamp || ''}</td>
                      <td>
                        {describeRouteBoundary(route.start_reason)} → {describeRouteBoundary(route.end_reason)}
                      </td>
                      <td>{route.duration_hours == null ? '' : route.duration_hours.toFixed(2)}</td>
                      <td>{route.distance_km.toFixed(2)}</td>
                      <td>{route.total_events}</td>
                      <td>{route.unique_event_types}</td>
                      <td>{route.total_gps_points}</td>
                      <td style={{ color: (route.total_events - route.total_gps_points) > 0 ? '#ea580c' : undefined, fontWeight: (route.total_events - route.total_gps_points) > 0 ? 600 : undefined }}>
                        {route.total_events - route.total_gps_points}
                      </td>
                      <td>
                        {route.road_match_ratio == null
                          ? '0.0%'
                          : `${(Number(route.road_match_ratio) * 100).toFixed(1)}%`}
                      </td>
                      <td>{routeRoadLabels.join(' | ')}</td>
                      <td>{mlPrediction?.predicted_route || (mlPrediction?.error ? 'N/A' : '')}</td>
                      <td>{mlConfidenceLabel || (mlPrediction?.error ? 'N/A' : '')}</td>
                      <td>{isExpanded ? '▲ Ocultar' : '▼ Ver puntos'}</td>
                    </tr>
                    )
                  })}
                  {routeDetection.complete_routes.length === 0 && (
                    <tr>
                      <td colSpan={15}>No se detectaron recorridos completos para este asset/client.</td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>

            <div className="toolbar" style={{ marginTop: 16 }}>
              <button onClick={loadLearnedRoutes} disabled={learnedRoutesLoading}>
                {learnedRoutesLoading ? 'Aprendiendo rutas históricas…' : '🧭 Aprender rutas históricas (origen → destino)'}
              </button>
            </div>

            {learnedRoutesData && !learnedRoutesData.error && (
              <div style={{ marginTop: 12 }}>
                <h3>
                  Rutas aprendidas para assetId={learnedRoutesData.asset_id}, clientId={learnedRoutesData.client_id}
                </h3>
                <p className="subtitle">
                  {learnedRoutesData.total_routes_used} de {learnedRoutesData.total_routes_input} recorridos agrupados por
                  origen/destino (radio {learnedRoutesData.origin_radius_km} km). El "punto de control" es la carretera
                  compartida por los recorridos hacia distintos destinos desde ese origen — proxy de caseta, ya que no hay
                  dataset real de casetas.
                </p>
                {learnedRoutesData.origins?.length > 0 ? (
                  learnedRoutesData.origins.map((o) => (
                    <div key={o.origin_cluster_id} style={{ marginBottom: 14 }}>
                      <p style={{ marginBottom: 4 }}>
                        <strong>Origen #{o.origin_cluster_id}</strong> ({o.lat.toFixed(4)}, {o.lon.toFixed(4)}) —{' '}
                        {o.total_trips} viajes históricos
                        {o.checkpoint_road && (
                          <> · punto de control: <strong>{o.checkpoint_road}</strong></>
                        )}
                      </p>
                      <div className="table-scroll">
                        <table>
                          <thead>
                            <tr>
                              <th>Destino</th>
                              <th>Coordenadas</th>
                              <th>Viajes</th>
                              <th>Frecuencia</th>
                              <th>Dist. prom.</th>
                              <th>Dur. prom.</th>
                            </tr>
                          </thead>
                          <tbody>
                            {o.destinations.map((d) => (
                              <tr key={d.destination_cluster_id}>
                                <td>#{d.destination_cluster_id}</td>
                                <td style={{ fontSize: 11 }}>{d.lat.toFixed(4)}, {d.lon.toFixed(4)}</td>
                                <td>{d.trip_count}</td>
                                <td><strong>{d.pct_of_origin_trips}%</strong></td>
                                <td>{d.avg_distance_km != null ? `${d.avg_distance_km} km` : '—'}</td>
                                <td>{d.avg_duration_hours != null ? `${d.avg_duration_hours} h` : '—'}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  ))
                ) : (
                  <p className="subtitle">No hay suficientes recorridos completos para aprender rutas de este asset.</p>
                )}
              </div>
            )}
            {learnedRoutesData?.error && (
              <p className="subtitle">Error: {learnedRoutesData.error}</p>
            )}

            <div className="toolbar" style={{ marginTop: 16 }}>
              <button onClick={loadDurationAnomalies} disabled={durationAnomaliesLoading}>
                {durationAnomaliesLoading ? 'Analizando tiempos de recorrido…' : '⏱ Demoras anómalas por ruta (Caso 5)'}
              </button>
            </div>

            {durationAnomaliesData && !durationAnomaliesData.error && (
              <div style={{ marginTop: 12 }}>
                <h3>
                  Viajes con demora anómala{' '}
                  <span className={durationAnomaliesData.delayed_trip_count > 0 ? 'warning' : 'success'}>
                    ({durationAnomaliesData.delayed_trip_count} de {durationAnomaliesData.total_trips_evaluated})
                  </span>
                </h3>
                <p className="subtitle">
                  Aprende el tiempo normal de recorrido (media, desviación estándar, percentiles) para cada ruta
                  habitual de este asset ({durationAnomaliesData.total_route_groups} rutas con suficiente historial),
                  filtrando primero recorridos atípicos previos, y marca los viajes cuya duración excede la banda
                  de normalidad (2 desviaciones estándar sobre el promedio).
                </p>
                {durationAnomaliesData.route_patterns?.length > 0 && (
                  <div className="table-scroll" style={{ marginBottom: 12 }}>
                    <table>
                      <thead>
                        <tr>
                          <th>Ruta</th>
                          <th>Viajes</th>
                          <th>Promedio</th>
                          <th>Mediana</th>
                          <th>Banda normal</th>
                        </tr>
                      </thead>
                      <tbody>
                        {durationAnomaliesData.route_patterns.map((p) => (
                          <tr key={`${p.origin_cluster_id}-${p.destination_cluster_id}`}>
                            <td>#{p.origin_cluster_id} → #{p.destination_cluster_id}</td>
                            <td>{p.total_trips}</td>
                            <td>{p.duration_stats.mean_hours.toFixed(2)} h</td>
                            <td>{p.duration_stats.median_hours.toFixed(2)} h</td>
                            <td style={{ fontSize: 11 }}>{p.normal_band_hours[0].toFixed(2)}–{p.normal_band_hours[1].toFixed(2)} h</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
                {durationAnomaliesData.alerts?.length > 0 ? (
                  <div className="table-scroll">
                    <table>
                      <thead>
                        <tr>
                          <th>Ruta</th>
                          <th>Inicio del viaje</th>
                          <th>Severidad</th>
                          <th>Duración</th>
                          <th>Esperado</th>
                          <th>% sobre promedio</th>
                        </tr>
                      </thead>
                      <tbody>
                        {durationAnomaliesData.alerts.map((a, i) => (
                          <tr key={i}>
                            <td>#{a.origin_cluster_id} → #{a.destination_cluster_id}</td>
                            <td style={{ fontSize: 11 }}>{a.start_timestamp ? new Date(a.start_timestamp).toLocaleString('es-MX') : '—'}</td>
                            <td>
                              <span className={a.severity === 'alta' ? 'warning' : ''}>{a.severity}</span>
                            </td>
                            <td><strong>{a.duration_hours.toFixed(2)} h</strong></td>
                            <td style={{ fontSize: 11 }}>{a.expected_mean_hours.toFixed(2)} h (banda {a.normal_band_hours[0].toFixed(2)}–{a.normal_band_hours[1].toFixed(2)})</td>
                            <td>{a.pct_over_average != null ? `+${a.pct_over_average}%` : '—'}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <p className="subtitle">Sin demoras anómalas detectadas en las rutas con historial suficiente.</p>
                )}
              </div>
            )}
            {durationAnomaliesData?.error && (
              <p className="subtitle">Error: {durationAnomaliesData.error}</p>
            )}

            <h3>Puntos de la ruta seleccionada</h3>
            {expandedRouteIndex === null ? (
              <p className="subtitle">Haz clic en una fila de la tabla para ver los puntos de esa ruta.</p>
            ) : (
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
            )}
            {expandedRouteIndex !== null && (() => {
              const route = routeDetection.complete_routes[expandedRouteIndex]
              const routeIndex = expandedRouteIndex
              if (!route) return null
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
                <div
                  className="route-points-block"
                  key={`route-points-${route.start_index}-${route.end_index}-${route.start_timestamp || 'na'}`}
                >
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
                        <button onClick={() => {
                          const r = routeDetection?.complete_routes?.[expandedRouteIndex]
                          if (r != null) openCompleteRouteIntervalMap(r, expandedRouteIndex)
                        }}>
                          Ver mapa de esta ruta completa (#{(expandedRouteIndex ?? 0) + 1})
                        </button>
                        <button
                          onClick={() => openMlPredictedRouteMapWindow(route, routeIndex)}
                          disabled={mlPredictionsLoading || !mlPrediction?.predicted_route}
                          title={
                            mlPredictionsLoading ? 'Calculando predicción ML…' :
                            mlPrediction?.error   ? `Error ML: ${mlPrediction.error}` :
                            !mlPrediction?.predicted_route ? 'Sin predicción ML para esta ruta' :
                            `Ruta ML: ${mlPrediction.predicted_route}`
                          }
                        >
                          {mlPredictionsLoading ? 'ML…' : 'Ver mapa ruta ML'}
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

              </div>
            )
          })()}
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

        {signalLossLoading && (
          <div style={{ marginTop: 12, marginBottom: 4 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 12, color: '#6b7280', marginBottom: 4 }}>
              <span>Analizando timestamps GPS vs servidor…</span>
              <span>{signalLossProgress}%</span>
            </div>
            <div style={{ background: '#e5e7eb', borderRadius: 6, height: 8 }}>
              <div style={{
                background: '#2563eb', borderRadius: 6, height: 8,
                width: `${signalLossProgress}%`, transition: 'width 0.4s ease',
              }} />
            </div>
          </div>
        )}

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
                <h3 style={{ display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap' }}>
                  Segmentos sin señal detectados
                  <button style={{ fontSize: 12, padding: '3px 10px' }} onClick={openSignalLossCoverageMap}>
                    🗺 Ver mapa de cobertura ↗
                  </button>
                </h3>
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
                        <th>Carretera / tramo</th>
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
                          <td>{seg.road_name || '—'}</td>
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
          <div className="toolbar">
            <button onClick={() => loadSignalLoss()}>Cargar análisis de señal</button>
          </div>
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
        {expandedRouteIndex !== null && routeDetection?.complete_routes?.[expandedRouteIndex] ? (
          <p className="subtitle" style={{ color: '#7c3aed', fontWeight: 600 }}>
            Analizando ruta #{expandedRouteIndex + 1} —{' '}
            {routeDetection.complete_routes[expandedRouteIndex].start_timestamp?.slice(0, 19).replace('T', ' ') ?? ''}
            {' → '}
            {routeDetection.complete_routes[expandedRouteIndex].end_timestamp?.slice(0, 19).replace('T', ' ') ?? ''}
          </p>
        ) : (
          <p className="subtitle" style={{ color: '#f59e0b' }}>
            ⚠ Sin ruta seleccionada en sección 3 — selecciona una ruta para ver sus paradas.
          </p>
        )}

        {stopsLoading && (
          <div style={{ marginTop: 12, marginBottom: 4 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 12, color: '#6b7280', marginBottom: 4 }}>
              <span>
                {stopsWithGeo
                  ? 'Detectando paradas y consultando OSM por carretera/POI… (puede tardar varios minutos)'
                  : 'Detectando y clasificando paradas…'}
              </span>
              <span>{stopsProgress}%</span>
            </div>
            <div style={{ background: '#e5e7eb', borderRadius: 6, height: 8 }}>
              <div style={{
                background: stopsWithGeo ? '#7c3aed' : '#2563eb',
                borderRadius: 6, height: 8,
                width: `${stopsProgress}%`, transition: 'width 0.5s ease',
              }} />
            </div>
            {stopsWithGeo && (
              <p style={{ fontSize: 11, color: '#9ca3af', marginTop: 4 }}>
                Consultando OpenStreetMap en lotes de 15 paradas. Puedes seguir usando la app mientras corre.
              </p>
            )}
          </div>
        )}

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

            {/* Resumen por categoría */}
            {stopsData.category_summary?.length > 0 && (() => {
              const CAT_COLORS = {
                gasolinera: '#f59e0b',
                hotel: '#8b5cf6',
                restaurante: '#22c55e',
                tienda_conveniencia: '#3b82f6',
                farmacia: '#06b6d4',
                hospital: '#0891b2',
                taller_mecanico: '#78716c',
                terminal_carga: '#6366f1',
                motor_apagado: '#64748b',
                ralenti: '#94a3b8',
                parada_no_autorizada: '#ef4444',
                estacionamiento_prolongado: '#f97316',
                pernocta_fuera_de_base: '#dc2626',
                parada_larga: '#b45309',
                parada_en_geocerca: '#0ea5e9',
                punto_de_control: '#10b981',
                entrega: '#10b981',
                en_carretera: '#78716c',
                parada_en_carretera: '#94a3b8',
              }
              return (
                <div style={{ marginBottom: 16 }}>
                  <div style={{ fontSize: 12, color: '#6b7280', marginBottom: 8, fontWeight: 600 }}>
                    Distribución por categoría
                  </div>
                  <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8 }}>
                    {stopsData.category_summary.map((c) => {
                      const color = CAT_COLORS[c.category] ?? '#94a3b8'
                      return (
                        <div
                          key={c.category}
                          style={{
                            background: color + '18',
                            border: `1px solid ${color}55`,
                            borderRadius: 8,
                            padding: '6px 12px',
                            minWidth: 110,
                            textAlign: 'center',
                          }}
                        >
                          <div style={{ fontSize: 18, fontWeight: 700, color }}>{c.count}</div>
                          <div style={{ fontSize: 11, color: '#374151', fontWeight: 600 }}>
                            {c.category.replace(/_/g, ' ')}
                          </div>
                          <div style={{ fontSize: 10, color: '#9ca3af' }}>
                            {c.total_minutes.toFixed(0)} min
                          </div>
                        </div>
                      )
                    })}
                  </div>
                </div>
              )
            })()}

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
                      <th>Deriva inicio→fin</th>
                    </tr>
                  </thead>
                  <tbody>
                    {stopsData.stops.map((s) => {
                      const catColors = {
                        gasolinera: '#f59e0b',
                        hotel: '#8b5cf6',
                        restaurante: '#22c55e',
                        tienda_conveniencia: '#3b82f6',
                        farmacia: '#06b6d4',
                        hospital: '#0891b2',
                        taller_mecanico: '#78716c',
                        terminal_carga: '#6366f1',
                        motor_apagado: '#64748b',
                        ralenti: '#94a3b8',
                        parada_no_autorizada: '#ef4444',
                        estacionamiento_prolongado: '#f97316',
                        pernocta_fuera_de_base: '#dc2626',
                        parada_larga: '#b45309',
                        parada_en_geocerca: '#0ea5e9',
                        punto_de_control: '#10b981',
                        entrega: '#10b981',
                        en_carretera: '#78716c',
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
                          <td style={{ fontSize: 11 }}>
                            {s.start_end_distance_m != null ? `${s.start_end_distance_m.toFixed(0)} m` : '—'}
                          </td>
                        </tr>
                      )
                    })}
                  </tbody>
                </table>
              </div>
            )}

            <div className="toolbar" style={{ marginTop: 16 }}>
              <button onClick={loadStopAnomalies} disabled={stopAnomaliesLoading}>
                {stopAnomaliesLoading ? 'Comparando contra historial…' : '🔎 Comparar contra patrón histórico del asset'}
              </button>
            </div>

            {stopAnomaliesData && !stopAnomaliesData.error && (
              <div style={{ marginTop: 12 }}>
                <h3>
                  Paradas atípicas vs. historial propio del asset{' '}
                  <span className={stopAnomaliesData.atypical_count > 0 ? 'warning' : 'success'}>
                    ({stopAnomaliesData.atypical_count} de {stopAnomaliesData.total_stops})
                  </span>
                </h3>
                <p className="subtitle">
                  Compara cada parada contra el propio histórico de ubicaciones frecuentes, categorías y
                  duraciones típicas de este assetId+clientId (precursor del caso de paradas atípicas por conductor+ruta).
                </p>
                {stopAnomaliesData.stops.filter((s) => s.is_atypical).length > 0 ? (
                  <div className="table-scroll">
                    <table>
                      <thead>
                        <tr>
                          <th>#</th>
                          <th>Inicio</th>
                          <th>Duración</th>
                          <th>Categoría</th>
                          <th>Coordenadas</th>
                          <th>Motivo</th>
                        </tr>
                      </thead>
                      <tbody>
                        {stopAnomaliesData.stops.filter((s) => s.is_atypical).map((s) => (
                          <tr key={s.stop_index}>
                            <td>{s.stop_index + 1}</td>
                            <td style={{ fontSize: 11 }}>{s.start_timestamp ? new Date(s.start_timestamp).toLocaleString('es-MX') : '—'}</td>
                            <td><strong className="warning">{s.duration_minutes.toFixed(1)} min</strong></td>
                            <td>{s.final_category.replace(/_/g, ' ')}</td>
                            <td style={{ fontSize: 11 }}>
                              {s.latitude != null ? `${s.latitude.toFixed(4)}, ${s.longitude.toFixed(4)}` : '—'}
                            </td>
                            <td style={{ fontSize: 11 }}>
                              {s.atypical_reasons.map((r) => r.replace(/_/g, ' ')).join(', ')}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <p className="subtitle">Sin paradas atípicas detectadas — el comportamiento coincide con el historial del asset.</p>
                )}
              </div>
            )}
            {stopAnomaliesData?.error && (
              <p className="subtitle">Error: {stopAnomaliesData.error}</p>
            )}

            <div className="toolbar" style={{ marginTop: 16 }}>
              <button onClick={loadRouteAnomalies} disabled={routeAnomaliesLoading}>
                {routeAnomaliesLoading ? 'Analizando viajes por ruta…' : '🚩 Paradas atípicas por ruta habitual (Caso 4)'}
              </button>
            </div>

            {routeAnomaliesData && !routeAnomaliesData.error && (
              <div style={{ marginTop: 12 }}>
                <h3>
                  Viajes atípicos por ruta habitual{' '}
                  <span className={routeAnomaliesData.atypical_trip_count > 0 ? 'warning' : 'success'}>
                    ({routeAnomaliesData.atypical_trip_count} de {routeAnomaliesData.total_trips_evaluated})
                  </span>
                </h3>
                <p className="subtitle">
                  Agrupa los viajes de este asset por ruta habitual (origen/destino, {routeAnomaliesData.total_route_groups}{' '}
                  rutas con suficiente historial) y compara cada viaje contra el número, ubicación, tipo, duración y
                  secuencia de paradas típicos de esa ruta específica. <strong>Nota:</strong> el dataset no tiene
                  driverId — esto es assetId + ruta, sin segmentación por conductor.
                </p>
                {routeAnomaliesData.alerts?.length > 0 ? (
                  <div className="table-scroll">
                    <table>
                      <thead>
                        <tr>
                          <th>Ruta (origen→destino)</th>
                          <th>Inicio del viaje</th>
                          <th>Severidad</th>
                          <th>Paradas</th>
                          <th>Rango esperado</th>
                          <th>Motivo</th>
                        </tr>
                      </thead>
                      <tbody>
                        {routeAnomaliesData.alerts.map((a, i) => (
                          <tr key={i}>
                            <td>#{a.origin_cluster_id} → #{a.destination_cluster_id}</td>
                            <td style={{ fontSize: 11 }}>{a.start_timestamp ? new Date(a.start_timestamp).toLocaleString('es-MX') : '—'}</td>
                            <td>
                              <span className={a.severity === 'alta' ? 'warning' : ''}>
                                {a.severity}
                              </span>
                            </td>
                            <td>{a.trip_stop_count}</td>
                            <td style={{ fontSize: 11 }}>{a.expected_stop_count_range[0]}–{a.expected_stop_count_range[1]}</td>
                            <td style={{ fontSize: 11 }}>{a.reasons.map((r) => r.replace(/_/g, ' ')).join(', ')}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <p className="subtitle">Sin viajes atípicos detectados en las rutas con historial suficiente.</p>
                )}
              </div>
            )}
            {routeAnomaliesData?.error && (
              <p className="subtitle">Error: {routeAnomaliesData.error}</p>
            )}
          </>
        )}

        {!stopsLoading && !stopsData && routeDetection && (
          <div className="toolbar">
            <button onClick={() => loadStops(false)}>Cargar paradas (rápido)</button>
            <button onClick={() => loadStops(true)}>Cargar paradas + clasificar OSM</button>
          </div>
        )}
        {!stopsLoading && !stopsData && !routeDetection && (
          <p className="subtitle">Ingresa assetId y clientId en la sección de rutas para analizar las paradas.</p>
        )}
      </section>

        </>
      )}

      {activeMenu === 'carreteras' && (
        <section className="card">
          <h2>Cobertura GPS global — todos los activos</h2>
          <p className="subtitle">
            Análisis de pérdida de señal GPS en <strong>todos los clientes y assets</strong> del dataset.
            Identifica zonas geográficas donde múltiples vehículos pierden señal sistemáticamente.
          </p>

          <div style={{ display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap', marginBottom: 12 }}>
            <button
              onClick={startGlobalCovJob}
              disabled={globalCovJob?.status === 'running'}
              style={{ fontSize: 13 }}
            >
              {globalCovJob?.status === 'running' ? 'Analizando…' : globalCovData ? '↺ Recalcular' : 'Calcular cobertura global'}
            </button>
            {globalCovData && (
              <button onClick={openGlobalCovMapWindow} style={{ fontSize: 13 }}>
                Ver mapa global ↗
              </button>
            )}
          </div>

          {/* Progreso del job */}
          {globalCovJob?.status === 'running' && (
            <div style={{ marginBottom: 12 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 12, color: '#6b7280', marginBottom: 4 }}>
                <span>{globalCovJob.message || 'Procesando…'}</span>
                <span>{Math.round((globalCovJob.progress ?? 0) * 100)}%</span>
              </div>
              <div style={{ background: '#e5e7eb', borderRadius: 6, height: 8 }}>
                <div style={{
                  background: '#2563eb', borderRadius: 6, height: 8,
                  width: `${Math.round((globalCovJob.progress ?? 0) * 100)}%`,
                  transition: 'width 0.5s ease',
                }} />
              </div>
              <p style={{ fontSize: 11, color: '#9ca3af', marginTop: 4 }}>
                Este proceso puede tardar varios minutos. Puedes seguir usando la app mientras corre.
              </p>
            </div>
          )}

          {globalCovJob?.status === 'error' && (
            <p style={{ color: '#dc2626', fontSize: 13 }}>{globalCovJob.message}</p>
          )}

          {/* Resultados */}
          {globalCovData && (
            <>
              <p style={{ fontSize: 11, color: '#9ca3af', marginBottom: 12 }}>
                Calculado el {new Date(globalCovData.computed_at * 1000).toLocaleString('es-MX')} ·
                Grid: {globalCovData.grid_size_deg}° (~1 km) · Clientes analizados: {globalCovData.total_clients}
              </p>

              {/* Tarjetas KPI */}
              <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', marginBottom: 18 }}>
                {[
                  { label: 'Clientes analizados', value: globalCovData.total_clients ?? 0, color: '#1d4ed8', bg: '#eff6ff' },
                  { label: 'Zonas sin cobertura', value: globalCovData.structural_zones ?? 0, color: '#dc2626', bg: '#fef2f2', note: '≥2 assets afectados' },
                  { label: 'Eventos aislados', value: globalCovData.isolated_events ?? 0, color: '#d97706', bg: '#fffbeb', note: '1 asset' },
                  { label: 'Total hotspots', value: globalCovData.total_hotspots ?? 0, color: '#7c3aed', bg: '#f5f3ff' },
                ].map((c) => (
                  <div key={c.label} style={{ background: c.bg, border: `1px solid ${c.color}33`, borderRadius: 8, padding: '10px 16px', minWidth: 150 }}>
                    <div style={{ fontSize: 11, color: '#6b7280', marginBottom: 2 }}>{c.label}</div>
                    <div style={{ fontSize: 22, fontWeight: 700, color: c.color }}>{(typeof c.value === 'number' ? c.value.toLocaleString() : c.value)}</div>
                    {c.note && <div style={{ fontSize: 11, color: '#9ca3af' }}>{c.note}</div>}
                  </div>
                ))}
              </div>

              {/* Tabla estructurales */}
              {globalCovData.structural_hotspots?.length > 0 && (
                <>
                  <h4 style={{ margin: '16px 0 6px', fontSize: 14, color: '#dc2626' }}>
                    ⛔ Zonas estructurales sin cobertura — top {Math.min(30, globalCovData.structural_hotspots.length)} de {globalCovData.structural_zones.toLocaleString()}
                  </h4>
                  <div className="table-wrap">
                    <table style={{ fontSize: 13 }}>
                      <thead><tr><th>#</th><th>Latitud</th><th>Longitud</th><th>Assets</th><th>Clientes</th><th>Eventos</th></tr></thead>
                      <tbody>
                        {globalCovData.structural_hotspots.slice(0, 30).map((h, i) => (
                          <tr key={i}>
                            <td>{i + 1}</td>
                            <td>{h.lat?.toFixed(4)}</td>
                            <td>{h.lon?.toFixed(4)}</td>
                            <td><strong style={{ color: '#dc2626' }}>{h.asset_count}</strong></td>
                            <td>{h.client_count}</td>
                            <td>{h.event_count?.toLocaleString()}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </>
              )}

              {/* Tabla aislados */}
              {globalCovData.isolated_hotspots?.length > 0 && (
                <>
                  <h4 style={{ margin: '16px 0 6px', fontSize: 14, color: '#d97706' }}>
                    ⚡ Eventos aislados — top {Math.min(30, globalCovData.isolated_hotspots.length)} de {globalCovData.isolated_events.toLocaleString()}
                  </h4>
                  <div className="table-wrap">
                    <table style={{ fontSize: 13 }}>
                      <thead><tr><th>#</th><th>Latitud</th><th>Longitud</th><th>Asset ID</th><th>Eventos</th></tr></thead>
                      <tbody>
                        {globalCovData.isolated_hotspots.slice(0, 30).map((h, i) => (
                          <tr key={i}>
                            <td>{i + 1}</td>
                            <td>{h.lat?.toFixed(4)}</td>
                            <td>{h.lon?.toFixed(4)}</td>
                            <td style={{ color: '#d97706' }}>{h.asset_ids?.[0] ?? '—'}</td>
                            <td>{h.event_count?.toLocaleString()}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </>
              )}
            </>
          )}

          {!globalCovData && globalCovJob?.status !== 'running' && (
            <p className="subtitle">Presiona "Calcular cobertura global" para iniciar el análisis de todos los {' '}
              clientes y assets del dataset.
            </p>
          )}
        </section>
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

      {activeMenu === 'carreteras' && (
        <section className="card">
          <h2>Cobertura GPS por carretera</h2>
          <p className="subtitle">
            Cruza las zonas de pérdida de señal GPS (análisis global) con el catálogo de carreteras
            para identificar qué tramos tienen problemas sistemáticos de cobertura.
          </p>

          <div className="toolbar" style={{ marginBottom: 12 }}>
            <button onClick={loadRoadCovMatch} disabled={roadCovLoading}>
              {roadCovLoading ? 'Calculando…' : roadCovMatch ? '↺ Recalcular match' : 'Calcular cobertura por carretera'}
            </button>
            {roadCovMatch && (
              <button onClick={openRoadCovMapWindow} style={{ fontSize: 13 }}>
                Ver mapa ↗
              </button>
            )}
            {roadCovMatch && (
              <select
                value={roadCovFilter}
                onChange={e => setRoadCovFilter(e.target.value)}
                style={{ fontSize: 13, padding: '4px 8px', borderRadius: 6, border: '1px solid #d1d5db' }}
              >
                <option value="all">Todas las severidades</option>
                <option value="alta">Alta (≥10 zonas estructurales)</option>
                <option value="media">Media (3–9 zonas)</option>
                <option value="baja">Baja (1–2 zonas)</option>
                <option value="aislada">Solo eventos aislados</option>
              </select>
            )}
          </div>

          {!roadCovMatch && !roadCovLoading && (
            <p className="subtitle">
              Requiere que el análisis de cobertura global (sección 7 del Dashboard) esté calculado primero.
            </p>
          )}

          {roadCovMatch && !roadCovLoading && (() => {
            const filtered = roadCovFilter === 'all'
              ? roadCovMatch.roads
              : roadCovMatch.roads.filter(r => r.severity === roadCovFilter)

            const sevColor = { alta: '#dc2626', media: '#d97706', baja: '#2563eb', aislada: '#6b7280' }
            const sevBg    = { alta: '#fef2f2', media: '#fffbeb', baja: '#eff6ff', aislada: '#f9fafb' }

            return (
              <>
                {/* KPIs */}
                <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', marginBottom: 16 }}>
                  {[
                    { label: 'Carreteras analizadas', value: roadCovMatch.total_roads_analyzed, color: '#1d4ed8', bg: '#eff6ff' },
                    { label: 'Con problemas GPS', value: roadCovMatch.roads_with_issues, color: '#dc2626', bg: '#fef2f2' },
                    { label: 'Sin problemas', value: roadCovMatch.roads_unaffected, color: '#16a34a', bg: '#f0fdf4' },
                  ].map(c => (
                    <div key={c.label} style={{ background: c.bg, border: `1px solid ${c.color}33`, borderRadius: 8, padding: '10px 16px', minWidth: 150 }}>
                      <div style={{ fontSize: 11, color: '#6b7280', marginBottom: 2 }}>{c.label}</div>
                      <div style={{ fontSize: 22, fontWeight: 700, color: c.color }}>{c.value.toLocaleString()}</div>
                    </div>
                  ))}
                </div>

                <p style={{ fontSize: 12, color: '#6b7280', marginBottom: 8 }}>
                  Mostrando {filtered.length} carreteras
                  {roadCovFilter !== 'all' ? ` · filtro: severidad "${roadCovFilter}"` : ''}
                </p>

                <div className="table-wrap">
                  <table style={{ fontSize: 13 }}>
                    <thead>
                      <tr>
                        <th>#</th>
                        <th>Carretera</th>
                        <th>Tipo</th>
                        <th>Km</th>
                        <th>Zonas sin cobertura</th>
                        <th>Eventos aislados</th>
                        <th>Total eventos GPS perdidos</th>
                        <th>Severidad</th>
                      </tr>
                    </thead>
                    <tbody>
                      {filtered.slice(0, 100).map((r, i) => (
                        <tr key={r.name}>
                          <td>{i + 1}</td>
                          <td style={{ maxWidth: 280, wordBreak: 'break-word' }}>{r.name}</td>
                          <td>{r.p_type || r.tipo}</td>
                          <td>{r.longitud_km ?? '—'}</td>
                          <td>
                            <strong style={{ color: r.structural_zones > 0 ? '#dc2626' : '#374151' }}>
                              {r.structural_zones}
                            </strong>
                          </td>
                          <td style={{ color: r.isolated_events > 0 ? '#d97706' : '#374151' }}>
                            {r.isolated_events}
                          </td>
                          <td>{r.total_events.toLocaleString()}</td>
                          <td>
                            <span style={{
                              background: sevBg[r.severity] || '#f9fafb',
                              color: sevColor[r.severity] || '#374151',
                              border: `1px solid ${sevColor[r.severity] || '#d1d5db'}44`,
                              borderRadius: 4, padding: '1px 8px', fontSize: 11, fontWeight: 600,
                            }}>
                              {r.severity}
                            </span>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                {filtered.length > 100 && (
                  <p style={{ fontSize: 12, color: '#9ca3af', marginTop: 6 }}>
                    Mostrando 100 de {filtered.length}. Usa el filtro de severidad para acotar.
                  </p>
                )}
              </>
            )
          })()}
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
