package com.pocketsignal

import android.os.Handler
import android.os.Looper
import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.compose.BackHandler
import androidx.compose.foundation.Canvas
import androidx.compose.foundation.background
import androidx.compose.foundation.gestures.detectDragGestures
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.layout.safeDrawingPadding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.Button
import androidx.compose.material3.ButtonDefaults
import androidx.compose.material3.Card
import androidx.compose.material3.CardDefaults
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.DropdownMenu
import androidx.compose.material3.DropdownMenuItem
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.material3.darkColorScheme
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableIntStateOf
import androidx.compose.runtime.mutableLongStateOf
import androidx.compose.runtime.mutableFloatStateOf
import androidx.compose.runtime.mutableStateListOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.geometry.Offset
import androidx.compose.ui.geometry.Size
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.input.pointer.pointerInput
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.RequestBody.Companion.toRequestBody
import org.json.JSONArray
import org.json.JSONObject
import java.net.URLEncoder
import java.time.LocalDate
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale
import java.util.concurrent.TimeUnit
import kotlin.math.max

private val Ink = Color(0xFF090E18)
private val Panel = Color(0xFF121B2A)
private val Muted = Color(0xFF8A9AB0)
private val Mint = Color(0xFF30D9A2)
private val Rose = Color(0xFFFF647C)
private val Gold = Color(0xFFFFCA72)
private val Expirations = listOf(3, 5, 10, 15, 20, 25, 30, 60, 120, 180, 240, 300, 600, 900)

private data class Instrument(
    val provider: String,
    val marketType: String,
    val symbol: String,
    val displayName: String,
    val firstHistoryTimestamp: Long? = null,
    val lastHistoryTimestamp: Long? = null,
    val candlesCount: Int = 0,
    val ticksCount: Int = 0,
    val availableIntervals: List<Int> = emptyList(),
    val availableExpiries: List<Int> = emptyList(),
)

private data class Candle(
    val timestamp: Long,
    val open: Double,
    val high: Double,
    val low: Double,
    val close: Double,
)

private data class Signal(
    val id: String,
    val symbol: String,
    val direction: String,
    val expirySeconds: Int,
    val issuedAt: Long,
    val entryAt: Long?,
    val expiresAt: Long,
    val quoteLatency: Long?,
    val probabilityUp: Double?,
    val probabilityDown: Double?,
    val explanation: String,
    val analytics: String,
)

private data class TradeRecord(
    val id: String,
    val symbol: String,
    val marketType: String,
    val direction: String,
    val expiry: Int,
    val probability: Double?,
    val outcome: String,
    val issuedAt: Long,
)

private data class Stats(
    val total: Int,
    val wins: Int,
    val losses: Int,
    val missed: Int,
    val pending: Int,
    val winRate: Double?,
    val durationMs: Long?,
    val byPair: Map<String, GroupStats>,
    val byExpiry: Map<String, GroupStats>,
    val byMarket: Map<String, GroupStats>,
)

private data class GroupStats(
    val total: Int,
    val wins: Int,
    val losses: Int,
    val missed: Int,
)

private data class DukascopyImportTotals(
    val filesRequested: Int = 0,
    val filesMissing: Int = 0,
    val ticksReceived: Int = 0,
    val candlesReceived: Int = 0,
    val candlesImported: Int = 0,
    val missingIntervals: Int = 0,
    val ticksImported: Int = 0,
)

private data class ExpiryBacktest(
    val expirySeconds: Int,
    val status: String,
    val sampleCount: Int,
    val tradeCount: Int,
    val wins: Int,
    val losses: Int,
    val winRatePercent: Double?,
)

private data class DukascopyBacktest(
    val symbol: String,
    val periodDays: Int,
    val candleCount: Int,
    val missingIntervals: Int,
    val availableIntervals: List<Int>,
    val availableExpiries: List<Int>,
    val costAdjusted: Boolean,
    val results: List<ExpiryBacktest>,
)

private data class OnlineSourceStatus(
    val status: String,
    val ready: Boolean,
    val reason: String?,
    val lastQuoteTimestamp: Long?,
    val updateFrequency: String,
    val referenceProvider: String,
    val referenceLastDate: String?,
    val referenceObservations: Int,
    val histdataStatus: String,
    val dukascopyStatus: String,
    val historySyncLoading: Boolean,
    val historySyncLastError: String?,
    val historySyncLastSuccess: Long?,
)

private object Api {
    private val http = OkHttpClient.Builder()
        .connectTimeout(8, TimeUnit.SECONDS)
        .readTimeout(60, TimeUnit.SECONDS)
        .build()

    private val base = BuildConfig.API_BASE_URL.trimEnd('/')

    private suspend fun request(
        path: String,
        method: String = "GET",
        body: String? = null,
    ): JSONObject = withContext(Dispatchers.IO) {
        val request = Request.Builder()
            .url("$base$path")
            .method(
                method,
                if (method == "GET") null
                else (body ?: "{}").toRequestBodyJson(),
            )
            .build()
        http.newCall(request).execute().use { response ->
            val text = response.body?.string().orEmpty()
            if (!response.isSuccessful) {
                val detail = runCatching { JSONObject(text).optString("detail") }
                    .getOrDefault(text)
                error(detail.ifBlank { "Server returned HTTP ${response.code}" })
            }
            JSONObject(text)
        }
    }

    suspend fun catalog(): List<Instrument> {
        val json = request("/api/v1/catalog").getJSONArray("instruments")
        return (0 until json.length()).map { index ->
            val item = json.getJSONObject(index)
            Instrument(
                item.getString("provider"),
                item.getString("market_type"),
                item.getString("symbol"),
                item.getString("display_name"),
            )
        }
    }

    suspend fun dukascopyInstruments(): List<Instrument> {
        val json = request("/api/v1/history/dukascopy/instruments")
            .getJSONArray("instruments")
        return (0 until json.length()).map { index ->
            val item = json.getJSONObject(index)
            Instrument(
                provider = item.getString("provider"),
                marketType = item.getString("market_type"),
                symbol = item.getString("symbol"),
                displayName = item.getString("display_name"),
                firstHistoryTimestamp = if (item.isNull("first_timestamp_ms")) null
                    else item.getLong("first_timestamp_ms"),
                lastHistoryTimestamp = if (item.isNull("last_timestamp_ms")) null
                    else item.getLong("last_timestamp_ms"),
                candlesCount = item.optInt("candles_count"),
                ticksCount = item.optInt("ticks_count"),
                availableIntervals = item.optJSONArray("available_intervals_seconds")
                    .toIntList(),
                availableExpiries = item.optJSONArray("available_expirations_seconds")
                    .toIntList(),
            )
        }
    }

    suspend fun sourceStatus(symbol: String?): OnlineSourceStatus {
        val symbolQuery = symbol?.let {
            "?symbol=${URLEncoder.encode(it, Charsets.UTF_8.name())}"
        }.orEmpty()
        val json = request("/api/v1/sources/status$symbolQuery")
        val online = json.getJSONObject("online")
        val history = json.getJSONObject("historical")
        val syncStates = history.getJSONArray("sync_status")
        val syncErrors = (0 until syncStates.length()).mapNotNull { index ->
            val item = syncStates.getJSONObject(index)
            if (item.isNull("error_message")) null else item.getString("error_message")
        }
        val syncSuccessTimes = (0 until syncStates.length()).mapNotNull { index ->
            val item = syncStates.getJSONObject(index)
            if (item.isNull("last_success_at_ms")) null
            else item.getLong("last_success_at_ms")
        }
        return OnlineSourceStatus(
            status = online.getString("status"),
            ready = online.getBoolean("ready"),
            reason = if (online.isNull("reason")) null else online.getString("reason"),
            lastQuoteTimestamp = if (online.isNull("last_quote_timestamp_ms")) null
                else online.getLong("last_quote_timestamp_ms"),
            updateFrequency = online.getString("update_frequency"),
            referenceProvider = history.getString("reference_provider"),
            referenceLastDate = if (history.isNull("reference_last_date")) null
                else history.getString("reference_last_date"),
            referenceObservations = history.getInt("reference_observations"),
            histdataStatus = when (history.optString("histdata_status")) {
                "ready" -> "доступен"
                "loading" -> "загружается"
                "unavailable" -> "недоступен"
                "not_yet_checked" -> "ожидает проверки"
                else -> "не развернут на сервере"
            },
            dukascopyStatus = history.getString("dukascopy_status"),
            historySyncLoading = (0 until syncStates.length()).any {
                syncStates.getJSONObject(it).getString("status") == "loading"
            },
            historySyncLastError = syncErrors.firstOrNull(),
            historySyncLastSuccess = syncSuccessTimes.maxOrNull(),
        )
    }

    suspend fun importDukascopy(
        symbol: String,
        date: LocalDate,
        startHour: Int,
        endHour: Int,
    ): DukascopyImportTotals {
        val encoded = URLEncoder.encode(symbol, Charsets.UTF_8.name())
        val json = request(
            "/api/v1/history/dukascopy/import?symbol=$encoded&date=$date" +
                "&start_hour=$startHour&end_hour=$endHour&interval_seconds=1",
            method = "POST",
        )
        return DukascopyImportTotals(
            filesRequested = json.getInt("files_requested"),
            filesMissing = json.getInt("files_missing"),
            ticksReceived = json.getInt("ticks_received"),
            candlesReceived = json.getInt("candles_received"),
            candlesImported = json.getInt("candles_imported"),
            missingIntervals = json.getInt("missing_intervals"),
            ticksImported = json.getInt("ticks_imported"),
        )
    }

    suspend fun backtestDukascopy(
        symbol: String,
        periodDays: Int,
        expiries: List<Int>,
        availabilityOnly: Boolean = false,
    ): DukascopyBacktest {
        val encoded = URLEncoder.encode(symbol, Charsets.UTF_8.name())
        val expiryQuery = expiries.joinToString("&") { "expiry_seconds=$it" }
        val availabilityQuery = if (availabilityOnly) "&availability_only=true" else ""
        val json = request(
            "/api/v1/backtest/dukascopy?symbol=$encoded&period_days=$periodDays&" +
                "$expiryQuery$availabilityQuery",
        )
        val resultItems = json.getJSONArray("results")
        return DukascopyBacktest(
            symbol = json.getString("symbol"),
            periodDays = json.getInt("period_days"),
            candleCount = json.getInt("candle_count"),
            missingIntervals = json.getInt("missing_intervals"),
            availableIntervals = json.getJSONArray("available_intervals_seconds")
                .toIntList(),
            availableExpiries = json.getJSONArray("available_expirations_seconds")
                .toIntList(),
            costAdjusted = json.getBoolean("cost_adjusted"),
            results = (0 until resultItems.length()).map { index ->
                val item = resultItems.getJSONObject(index)
                ExpiryBacktest(
                    expirySeconds = item.getInt("expiry_seconds"),
                    status = item.getString("status"),
                    sampleCount = item.getInt("sample_count"),
                    tradeCount = item.getInt("trade_count"),
                    wins = item.getInt("wins"),
                    losses = item.getInt("losses"),
                    winRatePercent = item.nullableDouble("win_rate_percent"),
                )
            },
        )
    }

    private fun JSONArray?.toIntList(): List<Int> =
        this?.let { values -> (0 until values.length()).map(values::getInt) }.orEmpty()

    suspend fun health(): Long =
        request("/health").getLong("server_time_ms")

    suspend fun startSession(): String =
        request("/api/v1/sessions", "POST").getString("id")

    suspend fun finishSession(id: String) {
        request("/api/v1/sessions/$id/finish", "POST")
    }

    suspend fun analyze(symbol: String, expiry: Int, sessionId: String?): Signal {
        val payload = JSONObject()
            .put("symbol", symbol)
            .put("market_type", "CLASSIC")
            .put("expiry_seconds", expiry)
        if (sessionId != null) payload.put("session_id", sessionId)
        val json = request("/api/v1/analyze", "POST", payload.toString())
        return Signal(
            id = json.getString("id"),
            symbol = json.getString("symbol"),
            direction = json.getString("direction"),
            expirySeconds = json.getInt("expiry_seconds"),
            issuedAt = json.getLong("issued_at_ms"),
            entryAt = if (json.isNull("recommended_entry_at_ms")) null
                else json.getLong("recommended_entry_at_ms"),
            expiresAt = json.getLong("expires_at_ms"),
            quoteLatency = if (json.isNull("quote_latency_ms")) null
                else json.getLong("quote_latency_ms"),
            probabilityUp = json.nullableDouble("probability_up"),
            probabilityDown = json.nullableDouble("probability_down"),
            explanation = json.getString("explanation"),
            analytics = json.optJSONObject("analytics")?.toString(2).orEmpty(),
        )
    }

    suspend fun candles(symbol: String): List<Candle> {
        val encoded = URLEncoder.encode(symbol, Charsets.UTF_8.name())
        val json = request(
            "/api/v1/candles?provider=TwelveData&symbol=$encoded" +
                "&interval_seconds=1&data_kind=LIVE&limit=500",
        ).getJSONArray("candles")
        return json.toCandles()
    }

    suspend fun allHistory(): List<TradeRecord> = withContext(Dispatchers.IO) {
        val request = Request.Builder().url("$base/api/v1/signals").build()
        http.newCall(request).execute().use { response ->
            val text = response.body?.string().orEmpty()
            if (!response.isSuccessful) error("Could not load signal history")
            JSONArray(text).toRecords()
        }
    }

    suspend fun statistics(period: String, sessionId: String?): Stats {
        val sessionQuery = sessionId?.let {
            "&session_id=${URLEncoder.encode(it, Charsets.UTF_8.name())}"
        }.orEmpty()
        val json = request("/api/v1/statistics?period=$period$sessionQuery")
        return Stats(
            total = json.getInt("total_signals"),
            wins = json.getInt("wins"),
            losses = json.getInt("losses"),
            missed = json.getInt("missed"),
            pending = json.getInt("pending"),
            winRate = json.nullableDouble("win_rate_percent"),
            durationMs = if (json.isNull("session_duration_ms")) null
                else json.getLong("session_duration_ms"),
            byPair = json.getJSONObject("by_pair").toGroupStats(),
            byExpiry = json.getJSONObject("by_expiry_seconds").toGroupStats(),
            byMarket = json.getJSONObject("by_market").toGroupStats(),
        )
    }

    suspend fun recordOutcome(signalId: String, outcome: String) {
        request(
            "/api/v1/signals/$signalId/outcome",
            "PATCH",
            JSONObject().put("outcome", outcome).toString(),
        )
    }

    fun streamUrl(symbol: String): String {
        val encoded = URLEncoder.encode(symbol, Charsets.UTF_8.name())
        return base.replaceFirst("https://", "wss://")
            .replaceFirst("http://", "ws://") + "/api/v1/stream?symbol=$encoded"
    }

    fun openStream(
        symbol: String,
        onConnected: () -> Unit,
        onTick: (Double, Long) -> Unit,
        onError: (String) -> Unit,
    ): WebSocket {
        val request = Request.Builder().url(streamUrl(symbol)).build()
        return http.newWebSocket(request, object : WebSocketListener() {
            override fun onOpen(webSocket: WebSocket, response: Response) {
                onConnected()
            }

            override fun onMessage(webSocket: WebSocket, text: String) {
                runCatching {
                    val json = JSONObject(text)
                    if (json.has("error")) {
                        onError(json.getString("error"))
                    } else if (json.optString("event") == "price") {
                        onTick(json.getDouble("price"), json.getDouble("timestamp").toLong() * 1000)
                    }
                }.onFailure { onError(it.message ?: "Invalid stream event") }
            }

            override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
                onError(t.message ?: "Live stream disconnected")
            }

            override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
                onError("Live stream disconnected")
            }
        })
    }
}

private fun String.toRequestBodyJson() =
    toRequestBody("application/json; charset=utf-8".toMediaType())

private fun JSONObject.nullableDouble(key: String): Double? =
    if (isNull(key)) null else getDouble(key)

private fun JSONObject.toGroupStats(): Map<String, GroupStats> =
    keys().asSequence().associateWith { key ->
        val item = getJSONObject(key)
        GroupStats(
            total = item.getInt("total"),
            wins = item.getInt("won"),
            losses = item.getInt("lost"),
            missed = item.getInt("missed"),
        )
    }

private fun JSONArray.toCandles(): List<Candle> =
    (0 until length()).map { index ->
        val value = getJSONObject(index)
        Candle(
            timestamp = value.getLong("timestamp_ms"),
            open = value.getDouble("open"),
            high = value.getDouble("high"),
            low = value.getDouble("low"),
            close = value.getDouble("close"),
        )
    }

private fun JSONArray.toRecords(): List<TradeRecord> =
    (0 until length()).map { index ->
        val value = getJSONObject(index)
        TradeRecord(
            id = value.getString("id"),
            symbol = value.getString("symbol"),
            marketType = value.getString("market_type"),
            direction = value.getString("direction"),
            expiry = value.getInt("expiry_seconds"),
            probability = if (value.getString("direction") == "DOWN") {
                value.nullableDouble("probability_down")
            } else {
                value.nullableDouble("probability_up")
            },
            outcome = value.getString("outcome"),
            issuedAt = value.getLong("issued_at_ms"),
        )
    }

private enum class Page { HOME, ANALYSIS, HISTORY, STATISTICS, TESTING }

class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContent { PocketSignalApp() }
    }
}

@Composable
private fun PocketSignalApp() {
    val scope = rememberCoroutineScope()
    var page by remember { mutableStateOf(Page.HOME) }
    var instruments by remember { mutableStateOf<List<Instrument>>(emptyList()) }
    var selected by remember { mutableStateOf<Instrument?>(null) }
    var market by remember { mutableStateOf("CLASSIC") }
    var expiry by remember { mutableIntStateOf(60) }
    var sessionId by remember { mutableStateOf<String?>(null) }
    var signal by remember { mutableStateOf<Signal?>(null) }
    var error by remember { mutableStateOf<String?>(null) }
    var busy by remember { mutableStateOf(false) }
    var history by remember { mutableStateOf<List<TradeRecord>>(emptyList()) }
    var stats by remember { mutableStateOf<Stats?>(null) }
    var statsPeriod by remember { mutableStateOf("day") }
    var serverStatus by remember { mutableStateOf("CONNECTING") }
    var onlineSourceStatus by remember { mutableStateOf<OnlineSourceStatus?>(null) }
    var serverClockOffsetMs by remember { mutableLongStateOf(0L) }

    LaunchedEffect(selected?.symbol) {
        var retryDelayMs = 1_000L
        while (true) {
            serverStatus = "CONNECTING"
            try {
                val requestStartedAtMs = System.currentTimeMillis()
                val serverTimeMs = Api.health()
                val responseReceivedAtMs = System.currentTimeMillis()
                serverClockOffsetMs =
                    serverTimeMs - (requestStartedAtMs + responseReceivedAtMs) / 2
                val catalog = Api.catalog()
                instruments = catalog.filter { it.marketType == "CLASSIC" }
                if (selected !in instruments) selected = instruments.firstOrNull()
                onlineSourceStatus = Api.sourceStatus(selected?.symbol)
                serverStatus = "CONNECTED"
                retryDelayMs = 1_000L
                if (catalog.none { it.marketType == "OTC" }) {
                    error = "OTC feed is not configured. Classic Forex prices are not OTC."
                }
                delay(5_000)
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (failure: Exception) {
                serverStatus = "UNAVAILABLE"
                error = failure.message ?: "Server connection failed"
                delay(retryDelayMs)
                retryDelayMs = (retryDelayMs * 2).coerceAtMost(30_000L)
            }
        }
    }

    DisposableEffect(page, selected?.symbol, serverStatus) {
        val instrument = selected
        if (page != Page.HOME || serverStatus != "CONNECTED" || instrument == null) {
            onDispose { }
        } else {
            val mainThread = Handler(Looper.getMainLooper())
            val socket = Api.openStream(
                instrument.symbol,
                onConnected = { },
                onTick = { _, _ -> },
                onError = { message ->
                    mainThread.post {
                        onlineSourceStatus = onlineSourceStatus?.copy(
                            ready = false,
                            status = "stale_or_unavailable",
                            reason = message,
                        )
                    }
                },
            )
            onDispose { socket.close(1000, "Leaving ONLINE home") }
        }
    }

    BackHandler(enabled = page != Page.HOME) {
        page = if (page == Page.ANALYSIS) Page.HOME else Page.HOME
    }

    MaterialTheme(
        colorScheme = darkColorScheme(
            primary = Mint,
            secondary = Gold,
            background = Ink,
            surface = Panel,
            onBackground = Color(0xFFF1F5FC),
            onSurface = Color(0xFFF1F5FC),
        ),
    ) {
        Surface(Modifier.fillMaxSize().safeDrawingPadding(), color = Ink) {
            when (page) {
                Page.HOME -> HomeScreen(
                    instruments = instruments,
                    selected = selected,
                    onSelect = { selected = it },
                    market = market,
                    onMarket = { market = it },
                    expiry = expiry,
                    onExpiry = { expiry = it },
                    sessionId = sessionId,
                    serverStatus = serverStatus,
                    onlineSourceStatus = onlineSourceStatus,
                    error = error,
                    busy = busy,
                    onSession = {
                        busy = true
                        error = null
                        runAsync(scope,
                            success = { id -> sessionId = id; busy = false },
                            failure = { error = it; busy = false },
                        ) {
                            val current = sessionId
                            if (current == null) Api.startSession()
                            else {
                                Api.finishSession(current)
                                null
                            }
                        }
                    },
                    onSignal = {
                        if (market == "OTC") {
                            error = "OTC is unavailable until an authorized Pocket Option OTC feed is configured."
                            return@HomeScreen
                        }
                        val instrument = selected ?: return@HomeScreen
                        busy = true
                        error = null
                        runAsync(scope,
                            success = {
                                signal = it
                                busy = false
                                page = Page.ANALYSIS
                            },
                            failure = { error = it; busy = false },
                        ) { Api.analyze(instrument.symbol, expiry, sessionId) }
                    },
                    onHistory = {
                        busy = true
                        runAsync(scope,
                            success = { history = it; busy = false; page = Page.HISTORY },
                            failure = { error = it; busy = false },
                        ) { Api.allHistory() }
                    },
                    onStatistics = {
                        busy = true
                        runAsync(scope,
                            success = { stats = it; busy = false; page = Page.STATISTICS },
                            failure = { error = it; busy = false },
                        ) { Api.statistics(statsPeriod, sessionId) }
                    },
                    onTesting = { page = Page.TESTING },
                )
                Page.ANALYSIS -> AnalysisScreen(
                    signal = signal,
                    serverClockOffsetMs = serverClockOffsetMs,
                    onBack = { page = Page.HOME },
                    onReanalyze = {
                        val instrument = selected ?: return@AnalysisScreen
                        busy = true
                        runAsync(
                            scope,
                            success = { signal = it; busy = false },
                            failure = { error = it; busy = false },
                        ) { Api.analyze(instrument.symbol, expiry, sessionId) }
                    },
                    onOutcome = { outcome ->
                        val current = signal ?: return@AnalysisScreen
                        busy = true
                        runAsync(scope,
                            success = {
                                busy = false
                                page = Page.HOME
                                signal = null
                            },
                            failure = { error = it; busy = false },
                        ) { Api.recordOutcome(current.id, outcome) }
                    },
                    busy = busy,
                )
                Page.HISTORY -> HistoryScreen(history, onBack = { page = Page.HOME })
                Page.STATISTICS -> StatisticsScreen(
                    stats = stats,
                    period = statsPeriod,
                    onPeriod = { period ->
                        statsPeriod = period
                        busy = true
                        runAsync(scope,
                            success = { stats = it; busy = false },
                            failure = { error = it; busy = false },
                        ) { Api.statistics(period, sessionId) }
                    },
                    onBack = { page = Page.HOME },
                )
                Page.TESTING -> TestingScreen(onBack = { page = Page.HOME })
            }
        }
    }
}

private fun <T> runAsync(
    scope: CoroutineScope,
    success: (T) -> Unit,
    failure: (String) -> Unit,
    block: suspend () -> T,
) {
    scope.launch(Dispatchers.Main) {
        runCatching { block() }
            .onSuccess(success)
            .onFailure { failure(it.message ?: "Request failed") }
    }
}

@Composable
private fun HomeScreen(
    instruments: List<Instrument>,
    selected: Instrument?,
    onSelect: (Instrument) -> Unit,
    market: String,
    onMarket: (String) -> Unit,
    expiry: Int,
    onExpiry: (Int) -> Unit,
    sessionId: String?,
    serverStatus: String,
    onlineSourceStatus: OnlineSourceStatus?,
    error: String?,
    busy: Boolean,
    onSession: () -> Unit,
    onSignal: () -> Unit,
    onHistory: () -> Unit,
    onStatistics: () -> Unit,
    onTesting: () -> Unit,
) {
    Column(
        Modifier.fillMaxSize().background(Ink).padding(horizontal = 22.dp, vertical = 18.dp),
        verticalArrangement = Arrangement.spacedBy(17.dp),
    ) {
        Row(verticalAlignment = Alignment.CenterVertically) {
            Column(Modifier.weight(1f)) {
                Text("POCKET SIGNAL", color = Color.White, fontWeight = FontWeight.Black, fontSize = 21.sp)
                Text("РЕЖИМ ONLINE • ТЕКУЩИЕ КОТИРОВКИ", color = Muted, fontSize = 10.sp, letterSpacing = 1.1.sp)
            }
            Row(
                Modifier.background(Color(0xFF162438), CircleShape).padding(horizontal = 11.dp, vertical = 8.dp),
                verticalAlignment = Alignment.CenterVertically,
            ) {
                val statusColor = when (serverStatus) {
                    "CONNECTED" -> Mint
                    "CONNECTING" -> Gold
                    else -> Rose
                }
                Box(Modifier.size(7.dp).background(statusColor, CircleShape))
                Spacer(Modifier.width(7.dp))
                Text(
                    when (serverStatus) {
                        "CONNECTED" -> "СЕРВЕР"
                        "CONNECTING" -> "СВЯЗЬ…"
                        else -> "НЕТ СВЯЗИ"
                    },
                    fontSize = 10.sp,
                    color = statusColor,
                )
            }
        }

        Card(
            colors = CardDefaults.cardColors(containerColor = Panel),
            shape = RoundedCornerShape(22.dp),
        ) {
            Column(Modifier.padding(18.dp), verticalArrangement = Arrangement.spacedBy(14.dp)) {
                Text("TRADE SETUP", color = Muted, fontSize = 11.sp, letterSpacing = 1.5.sp)
                Row(horizontalArrangement = Arrangement.spacedBy(10.dp)) {
                    SelectionMenu(
                        label = "INSTRUMENT",
                        value = selected?.displayName ?: "No data source",
                        items = instruments.map { it.displayName },
                        onSelected = { name -> instruments.firstOrNull { it.displayName == name }?.let(onSelect) },
                        modifier = Modifier.weight(1.3f),
                        enabled = instruments.isNotEmpty(),
                    )
                    SelectionMenu(
                        label = "MARKET",
                        value = market,
                        items = listOf("CLASSIC", "OTC"),
                        onSelected = onMarket,
                        modifier = Modifier.weight(0.8f),
                        enabled = true,
                    )
                }
                SelectionMenu(
                    label = "EXPIRATION",
                    value = formatExpiry(expiry),
                    items = Expirations.map(::formatExpiry),
                    onSelected = { value -> Expirations.firstOrNull { formatExpiry(it) == value }?.let(onExpiry) },
                    modifier = Modifier.fillMaxWidth(),
                    enabled = true,
                )
                Row(verticalAlignment = Alignment.CenterVertically) {
                    Text("MARKET SOURCE", color = Muted, fontSize = 10.sp, modifier = Modifier.weight(1f))
                    Text(selected?.provider ?: "NOT CONFIGURED", color = if (selected == null) Gold else Mint, fontSize = 11.sp)
                }
            }
        }

        Card(colors = CardDefaults.cardColors(containerColor = Color(0xFF101A29)), shape = RoundedCornerShape(18.dp)) {
            Column(Modifier.padding(16.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                Text(
                    if (onlineSourceStatus?.ready == true) "ONLINE • КОТИРОВКИ ДОСТУПНЫ"
                    else "ONLINE • КОТИРОВКИ НЕДОСТУПНЫ",
                    color = if (onlineSourceStatus?.ready == true) Mint else Gold,
                    fontWeight = FontWeight.Bold,
                    fontSize = 12.sp,
                )
                Text(
                    onlineSourceStatus?.reason
                        ?: if (instruments.isEmpty()) "Каталог пар не загружен."
                        else "Поставщик: ${selected?.provider}. Это не котировки Pocket Option OTC.",
                    color = Muted,
                    fontSize = 12.sp,
                    lineHeight = 17.sp,
                )
                Text(
                    "Частота: ${onlineSourceStatus?.updateFrequency ?: "проверка источника…"}",
                    color = Muted,
                    fontSize = 10.sp,
                    lineHeight = 15.sp,
                )
                Text(
                    "Последняя котировка: ${onlineSourceStatus?.lastQuoteTimestamp?.let(::formatDateTime) ?: "нет"}",
                    color = Muted,
                    fontSize = 10.sp,
                )
                Text(
                    "Последняя дневная справочная дата: ${onlineSourceStatus?.referenceLastDate ?: "нет данных"} " +
                        "(${onlineSourceStatus?.referenceObservations ?: 0} наблюдений, не подходит для кратких экспираций)",
                    color = Muted,
                    fontSize = 10.sp,
                    lineHeight = 15.sp,
                )
                Text(
                    when {
                        onlineSourceStatus?.historySyncLoading == true ->
                            "Историческая база: идёт синхронизация"
                        onlineSourceStatus?.historySyncLastError != null ->
                            "Последняя ошибка загрузки: ${onlineSourceStatus?.historySyncLastError}"
                        onlineSourceStatus?.historySyncLastSuccess != null ->
                            "Последняя успешная синхронизация: ${formatTime(onlineSourceStatus!!.historySyncLastSuccess!!)}"
                        else -> "Историческая база: ожидание первой синхронизации сервера"
                    },
                    color = Muted,
                    fontSize = 10.sp,
                    lineHeight = 15.sp,
                )
                Text(
                    "Архив тиков Dukascopy: ${onlineSourceStatus?.dukascopyStatus ?: "проверка…"}",
                    color = if (onlineSourceStatus?.dukascopyStatus == "ready") Mint else Gold,
                    fontSize = 10.sp,
                )
                Text(
                    "Архив HistData M1: ${onlineSourceStatus?.histdataStatus ?: "проверка…"}",
                    color = if (onlineSourceStatus?.histdataStatus == "доступен") Mint else Gold,
                    fontSize = 10.sp,
                )
            }
        }

        Button(
            onClick = onSignal,
            enabled = selected != null && serverStatus == "CONNECTED" &&
                onlineSourceStatus?.ready == true && !busy,
            modifier = Modifier.fillMaxWidth().height(62.dp),
            shape = RoundedCornerShape(17.dp),
            colors = ButtonDefaults.buttonColors(
                containerColor = Mint,
                contentColor = Color(0xFF06140F),
                disabledContainerColor = Color(0xFF283344),
            ),
        ) {
            if (busy) CircularProgressIndicator(Modifier.size(20.dp), color = Color.White, strokeWidth = 2.dp)
            else Text("АНАЛИЗ ТЕКУЩЕГО РЫНКА", fontWeight = FontWeight.Black, letterSpacing = 1.sp)
        }
        Row(horizontalArrangement = Arrangement.spacedBy(10.dp)) {
            OutlinedButton(onClick = onHistory, enabled = !busy, modifier = Modifier.weight(1f)) {
                Text("HISTORY")
            }
            OutlinedButton(onClick = onStatistics, enabled = !busy, modifier = Modifier.weight(1f)) {
                Text("STATISTICS")
            }
        }
        OutlinedButton(
            onClick = onTesting,
            enabled = !busy,
            modifier = Modifier.fillMaxWidth(),
            colors = ButtonDefaults.outlinedButtonColors(contentColor = Gold),
        ) {
            Text("РЕЖИМ ТЕСТИРОВАНИЯ • ИСТОРИЯ")
        }
        OutlinedButton(
            onClick = onSession,
            enabled = !busy,
            modifier = Modifier.fillMaxWidth(),
            colors = ButtonDefaults.outlinedButtonColors(contentColor = if (sessionId == null) Gold else Rose),
        ) {
            Text(if (sessionId == null) "START TRADING SESSION" else "END CURRENT SESSION")
        }
        if (error != null) Text(error, color = Rose, fontSize = 12.sp)
        Spacer(Modifier.weight(1f))
        Text("RECOMMENDATIONS ONLY  •  YOU PLACE EVERY TRADE", color = Muted, fontSize = 9.sp, letterSpacing = 1.sp)
    }
}

@Composable
private fun SelectionMenu(
    label: String,
    value: String,
    items: List<String>,
    onSelected: (String) -> Unit,
    modifier: Modifier,
    enabled: Boolean,
) {
    var expanded by remember { mutableStateOf(false) }
    Column(modifier) {
        Text(label, color = Muted, fontSize = 9.sp, letterSpacing = 1.sp)
        Spacer(Modifier.height(5.dp))
        Box {
            OutlinedButton(
                onClick = { expanded = true },
                enabled = enabled,
                modifier = Modifier.fillMaxWidth(),
                shape = RoundedCornerShape(12.dp),
            ) { Text(value, fontSize = 12.sp, maxLines = 1) }
            DropdownMenu(expanded = expanded, onDismissRequest = { expanded = false }) {
                items.forEach { item ->
                    DropdownMenuItem(
                        text = { Text(item) },
                        onClick = { onSelected(item); expanded = false },
                    )
                }
            }
        }
    }
}

@Composable
private fun TestingScreen(onBack: () -> Unit) {
    val scope = rememberCoroutineScope()
    var instruments by remember { mutableStateOf<List<Instrument>>(emptyList()) }
    var selectedSymbol by remember { mutableStateOf<String?>(null) }
    var periodDays by remember { mutableIntStateOf(1) }
    var expiries by remember { mutableStateOf(emptySet<Int>()) }
    var availableExpiries by remember { mutableStateOf<List<Int>>(emptyList()) }
    var importTotals by remember { mutableStateOf<DukascopyImportTotals?>(null) }
    var report by remember { mutableStateOf<DukascopyBacktest?>(null) }
    var sourceStatus by remember { mutableStateOf<OnlineSourceStatus?>(null) }
    var progress by remember { mutableStateOf<String?>(null) }
    var error by remember { mutableStateOf<String?>(null) }
    var loadingInstruments by remember { mutableStateOf(true) }
    var checkingAvailability by remember { mutableStateOf(false) }
    var busy by remember { mutableStateOf(false) }

    LaunchedEffect(Unit) {
        try {
            instruments = Api.dukascopyInstruments()
            sourceStatus = Api.sourceStatus(selectedSymbol)
            if (selectedSymbol == null || instruments.none { it.symbol == selectedSymbol }) {
                selectedSymbol = instruments.firstOrNull()?.symbol
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (failure: Exception) {
            error = "Не удалось получить список инструментов: ${failure.message ?: "ошибка сервера"}"
        } finally {
            loadingInstruments = false
        }
    }

    LaunchedEffect(selectedSymbol, periodDays) {
        val symbol = selectedSymbol ?: return@LaunchedEffect
        while (true) {
            checkingAvailability = true
            try {
                instruments = Api.dukascopyInstruments()
                sourceStatus = Api.sourceStatus(symbol)
                availableExpiries = Api.backtestDukascopy(
                    symbol,
                    periodDays,
                    listOf(60),
                    availabilityOnly = true,
                ).availableExpiries
                expiries = expiries.intersect(availableExpiries.toSet())
                error = null
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (failure: Exception) {
                availableExpiries = emptyList()
                expiries = emptySet()
                error = "Не удалось проверить точность истории: ${failure.message ?: "ошибка сервера"}"
            } finally {
                checkingAvailability = false
            }
            delay(60_000)
        }
    }

    fun runBacktest() {
        val symbol = selectedSymbol ?: return
        if (expiries.isEmpty() || busy) return
        busy = true
        error = null
        report = null
        scope.launch {
            try {
                report = Api.backtestDukascopy(symbol, periodDays, expiries.sorted())
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (failure: Exception) {
                error = "Не удалось выполнить тест: ${failure.message ?: "ошибка сервера"}"
            } finally {
                busy = false
            }
        }
    }
    val selectedInstrument = instruments.firstOrNull { it.symbol == selectedSymbol }

    val periodNoun = when (periodDays) {
        1 -> "день"
        2, 3, 4 -> "дня"
        else -> "дней"
    }

    Column(
        Modifier.fillMaxSize()
            .verticalScroll(rememberScrollState())
            .padding(horizontal = 18.dp, vertical = 12.dp),
        verticalArrangement = Arrangement.spacedBy(12.dp),
    ) {
        Row(verticalAlignment = Alignment.CenterVertically) {
            OutlinedButton(onClick = onBack) { Text("НАЗАД") }
            Spacer(Modifier.width(10.dp))
            Column {
                Text("ТЕСТИРОВАНИЕ", fontWeight = FontWeight.Black, fontSize = 18.sp)
                Text("ИСТОРИЧЕСКИЕ ДАННЫЕ", color = Muted, fontSize = 10.sp)
            }
        }
        Card(colors = CardDefaults.cardColors(containerColor = Panel), shape = RoundedCornerShape(18.dp)) {
            Column(
                Modifier.fillMaxWidth().padding(15.dp),
                verticalArrangement = Arrangement.spacedBy(12.dp),
            ) {
                SelectionMenu(
                    label = "ВАЛЮТНАЯ ПАРА",
                    value = selectedSymbol ?: if (loadingInstruments) "Загрузка…" else "Нет доступных пар",
                    items = instruments.map { it.symbol },
                    onSelected = {
                        selectedSymbol = it
                        report = null
                        importTotals = null
                        expiries = emptySet()
                        availableExpiries = emptyList()
                        error = null
                    },
                    modifier = Modifier.fillMaxWidth(),
                    enabled = !loadingInstruments && instruments.isNotEmpty() && !busy,
                )
                SelectionMenu(
                    label = "ПЕРИОД ДО ПОСЛЕДНЕЙ ДОСТУПНОЙ ИСТОРИИ",
                    value = "$periodDays $periodNoun",
                    items = listOf("1 день", "3 дня", "7 дней"),
                    onSelected = { value ->
                        periodDays = value.substringBefore(' ').toInt()
                        report = null
                        importTotals = null
                        expiries = emptySet()
                        availableExpiries = emptyList()
                        error = null
                    },
                    modifier = Modifier.fillMaxWidth(),
                    enabled = !busy && !checkingAvailability,
                )
                Text("ЭКСПИРАЦИЯ", color = Muted, fontSize = 10.sp, letterSpacing = 1.sp)
                if (availableExpiries.isEmpty()) {
                    Text(
                        "Для этой пары пока нет загруженных секундных или минутных свечей. Справочные дневные курсы доступны отдельно, но не подходят для этого теста.",
                        color = Gold,
                        fontSize = 11.sp,
                        lineHeight = 16.sp,
                    )
                } else {
                    availableExpiries.chunked(4).forEach { row ->
                        Row(horizontalArrangement = Arrangement.spacedBy(6.dp)) {
                            row.forEach { seconds ->
                                val checked = seconds in expiries
                                OutlinedButton(
                                    onClick = {
                                        expiries = if (checked) {
                                            expiries - seconds
                                        } else if (expiries.size < 3) {
                                            expiries + seconds
                                        } else {
                                            expiries
                                        }
                                        report = null
                                    },
                                    enabled = !busy,
                                    modifier = Modifier.weight(1f),
                                    colors = ButtonDefaults.outlinedButtonColors(
                                        contentColor = if (checked) Mint else Muted,
                                    ),
                                ) {
                                    Text(formatTestingExpiry(seconds), fontSize = 10.sp)
                                }
                            }
                            repeat(4 - row.size) { Spacer(Modifier.weight(1f)) }
                        }
                    }
                    Text(
                        "Можно выбрать до трёх экспираций. Доступность определяется реальными сохранёнными разрешениями.",
                        color = Muted,
                        fontSize = 10.sp,
                    )
                }
            }
        }

        Card(colors = CardDefaults.cardColors(containerColor = Color(0xFF101A29)), shape = RoundedCornerShape(18.dp)) {
            Column(Modifier.padding(15.dp), verticalArrangement = Arrangement.spacedBy(7.dp)) {
                Text("ТОЛЬКО ИСТОРИЯ • НЕ ONLINE", color = Gold, fontWeight = FontWeight.Bold, fontSize = 12.sp)
                Text(
                    "M1-архив HistData содержит минутные bid-свечи; тики Dukascopy сохраняются отдельно и могут агрегироваться в секундные свечи. Источники и пропуски не смешиваются с дневочными справочными курсами. Тест использует только завершённые свечи до входа и open следующей свечи.",
                    color = Muted,
                    fontSize = 11.sp,
                    lineHeight = 16.sp,
                )
                Text(
                    "Спред, комиссии и проскальзывание не учитываются. Результат не является гарантией будущей доходности.",
                    color = Muted,
                    fontSize = 11.sp,
                    lineHeight = 16.sp,
                )
                Text(
                    "Счётчик пропусков включает периоды закрытого рынка и выходные; расписание торгов отдельно не фильтруется.",
                    color = Muted,
                    fontSize = 10.sp,
                    lineHeight = 15.sp,
                )
                Text(
                    "Источник: ${selectedInstrument?.provider ?: "нет"} • свечей: " +
                        "${selectedInstrument?.candlesCount ?: 0} • " +
                        "тики: ${selectedInstrument?.ticksCount ?: 0} • разрешения: " +
                        (selectedInstrument?.availableIntervals?.joinToString("с, ") { it.toString() } ?: "нет"),
                    color = Muted,
                    fontSize = 10.sp,
                    lineHeight = 15.sp,
                )
                Text(
                    "HistData M1: ${sourceStatus?.histdataStatus ?: "проверка…"} • " +
                        "для M1 доступны только экспирации от 1 минуты; ONLINE не используется.",
                    color = Muted,
                    fontSize = 10.sp,
                    lineHeight = 15.sp,
                )
                Text(
                    "Доступный диапазон: ${selectedInstrument?.firstHistoryTimestamp?.let(::formatDate) ?: "не загружен"} — " +
                        (selectedInstrument?.lastHistoryTimestamp?.let(::formatDate) ?: "не загружен"),
                    color = Muted,
                    fontSize = 10.sp,
                )
                Text(
                    "${sourceStatus?.referenceProvider ?: "Frankfurter"}: дневной справочный ряд до " +
                        "${sourceStatus?.referenceLastDate ?: "пока не загружен"} " +
                        "(${sourceStatus?.referenceObservations ?: 0} значений); не используется для коротких экспираций.",
                    color = Muted,
                    fontSize = 10.sp,
                    lineHeight = 15.sp,
                )
                Text(
                    when {
                        sourceStatus?.historySyncLoading == true ->
                            "Синхронизация исторической базы выполняется."
                        sourceStatus?.historySyncLastError != null ->
                            "Последняя ошибка источника: ${sourceStatus?.historySyncLastError}"
                        sourceStatus?.historySyncLastSuccess != null ->
                            "Последняя успешная загрузка: ${formatTime(sourceStatus!!.historySyncLastSuccess!!)}"
                        else -> "Ожидается автоматический запуск синхронизации сервером."
                    },
                    color = Muted,
                    fontSize = 10.sp,
                    lineHeight = 15.sp,
                )
            }
        }

        Button(
            onClick = {
                val symbol = selectedSymbol ?: return@Button
                if (busy) return@Button
                busy = true
                error = null
                report = null
                importTotals = DukascopyImportTotals()
                scope.launch {
                    var totals = DukascopyImportTotals()
                    try {
                        val now = ZonedDateTime.now(ZoneOffset.UTC)
                        val today = now.toLocalDate()
                        val requests = buildList {
                            for (dayOffset in (periodDays - 1) downTo 0) {
                                val date = today.minusDays(dayOffset.toLong())
                                val endHour = if (dayOffset == 0) now.hour else 24
                                for (startHour in 0 until endHour step 3) {
                                    add(Triple(date, startHour, (startHour + 3).coerceAtMost(endHour)))
                                }
                            }
                        }
                        if (requests.isEmpty()) {
                            progress = "За выбранный период пока нет завершённых часов UTC для загрузки."
                            return@launch
                        }
                        for ((index, request) in requests.withIndex()) {
                            val (date, startHour, endHour) = request
                            val imported = Api.importDukascopy(symbol, date, startHour, endHour)
                            totals = totals.copy(
                                filesRequested = totals.filesRequested + imported.filesRequested,
                                filesMissing = totals.filesMissing + imported.filesMissing,
                                ticksReceived = totals.ticksReceived + imported.ticksReceived,
                                ticksImported = totals.ticksImported + imported.ticksImported,
                                candlesReceived = totals.candlesReceived + imported.candlesReceived,
                                candlesImported = totals.candlesImported + imported.candlesImported,
                                missingIntervals = totals.missingIntervals + imported.missingIntervals,
                            )
                            importTotals = totals
                            progress = "Загружено запросов: ${index + 1} из ${requests.size}"
                        }
                        progress = "Загрузка завершена. Выполняется тестирование…"
                        val refreshedInstruments = Api.dukascopyInstruments()
                        instruments = refreshedInstruments
                        sourceStatus = Api.sourceStatus(symbol)
                        val refreshedExpiries = Api.backtestDukascopy(
                            symbol,
                            periodDays,
                            listOf(60),
                            availabilityOnly = true,
                        ).availableExpiries
                        availableExpiries = refreshedExpiries
                        val testExpiries = expiries.ifEmpty {
                            refreshedExpiries.take(3).toSet()
                        }
                        expiries = testExpiries
                        if (testExpiries.isEmpty()) {
                            progress = "Источник пока не предоставил данные нужной точности для тестирования."
                        } else {
                            report = Api.backtestDukascopy(
                                symbol,
                                periodDays,
                                testExpiries.sorted(),
                            )
                            progress = "Загрузка и тестирование завершены."
                        }
                    } catch (cancelled: CancellationException) {
                        throw cancelled
                    } catch (failure: Exception) {
                        error = "Операция прервана: ${failure.message ?: "ошибка сервера"}. Уже сохранённые интервалы не удалены."
                    } finally {
                        busy = false
                    }
                }
            },
            enabled = selectedSymbol != null && !loadingInstruments && !busy,
            modifier = Modifier.fillMaxWidth().height(54.dp),
            colors = ButtonDefaults.buttonColors(containerColor = Mint, contentColor = Ink),
        ) {
            if (busy) CircularProgressIndicator(Modifier.size(18.dp), color = Ink, strokeWidth = 2.dp)
            else Text("ЗАГРУЗИТЬ И ПРОВЕСТИ ТЕСТ", fontWeight = FontWeight.Bold)
        }
        OutlinedButton(
            onClick = { runBacktest() },
            enabled = selectedSymbol != null && expiries.isNotEmpty() && !busy,
            modifier = Modifier.fillMaxWidth(),
        ) {
            Text("ТЕСТИРОВАТЬ УЖЕ ЗАГРУЖЕННЫЕ ДАННЫЕ")
        }

        if (progress != null) Text(progress!!, color = Muted, fontSize = 11.sp)
        if (importTotals != null) {
            val totals = importTotals!!
            StatCard("ФАЙЛЫ / НЕТ ФАЙЛОВ", "${totals.filesRequested} / ${totals.filesMissing}")
            StatCard("ПОЛУЧЕНО ТИКОВ", totals.ticksReceived.toString())
            StatCard("СОХРАНЕНО РЕАЛЬНЫХ ТИКОВ", totals.ticksImported.toString())
            StatCard("СВЕЧИ: ПОЛУЧЕНО / СОХРАНЕНО", "${totals.candlesReceived} / ${totals.candlesImported}")
            StatCard("ПРОПУЩЕННЫЕ ИНТЕРВАЛЫ", totals.missingIntervals.toString())
        }
        if (report != null) {
            val result = report!!
            Text(
                "${result.symbol} • ${result.periodDays} дн. UTC",
                color = Color.White,
                fontWeight = FontWeight.Bold,
            )
            StatCard("ЗАГРУЖЕННЫЕ СВЕЧИ", result.candleCount.toString())
            StatCard("ПРОПУЩЕННЫЕ МИНУТНЫЕ ИНТЕРВАЛЫ", result.missingIntervals.toString())
            result.results.forEach { expiryResult ->
                Card(colors = CardDefaults.cardColors(containerColor = Panel), shape = RoundedCornerShape(16.dp)) {
                    Column(
                        Modifier.fillMaxWidth().padding(15.dp),
                        verticalArrangement = Arrangement.spacedBy(5.dp),
                    ) {
                        Text(
                            "Экспирация ${formatTestingExpiry(expiryResult.expirySeconds)}",
                            color = Color.White,
                            fontWeight = FontWeight.Bold,
                        )
                        when (expiryResult.status) {
                            "ok" -> {
                                Text(
                                    "Сделок: ${expiryResult.tradeCount} • Win Rate: ${expiryResult.winRatePercent?.let { "%.2f%%".format(Locale.US, it) } ?: "—"}",
                                    color = Mint,
                                    fontSize = 12.sp,
                                )
                                Text(
                                    "Wins: ${expiryResult.wins} • Losses: ${expiryResult.losses} • Проверено сигналов: ${expiryResult.sampleCount}",
                                    color = Muted,
                                    fontSize = 11.sp,
                                )
                            }
                            "insufficient_signals" -> Text(
                                "В тестовом участке нет направленных сделок. Win Rate не рассчитан.",
                                color = Gold,
                                fontSize = 11.sp,
                            )
                            else -> Text(
                                "Недостаточно исторических свечей для достоверного теста. Win Rate не рассчитан.",
                                color = Gold,
                                fontSize = 11.sp,
                            )
                        }
                    }
                }
            }
            if (!result.costAdjusted) {
                Text(
                    "ВНИМАНИЕ: результат не скорректирован на спред, комиссии и проскальзывание.",
                    color = Gold,
                    fontSize = 11.sp,
                )
            }
        }
        if (error != null) Text(error!!, color = Rose, fontSize = 12.sp)
        if (loadingInstruments) CircularProgressIndicator(color = Mint)
        if (checkingAvailability) Text("Проверка доступной точности…", color = Muted, fontSize = 10.sp)
    }
}

@Composable
private fun AnalysisScreen(
    signal: Signal?,
    serverClockOffsetMs: Long,
    onBack: () -> Unit,
    onReanalyze: () -> Unit,
    onOutcome: (String) -> Unit,
    busy: Boolean,
) {
    val candles = remember { mutableStateListOf<Candle>() }
    val mainThread = remember { Handler(Looper.getMainLooper()) }
    var price by remember { mutableStateOf<Double?>(null) }
    var streamError by remember { mutableStateOf<String?>(null) }
    var reconnectAttempt by remember { mutableIntStateOf(0) }
    var reconnectTrigger by remember { mutableIntStateOf(0) }
    var clockPulse by remember { mutableIntStateOf(0) }
    val current = signal
    LaunchedEffect(current?.id) {
        while (true) {
            delay(1_000)
            clockPulse += 1
        }
    }
    LaunchedEffect(current?.id) {
        if (current == null) return@LaunchedEffect
        while (true) {
            runCatching { Api.candles(current.symbol) }
                .onSuccess { values ->
                    values.forEach { candle ->
                        val existingIndex = candles.indexOfFirst {
                            it.timestamp == candle.timestamp
                        }
                        if (existingIndex >= 0) candles[existingIndex] = candle
                        else candles.add(candle)
                    }
                    while (candles.size > 500) candles.removeAt(0)
                }
                .onFailure { streamError = it.message }
            delay(3000)
        }
    }
    DisposableEffect(current?.id, reconnectTrigger) {
        var retry: Runnable? = null
        var stableConnection: Runnable? = null
        var disposed = false
        val socket = if (current == null) null else Api.openStream(
            current.symbol,
            onConnected = {
                mainThread.post {
                    streamError = null
                    stableConnection?.let(mainThread::removeCallbacks)
                    val resetAttempts = Runnable { reconnectAttempt = 0 }
                    stableConnection = resetAttempts
                    mainThread.postDelayed(resetAttempts, 10_000L)
                }
            },
            onTick = { latest, timestamp ->
                mainThread.post {
                    price = latest
                    val bucket = timestamp / 1000 * 1000
                    val last = candles.lastOrNull()
                    if (last != null && last.timestamp == bucket) {
                        candles[candles.lastIndex] = last.copy(
                            high = max(last.high, latest),
                            low = minOf(last.low, latest),
                            close = latest,
                        )
                    } else {
                        candles.add(Candle(bucket, latest, latest, latest, latest))
                    }
                }
            },
            onError = { message ->
                mainThread.post {
                    if (disposed) return@post
                    streamError = message
                    stableConnection?.let(mainThread::removeCallbacks)
                    if (retry == null) {
                        val retryDelay = (1_000L * (1L shl reconnectAttempt.coerceIn(0, 5)))
                            .coerceAtMost(30_000L)
                        val next = Runnable {
                            reconnectAttempt += 1
                            reconnectTrigger += 1
                        }
                        retry = next
                        mainThread.postDelayed(next, retryDelay)
                    }
                }
            },
        )
        onDispose {
            disposed = true
            retry?.let(mainThread::removeCallbacks)
            stableConnection?.let(mainThread::removeCallbacks)
            socket?.close(1000, "Screen closed")
        }
    }
    val secondsToExpiry = clockPulse.let {
        current?.let { activeSignal ->
            ((activeSignal.expiresAt - System.currentTimeMillis() - serverClockOffsetMs + 999) / 1000)
                .coerceAtLeast(0)
        }
    }

    Column(
        Modifier.fillMaxSize().padding(horizontal = 18.dp, vertical = 12.dp),
        verticalArrangement = Arrangement.spacedBy(12.dp),
    ) {
        Row(verticalAlignment = Alignment.CenterVertically) {
            OutlinedButton(onClick = onBack) { Text("BACK") }
            Spacer(Modifier.width(12.dp))
            Column {
                Text(current?.symbol ?: "ANALYSIS", fontWeight = FontWeight.Bold, fontSize = 18.sp)
                Text("LIVE • ${current?.expirySeconds?.let(::formatExpiry) ?: ""}", color = Muted, fontSize = 11.sp)
            }
            Spacer(Modifier.weight(1f))
            Text(price?.let { "%.5f".format(Locale.US, it) } ?: "—", color = Mint, fontWeight = FontWeight.Bold)
        }

        Card(
            Modifier.fillMaxWidth().weight(1f),
            colors = CardDefaults.cardColors(containerColor = Panel),
            shape = RoundedCornerShape(18.dp),
        ) {
            if (candles.isEmpty()) {
                Box(Modifier.fillMaxSize(), contentAlignment = Alignment.Center) {
                    Text("Waiting for real quote ticks…", color = Muted)
                }
            } else {
                CandleChart(candles.toList(), Modifier.fillMaxSize().padding(10.dp))
            }
        }

        if (current != null) {
            Card(colors = CardDefaults.cardColors(containerColor = Panel), shape = RoundedCornerShape(18.dp)) {
                Column(Modifier.fillMaxWidth().padding(16.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                    Text(
                        when (current.direction) {
                            "UP" -> "UP  •  ${current.probabilityUp?.let { "%.1f%%".format(Locale.US, it) } ?: "—"}"
                            "DOWN" -> "DOWN  •  ${current.probabilityDown?.let { "%.1f%%".format(Locale.US, it) } ?: "—"}"
                            else -> "NO SIGNAL"
                        },
                        color = if (current.direction == "UP") Mint else if (current.direction == "DOWN") Rose else Gold,
                        fontWeight = FontWeight.Black,
                        fontSize = 20.sp,
                    )
                    Text(current.explanation, color = Muted, fontSize = 12.sp, lineHeight = 17.sp)
                    Text(
                        "Entry ${current.entryAt?.let(::formatTime) ?: "not recommended"}  •  expires ${formatTime(current.expiresAt)}",
                        color = Muted,
                        fontSize = 10.sp,
                    )
                    Text(
                        "Issued ${formatTime(current.issuedAt)}  •  ${secondsToExpiry}s left  •  quote delay ${current.quoteLatency?.let { "${it} ms" } ?: "unavailable"}",
                        color = Muted,
                        fontSize = 10.sp,
                    )
                    if (current.direction != "NO_SIGNAL") {
                        Row(horizontalArrangement = Arrangement.spacedBy(10.dp)) {
                            Button(
                                onClick = { onOutcome("WON") },
                                enabled = !busy,
                                modifier = Modifier.weight(1f),
                                colors = ButtonDefaults.buttonColors(containerColor = Color(0xFF158B68)),
                            ) { Text("WON") }
                            Button(
                                onClick = { onOutcome("LOST") },
                                enabled = !busy,
                                modifier = Modifier.weight(1f),
                                colors = ButtonDefaults.buttonColors(containerColor = Color(0xFFB63F57)),
                            ) { Text("LOST") }
                            OutlinedButton(
                                onClick = { onOutcome("MISSED") },
                                enabled = !busy,
                                modifier = Modifier.weight(1f),
                            ) { Text("SKIP") }
                        }
                    } else {
                        Text("No trade to score. NO SIGNAL is excluded from trade accuracy.", color = Gold, fontSize = 11.sp)
                        Button(
                            onClick = onReanalyze,
                            enabled = !busy,
                            modifier = Modifier.fillMaxWidth(),
                            colors = ButtonDefaults.buttonColors(containerColor = Color(0xFF25364B)),
                        ) {
                            Text(if (busy) "ANALYZING…" else "ANALYZE AGAIN")
                        }
                    }
                }
            }
        }
        if (streamError != null) Text(streamError!!, color = Rose, fontSize = 11.sp)
    }
}

@Composable
private fun CandleChart(candles: List<Candle>, modifier: Modifier = Modifier) {
    var drag by remember { mutableFloatStateOf(0f) }
    Canvas(
        modifier.pointerInput(candles.size) {
            detectDragGestures { change, amount ->
                change.consume()
                drag += amount.x
            }
        },
    ) {
        val visibleCount = 70
        val shift = (drag / 8).toInt().coerceIn(0, max(0, candles.size - visibleCount))
        val end = (candles.size - shift).coerceAtLeast(1)
        val start = (end - visibleCount).coerceAtLeast(0)
        val visible = candles.subList(start, end)
        if (visible.isEmpty()) return@Canvas
        val low = visible.minOf { it.low }
        val high = visible.maxOf { it.high }
        val range = (high - low).takeIf { it > 0 } ?: 0.00001
        val step = size.width / visible.size
        repeat(4) { line ->
            val y = size.height * line / 3
            drawLine(Color(0xFF273448), Offset(0f, y), Offset(size.width, y), 1f)
        }
        visible.forEachIndexed { index, candle ->
            val x = step * (index + 0.5f)
            fun y(value: Double) = size.height * (1 - ((value - low) / range).toFloat())
            val color = if (candle.close >= candle.open) Mint else Rose
            drawLine(color, Offset(x, y(candle.high)), Offset(x, y(candle.low)), 1.5f)
            val top = minOf(y(candle.open), y(candle.close))
            val bottom = maxOf(y(candle.open), y(candle.close))
            drawRect(
                color,
                topLeft = Offset(x - step * 0.31f, top),
                size = Size(step * 0.62f, max(2f, bottom - top)),
            )
        }
    }
}

@Composable
private fun HistoryScreen(records: List<TradeRecord>, onBack: () -> Unit) {
    Column(Modifier.fillMaxSize().padding(18.dp)) {
        Row(verticalAlignment = Alignment.CenterVertically) {
            OutlinedButton(onClick = onBack) { Text("BACK") }
            Spacer(Modifier.width(12.dp))
            Text("SIGNAL HISTORY", fontWeight = FontWeight.Black, fontSize = 18.sp)
        }
        Spacer(Modifier.height(12.dp))
        if (records.isEmpty()) Text("No saved signals yet.", color = Muted)
        LazyColumn(verticalArrangement = Arrangement.spacedBy(8.dp)) {
            items(records, key = { it.id }) { record ->
                Card(colors = CardDefaults.cardColors(containerColor = Panel)) {
                    Row(
                        Modifier.fillMaxWidth().padding(14.dp),
                        verticalAlignment = Alignment.CenterVertically,
                    ) {
                        Column(Modifier.weight(1f)) {
                            Text("${record.symbol}  •  ${record.marketType}", fontWeight = FontWeight.Bold)
                            Text("${formatTime(record.issuedAt)}  •  ${formatExpiry(record.expiry)}", color = Muted, fontSize = 11.sp)
                        }
                        Column(horizontalAlignment = Alignment.End) {
                            Text(record.direction, color = if (record.direction == "UP") Mint else if (record.direction == "DOWN") Rose else Gold)
                            Text(record.outcome, color = Muted, fontSize = 10.sp)
                        }
                    }
                }
            }
        }
    }
}

@Composable
private fun StatisticsScreen(
    stats: Stats?,
    period: String,
    onPeriod: (String) -> Unit,
    onBack: () -> Unit,
) {
    Column(Modifier.fillMaxSize().padding(18.dp), verticalArrangement = Arrangement.spacedBy(14.dp)) {
        Row(verticalAlignment = Alignment.CenterVertically) {
            OutlinedButton(onClick = onBack) { Text("BACK") }
            Spacer(Modifier.width(12.dp))
            Text("STATISTICS", fontWeight = FontWeight.Black, fontSize = 18.sp)
        }
        Row(horizontalArrangement = Arrangement.spacedBy(5.dp)) {
            listOf("hour", "day", "week", "all").forEach { item ->
                OutlinedButton(
                    onClick = { onPeriod(item) },
                    colors = ButtonDefaults.outlinedButtonColors(
                        contentColor = if (period == item) Mint else Muted,
                    ),
                ) { Text(item.uppercase(Locale.US), fontSize = 10.sp) }
            }
        }
        if (stats == null) {
            CircularProgressIndicator(color = Mint)
        } else {
            StatCard("SIGNALS", stats.total.toString())
            StatCard("WINS / LOSSES", "${stats.wins} / ${stats.losses}")
            StatCard("WIN RATE", stats.winRate?.let { "%.2f%%".format(Locale.US, it) } ?: "—")
            StatCard("MISSED / PENDING", "${stats.missed} / ${stats.pending}")
            StatCard("SESSION DURATION", stats.durationMs?.let(::formatDuration) ?: "All sessions")
            GroupSection("BY INSTRUMENT", stats.byPair)
            GroupSection("BY EXPIRATION (SECONDS)", stats.byExpiry)
            GroupSection("BY MARKET", stats.byMarket)
        }
    }
}

@Composable
private fun GroupSection(title: String, groups: Map<String, GroupStats>) {
    Text(title, color = Muted, fontSize = 11.sp, fontWeight = FontWeight.Bold)
    if (groups.isEmpty()) {
        Text("No results for this range.", color = Muted, fontSize = 11.sp)
    } else {
        groups.toSortedMap().forEach { (name, value) ->
            StatCard(
                name,
                "${value.total} trades  •  ${value.wins}W / ${value.losses}L / ${value.missed} skipped",
            )
        }
    }
}

@Composable
private fun StatCard(label: String, value: String) {
    Card(colors = CardDefaults.cardColors(containerColor = Panel), shape = RoundedCornerShape(16.dp)) {
        Row(
            Modifier.fillMaxWidth().padding(17.dp),
            verticalAlignment = Alignment.CenterVertically,
        ) {
            Text(label, Modifier.weight(1f), color = Muted, fontSize = 11.sp)
            Text(value, color = Color.White, fontSize = 18.sp, fontWeight = FontWeight.Bold)
        }
    }
}

private fun formatExpiry(seconds: Int): String =
    if (seconds < 60) "${seconds}s" else "${seconds / 60}m"

private fun formatTestingExpiry(seconds: Int): String =
    when (seconds) {
        60 -> "1 минута"
        300 -> "5 минут"
        900 -> "15 минут"
        else -> "$seconds сек."
    }

private fun formatTime(timestamp: Long): String =
    SimpleDateFormat("HH:mm:ss", Locale.getDefault()).format(Date(timestamp))

private fun formatDate(timestamp: Long): String =
    SimpleDateFormat("yyyy-MM-dd", Locale.getDefault()).format(Date(timestamp))

private fun formatDateTime(timestamp: Long): String =
    SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.getDefault()).format(Date(timestamp))

private fun formatDuration(milliseconds: Long): String {
    val seconds = milliseconds / 1000
    return "%02d:%02d:%02d".format(seconds / 3600, seconds / 60 % 60, seconds % 60)
}
