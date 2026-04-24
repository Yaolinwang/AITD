const THEME_STORAGE_KEY = "gs-ai-trader-theme";
const TAB_STORAGE_KEY = "gs-ai-trader-tab";
const MODE_STORAGE_KEY = "gs-ai-trader-mode";

const PROVIDER_DEFAULTS = {
  gpt: {
    apiStyle: "openai",
    baseUrl: "https://api.openai.com/v1"
  },
  claude: {
    apiStyle: "anthropic",
    baseUrl: "https://api.anthropic.com/v1"
  },
  deepseek: {
    apiStyle: "openai",
    baseUrl: "https://api.deepseek.com/v1"
  },
  qwen: {
    apiStyle: "openai",
    baseUrl: "https://dashscope.aliyuncs.com/compatible-mode/v1"
  },
  custom: {
    apiStyle: "openai",
    baseUrl: ""
  }
};

const PROVIDER_MODEL_OPTIONS = {
  gpt: [
    "gpt-5.4-mini",
    "gpt-5.4",
    "gpt-5.4-nano",
    "gpt-5.3-chat-latest",
    "gpt-5-mini",
    "gpt-5",
    "gpt-4.1-mini",
    "gpt-4.1",
    "gpt-4o-mini",
    "gpt-4o"
  ],
  claude: [
    "claude-opus-4-7",
    "claude-opus-4-6",
    "claude-sonnet-4-6",
    "claude-haiku-4-5",
    "claude-sonnet-4-20250514",
    "claude-3-7-sonnet-latest",
    "claude-3-5-sonnet-latest",
    "claude-3-5-haiku-latest"
  ],
  deepseek: [
    "deepseek-chat",
    "deepseek-reasoner"
  ],
  qwen: [
    "qwen-plus",
    "qwen-turbo",
    "qwen-max"
  ],
  custom: [
    "gpt-5.4-mini",
    "claude-opus-4-7",
    "deepseek-chat",
    "qwen-plus",
    "gpt-4.1-mini"
  ]
};

const PROMPT_KLINE_FEED_OPTIONS = ["1m", "5m", "15m"];
const PROMPT_KLINE_FEED_DEFAULTS = {
  "1m": { enabled: false, limit: 120 },
  "5m": { enabled: false, limit: 96 },
  "15m": { enabled: true, limit: 64 }
};
const LIVE_EXCHANGE_FALLBACK_CATALOG = [
  {
    id: "binance",
    label: "Binance",
    implemented: true,
    marketSupported: true,
    tradingSupported: true,
    defaultBaseUrl: "https://fapi.binance.com",
    apiKeyPlaceholder: "Binance API key",
    apiSecretPlaceholder: "Binance API secret",
    notes: "当前版本已实现 Binance USDT 永续的行情与实盘接口。"
  },
  {
    id: "okx",
    label: "OKX",
    implemented: true,
    marketSupported: true,
    tradingSupported: true,
    defaultBaseUrl: "https://www.okx.com",
    apiKeyPlaceholder: "OKX API key",
    apiSecretPlaceholder: "OKX API secret",
    apiPassphrasePlaceholder: "OKX API passphrase",
    requiresPassphrase: true,
    notes: "已实现 OKX 永续合约的行情与实盘接口，需要填写 API Passphrase。"
  },
  {
    id: "bybit",
    label: "Bybit",
    implemented: true,
    marketSupported: true,
    tradingSupported: false,
    defaultBaseUrl: "https://api.bybit.com",
    apiKeyPlaceholder: "Bybit API key",
    apiSecretPlaceholder: "Bybit API secret",
    apiPassphrasePlaceholder: "",
    requiresPassphrase: false,
    notes: "已实现 Bybit 线性永续公共行情，实盘接口会在后续版本接入。"
  },
  {
    id: "gateio",
    label: "Gate.io",
    implemented: false,
    marketSupported: false,
    tradingSupported: false,
    defaultBaseUrl: "https://api.gateio.ws",
    apiKeyPlaceholder: "Gate.io API key",
    apiSecretPlaceholder: "Gate.io API secret",
    apiPassphrasePlaceholder: "",
    requiresPassphrase: false,
    notes: "即将支持 Gate.io 合约接口。"
  }
];

function readStoredValue(key, fallback) {
  try {
    return window.localStorage.getItem(key) || fallback;
  } catch {
    return fallback;
  }
}

function writeStoredValue(key, value) {
  try {
    window.localStorage.setItem(key, value);
  } catch {
    // Ignore storage errors in private browsing or restricted environments.
  }
}

const state = {
  latest: null,
  scan: null,
  settings: null,
  tradingState: null,
  tradingSettings: null,
  provider: null,
  prompt: null,
  promptLibrary: null,
  promptTest: null,
  universe: null,
  universeTest: null,
  network: null,
  networkIp: null,
  liveConfig: null,
  logs: null,
  clientErrors: [],
  autoRefreshTimer: null,
  viewMode: readStoredValue(MODE_STORAGE_KEY, "paper") === "live" ? "live" : "paper",
  activeTab: readStoredValue(TAB_STORAGE_KEY, "trade"),
  theme: readStoredValue(THEME_STORAGE_KEY, "dark"),
  promptModalField: null,
  promptEditingPresetId: null,
  providerAdvancedOpen: false,
  positionSort: { key: "symbol", dir: "asc" },
  closedSort: {
    live: { key: "latestClosedAt", dir: "desc" },
    paper: { key: "closedAt", dir: "desc" }
  }
};

const els = {
  tradeRunMeta: document.querySelector("#tradeRunMeta"),
  tradeRefreshHint: document.querySelector("#tradeRefreshHint"),
  themeToggleBtn: document.querySelector("#themeToggleBtn"),
  toggleModeBtn: document.querySelector("#toggleModeBtn"),
  flattenBtn: document.querySelector("#flattenBtn"),
  resetBtn: document.querySelector("#resetBtn"),
  refreshBtn: document.querySelector("#refreshBtn"),
  modeRunnerBtn: document.querySelector("#modeRunnerBtn"),
  modeRunnerMeta: document.querySelector("#modeRunnerMeta"),
  tabButtons: Array.from(document.querySelectorAll("[data-tab-button]")),
  tabPanels: Array.from(document.querySelectorAll("[data-tab-panel]")),
  modeText: document.querySelector("#modeText"),
  equityText: document.querySelector("#equityText"),
  openCountText: document.querySelector("#openCountText"),
  drawdownText: document.querySelector("#drawdownText"),
  accountMeta: document.querySelector("#accountMeta"),
  accountGrid: document.querySelector("#accountGrid"),
  statusBanner: document.querySelector("#statusBanner"),
  positionMeta: document.querySelector("#positionMeta"),
  positionCards: document.querySelector("#positionCards"),
  closedPositionMeta: document.querySelector("#closedPositionMeta"),
  closedPositionList: document.querySelector("#closedPositionList"),
  candidateMeta: document.querySelector("#candidateMeta"),
  candidateList: document.querySelector("#candidateList"),
  decisionMeta: document.querySelector("#decisionMeta"),
  decisionLog: document.querySelector("#decisionLog"),
  tradingSettingsForm: document.querySelector("#tradingSettingsForm"),
  saveTradingSettingsBtn: document.querySelector("#saveTradingSettingsBtn"),
  activeExchangeInput: document.querySelector("#activeExchangeInput"),
  decisionIntervalInput: document.querySelector("#decisionIntervalInput"),
  initialCapitalInput: document.querySelector("#initialCapitalInput"),
  maxNewPositionsInput: document.querySelector("#maxNewPositionsInput"),
  maxOpenPositionsInput: document.querySelector("#maxOpenPositionsInput"),
  maxPositionNotionalInput: document.querySelector("#maxPositionNotionalInput"),
  maxGrossExposureInput: document.querySelector("#maxGrossExposureInput"),
  maxDrawdownInput: document.querySelector("#maxDrawdownInput"),
  riskPerTradeInput: document.querySelector("#riskPerTradeInput"),
  minConfidenceInput: document.querySelector("#minConfidenceInput"),
  paperFeesRow: document.querySelector("#paperFeesRow"),
  paperFeesInput: document.querySelector("#paperFeesInput"),
  allowShortsInput: document.querySelector("#allowShortsInput"),
  pageAutoRefreshInput: document.querySelector("#pageAutoRefreshInput"),
  settingsFeedback: document.querySelector("#settingsFeedback"),
  providerForm: document.querySelector("#providerForm"),
  saveProviderBtn: document.querySelector("#saveProviderBtn"),
  providerPresetInput: document.querySelector("#providerPresetInput"),
  providerApiStyleInput: document.querySelector("#providerApiStyleInput"),
  providerModelInput: document.querySelector("#providerModelInput"),
  providerCustomModelWrap: document.querySelector("#providerCustomModelWrap"),
  providerCustomModelInput: document.querySelector("#providerCustomModelInput"),
  providerBaseUrlInput: document.querySelector("#providerBaseUrlInput"),
  providerApiKeyInput: document.querySelector("#providerApiKeyInput"),
  providerAdvancedToggleBtn: document.querySelector("#providerAdvancedToggleBtn"),
  providerAdvancedFields: document.querySelector("#providerAdvancedFields"),
  providerTimeoutInput: document.querySelector("#providerTimeoutInput"),
  providerTemperatureInput: document.querySelector("#providerTemperatureInput"),
  providerMaxTokensInput: document.querySelector("#providerMaxTokensInput"),
  providerFeedback: document.querySelector("#providerFeedback"),
  promptForm: document.querySelector("#promptForm"),
  promptNameInput: document.querySelector("#promptNameInput"),
  promptRoleInput: document.querySelector("#promptRoleInput"),
  promptCorePrinciplesInput: document.querySelector("#promptCorePrinciplesInput"),
  promptEntryPreferencesInput: document.querySelector("#promptEntryPreferencesInput"),
  promptPositionManagementInput: document.querySelector("#promptPositionManagementInput"),
  promptKlineEnabledInputs: Array.from(document.querySelectorAll("[data-prompt-kline-enabled]")),
  promptKlineLimitInputs: Array.from(document.querySelectorAll("[data-prompt-kline-limit]")),
  promptExpandButtons: Array.from(document.querySelectorAll("[data-prompt-expand]")),
  promptModal: document.querySelector("#promptModal"),
  promptModalTitle: document.querySelector("#promptModalTitle"),
  promptModalInput: document.querySelector("#promptModalInput"),
  promptModalApplyBtn: document.querySelector("#promptModalApplyBtn"),
  promptModalCloseBtn: document.querySelector("#promptModalCloseBtn"),
  savePromptPresetBtn: document.querySelector("#savePromptPresetBtn"),
  promptPresetMeta: document.querySelector("#promptPresetMeta"),
  testPromptBtn: document.querySelector("#testPromptBtn"),
  promptFeedback: document.querySelector("#promptFeedback"),
  promptTestMeta: document.querySelector("#promptTestMeta"),
  promptTestOutput: document.querySelector("#promptTestOutput"),
  savedPromptMeta: document.querySelector("#savedPromptMeta"),
  savedPromptList: document.querySelector("#savedPromptList"),
  universeMeta: document.querySelector("#universeMeta"),
  staticUniverseEnabledInput: document.querySelector("#staticUniverseEnabledInput"),
  universeForm: document.querySelector("#universeForm"),
  universeSymbolsInput: document.querySelector("#universeSymbolsInput"),
  staticUniverseSection: document.querySelector("#staticUniverseSection"),
  dynamicUniverseSection: document.querySelector("#dynamicUniverseSection"),
  dynamicUniverseEnabledInput: document.querySelector("#dynamicUniverseEnabledInput"),
  candidateSourceCodeInput: document.querySelector("#candidateSourceCodeInput"),
  saveUniverseBtn: document.querySelector("#saveUniverseBtn"),
  saveDynamicUniverseBtn: document.querySelector("#saveDynamicUniverseBtn"),
  testUniverseBtn: document.querySelector("#testUniverseBtn"),
  universeFeedback: document.querySelector("#universeFeedback"),
  universePreviewMeta: document.querySelector("#universePreviewMeta"),
  universePreview: document.querySelector("#universePreview"),
  universeTestMeta: document.querySelector("#universeTestMeta"),
  logMeta: document.querySelector("#logMeta"),
  logSummaryGrid: document.querySelector("#logSummaryGrid"),
  logOutput: document.querySelector("#logOutput"),
  networkForm: document.querySelector("#networkForm"),
  saveNetworkBtn: document.querySelector("#saveNetworkBtn"),
  proxyEnabledInput: document.querySelector("#proxyEnabledInput"),
  proxyUrlInput: document.querySelector("#proxyUrlInput"),
  noProxyInput: document.querySelector("#noProxyInput"),
  networkFeedback: document.querySelector("#networkFeedback"),
  liveConfigSection: document.querySelector("#liveConfigSection"),
  liveConfigForm: document.querySelector("#liveConfigForm"),
  saveLiveConfigBtn: document.querySelector("#saveLiveConfigBtn"),
  liveIpText: document.querySelector("#liveIpText"),
  liveExchangeInput: document.querySelector("#liveExchangeInput"),
  liveExchangeMeta: document.querySelector("#liveExchangeMeta"),
  liveConfigEnabledInput: document.querySelector("#liveConfigEnabledInput"),
  liveDryRunInput: document.querySelector("#liveDryRunInput"),
  liveApiKeyInput: document.querySelector("#liveApiKeyInput"),
  liveApiSecretInput: document.querySelector("#liveApiSecretInput"),
  liveApiPassphraseWrap: document.querySelector("#liveApiPassphraseWrap"),
  liveApiPassphraseInput: document.querySelector("#liveApiPassphraseInput"),
  liveBaseUrlInput: document.querySelector("#liveBaseUrlInput"),
  liveDefaultLeverageInput: document.querySelector("#liveDefaultLeverageInput"),
  liveMarginTypeInput: document.querySelector("#liveMarginTypeInput"),
  liveConfigFeedback: document.querySelector("#liveConfigFeedback")
};

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function fmtNumber(value, digits = 2) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "n/a";
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: digits }).format(n);
}

function fmtUsd(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "n/a";
  return `$${fmtNumber(n, 2)}`;
}

function fmtSignedUsd(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "n/a";
  return `${n > 0 ? "+" : ""}${fmtUsd(n)}`;
}

function fmtPct(value, digits = 2) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "n/a";
  return `${n.toFixed(digits)}%`;
}

function fmtPrice(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "n/a";
  if (Math.abs(n) >= 1000) return `$${fmtNumber(n, 2)}`;
  if (Math.abs(n) >= 1) return `$${n.toFixed(4).replace(/\.?0+$/, "")}`;
  return `$${n.toPrecision(4)}`;
}

function recordClientError(message) {
  const text = String(message || "").trim() || "未知前端错误";
  const line = `[${new Date().toLocaleString("zh-CN", { hour12: false })}] [CLIENT] ${text}`;
  state.clientErrors = [...(state.clientErrors || []), line].slice(-20);
  if (els.logMeta) {
    els.logMeta.textContent = "检测到前端错误";
  }
  if (els.logOutput) {
    const serverText = els.logOutput.textContent && !els.logOutput.textContent.startsWith("暂无日志")
      ? els.logOutput.textContent
      : "";
    els.logOutput.textContent = [...state.clientErrors, serverText].filter(Boolean).join("\n");
  }
  if (els.statusBanner) {
    els.statusBanner.classList.add("visible", "active");
    els.statusBanner.classList.remove("quiet");
    els.statusBanner.innerHTML = `<div class="adaptive-note">前端错误：${escapeHtml(text)}</div>`;
  }
  try {
    console.error("[AITD client error]", text);
  } catch {
    // Ignore console access issues.
  }
}

function modeName(mode) {
  return mode === "live" ? "实盘" : "模拟盘";
}

function currentViewMode() {
  return state.viewMode === "live" ? "live" : "paper";
}

function fmtDateTime(value) {
  if (!value) return "n/a";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "n/a";
  return date.toLocaleString("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false
  });
}

function parseSortTime(value) {
  if (!value) return null;
  const ms = Date.parse(value);
  return Number.isFinite(ms) ? ms : null;
}

function compareSortValues(left, right, dir = "asc") {
  const direction = dir === "desc" ? -1 : 1;
  if (left === right) return 0;
  if (left === null || left === undefined || left === "") return 1;
  if (right === null || right === undefined || right === "") return -1;
  if (typeof left === "number" && typeof right === "number") {
    return (left - right) * direction;
  }
  return String(left).localeCompare(String(right), "en-US", { numeric: true, sensitivity: "base" }) * direction;
}

function sortIndicator(sortState, key) {
  if (!sortState || sortState.key !== key) return "·";
  return sortState.dir === "desc" ? "↓" : "↑";
}

function nextSortState(current, key, defaultDir = "asc") {
  if (current?.key === key) {
    return { key, dir: current.dir === "asc" ? "desc" : "asc" };
  }
  return { key, dir: defaultDir };
}

function sortHeaderButton(label, target, key, sortState, defaultDir = "asc") {
  const active = sortState?.key === key;
  return `
    <button
      type="button"
      class="sort-header-button ${active ? "active" : ""}"
      data-sort-target="${escapeHtml(target)}"
      data-sort-key="${escapeHtml(key)}"
      data-sort-default-dir="${escapeHtml(defaultDir)}"
    >
      <span>${escapeHtml(label)}</span>
      <em>${escapeHtml(sortIndicator(sortState, key))}</em>
    </button>
  `;
}

function fmtMiniTime(value) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "";
  return date.toLocaleString("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false
  });
}

function parseTimeMs(value) {
  const date = new Date(value || "");
  const ms = date.getTime();
  return Number.isFinite(ms) ? ms : null;
}

function alignedDecisionSlotMs(timestampMs, intervalMinutes) {
  const intervalMs = Math.max(1, Number(intervalMinutes || 5)) * 60 * 1000;
  const timezoneOffsetMs = 8 * 60 * 60 * 1000;
  return Math.floor((timestampMs + timezoneOffsetMs) / intervalMs) * intervalMs - timezoneOffsetMs;
}

function classifyActionKinds(actions) {
  const kinds = new Set();
  (Array.isArray(actions) ? actions : []).forEach((action) => {
    const type = String(action?.type || "").trim().toLowerCase();
    if (type === "open") {
      kinds.add("open");
      return;
    }
    if (type === "close") {
      kinds.add("close");
      return;
    }
    if (type === "reduce" || type === "update" || type === "circuit_breaker") {
      kinds.add("adjust");
    }
  });
  return [...kinds];
}

function actionKindLabel(kind) {
  if (kind === "open") return "开仓";
  if (kind === "close") return "平仓";
  if (kind === "adjust") return "调整";
  return kind || "无";
}

function buildDecisionActionDetails(decision) {
  const beforePositions = Array.isArray(decision?.accountBefore?.openPositions) ? decision.accountBefore.openPositions : [];
  const afterPositions = Array.isArray(decision?.accountAfter?.openPositions) ? decision.accountAfter.openPositions : [];
  const beforeBySymbol = new Map(beforePositions.map((position) => [String(position.symbol || "").toUpperCase(), position]));
  const afterBySymbol = new Map(afterPositions.map((position) => [String(position.symbol || "").toUpperCase(), position]));
  const lines = [];
  (Array.isArray(decision?.actions) ? decision.actions : []).forEach((action) => {
    const type = String(action?.type || "").trim().toLowerCase();
    const symbol = String(action?.symbol || "").trim().toUpperCase();
    const side = String(action?.side || "").trim().toLowerCase();
    const sideLabel = side === "short" ? "空" : "多";
    const before = beforeBySymbol.get(symbol);
    const after = afterBySymbol.get(symbol);
    if (type === "open") {
      const quantity = Number(after?.quantity);
      const entryPrice = after?.entryPrice ?? after?.markPrice;
      const quantityText = Number.isFinite(quantity) ? fmtNumber(quantity, 4) : fmtUsd(action?.notionalUsd);
      lines.push(`${symbol} 开${sideLabel} ${quantityText} @ ${fmtPrice(entryPrice)}`);
      return;
    }
    if (type === "close" || type === "circuit_breaker") {
      const quantity = Number(before?.quantity);
      const exitPrice = before?.markPrice ?? before?.lastMarkPrice ?? before?.entryPrice;
      const actionLabel = type === "circuit_breaker" ? "风控平仓" : `平${sideLabel}`;
      const quantityText = Number.isFinite(quantity) ? fmtNumber(quantity, 4) : "n/a";
      lines.push(`${symbol} ${actionLabel} ${quantityText} @ ${fmtPrice(exitPrice)}`);
      return;
    }
    if (type === "reduce") {
      const baseQty = Number(before?.quantity);
      const reduceFraction = Number(action?.reduceFraction || 0);
      const reduceQty = Number.isFinite(baseQty) && Number.isFinite(reduceFraction) ? baseQty * reduceFraction : null;
      const execPrice = before?.markPrice ?? before?.lastMarkPrice ?? before?.entryPrice;
      const qtyText = Number.isFinite(reduceQty) ? fmtNumber(reduceQty, 4) : `${fmtPct((reduceFraction || 0) * 100, 0)}`;
      lines.push(`${symbol} 减仓${sideLabel} ${qtyText} @ ${fmtPrice(execPrice)}`);
      return;
    }
    if (type === "update") {
      const quantity = Number(after?.quantity ?? before?.quantity);
      const stopLoss = action?.stopLoss ?? after?.stopLoss;
      const takeProfit = action?.takeProfit ?? after?.takeProfit;
      const qtyText = Number.isFinite(quantity) ? fmtNumber(quantity, 4) : "n/a";
      lines.push(`${symbol} ${sideLabel}单 ${qtyText} | 止损 ${fmtPrice(stopLoss)} | 止盈 ${fmtPrice(takeProfit)}`);
      return;
    }
    lines.push(`${symbol || "仓位"} ${actionKindLabel(type)}`);
  });
  return lines;
}

function pnlClass(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n === 0) return "";
  return n > 0 ? "pnl-up" : "pnl-down";
}

function applyTheme(theme) {
  const nextTheme = theme === "light" ? "light" : "dark";
  state.theme = nextTheme;
  document.documentElement.dataset.theme = nextTheme;
  writeStoredValue(THEME_STORAGE_KEY, nextTheme);
  els.themeToggleBtn.textContent = nextTheme === "light" ? "切到深色" : "切到浅色";
}

function setActiveTab(tab) {
  const nextTab = ["trade", "prompt", "universe", "log"].includes(tab) ? tab : "trade";
  state.activeTab = nextTab;
  writeStoredValue(TAB_STORAGE_KEY, nextTab);
  els.tabButtons.forEach((button) => {
    const active = button.dataset.tabButton === nextTab;
    button.classList.toggle("active", active);
    button.setAttribute("aria-selected", String(active));
    button.tabIndex = active ? 0 : -1;
  });
  els.tabPanels.forEach((panel) => {
    const active = panel.dataset.tabPanel === nextTab;
    panel.hidden = !active;
    panel.classList.toggle("is-active", active);
  });
}

async function getJson(url) {
  const response = await fetch(url, { cache: "no-store" });
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.error || `${response.status} ${response.statusText}`);
  return payload;
}

async function postJson(url, body = {}) {
  const response = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body)
  });
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.error || `${response.status} ${response.statusText}`);
  return payload;
}

function activeHistory() {
  if (!state.tradingState) return { decisions: [], closedTrades: [], exchangeClosedTrades: [], sessionStartedAt: null };
  return currentViewMode() === "live"
    ? state.tradingState.liveHistory
    : state.tradingState.paperHistory;
}

function activeAccount() {
  return currentViewMode() === "live"
    ? (state.tradingState?.liveAccount || {})
    : (state.tradingState?.paperAccount || {});
}

function activeBook() {
  return currentViewMode() === "live"
    ? (state.tradingState?.liveBook || {})
    : (state.tradingState?.paperBook || {});
}

function sortedPositions() {
  const positions = (activeAccount().openPositions || []).slice();
  const sortState = state.positionSort || { key: "symbol", dir: "asc" };
  const getters = {
    symbol: (item) => item.symbol || "",
    side: (item) => item.side || "",
    entryPrice: (item) => Number(item.entryPrice),
    markPrice: (item) => Number(item.markPrice),
    quantity: (item) => Number(item.quantity),
    notionalUsd: (item) => Number(item.notionalUsd),
    stopLoss: (item) => Number(item.stopLoss),
    takeProfit: (item) => Number(item.takeProfit),
    unrealizedPnl: (item) => Number(item.unrealizedPnl),
    pnlPct: (item) => Number(item.pnlPct)
  };
  const getter = getters[sortState.key] || getters.symbol;
  return positions.sort((left, right) => compareSortValues(getter(left), getter(right), sortState.dir));
}

function liveClosedDisplaySource() {
  const history = activeHistory();
  const exchangeRecords = Array.isArray(history.exchangeClosedTrades) ? history.exchangeClosedTrades : [];
  if (exchangeRecords.length) {
    return {
      source: "exchange",
      records: exchangeRecords.map((trade) => ({
        symbol: trade.symbol,
        side: trade.side || "",
        quantity: trade.quantity,
        notionalUsd: trade.notionalUsd,
        realizedPnl: trade.realizedPnl,
        closedAt: trade.closedAt,
        info: trade.info || "交易所已实现记录"
      }))
    };
  }
  const localRecords = Array.isArray(history.closedTrades) ? history.closedTrades : [];
  return {
    source: "local",
    records: localRecords.map((trade) => ({
      symbol: trade.symbol,
      side: trade.side || "",
      quantity: trade.quantity,
      notionalUsd: trade.notionalUsd,
      realizedPnl: trade.realizedPnl,
      closedAt: trade.closedAt,
      info: trade.exitReason || "本地已同步平仓记录"
    }))
  };
}

function groupedLiveClosedTrades() {
  const display = liveClosedDisplaySource();
  const records = display.records;
  const groups = new Map();
  records.forEach((trade) => {
    const symbol = String(trade.symbol || "").trim().toUpperCase();
    if (!symbol) return;
    if (!groups.has(symbol)) {
      groups.set(symbol, {
        symbol,
        totalRealized: 0,
        count: 0,
        winCount: 0,
        winRate: 0,
        latestClosedAt: null,
        trades: []
      });
    }
    const group = groups.get(symbol);
    const closedAt = trade.closedAt || null;
    const realizedPnl = Number(trade.realizedPnl || 0);
    group.totalRealized += Number(trade.realizedPnl || 0);
    group.count += 1;
    if (realizedPnl > 0) group.winCount += 1;
    group.winRate = group.count ? (group.winCount / group.count) * 100 : 0;
    if (!group.latestClosedAt || compareSortValues(parseSortTime(closedAt), parseSortTime(group.latestClosedAt), "desc") < 0) {
      group.latestClosedAt = closedAt;
    }
    group.trades.push(trade);
  });
  const sortState = state.closedSort?.live || { key: "latestClosedAt", dir: "desc" };
  const getters = {
    symbol: (item) => item.symbol,
    totalRealized: (item) => Number(item.totalRealized),
    count: (item) => Number(item.count),
    winRate: (item) => Number(item.winRate),
    latestClosedAt: (item) => parseSortTime(item.latestClosedAt)
  };
  const getter = getters[sortState.key] || getters.latestClosedAt;
  return {
    source: display.source,
    groups: Array.from(groups.values())
      .map((group) => ({
        ...group,
        trades: group.trades.slice().sort((left, right) => compareSortValues(parseSortTime(left.closedAt), parseSortTime(right.closedAt), "desc"))
      }))
      .sort((left, right) => compareSortValues(getter(left), getter(right), sortState.dir))
  };
}

function groupedPaperClosedTrades() {
  const records = (activeHistory().closedTrades || []).slice();
  const groups = new Map();
  records.forEach((trade) => {
    const symbol = String(trade.symbol || "").trim().toUpperCase();
    if (!symbol) return;
    if (!groups.has(symbol)) {
      groups.set(symbol, {
        symbol,
        totalRealized: 0,
        count: 0,
        winCount: 0,
        winRate: 0,
        latestClosedAt: null,
        trades: []
      });
    }
    const group = groups.get(symbol);
    const closedAt = trade.closedAt || null;
    const realizedPnl = Number(trade.realizedPnl || 0);
    group.totalRealized += realizedPnl;
    group.count += 1;
    if (realizedPnl > 0) group.winCount += 1;
    group.winRate = group.count ? (group.winCount / group.count) * 100 : 0;
    if (!group.latestClosedAt || compareSortValues(parseSortTime(closedAt), parseSortTime(group.latestClosedAt), "desc") < 0) {
      group.latestClosedAt = closedAt;
    }
    group.trades.push(trade);
  });
  const sortState = state.closedSort?.paper || { key: "latestClosedAt", dir: "desc" };
  const getters = {
    symbol: (item) => item.symbol,
    totalRealized: (item) => Number(item.totalRealized),
    count: (item) => Number(item.count),
    winRate: (item) => Number(item.winRate),
    latestClosedAt: (item) => parseSortTime(item.latestClosedAt)
  };
  const getter = getters[sortState.key] || getters.latestClosedAt;
  return Array.from(groups.values())
    .map((group) => ({
      ...group,
      trades: group.trades.slice().sort((left, right) => compareSortValues(parseSortTime(left.closedAt), parseSortTime(right.closedAt), "desc"))
    }))
    .sort((left, right) => compareSortValues(getter(left), getter(right), sortState.dir));
}

function currentRunner() {
  return currentViewMode() === "live"
    ? (state.tradingState?.liveRunner || {})
    : (state.tradingState?.paperRunner || {});
}

function currentRunnerEnabled() {
  return currentViewMode() === "live"
    ? state.tradingState?.liveTradingEnabled === true
    : state.tradingState?.paperTradingEnabled === true;
}

function currentNextDueAt() {
  return currentViewMode() === "live"
    ? state.tradingState?.liveNextDecisionDueAt
    : state.tradingState?.paperNextDecisionDueAt;
}

function desiredCandidateExchange() {
  if (currentViewMode() === "live") {
    return String(state.liveConfig?.exchange || state.tradingSettings?.activeExchange || "binance").trim().toLowerCase();
  }
  return String(state.tradingSettings?.activeExchange || "binance").trim().toLowerCase();
}

function latestCandidateUniverse() {
  const desiredExchange = desiredCandidateExchange();
  const scanExchange = String(state.scan?.exchange || "").trim().toLowerCase();
  const scanCandidates = Array.isArray(state.scan?.opportunities) ? state.scan.opportunities : [];
  if (scanCandidates.length && (!desiredExchange || !scanExchange || scanExchange === desiredExchange)) {
    return scanCandidates;
  }
  const decisions = (activeHistory().decisions || []).slice().reverse();
  const latestDecision = decisions[0];
  if (latestDecision?.candidateUniverse?.length) return latestDecision.candidateUniverse;
  return scanCandidates;
}

function candidateDiagnostics() {
  const savedSymbols = Array.isArray(state.universe?.symbols) ? state.universe.symbols : [];
  const candidates = latestCandidateUniverse();
  const invalidSymbols = Array.isArray(state.scan?.candidateSource?.invalidSymbols) ? state.scan.candidateSource.invalidSymbols : [];
  const skippedSymbols = Array.isArray(state.scan?.skippedSymbols) ? state.scan.skippedSymbols : [];
  const validSymbolCount = Number.isFinite(Number(state.scan?.universeSize)) && Number(state.scan?.universeSize) > 0
    ? Number(state.scan.universeSize)
    : Math.max(0, savedSymbols.length - invalidSymbols.length);
  return {
    savedSymbols,
    validSymbolCount,
    opportunityCount: candidates.length,
    invalidSymbols,
    skippedSymbols
  };
}

function latestScanOpportunities() {
  return Array.isArray(state.scan?.opportunities) ? state.scan.opportunities : [];
}

function buildEquitySeries() {
  const account = activeAccount();
  const history = activeHistory();
  const book = activeBook();
  const decisions = Array.isArray(history?.decisionTimeline) && history.decisionTimeline.length
    ? history.decisionTimeline
    : (Array.isArray(book?.decisions) ? book.decisions : []);
  const intervalMinutes = Math.max(1, Number(state.tradingSettings?.decisionIntervalMinutes || 5));
  const initial = Number(account.initialCapitalUsd || 0);
  const pointsBySlot = new Map();
  decisions.forEach((decision) => {
    const equity = Number(
      decision?.equityUsd
      ?? decision?.accountAfter?.equityUsd
    );
    const actualAt = decision?.finishedAt || decision?.startedAt || null;
    const actualMs = parseTimeMs(actualAt);
    if (!Number.isFinite(equity) || actualMs === null) return;
    const slotMs = alignedDecisionSlotMs(actualMs, intervalMinutes);
    const slotKey = String(slotMs);
    const kinds = classifyActionKinds(decision?.actions);
    const existing = pointsBySlot.get(slotKey);
    if (!existing) {
      pointsBySlot.set(slotKey, {
        label: decision.id || "decision",
        at: new Date(slotMs).toISOString(),
        actualAt,
        actualMs,
        slotMs,
        equity,
        actionKinds: new Set(kinds),
        actionDetails: buildDecisionActionDetails(decision)
      });
      return;
    }
    kinds.forEach((kind) => existing.actionKinds.add(kind));
    existing.actionDetails = [...(existing.actionDetails || []), ...buildDecisionActionDetails(decision)];
    if (actualMs >= existing.actualMs) {
      existing.label = decision.id || existing.label;
      existing.at = new Date(slotMs).toISOString();
      existing.actualAt = actualAt;
      existing.actualMs = actualMs;
      existing.slotMs = slotMs;
      existing.equity = equity;
    }
  });
  const currentEquity = Number(account.equityUsd);
  if (Number.isFinite(currentEquity)) {
    const currentMs = Date.now();
    const slotMs = alignedDecisionSlotMs(currentMs, intervalMinutes);
    const slotKey = String(slotMs);
    const existing = pointsBySlot.get(slotKey);
    if (!existing) {
      pointsBySlot.set(slotKey, {
        label: "当前",
        at: new Date(slotMs).toISOString(),
        actualAt: new Date(currentMs).toISOString(),
        actualMs: currentMs,
        slotMs,
        equity: currentEquity,
        actionKinds: new Set(),
        actionDetails: []
      });
    } else if (Math.abs(existing.equity - currentEquity) > 1e-9) {
      existing.label = "当前";
      existing.at = new Date(slotMs).toISOString();
      existing.actualAt = new Date(currentMs).toISOString();
      existing.actualMs = currentMs;
      existing.equity = currentEquity;
    }
  }
  const series = [...pointsBySlot.values()]
    .sort((left, right) => left.slotMs - right.slotMs)
    .map((point) => ({
      ...point,
      actionKinds: [...point.actionKinds],
      actionDetails: Array.from(new Set(point.actionDetails || []))
    }))
    .filter((point) => Number.isFinite(point.equity));
  if (series.length || !Number.isFinite(initial) || initial <= 0) {
    return series;
  }
  return [{
    label: "起点",
    at: null,
    actualAt: null,
    actualMs: null,
    slotMs: null,
    equity: initial,
    actionKinds: [],
    actionDetails: []
  }];
}

function renderEquityChart() {
  const series = buildEquitySeries();
  if (!series.length) {
    return `<div class="account-chart-empty">暂无权益数据。</div>`;
  }
  if (series.length === 1) {
    return `
      <div class="account-chart-empty">
        <strong>${escapeHtml(fmtUsd(series[0].equity))}</strong>
        <span>只有一个权益点，等更多交易后这里会显示曲线。</span>
      </div>
    `;
  }
  const values = series.map((item) => item.equity);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = Math.max(max - min, 1);
  const width = 520;
  const height = 180;
  const padX = 18;
  const padY = 18;
  const innerWidth = width - padX * 2;
  const innerHeight = height - padY * 2;
  const points = series.map((item, index) => {
    const x = padX + (innerWidth * index / Math.max(series.length - 1, 1));
    const y = padY + innerHeight - (((item.equity - min) / span) * innerHeight);
    return { x, y, ...item };
  });
  const polyline = points.map((point) => `${point.x},${point.y}`).join(" ");
  const areaPath = `M ${points[0].x} ${height - padY} L ${points.map((point) => `${point.x} ${point.y}`).join(" L ")} L ${points[points.length - 1].x} ${height - padY} Z`;
  const firstLabel = fmtMiniTime(series[0].at) || "开始";
  const lastLabel = fmtMiniTime(series[series.length - 1].at) || "当前";
  return `
    <div class="equity-chart-frame">
      <div class="equity-chart-meta-row">
        <span>最低 ${escapeHtml(fmtUsd(min))}</span>
        <span>最高 ${escapeHtml(fmtUsd(max))}</span>
      </div>
      <div class="equity-chart-surface">
        <svg viewBox="0 0 ${width} ${height}" class="equity-chart-svg" aria-label="Equity chart" preserveAspectRatio="none">
          <path d="${areaPath}" class="equity-chart-area"></path>
          <polyline points="${polyline}" class="equity-chart-line"></polyline>
          ${points.map((point) => `<circle cx="${point.x}" cy="${point.y}" r="3" class="equity-chart-point"></circle>`).join("")}
          ${points.map((point) => `
            <circle
              cx="${point.x}"
              cy="${point.y}"
              r="11"
              class="equity-chart-hit"
              data-time="${escapeHtml(fmtDateTime(point.actualAt || point.at))}"
              data-equity="${escapeHtml(fmtUsd(point.equity))}"
              data-actions="${escapeHtml((point.actionDetails || []).join("||") || "无")}"
            ></circle>
          `).join("")}
        </svg>
        <div class="equity-chart-tooltip" hidden></div>
      </div>
      <div class="equity-chart-axis">
        <span>${escapeHtml(firstLabel)}</span>
        <span>${escapeHtml(lastLabel)}</span>
      </div>
    </div>
  `;
}

function wireEquityChartTooltip() {
  const frame = els.accountGrid?.querySelector(".equity-chart-frame");
  if (!frame) return;
  const tooltip = frame.querySelector(".equity-chart-tooltip");
  const hits = Array.from(frame.querySelectorAll(".equity-chart-hit"));
  if (!tooltip || !hits.length) return;
  const positionTooltip = (event) => {
    const frameRect = frame.getBoundingClientRect();
    const tooltipRect = tooltip.getBoundingClientRect();
    const rawLeft = event.clientX - frameRect.left + 14;
    const rawTop = event.clientY - frameRect.top - tooltipRect.height - 14;
    const maxLeft = Math.max(8, frameRect.width - tooltipRect.width - 8);
    const left = Math.min(Math.max(8, rawLeft), maxLeft);
    const top = rawTop < 8 ? (event.clientY - frameRect.top + 18) : rawTop;
    tooltip.style.left = `${left}px`;
    tooltip.style.top = `${top}px`;
  };
  hits.forEach((hit) => {
    const showTooltip = (event) => {
      const actionLines = String(hit.dataset.actions || "无")
        .split("||")
        .map((item) => item.trim())
        .filter(Boolean);
      tooltip.innerHTML = `
        <strong>${escapeHtml(hit.dataset.time || "n/a")}</strong>
        <span>Equity ${escapeHtml(hit.dataset.equity || "n/a")}</span>
        ${actionLines.map((line) => `<span>${escapeHtml(line)}</span>`).join("")}
      `;
      tooltip.hidden = false;
      positionTooltip(event);
    };
    hit.addEventListener("mouseenter", showTooltip);
    hit.addEventListener("mousemove", showTooltip);
    hit.addEventListener("mouseleave", () => {
      tooltip.hidden = true;
    });
  });
}

function buildSymbolPnlRows() {
  const account = activeAccount();
  const history = activeHistory();
  const liveExchange = currentViewMode() === "live" && account.accountSource === "exchange";
  const aggregates = new Map();
  const ensureRow = (symbol) => {
    const key = String(symbol || "").trim().toUpperCase();
    if (!key) return null;
    if (!aggregates.has(key)) {
      aggregates.set(key, { symbol: key, pnl: 0 });
    }
    return aggregates.get(key);
  };
  if (liveExchange) {
    const closedSource = liveClosedDisplaySource();
    (closedSource.records || []).forEach((trade) => {
      const row = ensureRow(trade.symbol);
      if (!row) return;
      row.pnl += Number(trade.realizedPnl || 0);
    });
    (account.openPositions || []).forEach((position) => {
      const row = ensureRow(position.symbol);
      if (!row) return;
      row.pnl += Number(position.unrealizedPnl || 0);
    });
    return Array.from(aggregates.values())
      .sort((left, right) => Math.abs(right.pnl) - Math.abs(left.pnl))
      .slice(0, 10);
  }
  (history.closedTrades || []).forEach((trade) => {
    const row = ensureRow(trade.symbol);
    if (!row) return;
    row.pnl += Number(trade.realizedPnl || 0);
  });
  (account.openPositions || []).forEach((position) => {
    const row = ensureRow(position.symbol);
    if (!row) return;
    row.pnl += Number(position.unrealizedPnl || 0);
  });
  return Array.from(aggregates.values())
    .sort((left, right) => right.pnl - left.pnl)
    .slice(0, 10);
}

function renderSymbolPnlChart() {
  const rows = buildSymbolPnlRows();
  if (!rows.length) {
    return `<div class="account-chart-empty">${currentViewMode() === "live" ? "暂无可展示的实盘持仓浮盈亏。" : "暂无品种收益数据。"}</div>`;
  }
  const maxAbs = Math.max(...rows.map((row) => Math.abs(row.pnl)), 1);
  return `
    <div class="symbol-pnl-chart">
      ${rows.map((row) => {
        const widthPct = Math.max(6, (Math.abs(row.pnl) / maxAbs) * 100);
        const cls = row.pnl >= 0 ? "up" : "down";
        return `
          <div class="symbol-pnl-row">
            <span class="symbol-pnl-label">${escapeHtml(row.symbol)}</span>
            <div class="symbol-pnl-track">
              <div class="symbol-pnl-fill ${cls}" style="width:${widthPct}%"></div>
            </div>
            <span class="symbol-pnl-value ${pnlClass(row.pnl)}">${escapeHtml(fmtSignedUsd(row.pnl))}</span>
          </div>
        `;
      }).join("")}
    </div>
  `;
}

function providerLabel(provider) {
  if (!provider || typeof provider !== "object") return "n/a";
  const parts = [provider.preset || provider.apiStyle, provider.model];
  return parts.filter(Boolean).join(" / ") || "n/a";
}

function providerModelsForPreset(preset) {
  return PROVIDER_MODEL_OPTIONS[preset] || PROVIDER_MODEL_OPTIONS.custom;
}

function refreshProviderModelOptions(currentModel = "") {
  const preset = els.providerPresetInput.value || "gpt";
  const options = providerModelsForPreset(preset);
  const hasCurrent = currentModel && options.includes(currentModel);
  const useCustom = currentModel && !hasCurrent;
  const optionValues = hasCurrent ? options : options.slice();
  els.providerModelInput.innerHTML = optionValues.map((value) => `
    <option value="${escapeHtml(value)}">${escapeHtml(value)}</option>
  `).join("") + `<option value="__custom__">自定义模型名</option>`;
  els.providerModelInput.value = useCustom ? "__custom__" : (currentModel || options[0] || "__custom__");
  els.providerCustomModelInput.value = useCustom ? currentModel : "";
  syncProviderCustomModelVisibility();
}

function syncProviderCustomModelVisibility() {
  const useCustom = els.providerModelInput.value === "__custom__";
  els.providerCustomModelWrap.hidden = !useCustom;
  if (!useCustom) {
    els.providerCustomModelInput.value = "";
  }
}

function currentProviderModel() {
  return els.providerModelInput.value === "__custom__"
    ? els.providerCustomModelInput.value.trim()
    : els.providerModelInput.value.trim();
}

function buildProviderPayload() {
  return {
    preset: els.providerPresetInput.value,
    apiStyle: els.providerApiStyleInput.value,
    model: currentProviderModel(),
    baseUrl: els.providerBaseUrlInput.value.trim(),
    apiKey: els.providerApiKeyInput.value.trim(),
    timeoutSeconds: Number(els.providerTimeoutInput.value || 45),
    temperature: Number(els.providerTemperatureInput.value || 0.2),
    maxOutputTokens: Number(els.providerMaxTokensInput.value || 1200)
  };
}

function setProviderAdvancedOpen(open) {
  const isOpen = open === true;
  state.providerAdvancedOpen = isOpen;
  els.providerAdvancedFields.hidden = !isOpen;
  els.providerAdvancedToggleBtn.textContent = isOpen ? "收起高级参数" : "展开高级参数";
}

function handleProviderAdvancedToggle() {
  setProviderAdvancedOpen(!state.providerAdvancedOpen);
}

function linesToText(lines) {
  if (!Array.isArray(lines)) return "";
  return lines.map((item) => String(item || "").trim()).filter(Boolean).join("\n");
}

function normalizeMultilineText(value) {
  return String(value || "")
    .split("\n")
    .map((item) => item.trim())
    .filter(Boolean)
    .join("\n");
}

function normalizeSymbolListText(value) {
  return String(value || "")
    .replaceAll(",", "\n")
    .split("\n")
    .map((item) => item.trim().toUpperCase())
    .filter(Boolean)
    .filter((item, index, list) => list.indexOf(item) === index)
    .join("\n");
}

function normalizePromptKlineFeeds(value) {
  const feeds = {};
  const source = value && typeof value === "object" && !Array.isArray(value)
    ? value
    : {};
  PROMPT_KLINE_FEED_OPTIONS.forEach((interval) => {
    const raw = source[interval] && typeof source[interval] === "object" ? source[interval] : {};
    feeds[interval] = {
      enabled: Boolean(raw.enabled),
      limit: Math.max(1, Math.min(300, Number(raw.limit || PROMPT_KLINE_FEED_DEFAULTS[interval].limit)))
    };
  });
  if (!PROMPT_KLINE_FEED_OPTIONS.some((interval) => feeds[interval].enabled)) {
    feeds["15m"].enabled = true;
  }
  return feeds;
}

function currentPromptKlineFeeds() {
  const feeds = {};
  PROMPT_KLINE_FEED_OPTIONS.forEach((interval) => {
    const enabledInput = els.promptKlineEnabledInputs.find((input) => input.dataset.promptKlineEnabled === interval);
    const limitInput = els.promptKlineLimitInputs.find((input) => input.dataset.promptKlineLimit === interval);
    feeds[interval] = {
      enabled: enabledInput?.checked === true,
      limit: Number(limitInput?.value || PROMPT_KLINE_FEED_DEFAULTS[interval].limit)
    };
  });
  return normalizePromptKlineFeeds(feeds);
}

function syncPromptKlineInputs() {
  const feeds = currentPromptKlineFeeds();
  PROMPT_KLINE_FEED_OPTIONS.forEach((interval) => {
    const enabledInput = els.promptKlineEnabledInputs.find((input) => input.dataset.promptKlineEnabled === interval);
    const limitInput = els.promptKlineLimitInputs.find((input) => input.dataset.promptKlineLimit === interval);
    if (!enabledInput || !limitInput) return;
    limitInput.disabled = enabledInput.checked !== true;
    if (!limitInput.value) {
      limitInput.value = feeds[interval].limit;
    }
  });
}

function handlePromptKlineEnabledChange(changedInterval) {
  const feeds = currentPromptKlineFeeds();
  const enabledIntervals = PROMPT_KLINE_FEED_OPTIONS.filter((interval) => feeds[interval].enabled);
  if (!enabledIntervals.length) {
    const fallback = changedInterval && PROMPT_KLINE_FEED_OPTIONS.includes(changedInterval) ? changedInterval : "15m";
    const fallbackInput = els.promptKlineEnabledInputs.find((input) => input.dataset.promptKlineEnabled === fallback);
    if (fallbackInput) fallbackInput.checked = true;
  }
  syncPromptKlineInputs();
}

function providerConfigMatches(expected, actual) {
  if (!actual || typeof actual !== "object") return false;
  return String(actual.preset || "") === String(expected.preset || "")
    && String(actual.apiStyle || "") === String(expected.apiStyle || "")
    && String(actual.model || "") === String(expected.model || "")
    && String(actual.baseUrl || "") === String(expected.baseUrl || "")
    && String(actual.apiKey || "") === String(expected.apiKey || "")
    && Number(actual.timeoutSeconds || 0) === Number(expected.timeoutSeconds || 0)
    && Number(actual.temperature || 0) === Number(expected.temperature || 0)
    && Number(actual.maxOutputTokens || 0) === Number(expected.maxOutputTokens || 0);
}

function tradingSettingsMatch(expectedTrading, actualTrading, expectedDashboard, actualDashboard) {
  const trading = actualTrading || {};
  const settings = actualDashboard || {};
  return String(trading.activeExchange || "") === String(expectedTrading.activeExchange || "")
    && Number(trading.decisionIntervalMinutes || 0) === Number(expectedTrading.decisionIntervalMinutes || 0)
    && Number(trading.initialCapitalUsd || 0) === Number(expectedTrading.initialCapitalUsd || 0)
    && Number(trading.maxNewPositionsPerCycle || 0) === Number(expectedTrading.maxNewPositionsPerCycle || 0)
    && Number(trading.maxOpenPositions || 0) === Number(expectedTrading.maxOpenPositions || 0)
    && Number(trading.maxPositionNotionalUsd || 0) === Number(expectedTrading.maxPositionNotionalUsd || 0)
    && Number(trading.maxGrossExposurePct || 0) === Number(expectedTrading.maxGrossExposurePct || 0)
    && Number(trading.maxAccountDrawdownPct || 0) === Number(expectedTrading.maxAccountDrawdownPct || 0)
    && Number(trading.riskPerTradePct || 0) === Number(expectedTrading.riskPerTradePct || 0)
    && Number(trading.minConfidence || 0) === Number(expectedTrading.minConfidence || 0)
    && Number(trading.paperFeesBps || 0) === Number(expectedTrading.paperFeesBps || 0)
    && Boolean(trading.allowShorts) === Boolean(expectedTrading.allowShorts)
    && Number(settings.pageAutoRefreshSeconds || 0) === Number(expectedDashboard.pageAutoRefreshSeconds || 0);
}

function tradingSettingsMismatchMessage(actualTrading, actualDashboard) {
  return `运行设置保存后读回不一致：交易所=${actualTrading?.activeExchange ?? "n/a"}，决策间隔=${actualTrading?.decisionIntervalMinutes ?? "n/a"} 分钟，页面刷新=${actualDashboard?.pageAutoRefreshSeconds ?? "n/a"} 秒。请检查 Log。`;
}

function networkSettingsMatch(expected, actual) {
  if (!actual || typeof actual !== "object") return false;
  const expectedNoProxy = String(expected.noProxy || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean)
    .join(",");
  const actualNoProxy = Array.isArray(actual.noProxy) ? actual.noProxy.join(",") : String(actual.noProxy || "");
  return Boolean(actual.proxyEnabled) === Boolean(expected.proxyEnabled)
    && String(actual.proxyUrl || "") === String(expected.proxyUrl || "")
    && actualNoProxy === expectedNoProxy;
}

function liveConfigMatches(expected, actual) {
  if (!actual || typeof actual !== "object") return false;
  return Boolean(actual.enabled) === Boolean(expected.enabled)
    && Boolean(actual.dryRun) === Boolean(expected.dryRun)
    && String(actual.exchange || "") === String(expected.exchange || "")
    && String(actual.apiKey || "") === String(expected.apiKey || "")
    && String(actual.apiSecret || "") === String(expected.apiSecret || "")
    && String(actual.apiPassphrase || "") === String(expected.apiPassphrase || "")
    && String(actual.baseUrl || "") === String(expected.baseUrl || "")
    && Number(actual.defaultLeverage || 0) === Number(expected.defaultLeverage || 0)
    && String(actual.marginType || "") === String(expected.marginType || "");
}

function exchangeCatalog() {
  const tradingCatalog = state.tradingSettings?.exchangeCatalog;
  if (Array.isArray(tradingCatalog) && tradingCatalog.length) return tradingCatalog;
  const liveCatalog = state.liveConfig?.exchangeCatalog;
  if (Array.isArray(liveCatalog) && liveCatalog.length) return liveCatalog;
  return LIVE_EXCHANGE_FALLBACK_CATALOG;
}

function liveExchangeCatalog() {
  return exchangeCatalog();
}

function liveExchangeOption(exchangeId) {
  const normalized = String(exchangeId || "").trim().toLowerCase();
  return liveExchangeCatalog().find((item) => item.id === normalized) || liveExchangeCatalog()[0];
}

function renderExchangeSelect(selectEl, currentExchange = "binance", options = exchangeCatalog(), capability = "market") {
  const supportKey = capability === "trade" ? "tradingSupported" : "marketSupported";
  const resolved = options.some((item) => item.id === currentExchange && item[supportKey] === true)
    ? currentExchange
    : (options.find((item) => item[supportKey] === true)?.id || "binance");
  selectEl.innerHTML = options.map((item) => `
    <option value="${escapeHtml(item.id)}" ${item[supportKey] === true ? "" : "disabled"}>
      ${escapeHtml(item.label)}${item[supportKey] === true ? "" : capability === "trade" ? "（实盘即将支持）" : "（即将支持）"}
    </option>
  `).join("");
  selectEl.value = resolved;
}

function renderActiveExchangeOptions(currentExchange = "binance") {
  renderExchangeSelect(els.activeExchangeInput, currentExchange, exchangeCatalog(), "market");
}

function renderLiveExchangeOptions(currentExchange = "binance") {
  renderExchangeSelect(els.liveExchangeInput, currentExchange, liveExchangeCatalog(), "trade");
}

function syncLiveExchangeUi() {
  const option = liveExchangeOption(els.liveExchangeInput.value || state.liveConfig?.exchange || "binance");
  els.liveApiKeyInput.placeholder = option.apiKeyPlaceholder || "API key";
  els.liveApiSecretInput.placeholder = option.apiSecretPlaceholder || "API secret";
  if (els.liveApiPassphraseWrap) {
    els.liveApiPassphraseWrap.hidden = option.requiresPassphrase !== true;
  }
  if (els.liveApiPassphraseInput) {
    els.liveApiPassphraseInput.placeholder = option.apiPassphrasePlaceholder || "API passphrase";
  }
  els.liveBaseUrlInput.placeholder = option.defaultBaseUrl || "";
  els.liveExchangeMeta.textContent = option.notes || "";
}

function handleLiveExchangeChange() {
  const previousOption = liveExchangeOption(state.liveConfig?.exchange || "binance");
  const nextOption = liveExchangeOption(els.liveExchangeInput.value || "binance");
  const currentBaseUrl = String(els.liveBaseUrlInput.value || "").trim();
  if (!currentBaseUrl || currentBaseUrl === previousOption.defaultBaseUrl) {
    els.liveBaseUrlInput.value = nextOption.defaultBaseUrl || "";
  }
  syncLiveExchangeUi();
}

function promptConfigMatches(expected, actual) {
  if (!actual || typeof actual !== "object") return false;
  const actualFeeds = normalizePromptKlineFeeds(actual.klineFeeds);
  const expectedFeeds = normalizePromptKlineFeeds(expected.klineFeeds);
  return String(actual.name || "") === String(expected.name || "")
    && String(actual.role || actual.decision_logic?.role || "") === String(expected.role || "")
    && normalizeMultilineText(actual.corePrinciplesText || linesToText(actual.decision_logic?.core_principles)) === normalizeMultilineText(expected.corePrinciplesText || "")
    && normalizeMultilineText(actual.entryPreferencesText || linesToText(actual.decision_logic?.entry_preferences)) === normalizeMultilineText(expected.entryPreferencesText || "")
    && normalizeMultilineText(actual.positionManagementText || linesToText(actual.decision_logic?.position_management)) === normalizeMultilineText(expected.positionManagementText || "")
    && PROMPT_KLINE_FEED_OPTIONS.every((interval) =>
      Boolean(actualFeeds[interval].enabled) === Boolean(expectedFeeds[interval].enabled)
      && Number(actualFeeds[interval].limit) === Number(expectedFeeds[interval].limit)
    );
}

function currentPromptPreset() {
  const prompts = state.promptLibrary?.prompts || [];
  return prompts.find((item) => item.id === state.promptEditingPresetId) || null;
}

function renderPromptPresetMeta() {
  const preset = currentPromptPreset();
  if (!els.promptPresetMeta) return;
  if (!preset) {
    els.promptPresetMeta.textContent = "当前编辑内容还没有关联到已保存 Prompt。";
    return;
  }
  els.promptPresetMeta.textContent = `当前编辑已关联到已保存 Prompt：${preset.name}`;
}

function applyPromptEditor(prompt, presetId = null) {
  const normalizedPrompt = prompt || {};
  els.promptNameInput.value = normalizedPrompt.name || "default_trading_logic";
  els.promptRoleInput.value = normalizedPrompt.role || normalizedPrompt.decision_logic?.role || "";
  els.promptCorePrinciplesInput.value = normalizedPrompt.corePrinciplesText || linesToText(normalizedPrompt.decision_logic?.core_principles);
  els.promptEntryPreferencesInput.value = normalizedPrompt.entryPreferencesText || linesToText(normalizedPrompt.decision_logic?.entry_preferences);
  els.promptPositionManagementInput.value = normalizedPrompt.positionManagementText || linesToText(normalizedPrompt.decision_logic?.position_management);
  const feeds = normalizePromptKlineFeeds(normalizedPrompt.klineFeeds);
  els.promptKlineEnabledInputs.forEach((input) => {
    const interval = input.dataset.promptKlineEnabled;
    input.checked = feeds[interval]?.enabled === true;
  });
  els.promptKlineLimitInputs.forEach((input) => {
    const interval = input.dataset.promptKlineLimit;
    input.value = feeds[interval]?.limit ?? PROMPT_KLINE_FEED_DEFAULTS[interval].limit;
  });
  state.promptEditingPresetId = presetId || normalizedPrompt.presetId || normalizedPrompt.id || null;
  syncPromptKlineInputs();
  renderPromptPresetMeta();
}

function promptPresetPreview(prompt) {
  const role = String(prompt?.role || prompt?.decision_logic?.role || "").trim();
  const core = normalizeMultilineText(prompt?.corePrinciplesText || linesToText(prompt?.decision_logic?.core_principles || []));
  return role || core || "还没有填写交易逻辑摘要。";
}

function universeConfigMatches(expected, actual) {
  if (!actual || typeof actual !== "object") return false;
  return normalizeSymbolListText(actual.rawSymbols || (actual.symbols || []).join("\n")) === normalizeSymbolListText(expected.symbols || "")
    && Boolean(actual.dynamicSource?.enabled) === Boolean(expected.dynamicSource?.enabled)
    && String(actual.candidateSourceCode || "") === String(expected.candidateSourceCode || "");
}

function buildPromptFormPayload() {
  return {
    name: String(els.promptNameInput.value || "").trim() || "default_trading_logic",
    presetId: state.promptEditingPresetId || null,
    role: els.promptRoleInput.value.trim(),
    corePrinciplesText: els.promptCorePrinciplesInput.value,
    entryPreferencesText: els.promptEntryPreferencesInput.value,
    positionManagementText: els.promptPositionManagementInput.value,
    klineFeeds: currentPromptKlineFeeds()
  };
}

function selectedUniverseMode() {
  return els.dynamicUniverseEnabledInput.checked ? "dynamic" : "static";
}

function syncUniverseModeUi() {
  const dynamic = selectedUniverseMode() === "dynamic";
  els.staticUniverseEnabledInput.checked = !dynamic;
  els.dynamicUniverseEnabledInput.checked = dynamic;
  els.staticUniverseSection.hidden = dynamic;
  els.dynamicUniverseSection.hidden = !dynamic;
}

function handleUniverseModeToggle(mode) {
  if (mode === "dynamic") {
    els.dynamicUniverseEnabledInput.checked = true;
    els.staticUniverseEnabledInput.checked = false;
  } else {
    els.dynamicUniverseEnabledInput.checked = false;
    els.staticUniverseEnabledInput.checked = true;
  }
  syncUniverseModeUi();
  renderUniverseTest();
}

function promptFieldConfig(field) {
  switch (field) {
    case "role":
      return { input: els.promptRoleInput, title: "编辑 role" };
    case "corePrinciples":
      return { input: els.promptCorePrinciplesInput, title: "编辑 core_principles" };
    case "entryPreferences":
      return { input: els.promptEntryPreferencesInput, title: "编辑 entry_preferences" };
    case "positionManagement":
      return { input: els.promptPositionManagementInput, title: "编辑 position_management" };
    default:
      return null;
  }
}

function syncTradingSettingsForm() {
  const settings = state.tradingSettings || {};
  renderActiveExchangeOptions(settings.activeExchange || "binance");
  els.activeExchangeInput.value = settings.activeExchange || "binance";
  els.decisionIntervalInput.value = settings.decisionIntervalMinutes ?? 5;
  els.initialCapitalInput.value = settings.initialCapitalUsd ?? 1000;
  els.maxNewPositionsInput.value = settings.maxNewPositionsPerCycle ?? 1;
  els.maxOpenPositionsInput.value = settings.maxOpenPositions ?? 4;
  els.maxPositionNotionalInput.value = settings.maxPositionNotionalUsd ?? 150;
  els.maxGrossExposureInput.value = settings.maxGrossExposurePct ?? 100;
  els.maxDrawdownInput.value = settings.maxAccountDrawdownPct ?? 20;
  els.riskPerTradeInput.value = settings.riskPerTradePct ?? 2.5;
  els.minConfidenceInput.value = settings.minConfidence ?? 60;
  els.paperFeesInput.value = settings.paperFeesBps ?? 4;
  els.allowShortsInput.checked = settings.allowShorts !== false;
  els.pageAutoRefreshInput.value = state.settings?.pageAutoRefreshSeconds ?? 600;
}

function syncProviderForm() {
  const provider = state.provider || {};
  els.providerPresetInput.value = provider.preset || "gpt";
  els.providerApiStyleInput.value = provider.apiStyle || "openai";
  refreshProviderModelOptions(provider.model || providerModelsForPreset(provider.preset || "gpt")[0] || "");
  els.providerBaseUrlInput.value = provider.baseUrl || "";
  els.providerApiKeyInput.value = provider.apiKey || "";
  els.providerTimeoutInput.value = provider.timeoutSeconds ?? 45;
  els.providerTemperatureInput.value = provider.temperature ?? 0.2;
  els.providerMaxTokensInput.value = provider.maxOutputTokens ?? 1200;
  setProviderAdvancedOpen(false);
}

function syncPromptForm() {
  const prompt = state.prompt || {};
  applyPromptEditor(prompt, prompt.presetId || null);
}

function syncUniverseForm() {
  const universe = state.universe || {};
  const dynamicSource = universe.dynamicSource || {};
  els.universeSymbolsInput.value = universe.rawSymbols || (universe.symbols || []).join("\n");
  els.staticUniverseEnabledInput.checked = dynamicSource.enabled !== true;
  els.dynamicUniverseEnabledInput.checked = dynamicSource.enabled === true;
  els.candidateSourceCodeInput.value = universe.candidateSourceCode || "";
  syncUniverseModeUi();
}

function syncNetworkForm() {
  const network = state.network || {};
  els.proxyEnabledInput.checked = network.proxyEnabled === true;
  els.proxyUrlInput.value = network.proxyUrl || "";
  els.noProxyInput.value = Array.isArray(network.noProxy) ? network.noProxy.join(",") : (network.noProxy || "");
}

function syncLiveConfigForm() {
  const live = state.liveConfig || {};
  renderLiveExchangeOptions(live.exchange || "binance");
  const enabled = live.enabled === true;
  const dryRun = enabled ? false : (live.dryRun !== false || live.dryRun === undefined);
  els.liveExchangeInput.value = live.exchange || "binance";
  els.liveConfigEnabledInput.checked = enabled;
  els.liveDryRunInput.checked = dryRun;
  els.liveApiKeyInput.value = live.apiKey || "";
  els.liveApiSecretInput.value = live.apiSecret || "";
  if (els.liveApiPassphraseInput) {
    els.liveApiPassphraseInput.value = live.apiPassphrase || "";
  }
  els.liveBaseUrlInput.value = live.baseUrl || liveExchangeOption(live.exchange).defaultBaseUrl || "";
  els.liveDefaultLeverageInput.value = live.defaultLeverage ?? 3;
  els.liveMarginTypeInput.value = live.marginType || "cross";
  syncLiveExchangeUi();
  const ipPayload = state.networkIp || {};
  if (ipPayload.ip) {
    if (ipPayload.scope === "local") {
      const suffix = ipPayload.error ? "，公网获取失败" : "";
      els.liveIpText.textContent = `${ipPayload.ip}（本机内网${suffix}）`;
    } else {
      els.liveIpText.textContent = `${ipPayload.ip}`;
    }
  } else if (ipPayload.error) {
    els.liveIpText.textContent = `获取失败：${ipPayload.error}`;
  } else {
    els.liveIpText.textContent = "读取中…";
  }
}

function renderMeta() {
  const mode = currentViewMode();
  const runner = currentRunner();
  const nextDueRaw = currentNextDueAt();
  const nextDue = nextDueRaw ? fmtDateTime(nextDueRaw) : "n/a";
  const lastFinished = runner.lastFinishedAt ? fmtDateTime(runner.lastFinishedAt) : "尚未执行";
  const status = runner.running ? "正在执行" : "空闲";
  els.tradeRunMeta.textContent = `${modeName(mode)} · ${status} · 上次完成 ${lastFinished} · 下次调度 ${nextDue}`;
  const refreshSeconds = state.settings?.pageAutoRefreshSeconds || 600;
  els.tradeRefreshHint.textContent = `自动刷新 ${refreshSeconds} 秒`;
  els.toggleModeBtn.textContent = mode === "live" ? "查看模拟盘" : "查看实盘";
  els.resetBtn.hidden = false;
  els.resetBtn.textContent = mode === "live" ? "重置实盘" : "重置模拟盘";
  els.liveConfigSection.hidden = mode !== "live";
  els.paperFeesInput.disabled = mode === "live";
  els.paperFeesRow.classList.toggle("disabled-setting", mode === "live");
  const enabled = currentRunnerEnabled();
  els.modeRunnerBtn.textContent = enabled ? "暂停交易" : "启动交易";
  els.modeRunnerMeta.textContent = enabled
    ? `当前${modeName(mode)}已启动，到了下一次调度时间会自动执行。`
    : `当前${modeName(mode)}已暂停，点击后会从下一次调度时间开始运行。`;
}

function renderAccount() {
  const account = activeAccount();
  const liveStatus = state.tradingState?.liveExecutionStatus || {};
  const providerStatus = state.tradingState?.providerStatus || {};
  const mode = currentViewMode();
  const liveReconcile = mode === "live" && account.accountSource === "exchange";
  els.modeText.textContent = mode.toUpperCase();
  els.equityText.textContent = fmtUsd(account.equityUsd);
  els.openCountText.textContent = String((account.openPositions || []).length);
  els.drawdownText.textContent = fmtPct(account.drawdownPct);
  if (liveReconcile) {
    els.accountMeta.textContent = `${mode.toUpperCase()} · Wallet ${fmtUsd(account.exchangeWalletBalanceUsd)} · Equity ${fmtUsd(account.equityUsd)} · Realized ${fmtUsd(account.realizedPnlUsd)}`;
  } else {
    els.accountMeta.textContent = `${mode.toUpperCase()} · Realized ${fmtUsd(account.realizedPnlUsd)} · Unrealized ${fmtUsd(account.unrealizedPnlUsd)} · Gross ${fmtUsd(account.grossExposureUsd)}`;
  }
  els.accountGrid.innerHTML = `
    <div class="account-panel-grid">
      <div class="compact-table bare-table">
        <div class="compact-row compact-header compact-account-grid">
          <span>${liveReconcile ? "Baseline" : "Initial"}</span>
          <span>Equity</span>
          <span>Realized</span>
          <span>Unrealized</span>
          <span>Gross Exposure</span>
          <span>Available Exposure</span>
          <span>Drawdown</span>
          <span>High Watermark</span>
        </div>
        <div class="compact-row compact-account-grid">
          <span>${escapeHtml(fmtUsd(account.initialCapitalUsd))}</span>
          <span>${escapeHtml(fmtUsd(account.equityUsd))}</span>
          <span class="${pnlClass(account.realizedPnlUsd)}">${escapeHtml(fmtSignedUsd(account.realizedPnlUsd))}</span>
          <span class="${pnlClass(liveReconcile ? account.exchangeUnrealizedPnlUsd : account.unrealizedPnlUsd)}">${escapeHtml(fmtSignedUsd(liveReconcile ? account.exchangeUnrealizedPnlUsd : account.unrealizedPnlUsd))}</span>
          <span>${escapeHtml(fmtUsd(account.grossExposureUsd))}</span>
          <span>${escapeHtml(fmtUsd(account.availableExposureUsd))}</span>
          <span>${escapeHtml(fmtPct(account.drawdownPct))}</span>
          <span>${escapeHtml(fmtUsd(account.highWatermarkEquity))}</span>
        </div>
      </div>
      <div class="account-chart-grid">
        <section class="account-chart-card">
          <div class="account-chart-head">
            <h3>Equity</h3>
            <p class="meta">${liveReconcile ? "按交易所同步到的真实权益绘制" : "按历史决策后的账户权益绘制"}</p>
          </div>
          ${renderEquityChart()}
        </section>
        <section class="account-chart-card">
          <div class="account-chart-head">
            <h3>${liveReconcile ? "当前持仓浮盈亏" : "品种收益 Top 10"}</h3>
            <p class="meta">${liveReconcile ? "当前实盘会话以来的已实现加当前持仓浮盈亏" : "已平仓收益加当前持仓浮盈亏"}</p>
          </div>
          ${renderSymbolPnlChart()}
        </section>
      </div>
    </div>
  `;
  wireEquityChartTooltip();
  const issues = [
    ...(providerStatus.issues || []).map((item) => `AI模型配置：${item}`),
    ...(mode === "live" ? (liveStatus.issues || []).map((item) => `实盘账号配置：${item}`) : []),
    ...(liveReconcile ? ["实盘账户汇总优先使用交易所同步到的真实 Wallet、Equity 与 Unrealized；已实现收益只统计当前实盘会话以来、交易所返回的已平仓 realized。"] : []),
    ...((state.tradingState?.adaptive?.notes || []).slice(0, 2))
  ];
  els.statusBanner.classList.add("visible");
  els.statusBanner.classList.toggle("active", issues.length > 0);
  els.statusBanner.classList.toggle("quiet", issues.length === 0);
  els.statusBanner.innerHTML = issues.length
    ? issues.map((item) => `<div class="adaptive-note">${escapeHtml(item)}</div>`).join("")
    : `<div class="adaptive-note">运行配置正常。当前只会执行 ${escapeHtml(mode.toUpperCase())} 模式。</div>`;
}

function renderPositions() {
  const mode = currentViewMode();
  const positions = sortedPositions();
  const sortState = state.positionSort || { key: "symbol", dir: "asc" };
  els.positionMeta.textContent = positions.length ? `${positions.length} 个持仓` : "当前无持仓";
  if (!positions.length) {
    const liveOpenCount = (state.tradingState?.liveAccount?.openPositions || []).length;
    if (mode === "paper" && liveOpenCount > 0) {
      els.positionMeta.textContent = `模拟盘无持仓，实盘有 ${liveOpenCount} 个持仓`;
      els.positionCards.innerHTML = `<p class="empty">你当前看的还是模拟盘页面。实盘现在有 ${liveOpenCount} 个持仓，点击右上角“查看实盘”就能看到。</p>`;
      return;
    }
    els.positionCards.innerHTML = `<p class="empty">没有活动持仓。</p>`;
    return;
  }
  els.positionCards.innerHTML = `
    <div class="table-scroll">
      <div class="compact-table bare-table">
        <div class="compact-row compact-header compact-position-grid">
          <span>${sortHeaderButton("Symbol", "positions", "symbol", sortState, "asc")}</span>
          <span>${sortHeaderButton("Side", "positions", "side", sortState, "asc")}</span>
          <span>${sortHeaderButton("Entry", "positions", "entryPrice", sortState, "desc")}</span>
          <span>${sortHeaderButton("Mark", "positions", "markPrice", sortState, "desc")}</span>
          <span>${sortHeaderButton("Qty", "positions", "quantity", sortState, "desc")}</span>
          <span>${sortHeaderButton("Notional", "positions", "notionalUsd", sortState, "desc")}</span>
          <span>${sortHeaderButton("Stop", "positions", "stopLoss", sortState, "desc")}</span>
          <span>${sortHeaderButton("Target", "positions", "takeProfit", sortState, "desc")}</span>
          <span>${sortHeaderButton("Unrealized", "positions", "unrealizedPnl", sortState, "desc")}</span>
          <span>${sortHeaderButton("Pnl%", "positions", "pnlPct", sortState, "desc")}</span>
        </div>
        ${positions.map((position) => `
          <div class="compact-row compact-position-grid">
            <strong>${escapeHtml(position.symbol)}</strong>
            <span><span class="mode-tag ${escapeHtml(position.side === "short" ? "short" : "long")}">${escapeHtml(position.side.toUpperCase())}</span></span>
            <span>${escapeHtml(fmtPrice(position.entryPrice))}</span>
            <span>${escapeHtml(fmtPrice(position.markPrice))}</span>
            <span>${escapeHtml(fmtNumber(position.quantity, 4))}</span>
            <span>${escapeHtml(fmtUsd(position.notionalUsd))}</span>
            <span>${escapeHtml(fmtPrice(position.stopLoss))}</span>
            <span>${escapeHtml(fmtPrice(position.takeProfit))}</span>
            <span class="${pnlClass(position.unrealizedPnl)}">${escapeHtml(fmtSignedUsd(position.unrealizedPnl))}</span>
            <span class="${pnlClass(position.unrealizedPnl)}">${escapeHtml(fmtPct(position.pnlPct))}</span>
          </div>
        `).join("")}
      </div>
    </div>
  `;
}

function renderClosedPositions() {
  const mode = currentViewMode();
  const history = activeHistory();
  if (mode === "live") {
    const liveClosed = groupedLiveClosedTrades();
    const groups = liveClosed.groups;
    const records = liveClosedDisplaySource().records;
    const sessionText = history.sessionStartedAt ? `${fmtDateTime(history.sessionStartedAt)} 以来` : "当前实盘会话以来";
    const sortState = state.closedSort?.live || { key: "latestClosedAt", dir: "desc" };
    const sourceNote = liveClosed.source === "exchange" ? "交易所明细" : "本地已同步记录";
    els.closedPositionMeta.textContent = records.length ? `${sessionText} ${groups.length} 个品种，${records.length} 条已平仓记录 · ${sourceNote}` : `${sessionText} 暂无已平仓记录`;
    if (!groups.length) {
      els.closedPositionList.innerHTML = `<p class="empty">当前实盘会话以来，还没有从交易所同步到已平仓记录。</p>`;
      return;
    }
    els.closedPositionList.innerHTML = `
      <div class="table-scroll">
        <div class="compact-table bare-table">
          <div class="compact-row compact-header compact-closed-live-grid">
            <span>${sortHeaderButton("Symbol", "closed-live", "symbol", sortState, "asc")}</span>
            <span>${sortHeaderButton("Realized", "closed-live", "totalRealized", sortState, "desc")}</span>
            <span>${sortHeaderButton("Count", "closed-live", "count", sortState, "desc")}</span>
            <span>${sortHeaderButton("Win", "closed-live", "winRate", sortState, "desc")}</span>
            <span>${sortHeaderButton("Latest", "closed-live", "latestClosedAt", sortState, "desc")}</span>
          </div>
          ${groups.map((group) => `
            <details class="trade-symbol-entry">
              <summary>
                <div class="compact-row bare-summary-row compact-closed-live-grid">
                  <strong>${escapeHtml(group.symbol)}</strong>
                  <span class="${pnlClass(group.totalRealized)}">${escapeHtml(fmtSignedUsd(group.totalRealized))}</span>
                  <span>${escapeHtml(fmtNumber(group.count, 0))}</span>
                  <span>${escapeHtml(fmtPct(group.winRate, 0))}</span>
                  <span>${escapeHtml(fmtDateTime(group.latestClosedAt))}</span>
                </div>
              </summary>
              <div class="trade-symbol-body">
                <div class="compact-table bare-table">
                  <div class="compact-row compact-header compact-closed-live-detail-grid">
                    <span>Closed At</span>
                    <span>Side</span>
                    <span>Qty</span>
                    <span>Notional</span>
                    <span>Realized</span>
                    <span>Note</span>
                  </div>
                  ${group.trades.map((trade) => `
                    <div class="compact-row compact-closed-live-detail-grid">
                      <span>${escapeHtml(fmtDateTime(trade.closedAt))}</span>
                      <span>${trade.side ? `<span class="mode-tag ${escapeHtml(trade.side === "short" ? "short" : "long")}">${escapeHtml(String(trade.side).toUpperCase())}</span>` : "-"}</span>
                      <span>${Number.isFinite(Number(trade.quantity)) ? escapeHtml(fmtNumber(trade.quantity, 4)) : "-"}</span>
                      <span>${Number.isFinite(Number(trade.notionalUsd)) ? escapeHtml(fmtUsd(trade.notionalUsd)) : "-"}</span>
                      <span class="${pnlClass(trade.realizedPnl)}">${escapeHtml(fmtSignedUsd(trade.realizedPnl))}</span>
                      <span>${escapeHtml(trade.info || "交易所已实现记录")}</span>
                    </div>
                  `).join("")}
                </div>
              </div>
            </details>
          `).join("")}
        </div>
      </div>
    `;
    return;
  }
  const groups = groupedPaperClosedTrades();
  const totalCount = groups.reduce((sum, group) => sum + group.count, 0);
  const sortState = state.closedSort?.paper || { key: "latestClosedAt", dir: "desc" };
  els.closedPositionMeta.textContent = totalCount ? `${groups.length} 个品种，${totalCount} 条已平仓记录` : "当前无已平仓记录";
  if (!groups.length) {
    els.closedPositionList.innerHTML = `<p class="empty">模拟盘还没有已平仓记录。</p>`;
    return;
  }
  els.closedPositionList.innerHTML = `
    <div class="table-scroll">
      <div class="compact-table bare-table">
        <div class="compact-row compact-header compact-closed-paper-grid">
          <span>${sortHeaderButton("Symbol", "closed-paper", "symbol", sortState, "asc")}</span>
          <span>${sortHeaderButton("Realized", "closed-paper", "totalRealized", sortState, "desc")}</span>
          <span>${sortHeaderButton("Count", "closed-paper", "count", sortState, "desc")}</span>
          <span>${sortHeaderButton("Win", "closed-paper", "winRate", sortState, "desc")}</span>
          <span>${sortHeaderButton("Latest", "closed-paper", "latestClosedAt", sortState, "desc")}</span>
        </div>
        ${groups.map((group) => `
          <details class="trade-symbol-entry">
            <summary>
              <div class="compact-row bare-summary-row compact-closed-paper-grid">
                <strong>${escapeHtml(group.symbol)}</strong>
                <span class="${pnlClass(group.totalRealized)}">${escapeHtml(fmtSignedUsd(group.totalRealized))}</span>
                <span>${escapeHtml(fmtNumber(group.count, 0))}</span>
                <span>${escapeHtml(fmtPct(group.winRate, 0))}</span>
                <span>${escapeHtml(fmtDateTime(group.latestClosedAt))}</span>
              </div>
            </summary>
            <div class="trade-symbol-body">
              <div class="compact-table bare-table">
                <div class="compact-row compact-header compact-closed-paper-detail-grid">
                  <span>Closed At</span>
                  <span>Side</span>
                  <span>Qty</span>
                  <span>Notional</span>
                  <span>Realized</span>
                  <span>Reason</span>
                </div>
                ${group.trades.map((trade) => `
                  <div class="compact-row compact-closed-paper-detail-grid">
                    <span>${escapeHtml(fmtDateTime(trade.closedAt))}</span>
                    <span><span class="mode-tag ${escapeHtml(trade.side === "short" ? "short" : "long")}">${escapeHtml((trade.side || "long").toUpperCase())}</span></span>
                    <span>${escapeHtml(fmtNumber(trade.quantity, 4))}</span>
                    <span>${escapeHtml(fmtUsd(trade.notionalUsd))}</span>
                    <span class="${pnlClass(trade.realizedPnl)}">${escapeHtml(fmtSignedUsd(trade.realizedPnl))}</span>
                    <span>${escapeHtml(trade.exitReason || "manual")}</span>
                  </div>
                `).join("")}
              </div>
            </div>
          </details>
        `).join("")}
      </div>
    </div>
  `;
}

function renderCandidates() {
  const candidates = latestCandidateUniverse();
  const diagnostics = candidateDiagnostics();
  const summaryParts = [
    `已保存 ${diagnostics.savedSymbols.length} 个`,
    `校验有效 ${diagnostics.validSymbolCount} 个`,
    `当前机会池 ${diagnostics.opportunityCount} 个`
  ];
  els.candidateMeta.textContent = summaryParts.join(" · ");
  const invalidBlock = diagnostics.invalidSymbols.length
    ? `
      <p class="meta">未进入机会池的无效 symbols：${escapeHtml(diagnostics.invalidSymbols.join("、"))}</p>
    `
    : "";
  const skippedBlock = diagnostics.skippedSymbols.length
    ? `
      <p class="meta">已通过 symbol 校验但本轮未拉到行情：${escapeHtml(diagnostics.skippedSymbols.join("、"))}</p>
    `
    : "";
  if (!candidates.length) {
    els.candidateList.innerHTML = `
      ${invalidBlock}
      ${skippedBlock}
      <p class="empty">先保存候选池配置或执行一轮交易决策。</p>
    `;
    return;
  }
  els.candidateList.innerHTML = `
    ${invalidBlock}
    ${skippedBlock}
    <div class="universe-symbol-list">
      ${candidates.map((candidate) => `<span class="symbol-chip">${escapeHtml(candidate.symbol)}</span>`).join("")}
    </div>
  `;
}

function normalizeDecisionOutputPayload(output) {
  const payload = output && typeof output === "object" ? output : {};
  return {
    summary: String(payload.summary || "").trim(),
    positionActions: Array.isArray(payload.positionActions)
      ? payload.positionActions
      : (Array.isArray(payload.position_actions) ? payload.position_actions : []),
    entryActions: Array.isArray(payload.entryActions)
      ? payload.entryActions
      : (Array.isArray(payload.entry_actions) ? payload.entry_actions : []),
    watchlist: Array.isArray(payload.watchlist) ? payload.watchlist : [],
    providerStatus: payload.providerStatus && typeof payload.providerStatus === "object" ? payload.providerStatus : null,
    liveExecutionStatus: payload.liveExecutionStatus && typeof payload.liveExecutionStatus === "object" ? payload.liveExecutionStatus : null,
    raw: payload
  };
}

function decisionBadgeTone(value, kind = "decision") {
  const text = String(value || "").trim().toLowerCase();
  if (kind === "side") {
    if (text === "long") return "positive";
    if (text === "short") return "danger";
    return "neutral";
  }
  if (text === "open") return "positive";
  if (text === "hold") return "neutral";
  if (text === "update") return "active";
  if (text === "reduce") return "warning";
  if (text === "close") return "danger";
  return "neutral";
}

function decisionBadge(label, tone = "neutral") {
  return `<span class="decision-output-badge is-${escapeHtml(tone)}">${escapeHtml(label)}</span>`;
}

function decisionMetric(label, value, extraClass = "") {
  return `
    <div class="decision-output-metric ${extraClass}">
      <span>${escapeHtml(label)}</span>
      <strong>${escapeHtml(value)}</strong>
    </div>
  `;
}

function renderPositionActionCard(action) {
  const symbol = String(action?.symbol || "n/a").toUpperCase();
  const decision = String(action?.decision || "hold").trim().toLowerCase() || "hold";
  const side = String(action?.side || "").trim().toLowerCase();
  const metrics = [];
  if (decision === "reduce" && Number.isFinite(Number(action?.reduceFraction))) {
    metrics.push(decisionMetric("减仓比例", fmtPct(Number(action.reduceFraction) * 100, 0)));
  }
  if (Number.isFinite(Number(action?.confidence))) {
    metrics.push(decisionMetric("置信度", fmtPct(Number(action.confidence), 0)));
  }
  if (Number.isFinite(Number(action?.stopLoss))) {
    metrics.push(decisionMetric("止损", fmtPrice(action.stopLoss)));
  }
  if (Number.isFinite(Number(action?.takeProfit))) {
    metrics.push(decisionMetric("止盈", fmtPrice(action.takeProfit)));
  }
  return `
    <article class="decision-output-card">
      <div class="decision-output-card-head">
        <strong>${escapeHtml(symbol)}</strong>
        <div class="decision-output-badges">
          ${decisionBadge(decision.toUpperCase(), decisionBadgeTone(decision))}
          ${side ? decisionBadge(side.toUpperCase(), decisionBadgeTone(side, "side")) : ""}
        </div>
      </div>
      <p class="decision-output-reason">${escapeHtml(action?.reason || "无说明")}</p>
      ${metrics.length ? `<div class="decision-output-metrics">${metrics.join("")}</div>` : ""}
    </article>
  `;
}

function renderEntryActionCard(action) {
  const symbol = String(action?.symbol || "n/a").toUpperCase();
  const side = String(action?.side || "").trim().toLowerCase();
  const actionType = String(action?.action || "open").trim().toLowerCase() || "open";
  const metrics = [];
  if (Number.isFinite(Number(action?.confidence))) {
    metrics.push(decisionMetric("置信度", fmtPct(Number(action.confidence), 0)));
  }
  if (Number.isFinite(Number(action?.stopLoss))) {
    metrics.push(decisionMetric("止损", fmtPrice(action.stopLoss)));
  }
  if (Number.isFinite(Number(action?.takeProfit))) {
    metrics.push(decisionMetric("止盈", fmtPrice(action.takeProfit)));
  }
  return `
    <article class="decision-output-card">
      <div class="decision-output-card-head">
        <strong>${escapeHtml(symbol)}</strong>
        <div class="decision-output-badges">
          ${decisionBadge(actionType.toUpperCase(), decisionBadgeTone(actionType))}
          ${side ? decisionBadge(side.toUpperCase(), decisionBadgeTone(side, "side")) : ""}
        </div>
      </div>
      <p class="decision-output-reason">${escapeHtml(action?.reason || "无说明")}</p>
      ${metrics.length ? `<div class="decision-output-metrics">${metrics.join("")}</div>` : ""}
    </article>
  `;
}

function renderWatchlistCard(item) {
  const symbol = String(item?.symbol || "n/a").toUpperCase();
  return `
    <article class="decision-output-card">
      <div class="decision-output-card-head">
        <strong>${escapeHtml(symbol)}</strong>
        <div class="decision-output-badges">
          ${decisionBadge("WATCH", "neutral")}
        </div>
      </div>
      <p class="decision-output-reason">${escapeHtml(item?.reason || "无说明")}</p>
    </article>
  `;
}

function renderDecisionCardSection(title, records, renderer, emptyText) {
  const items = Array.isArray(records) ? records : [];
  return `
    <section class="decision-output-section">
      <div class="decision-output-section-head">
        <h4>${escapeHtml(title)}</h4>
        <span>${escapeHtml(String(items.length))}</span>
      </div>
      ${items.length
        ? `<div class="decision-output-grid">${items.map((item) => renderer(item)).join("")}</div>`
        : `<p class="decision-output-empty">${escapeHtml(emptyText)}</p>`}
    </section>
  `;
}

function renderDecisionOutput(output) {
  const normalized = normalizeDecisionOutputPayload(output);
  const providerText = normalized.providerStatus
    ? `${normalized.providerStatus.preset || "provider"} / ${normalized.providerStatus.model || "model"}`
    : "";
  const liveExec = normalized.liveExecutionStatus;
  const liveExecText = liveExec
    ? (liveExec.canExecute ? "实盘可执行" : "实盘未执行")
    : "";
  return `
    <div class="decision-output-stack">
      <section class="decision-summary-card">
        <div class="decision-output-card-head">
          <strong>摘要</strong>
          <div class="decision-output-badges">
            ${providerText ? decisionBadge(providerText, "active") : ""}
            ${liveExecText ? decisionBadge(liveExecText, liveExec?.canExecute ? "positive" : "warning") : ""}
          </div>
        </div>
        <p class="decision-output-summary">${escapeHtml(normalized.summary || "本轮没有额外摘要。")}</p>
      </section>
      ${renderDecisionCardSection("持仓动作", normalized.positionActions, renderPositionActionCard, "这轮没有持仓管理动作。")}
      ${renderDecisionCardSection("开仓动作", normalized.entryActions, renderEntryActionCard, "这轮没有新开仓建议。")}
      ${renderDecisionCardSection("观察列表", normalized.watchlist, renderWatchlistCard, "这轮没有额外观察标的。")}
      <details class="decision-raw-json">
        <summary>查看原始 JSON</summary>
        <pre class="pre-block">${escapeHtml(JSON.stringify(normalized.raw || {}, null, 2))}</pre>
      </details>
    </div>
  `;
}

function renderDecisionLog() {
  const decisions = (activeHistory().decisions || []).slice().reverse();
  els.decisionMeta.textContent = decisions.length ? `${decisions.length} 条最近决策` : "暂无决策日志";
  if (!decisions.length) {
    els.decisionLog.innerHTML = `<p class="empty">等第一轮决策跑完，这里会显示 prompt、模型输出和执行动作。</p>`;
    return;
  }
  els.decisionLog.innerHTML = decisions.map((decision) => `
    <details class="decision-entry">
      <summary>
        <div>
          <strong>${escapeHtml(decision.id)}</strong>
          <span>${escapeHtml(fmtDateTime(decision.finishedAt))}</span>
        </div>
        <div class="decision-chip-row">
          <span>${escapeHtml(decision.runnerReason || "manual")}</span>
          <span>${escapeHtml(decision.mode || state.tradingState?.activeMode || "paper")}</span>
          <span>${escapeHtml(String((decision.actions || []).length))} actions</span>
        </div>
      </summary>
      <div class="decision-entry-body">
        <p>${escapeHtml(decision.promptSummary || "No summary")}</p>
        <div class="decision-subsection">
          <h3>Actions</h3>
          <div class="decision-chip-row">
            ${(decision.actions || []).map((action) => `
              <span class="decision-action-chip">${escapeHtml(action.label || action.type || "action")}</span>
            `).join("") || "<span class=\"decision-action-chip\">none</span>"}
          </div>
        </div>
        <div class="decision-subsection">
          <h3>Warnings</h3>
          <pre class="pre-block">${escapeHtml(JSON.stringify(decision.warnings || [], null, 2))}</pre>
        </div>
        <div class="decision-subsection">
          <div class="decision-content-head">
            <h3>决策详情</h3>
            <div class="decision-mini-tabs" role="tablist" aria-label="Decision content tabs">
              <button type="button" class="decision-mini-tab is-active" data-decision-tab-target="output" aria-selected="true">模型输出</button>
              <button type="button" class="decision-mini-tab" data-decision-tab-target="prompt" aria-selected="false">输入 Prompt</button>
            </div>
          </div>
          <div class="decision-tab-panel is-active" data-decision-tab-panel="output">
            ${renderDecisionOutput(decision.output || {})}
          </div>
          <div class="decision-tab-panel" data-decision-tab-panel="prompt" hidden>
            <pre class="pre-block">${escapeHtml(decision.prompt || "")}</pre>
          </div>
        </div>
      </div>
    </details>
  `).join("");
}

function handleDecisionTabClick(event) {
  const button = event.target.closest("[data-decision-tab-target]");
  if (!button) return;
  const section = button.closest(".decision-subsection");
  if (!section) return;
  const target = button.dataset.decisionTabTarget || "output";
  section.querySelectorAll("[data-decision-tab-target]").forEach((item) => {
    const isActive = item === button;
    item.classList.toggle("is-active", isActive);
    item.setAttribute("aria-selected", isActive ? "true" : "false");
  });
  section.querySelectorAll("[data-decision-tab-panel]").forEach((panel) => {
    const isActive = panel.dataset.decisionTabPanel === target;
    panel.classList.toggle("is-active", isActive);
    panel.hidden = !isActive;
  });
}

function handleSortClick(event) {
  const button = event.target.closest("[data-sort-target][data-sort-key]");
  if (!button) return;
  const target = button.dataset.sortTarget || "";
  const key = button.dataset.sortKey || "";
  const defaultDir = button.dataset.sortDefaultDir || "asc";
  if (target === "positions") {
    state.positionSort = nextSortState(state.positionSort, key, defaultDir);
    renderPositions();
    return;
  }
  if (target === "closed-live") {
    state.closedSort = {
      ...(state.closedSort || {}),
      live: nextSortState(state.closedSort?.live, key, defaultDir)
    };
    renderClosedPositions();
    return;
  }
  if (target === "closed-paper") {
    state.closedSort = {
      ...(state.closedSort || {}),
      paper: nextSortState(state.closedSort?.paper, key, defaultDir)
    };
    renderClosedPositions();
  }
}

function renderPromptTest() {
  const test = state.promptTest;
  if (!test) {
    els.promptTestMeta.textContent = "点击“测试交易逻辑”后显示结果。";
    els.promptTestOutput.textContent = "尚未执行测试。";
    return;
  }
  if (test.error) {
    els.promptTestMeta.textContent = `最近测试失败 · ${test.testedAt ? fmtDateTime(test.testedAt) : "刚刚"}`;
    els.promptTestOutput.textContent = `测试失败：${test.error}`;
    return;
  }
  const providerText = providerLabel(test.provider);
  const autoConfiguredText = test.provider?.autoConfiguredSaved
    ? ` · 已自动保存协议 ${test.provider?.resolvedApiStyle || ""}`
    : "";
  els.promptTestMeta.textContent = `${modeName(test.mode)} · ${test.candidateCount || 0} 个候选 · ${providerText}${autoConfiguredText} · 仅测试，不发单`;
  els.promptTestOutput.textContent = JSON.stringify(
    {
      warnings: test.warnings || [],
      parsed: test.parsed || {},
      rawText: test.rawText || "",
      provider: test.provider || {},
      account: {
        equityUsd: test.account?.equityUsd,
        grossExposureUsd: test.account?.grossExposureUsd,
        openPositions: (test.account?.openPositions || []).length
      }
    },
    null,
    2
  );
}

function renderPromptLibrary() {
  const prompts = (state.promptLibrary?.prompts || []).slice();
  els.savedPromptMeta.textContent = prompts.length ? `${prompts.length} 个已保存 Prompt` : "还没有已保存 Prompt。";
  if (!prompts.length) {
    els.savedPromptList.innerHTML = `<p class="empty">先在上方填写名称，然后点“保存到已保存 Prompt”。</p>`;
    renderPromptPresetMeta();
    return;
  }
  els.savedPromptList.innerHTML = prompts.map((prompt) => {
    const isActive = state.prompt?.presetId === prompt.id;
    const isEditing = state.promptEditingPresetId === prompt.id;
    return `
      <article class="saved-prompt-card ${isActive ? "is-active" : ""}">
        <div class="saved-prompt-head">
          <div>
            <h3>${escapeHtml(prompt.name || "untitled")}</h3>
            <p class="meta">更新于 ${escapeHtml(fmtDateTime(prompt.updatedAt))}</p>
          </div>
          <div class="decision-chip-row">
            ${isActive ? '<span>当前使用中</span>' : ""}
            ${isEditing ? '<span>当前编辑中</span>' : ""}
          </div>
        </div>
        <p class="saved-prompt-preview">${escapeHtml(promptPresetPreview(prompt))}</p>
        <div class="saved-prompt-actions">
          <button type="button" class="secondary-button" data-prompt-preset-action="use" data-prompt-preset-id="${escapeHtml(prompt.id)}">一键使用</button>
          <button type="button" class="secondary-button" data-prompt-preset-action="load" data-prompt-preset-id="${escapeHtml(prompt.id)}">加载修改</button>
          <button type="button" class="secondary-button" data-prompt-preset-action="rename" data-prompt-preset-id="${escapeHtml(prompt.id)}">重命名</button>
          <button type="button" class="secondary-button" data-prompt-preset-action="delete" data-prompt-preset-id="${escapeHtml(prompt.id)}">删除</button>
        </div>
      </article>
    `;
  }).join("");
  renderPromptPresetMeta();
}

function renderUniverseTest() {
  const universe = state.universe || {};
  const dynamicSource = universe.dynamicSource || {};
  const symbols = universe.symbols || [];
  els.universeMeta.textContent = dynamicSource.enabled
    ? `当前模式：动态候选池。手动列表会作为函数的备用输入。`
    : `当前模式：静态候选池。当前保存了 ${symbols.length} 个 symbols。`;
  const test = state.universeTest;
  if (!dynamicSource.enabled) {
    els.universePreview.textContent = "动态候选池未启用。";
    els.universeTestMeta.textContent = "启用动态候选池后，这里会显示测试结果。";
    return;
  }
  if (!test) {
    els.universeTestMeta.textContent = "点击“测试获取”后检查 Python 函数是否成功返回 symbols。";
    els.universePreview.textContent = "尚未执行测试。";
    return;
  }
  if (test.error) {
    els.universeTestMeta.textContent = `最近测试失败 · ${test.testedAt ? fmtDateTime(test.testedAt) : "刚刚"}`;
    els.universePreview.textContent = `测试失败：${test.error}`;
    return;
  }
  const metaParts = [
    test.mode === "python_function" ? "Python 动态获取" : "手动 symbols",
    `${test.count || 0} 个 symbols`,
    `${test.durationMs || 0}ms`
  ];
  const invalidCount = Array.isArray(test.invalidSymbols) ? test.invalidSymbols.length : 0;
  if (invalidCount) metaParts.push(`无效 ${invalidCount} 个`);
  if (test.note) metaParts.push(`备注：${test.note}`);
  if (test.stdout) metaParts.push(`stdout 已输出`);
  els.universeTestMeta.textContent = metaParts.join(" · ");
  const outputSymbols = Array.isArray(test.symbols) ? test.symbols : [];
  els.universePreview.textContent = outputSymbols.length
    ? outputSymbols.join("\n")
    : "函数返回了空 list。";
}

function renderLogs() {
  const entries = state.logs?.entries || [];
  const clientErrors = state.clientErrors || [];
  const paperRunner = state.tradingState?.paperRunner || {};
  const liveRunner = state.tradingState?.liveRunner || {};
  const scanRunner = state.tradingState?.scanRunner || {};
  const latestError = paperRunner.lastError || liveRunner.lastError || scanRunner.lastError || "无";
  const sessionStartedAt = state.logs?.sessionStartedAt ? fmtDateTime(state.logs.sessionStartedAt) : "n/a";
  els.logMeta.textContent = clientErrors.length
    ? `最近 ${entries.length} 条系统输出，另有 ${clientErrors.length} 条前端错误`
    : (entries.length ? `最近 ${entries.length} 条系统输出` : "暂时还没有日志输出");
  els.logSummaryGrid.innerHTML = `
    <article class="log-pill">
      <span>本次启动</span>
      <strong>${escapeHtml(sessionStartedAt)}</strong>
      <p class="subtle">重启服务后，这里的系统日志会从新的启动时间开始</p>
    </article>
    <article class="log-pill">
      <span>模拟盘</span>
      <strong>${escapeHtml(paperRunner.running ? "执行中" : (state.tradingState?.paperTradingEnabled ? "已启动" : "已暂停"))}</strong>
      <p class="subtle">下次调度 ${escapeHtml(fmtDateTime(state.tradingState?.paperNextDecisionDueAt))}</p>
    </article>
    <article class="log-pill">
      <span>实盘</span>
      <strong>${escapeHtml(liveRunner.running ? "执行中" : (state.tradingState?.liveTradingEnabled ? "已启动" : "已暂停"))}</strong>
      <p class="subtle">下次调度 ${escapeHtml(fmtDateTime(state.tradingState?.liveNextDecisionDueAt))}</p>
    </article>
    <article class="log-pill">
      <span>候选池刷新</span>
      <strong>${escapeHtml(scanRunner.running ? "执行中" : "空闲")}</strong>
      <p class="subtle">最近错误 ${escapeHtml(latestError)}</p>
    </article>
  `;
  const serverLines = entries.map((entry) => entry.line || `[${entry.level || "INFO"}] ${entry.message || ""}`);
  const allLines = [...clientErrors, ...serverLines];
  els.logOutput.textContent = allLines.length
    ? allLines.join("\n")
    : "暂无日志。候选池刷新、交易调度、配置保存和错误都会显示在这里。";
  if (state.activeTab === "log") {
    window.requestAnimationFrame(() => {
      els.logOutput.scrollTop = els.logOutput.scrollHeight;
    });
  }
}

function renderAll() {
  renderMeta();
  renderAccount();
  renderPositions();
  renderClosedPositions();
  renderCandidates();
  renderDecisionLog();
  renderPromptTest();
  renderPromptLibrary();
  renderUniverseTest();
  renderLogs();
  setActiveTab(state.activeTab);
}

function scheduleRefresh() {
  if (state.autoRefreshTimer) window.clearTimeout(state.autoRefreshTimer);
  const delayMs = (state.settings?.pageAutoRefreshSeconds || 600) * 1000;
  state.autoRefreshTimer = window.setTimeout(loadData, delayMs);
}

async function loadData() {
  els.toggleModeBtn.disabled = true;
  els.modeRunnerBtn.disabled = true;
  try {
    const [latest, scan, settings, tradingState, tradingSettings, provider, prompt, promptLibrary, universe, network, liveConfig, logs] = await Promise.all([
      getJson("/api/latest"),
      getJson("/api/opportunities"),
      getJson("/api/settings"),
      getJson("/api/trading/state"),
      getJson("/api/trading/settings"),
      getJson("/api/trading/provider"),
      getJson("/api/trading/prompt"),
      getJson("/api/trading/prompt-library"),
      getJson("/api/trading/universe"),
      getJson("/api/network"),
      getJson("/api/trading/live-config"),
      getJson("/api/logs")
    ]);
    state.latest = latest;
    state.scan = scan;
    state.settings = settings;
    state.tradingState = tradingState;
    state.tradingSettings = tradingSettings;
    state.provider = provider;
    state.prompt = prompt;
    state.promptLibrary = promptLibrary;
    state.universe = universe;
    state.network = network;
    state.liveConfig = liveConfig;
    state.logs = logs;
    syncTradingSettingsForm();
    syncProviderForm();
    syncPromptForm();
    syncUniverseForm();
    syncNetworkForm();
    syncLiveConfigForm();
    renderAll();
  } catch (error) {
    els.decisionLog.innerHTML = `<p class="empty">加载失败：${escapeHtml(error.message)}</p>`;
    els.logOutput.textContent = `加载失败：${error.message}`;
    els.logMeta.textContent = "日志加载失败";
  } finally {
    els.toggleModeBtn.disabled = false;
    els.modeRunnerBtn.disabled = false;
    scheduleRefresh();
  }
}

async function loadNetworkIp() {
  try {
    state.networkIp = await getJson("/api/network/ip");
  } catch (error) {
    state.networkIp = {
      ip: null,
      error: error.message,
      proxyEnabled: state.network?.proxyEnabled === true
    };
  }
  syncLiveConfigForm();
}

async function handleManualRefresh() {
  if (els.refreshBtn) {
    els.refreshBtn.disabled = true;
    els.refreshBtn.textContent = "刷新中";
  }
  try {
    await loadData();
    await loadNetworkIp();
    els.settingsFeedback.textContent = "页面数据已刷新。";
  } catch (error) {
    els.settingsFeedback.textContent = `刷新失败：${error.message}`;
  } finally {
    if (els.refreshBtn) {
      els.refreshBtn.disabled = false;
      els.refreshBtn.textContent = "刷新";
    }
  }
}

function handleThemeToggle() {
  applyTheme(state.theme === "light" ? "dark" : "light");
}

function handleProviderPresetChange() {
  const preset = els.providerPresetInput.value || "gpt";
  const defaults = PROVIDER_DEFAULTS[preset] || PROVIDER_DEFAULTS.custom;
  els.providerApiStyleInput.value = defaults.apiStyle;
  els.providerBaseUrlInput.value = defaults.baseUrl;
  refreshProviderModelOptions(providerModelsForPreset(preset)[0] || "");
}

async function handleRunScan() {
  try {
    await postJson("/api/scan/run");
    els.settingsFeedback.textContent = "已触发候选池刷新。";
    window.setTimeout(loadData, 1200);
  } catch (error) {
    els.settingsFeedback.textContent = `候选池刷新失败：${error.message}`;
  }
}

function handleToggleMode() {
  state.viewMode = currentViewMode() === "live" ? "paper" : "live";
  writeStoredValue(MODE_STORAGE_KEY, state.viewMode);
  renderAll();
}

async function handleToggleRunner() {
  const mode = currentViewMode();
  const nextEnabled = !currentRunnerEnabled();
  els.modeRunnerBtn.disabled = true;
  try {
    state.tradingSettings = await postJson("/api/trading/settings", {
      [`${mode}Trading`]: {
        enabled: nextEnabled
      }
    });
    const label = nextEnabled ? "已启动" : "已暂停";
    els.settingsFeedback.textContent = `${modeName(mode)}${label}。调度会在下一次计划时间执行。`;
    await loadData();
  } catch (error) {
    els.settingsFeedback.textContent = `切换交易状态失败：${error.message}`;
  } finally {
    els.modeRunnerBtn.disabled = false;
  }
}

async function handleFlatten() {
  const mode = currentViewMode();
  if (!window.confirm(`确认平掉当前${modeName(mode)}下的全部持仓？`)) return;
  els.flattenBtn.disabled = true;
  try {
    await postJson("/api/trading/flatten", { mode });
    els.settingsFeedback.textContent = `${modeName(mode)}持仓已执行全部平仓。`;
    await loadData();
  } catch (error) {
    els.settingsFeedback.textContent = `平仓失败：${error.message}`;
    try {
      state.logs = await getJson("/api/logs");
      renderLogs();
    } catch {
      // Ignore best-effort log refresh failures after flatten error.
    }
  } finally {
    els.flattenBtn.disabled = false;
  }
}

async function handleReset() {
  const mode = currentViewMode();
  const confirmText = mode === "live"
    ? "确认重置实盘账户？这会清空本地 live 决策历史、历史收益、回撤基线；如果交易所当前仍有持仓，还会先执行真实平仓。"
    : "确认重置模拟盘账户？这会清空模拟盘持仓和历史。";
  if (!window.confirm(confirmText)) return;
  els.resetBtn.disabled = true;
  try {
    await postJson("/api/trading/reset", { mode });
    els.settingsFeedback.textContent = mode === "live" ? "实盘账户已重置。" : "模拟盘账户已重置。";
    await loadData();
  } catch (error) {
    els.settingsFeedback.textContent = `重置失败：${error.message}`;
    try {
      state.logs = await getJson("/api/logs");
      renderLogs();
    } catch {
      // Ignore best-effort log refresh failures after reset error.
    }
  } finally {
    els.resetBtn.disabled = false;
  }
}

async function handleSaveTradingSettings(event) {
  event?.preventDefault?.();
  const previousActiveExchange = String(state.tradingSettings?.activeExchange || "binance");
  const tradingPayload = {
    activeExchange: els.activeExchangeInput.value || "binance",
    decisionIntervalMinutes: Number(els.decisionIntervalInput.value || 5),
    initialCapitalUsd: Number(els.initialCapitalInput.value || 1000),
    maxNewPositionsPerCycle: Number(els.maxNewPositionsInput.value || 1),
    maxOpenPositions: Number(els.maxOpenPositionsInput.value || 4),
    maxPositionNotionalUsd: Number(els.maxPositionNotionalInput.value || 150),
    maxGrossExposurePct: Number(els.maxGrossExposureInput.value || 100),
    maxAccountDrawdownPct: Number(els.maxDrawdownInput.value || 20),
    riskPerTradePct: Number(els.riskPerTradeInput.value || 2.5),
    minConfidence: Number(els.minConfidenceInput.value || 60),
    paperFeesBps: Number(els.paperFeesInput.value || 4),
    allowShorts: els.allowShortsInput.checked
  };
  const dashboardPayload = {
    pageAutoRefreshSeconds: Number(els.pageAutoRefreshInput.value || 600)
  };
  if (els.saveTradingSettingsBtn) els.saveTradingSettingsBtn.disabled = true;
  els.settingsFeedback.textContent = "正在保存运行设置…";
  try {
    const [tradingSettings, dashboardSettings] = await Promise.all([
      postJson("/api/trading/settings", tradingPayload),
      postJson("/api/settings", dashboardPayload)
    ]);
    const exchangeChanged = previousActiveExchange !== String(tradingSettings?.activeExchange || "binance");
    if (exchangeChanged) {
      try {
        await postJson("/api/scan/run");
      } catch {
        // Candidate scan runs asynchronously; a failed trigger should not block saving settings.
      }
    }
    state.tradingSettings = tradingSettings;
    state.settings = dashboardSettings;
    const writeMatch = tradingSettingsMatch(tradingPayload, tradingSettings, dashboardPayload, dashboardSettings);
    await loadData();
    const reloadMatch = tradingSettingsMatch(tradingPayload, state.tradingSettings, dashboardPayload, state.settings);
    els.settingsFeedback.textContent = (writeMatch && reloadMatch)
      ? (exchangeChanged ? "运行设置已保存并校验成功，已按新交易所刷新候选池。" : "运行设置已保存并校验成功。")
      : tradingSettingsMismatchMessage(state.tradingSettings, state.settings);
    if (exchangeChanged) {
      window.setTimeout(loadData, 1200);
    }
  } catch (error) {
    els.settingsFeedback.textContent = `保存失败：${error.message}`;
  } finally {
    if (els.saveTradingSettingsBtn) els.saveTradingSettingsBtn.disabled = false;
  }
}

async function handleSaveProvider(event) {
  event?.preventDefault?.();
  const payload = buildProviderPayload();
  if (!payload.model) {
    els.providerFeedback.textContent = "请选择一个模型，或填写自定义模型名。";
    return;
  }
  if (els.saveProviderBtn) els.saveProviderBtn.disabled = true;
  els.providerFeedback.textContent = "正在保存AI模型配置…";
  try {
    state.provider = await postJson("/api/trading/provider", payload);
    await loadData();
    els.providerFeedback.textContent = providerConfigMatches(payload, state.provider)
      ? `AI模型配置已保存并校验成功：${providerLabel(state.provider)}`
      : "AI模型配置保存后读回不一致，请检查 Log。";
  } catch (error) {
    els.providerFeedback.textContent = `保存失败：${error.message}`;
  } finally {
    if (els.saveProviderBtn) els.saveProviderBtn.disabled = false;
  }
}

function openPromptModal(field) {
  const config = promptFieldConfig(field);
  if (!config) return;
  state.promptModalField = field;
  els.promptModalTitle.textContent = config.title;
  els.promptModalInput.value = config.input.value;
  els.promptModal.hidden = false;
  document.body.classList.add("modal-open");
  window.requestAnimationFrame(() => {
    els.promptModalInput.focus();
    els.promptModalInput.setSelectionRange(els.promptModalInput.value.length, els.promptModalInput.value.length);
  });
}

function closePromptModal() {
  state.promptModalField = null;
  els.promptModal.hidden = true;
  document.body.classList.remove("modal-open");
}

function applyPromptModal() {
  const config = promptFieldConfig(state.promptModalField);
  if (!config) {
    closePromptModal();
    return;
  }
  config.input.value = els.promptModalInput.value;
  closePromptModal();
  config.input.focus();
}

function handleLiveConfigModeToggle(source) {
  if (source === "enabled" && els.liveConfigEnabledInput.checked) {
    els.liveDryRunInput.checked = false;
  }
  if (source === "dryRun" && els.liveDryRunInput.checked) {
    els.liveConfigEnabledInput.checked = false;
  }
  if (!els.liveConfigEnabledInput.checked && !els.liveDryRunInput.checked) {
    if (source === "enabled") {
      els.liveDryRunInput.checked = true;
    } else {
      els.liveConfigEnabledInput.checked = true;
    }
  }
}

async function handleSavePrompt(event) {
  event?.preventDefault?.();
  const payload = buildPromptFormPayload();
  els.promptFeedback.textContent = "正在保存交易逻辑…";
  try {
    state.prompt = await postJson("/api/trading/prompt", payload);
    await loadData();
    els.promptFeedback.textContent = promptConfigMatches(payload, state.prompt)
      ? "交易逻辑已保存并校验成功。"
      : "交易逻辑保存后读回不一致，请检查 Log。";
  } catch (error) {
    els.promptFeedback.textContent = `保存失败：${error.message}`;
  }
}

async function handleSavePromptPreset() {
  const payload = buildPromptFormPayload();
  if (!payload.name || !String(payload.name).trim()) {
    els.promptFeedback.textContent = "请先填写 Prompt 名称。";
    return;
  }
  const linkedPreset = currentPromptPreset();
  if (linkedPreset && String(payload.name).trim() !== String(linkedPreset.name || "").trim()) {
    payload.presetId = null;
  }
  if (els.savePromptPresetBtn) els.savePromptPresetBtn.disabled = true;
  els.promptFeedback.textContent = "正在保存到已保存 Prompt…";
  try {
    const result = await postJson("/api/trading/prompt-library/save", payload);
    state.promptLibrary = {
      ...(state.promptLibrary || {}),
      prompts: result.prompts || [],
    };
    state.promptEditingPresetId = result.preset?.id || state.promptEditingPresetId;
    if (result.preset?.name) {
      els.promptNameInput.value = result.preset.name;
    }
    renderPromptLibrary();
    els.promptFeedback.textContent = `已保存 Prompt：${result.preset?.name || payload.name}`;
  } catch (error) {
    els.promptFeedback.textContent = `保存失败：${error.message}`;
  } finally {
    if (els.savePromptPresetBtn) els.savePromptPresetBtn.disabled = false;
  }
}

async function handlePromptPresetAction(event) {
  const button = event.target.closest("[data-prompt-preset-action]");
  if (!button) return;
  const action = button.dataset.promptPresetAction;
  const presetId = button.dataset.promptPresetId;
  const prompts = state.promptLibrary?.prompts || [];
  const preset = prompts.find((item) => item.id === presetId);
  if (!preset) {
    els.promptFeedback.textContent = "没有找到这个 Prompt。请刷新页面后重试。";
    return;
  }
  if (action === "load") {
    applyPromptEditor({ ...preset, name: preset.name }, null);
    els.promptFeedback.textContent = `已加载 Prompt 副本，后续保存会作为新 Prompt：${preset.name}`;
    return;
  }
  if (action === "rename") {
    const nextName = window.prompt("输入新的 Prompt 名称", preset.name || "");
    if (nextName === null) return;
    const trimmed = String(nextName || "").trim();
    if (!trimmed) {
      els.promptFeedback.textContent = "Prompt 名称不能为空。";
      return;
    }
    els.promptFeedback.textContent = "正在重命名 Prompt…";
    try {
      const result = await postJson("/api/trading/prompt-library/rename", { id: preset.id, name: trimmed });
      state.promptLibrary = {
        ...(state.promptLibrary || {}),
        prompts: result.prompts || [],
      };
      if (state.prompt?.presetId === preset.id) {
        state.prompt = await postJson("/api/trading/prompt", {
          name: result.preset?.name || trimmed,
          presetId: preset.id,
          role: state.prompt.role || state.prompt.decision_logic?.role || "",
          corePrinciplesText: state.prompt.corePrinciplesText || linesToText(state.prompt.decision_logic?.core_principles),
          entryPreferencesText: state.prompt.entryPreferencesText || linesToText(state.prompt.decision_logic?.entry_preferences),
          positionManagementText: state.prompt.positionManagementText || linesToText(state.prompt.decision_logic?.position_management),
          klineFeeds: state.prompt.klineFeeds,
        });
      }
      if (state.promptEditingPresetId === preset.id) {
        els.promptNameInput.value = result.preset?.name || trimmed;
      }
      if (state.prompt?.presetId === preset.id && state.prompt) {
        state.prompt.name = result.preset?.name || trimmed;
      }
      renderPromptLibrary();
      els.promptFeedback.textContent = `已重命名 Prompt：${result.preset?.name || trimmed}`;
    } catch (error) {
      els.promptFeedback.textContent = `重命名失败：${error.message}`;
    }
    return;
  }
  if (action === "delete") {
    if (!window.confirm(`确认删除这个已保存 Prompt？\n\n${preset.name}`)) return;
    els.promptFeedback.textContent = "正在删除 Prompt…";
    try {
      const result = await postJson("/api/trading/prompt-library/delete", { id: preset.id });
      state.promptLibrary = {
        ...(state.promptLibrary || {}),
        prompts: result.prompts || [],
      };
      if (state.promptEditingPresetId === preset.id) {
        state.promptEditingPresetId = null;
        renderPromptPresetMeta();
      }
      if (state.prompt?.presetId === preset.id) {
        state.prompt = await postJson("/api/trading/prompt", {
          name: state.prompt.name || els.promptNameInput.value || "default_trading_logic",
          presetId: null,
          role: state.prompt.role || state.prompt.decision_logic?.role || "",
          corePrinciplesText: state.prompt.corePrinciplesText || linesToText(state.prompt.decision_logic?.core_principles),
          entryPreferencesText: state.prompt.entryPreferencesText || linesToText(state.prompt.decision_logic?.entry_preferences),
          positionManagementText: state.prompt.positionManagementText || linesToText(state.prompt.decision_logic?.position_management),
          klineFeeds: state.prompt.klineFeeds,
        });
      }
      renderPromptLibrary();
      els.promptFeedback.textContent = `已删除 Prompt：${preset.name}`;
    } catch (error) {
      els.promptFeedback.textContent = `删除失败：${error.message}`;
    }
    return;
  }
  if (action === "use") {
    els.promptFeedback.textContent = "正在启用已保存 Prompt…";
    try {
      state.prompt = await postJson("/api/trading/prompt-library/use", { id: preset.id });
      await loadData();
      els.promptFeedback.textContent = `已启用 Prompt：${preset.name}`;
    } catch (error) {
      els.promptFeedback.textContent = `启用失败：${error.message}`;
    }
  }
}

async function handleTestPrompt() {
  els.testPromptBtn.disabled = true;
  const originalText = els.testPromptBtn.textContent;
  els.testPromptBtn.textContent = "测试中";
  els.promptTestMeta.textContent = "正在调用模型测试当前交易逻辑…";
  els.promptTestOutput.textContent = "测试中…";
  try {
    const providerPayload = buildProviderPayload();
    if (!providerPayload.model) {
      throw new Error("请先选择模型，或填写自定义模型名。");
    }
    els.providerFeedback.textContent = "正在保存AI模型配置，并自动探测可用协议…";
    state.provider = await postJson("/api/trading/provider", providerPayload);
    state.promptTest = {
      ...(await postJson("/api/trading/prompt/test", {
        ...buildPromptFormPayload(),
        mode: currentViewMode()
      })),
      testedAt: new Date().toISOString()
    };
    await loadData();
    if (state.promptTest.provider?.autoConfiguredSaved) {
      els.providerFeedback.textContent = `已自动识别并保存可用协议：${state.promptTest.provider.resolvedApiStyle}`;
      els.promptFeedback.textContent = "交易逻辑测试已完成，并已自动保存可用模型协议。";
    } else {
      els.promptFeedback.textContent = "交易逻辑测试已完成。";
    }
    renderPromptTest();
    window.setTimeout(async () => {
      try {
        state.logs = await getJson("/api/logs");
        renderLogs();
      } catch {
        // Ignore best-effort log refresh failures after prompt test.
      }
    }, 500);
  } catch (error) {
    state.promptTest = {
      error: error.message,
      testedAt: new Date().toISOString()
    };
    els.promptFeedback.textContent = `测试失败：${error.message}`;
    renderPromptTest();
  } finally {
    els.testPromptBtn.disabled = false;
    els.testPromptBtn.textContent = originalText;
  }
}

async function saveUniverse() {
  const previousFetchedAt = state.scan?.fetchedAt || null;
  const payload = {
    symbols: els.universeSymbolsInput.value,
    dynamicSource: {
      enabled: selectedUniverseMode() === "dynamic",
      functionName: "load_candidate_symbols"
    },
    candidateSourceCode: els.candidateSourceCodeInput.value
  };
  els.saveUniverseBtn.disabled = true;
  if (els.saveDynamicUniverseBtn) els.saveDynamicUniverseBtn.disabled = true;
  try {
    state.universe = await postJson("/api/trading/universe", payload);
    syncUniverseForm();
    state.universeTest = null;
    renderUniverseTest();
    const refreshResult = await postJson("/api/scan/run");
    await waitForLatestScan(previousFetchedAt);
    els.universeFeedback.textContent = universeConfigMatches(payload, state.universe)
      ? (refreshResult.started
        ? "候选池已保存并校验成功，且已自动刷新。"
        : "候选池已保存并校验成功，当前刷新任务已经在运行中。")
      : "候选池保存后读回不一致，请检查 Log。";
  } catch (error) {
    els.universeFeedback.textContent = `保存失败：${error.message}`;
  } finally {
    els.saveUniverseBtn.disabled = false;
    if (els.saveDynamicUniverseBtn) els.saveDynamicUniverseBtn.disabled = false;
  }
}

async function handleSaveUniverse(event) {
  event?.preventDefault?.();
  await saveUniverse();
}

async function handleSaveDynamicUniverse() {
  await saveUniverse();
}

async function handleTestUniverse() {
  els.testUniverseBtn.disabled = true;
  const originalText = els.testUniverseBtn.textContent;
  els.testUniverseBtn.textContent = "测试中";
  els.universeTestMeta.textContent = "正在测试候选池获取逻辑…";
  els.universePreview.textContent = "测试中…";
  try {
    state.universeTest = {
      ...(await postJson("/api/trading/universe/test", {
        symbols: els.universeSymbolsInput.value,
        dynamicSource: {
          enabled: selectedUniverseMode() === "dynamic",
          functionName: "load_candidate_symbols"
        },
        candidateSourceCode: els.candidateSourceCodeInput.value
      })),
      testedAt: new Date().toISOString()
    };
    els.universeFeedback.textContent = "候选池测试已完成。";
    renderUniverseTest();
    window.setTimeout(async () => {
      try {
        state.logs = await getJson("/api/logs");
        renderLogs();
      } catch {
        // Ignore best-effort log refresh failures after candidate-source test.
      }
    }, 500);
  } catch (error) {
    state.universeTest = {
      error: error.message,
      testedAt: new Date().toISOString()
    };
    els.universeFeedback.textContent = `测试失败：${error.message}`;
    renderUniverseTest();
  } finally {
    els.testUniverseBtn.disabled = false;
    els.testUniverseBtn.textContent = originalText;
  }
}

async function waitForLatestScan(previousFetchedAt) {
  for (let attempt = 0; attempt < 12; attempt += 1) {
    await new Promise((resolve) => window.setTimeout(resolve, attempt === 0 ? 900 : 700));
    try {
      const [latest, scan] = await Promise.all([
        getJson("/api/latest"),
        getJson("/api/opportunities")
      ]);
      const scanFetchedAt = scan?.fetchedAt || null;
      const scanRunning = latest?.scan?.scanRunner?.running === true;
      if (!scanRunning && scanFetchedAt && scanFetchedAt !== previousFetchedAt) {
        await loadData();
        return;
      }
      if (!scanRunning && scanFetchedAt) {
        state.latest = latest;
        state.scan = scan;
        renderCandidates();
        return;
      }
    } catch {
      // Ignore transient polling errors and continue polling briefly.
    }
  }
  await loadData();
}

async function handleSaveNetwork(event) {
  event?.preventDefault?.();
  const payload = {
    proxyEnabled: els.proxyEnabledInput.checked,
    proxyUrl: els.proxyUrlInput.value.trim(),
    noProxy: els.noProxyInput.value.trim()
  };
  if (els.saveNetworkBtn) els.saveNetworkBtn.disabled = true;
  els.networkFeedback.textContent = "正在保存代理配置…";
  try {
    state.network = await postJson("/api/network", payload);
    await loadNetworkIp();
    await loadData();
    els.networkFeedback.textContent = networkSettingsMatch(payload, state.network)
      ? "代理配置已保存并校验成功。"
      : "代理配置保存后读回不一致，请检查 Log。";
  } catch (error) {
    els.networkFeedback.textContent = `保存失败：${error.message}`;
  } finally {
    if (els.saveNetworkBtn) els.saveNetworkBtn.disabled = false;
  }
}

async function handleSaveLiveConfig(event) {
  event?.preventDefault?.();
  const enabled = els.liveConfigEnabledInput.checked;
  const dryRun = enabled ? false : true;
  const liveOption = liveExchangeOption(els.liveExchangeInput.value);
  const payload = {
    exchange: els.liveExchangeInput.value,
    enabled,
    dryRun,
    apiKey: els.liveApiKeyInput.value.trim(),
    apiSecret: els.liveApiSecretInput.value.trim(),
    apiPassphrase: liveOption.requiresPassphrase === true && els.liveApiPassphraseInput
      ? els.liveApiPassphraseInput.value.trim()
      : "",
    baseUrl: els.liveBaseUrlInput.value.trim(),
    defaultLeverage: Number(els.liveDefaultLeverageInput.value || 3),
    marginType: els.liveMarginTypeInput.value
  };
  if (els.saveLiveConfigBtn) els.saveLiveConfigBtn.disabled = true;
  els.liveConfigFeedback.textContent = "正在保存实盘账号配置…";
  try {
    state.liveConfig = await postJson("/api/trading/live-config", payload);
    await loadData();
    els.liveConfigFeedback.textContent = liveConfigMatches(payload, state.liveConfig)
      ? "实盘账号配置已保存并校验成功。"
      : "实盘账号配置保存后读回不一致，请检查 Log。";
  } catch (error) {
    els.liveConfigFeedback.textContent = `保存失败：${error.message}`;
  } finally {
    if (els.saveLiveConfigBtn) els.saveLiveConfigBtn.disabled = false;
  }
}

els.themeToggleBtn.addEventListener("click", handleThemeToggle);
els.toggleModeBtn.addEventListener("click", handleToggleMode);
els.modeRunnerBtn.addEventListener("click", handleToggleRunner);
els.flattenBtn.addEventListener("click", handleFlatten);
els.resetBtn.addEventListener("click", handleReset);
els.refreshBtn.addEventListener("click", handleManualRefresh);
els.tradingSettingsForm.addEventListener("submit", handleSaveTradingSettings);
els.saveTradingSettingsBtn.addEventListener("click", handleSaveTradingSettings);
els.providerForm.addEventListener("submit", handleSaveProvider);
els.saveProviderBtn.addEventListener("click", handleSaveProvider);
els.providerPresetInput.addEventListener("change", handleProviderPresetChange);
els.providerModelInput.addEventListener("change", syncProviderCustomModelVisibility);
els.providerAdvancedToggleBtn.addEventListener("click", handleProviderAdvancedToggle);
els.promptKlineEnabledInputs.forEach((input) => {
  input.addEventListener("change", () => handlePromptKlineEnabledChange(input.dataset.promptKlineEnabled));
});
els.promptExpandButtons.forEach((button) => {
  button.addEventListener("click", () => openPromptModal(button.dataset.promptExpand));
});
els.promptModalApplyBtn.addEventListener("click", applyPromptModal);
els.promptModalCloseBtn.addEventListener("click", closePromptModal);
els.promptModal.addEventListener("click", (event) => {
  if (event.target === els.promptModal) {
    closePromptModal();
  }
});
els.decisionLog.addEventListener("click", handleDecisionTabClick);
els.positionCards.addEventListener("click", handleSortClick);
els.closedPositionList.addEventListener("click", handleSortClick);
els.promptForm.addEventListener("submit", handleSavePrompt);
els.savePromptPresetBtn.addEventListener("click", handleSavePromptPreset);
els.testPromptBtn.addEventListener("click", handleTestPrompt);
els.savedPromptList.addEventListener("click", handlePromptPresetAction);
els.universeForm.addEventListener("submit", handleSaveUniverse);
els.saveUniverseBtn.addEventListener("click", handleSaveUniverse);
els.saveDynamicUniverseBtn.addEventListener("click", handleSaveDynamicUniverse);
els.testUniverseBtn.addEventListener("click", handleTestUniverse);
els.staticUniverseEnabledInput.addEventListener("change", () => handleUniverseModeToggle("static"));
els.dynamicUniverseEnabledInput.addEventListener("change", () => handleUniverseModeToggle("dynamic"));
els.networkForm.addEventListener("submit", handleSaveNetwork);
els.saveNetworkBtn.addEventListener("click", handleSaveNetwork);
els.liveExchangeInput.addEventListener("change", handleLiveExchangeChange);
els.liveConfigEnabledInput.addEventListener("change", () => handleLiveConfigModeToggle("enabled"));
els.liveDryRunInput.addEventListener("change", () => handleLiveConfigModeToggle("dryRun"));
els.liveConfigForm.addEventListener("submit", handleSaveLiveConfig);
els.saveLiveConfigBtn.addEventListener("click", handleSaveLiveConfig);
els.tabButtons.forEach((button) => {
  button.addEventListener("click", () => setActiveTab(button.dataset.tabButton));
});
document.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && els.promptModal.hidden === false) {
    closePromptModal();
  }
});

window.addEventListener("error", (event) => {
  recordClientError(event?.error?.message || event?.message || "未知脚本错误");
});

window.addEventListener("unhandledrejection", (event) => {
  const reason = event?.reason;
  if (reason instanceof Error) {
    recordClientError(reason.message);
    return;
  }
  recordClientError(typeof reason === "string" ? reason : JSON.stringify(reason));
});

applyTheme(state.theme);
setActiveTab(state.activeTab);
loadData();
loadNetworkIp();
