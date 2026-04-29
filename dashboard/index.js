const THEME_STORAGE_KEY = "gs-ai-trader-theme";

const els = {
  workbenchMeta: document.querySelector("#workbenchMeta"),
  workbenchSummary: document.querySelector("#workbenchSummary"),
  instanceGrid: document.querySelector("#instanceGrid"),
  createPaperBtn: document.querySelector("#createPaperBtn"),
  createLiveBtn: document.querySelector("#createLiveBtn"),
  refreshWorkbenchBtn: document.querySelector("#refreshWorkbenchBtn")
};

const state = {
  payload: null,
  filter: "all"
};

function readStoredTheme() {
  try {
    return window.localStorage.getItem(THEME_STORAGE_KEY) || "dark";
  } catch {
    return "dark";
  }
}

function applyTheme() {
  document.documentElement.dataset.theme = readStoredTheme() === "light" ? "light" : "dark";
}

function fmtUsd(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return "$0";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  }).format(num);
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

function fmtDurationSeconds(value) {
  const seconds = Math.max(0, Math.ceil(Number(value) || 0));
  const minutes = Math.floor(seconds / 60);
  const rest = seconds % 60;
  if (minutes <= 0) return `${rest} 秒`;
  return `${minutes} 分 ${rest} 秒`;
}

function cooldownText(cooldown) {
  if (!cooldown || !cooldown.active) return "";
  const exchange = String(cooldown.exchange || "binance").toUpperCase();
  const until = fmtDateTime(cooldown.untilAt);
  const remaining = fmtDurationSeconds(cooldown.remainingSeconds);
  return `${exchange} API 冷却中，预计到 ${until}，剩余 ${remaining}`;
}

function renderCooldownNotice(cooldown, className = "exchange-cooldown-banner") {
  const text = cooldownText(cooldown);
  if (!text) return "";
  const reason = cooldown.reason ? ` · ${cooldown.reason}` : "";
  return `<div class="${className}"><strong>${escapeHtml(text)}</strong><span>${escapeHtml(reason)}</span></div>`;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
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

function visibleInstances() {
  const instances = state.payload?.instances || [];
  if (state.filter === "paper") return instances.filter((item) => item.type === "paper");
  if (state.filter === "live") return instances.filter((item) => item.type === "live");
  if (state.filter === "running") return instances.filter((item) => item.running);
  return instances;
}

function buildSparklineSvg(points) {
  const dataset = (Array.isArray(points) ? points : [])
    .map((item) => ({
      at: item?.at || null,
      equityUsd: Number(item?.equityUsd)
    }))
    .filter((item) => Number.isFinite(item.equityUsd));
  const values = dataset
    .map((item) => item.equityUsd)
    .filter((item) => Number.isFinite(item));
  if (!values.length) {
    return `<div class="instance-sparkline-empty">暂无曲线</div>`;
  }
  const width = 320;
  const height = 156;
  const padding = 8;
  const min = Math.min(...values);
  const max = Math.max(...values);
  const flat = max === min;
  const span = flat ? 1 : (max - min);
  const coords = values.map((value, index) => {
    const x = padding + ((width - padding * 2) * index) / Math.max(1, values.length - 1);
    const y = flat
      ? height / 2
      : height - padding - ((value - min) / span) * (height - padding * 2);
    return { x, y };
  });
  const path = coords.map((point, index) => `${index === 0 ? "M" : "L"}${point.x.toFixed(2)},${point.y.toFixed(2)}`).join(" ");
  const fillPath = `${path} L${coords.at(-1).x.toFixed(2)},${(height - padding).toFixed(2)} L${coords[0].x.toFixed(2)},${(height - padding).toFixed(2)} Z`;
  const startPoint = coords[0];
  const endPoint = coords.at(-1);
  return `
    <svg viewBox="0 0 ${width} ${height}" class="instance-sparkline" preserveAspectRatio="none" aria-hidden="true">
      <path d="${fillPath}" class="instance-sparkline-fill"></path>
      <path d="${path}" class="instance-sparkline-line"></path>
      <circle cx="${startPoint.x.toFixed(2)}" cy="${startPoint.y.toFixed(2)}" r="4" class="instance-sparkline-dot is-start"></circle>
      <circle cx="${endPoint.x.toFixed(2)}" cy="${endPoint.y.toFixed(2)}" r="4.5" class="instance-sparkline-dot is-end"></circle>
    </svg>
  `;
}

function renderWorkbench() {
  const instances = state.payload?.instances || [];
  const runningCount = instances.filter((item) => item.running).length;
  const paperCount = instances.filter((item) => item.type === "paper").length;
  const liveCount = instances.filter((item) => item.type === "live").length;
  const activeCooldowns = Object.values(state.payload?.exchangeCooldowns || {}).filter((item) => item?.active);
  els.workbenchMeta.textContent = activeCooldowns.length ? activeCooldowns.map(cooldownText).filter(Boolean).join("；") : "";
  els.workbenchSummary.innerHTML = `
    <article class="workbench-summary-card ${state.filter === "all" ? "active" : ""}" data-filter-id="all">
      <span>全部</span>
      <strong>${instances.length}</strong>
    </article>
    <article class="workbench-summary-card ${state.filter === "paper" ? "active" : ""}" data-filter-id="paper">
      <span>模拟盘</span>
      <strong>${paperCount}</strong>
    </article>
    <article class="workbench-summary-card ${state.filter === "live" ? "active" : ""}" data-filter-id="live">
      <span>实盘</span>
      <strong>${liveCount}</strong>
    </article>
    <article class="workbench-summary-card ${state.filter === "running" ? "active" : ""}" data-filter-id="running">
      <span>运行中</span>
      <strong>${runningCount}</strong>
    </article>
  `;
  const cards = visibleInstances();
  els.instanceGrid.innerHTML = cards.map((instance) => `
    <article class="instance-card">
      <div class="instance-card-top">
        <div>
          <p class="instance-type">${escapeHtml(instance.type.toUpperCase())}</p>
          <h2>${escapeHtml(instance.name)}</h2>
          <p class="meta">交易所 ${escapeHtml(instance.exchange || "binance")} · ${escapeHtml(instance.running ? "已启动" : "已暂停")}</p>
        </div>
        <div class="instance-status-indicator ${instance.running ? "is-running" : "is-stopped"}" aria-label="${escapeHtml(instance.running ? "已启动" : "已暂停")}" title="${escapeHtml(instance.running ? "已启动" : "已暂停")}"></div>
      </div>
      <div class="instance-stats-grid">
        <span>Equity <strong>${escapeHtml(fmtUsd(instance.equityUsd))}</strong></span>
        <span>Open <strong>${escapeHtml(String(instance.openPositions || 0))}</strong></span>
        <span>候选池 <strong>${escapeHtml(String(instance.candidateUniverseSize || 0))}</strong></span>
        <span>下次调度 <strong>${escapeHtml(fmtDateTime(instance.nextDecisionDueAt))}</strong></span>
      </div>
      <div class="instance-chart-block">
        <div class="instance-chart-head">
          <span>Equity Curve</span>
          <strong>${escapeHtml(fmtUsd(instance.equityUsd))}</strong>
        </div>
        ${buildSparklineSvg(instance.equityCurve)}
      </div>
      <p class="meta">最近决策 ${escapeHtml(fmtDateTime(instance.lastDecisionAt))}</p>
      ${renderCooldownNotice(instance.exchangeCooldown, "instance-cooldown")}
      ${(instance.warnings || []).length ? `<p class="instance-warning">${escapeHtml(instance.warnings.join("；"))}</p>` : ""}
      <div class="instance-card-actions">
        <button type="button" data-instance-view="${escapeHtml(instance.id)}">查看</button>
        <button type="button" class="secondary-button" data-instance-toggle="${escapeHtml(instance.id)}">${instance.running ? "暂停" : "启动"}</button>
        <button type="button" class="secondary-button" data-instance-rename="${escapeHtml(instance.id)}">重命名</button>
        <button type="button" class="secondary-button danger-outline" data-instance-delete="${escapeHtml(instance.id)}">删除</button>
      </div>
    </article>
  `).join("");
  if (!cards.length) {
    els.instanceGrid.innerHTML = `<p class="empty">当前筛选条件下还没有实例。</p>`;
  }
}

async function loadWorkbench() {
  state.payload = await getJson("/api/instances");
  renderWorkbench();
}

async function handleCreate(type) {
  const defaultName = type === "live" ? "New Live" : "New Paper";
  const name = window.prompt("请输入实例名称", defaultName);
  if (!name) return;
  await postJson("/api/instances", { name, type });
  await loadWorkbench();
}

async function handleToggle(instanceId) {
  const instance = (state.payload?.instances || []).find((item) => item.id === instanceId);
  if (!instance) return;
  const nextEnabled = !instance.running;
  const payload = instance.type === "live"
    ? { liveTrading: { enabled: nextEnabled } }
    : { paperTrading: { enabled: nextEnabled } };
  await postJson(`/api/instances/${encodeURIComponent(instanceId)}/trading/settings`, payload);
  await loadWorkbench();
}

async function handleRename(instanceId) {
  const instance = (state.payload?.instances || []).find((item) => item.id === instanceId);
  if (!instance) return;
  const name = window.prompt("请输入新的实例名称", instance.name);
  if (!name) return;
  await postJson(`/api/instances/${encodeURIComponent(instanceId)}/rename`, { name });
  await loadWorkbench();
}

async function handleDelete(instanceId) {
  const instance = (state.payload?.instances || []).find((item) => item.id === instanceId);
  if (!instance) return;
  if (!window.confirm(`确认删除实例「${instance.name}」？这只会删除本地实例数据。`)) return;
  await postJson(`/api/instances/${encodeURIComponent(instanceId)}/delete`, {});
  await loadWorkbench();
}

els.createPaperBtn?.addEventListener("click", () => handleCreate("paper"));
els.createLiveBtn?.addEventListener("click", () => handleCreate("live"));
els.refreshWorkbenchBtn?.addEventListener("click", () => loadWorkbench());
els.workbenchSummary?.addEventListener("click", (event) => {
  const target = event.target;
  if (!(target instanceof HTMLElement)) return;
  const button = target.closest("[data-filter-id]");
  if (!(button instanceof HTMLElement)) return;
  state.filter = button.dataset.filterId || "all";
  renderWorkbench();
});
els.instanceGrid?.addEventListener("click", (event) => {
  const target = event.target;
  if (!(target instanceof HTMLElement)) return;
  const viewId = target.dataset.instanceView;
  if (viewId) {
    window.location.href = `/trader.html?instance=${encodeURIComponent(viewId)}`;
    return;
  }
  const toggleId = target.dataset.instanceToggle;
  if (toggleId) {
    void handleToggle(toggleId);
    return;
  }
  const renameId = target.dataset.instanceRename;
  if (renameId) {
    void handleRename(renameId);
    return;
  }
  const deleteId = target.dataset.instanceDelete;
  if (deleteId) {
    void handleDelete(deleteId);
  }
});

applyTheme();
void loadWorkbench().catch((error) => {
  els.workbenchMeta.textContent = `加载失败：${error.message}`;
});
