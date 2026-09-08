"use strict";

// No inline handlers anywhere: CSP script-src 'self' blocks attribute
// handlers, so every listener is attached here.

const $ = (id) => document.getElementById(id);

const state = {
  runId: null,
  source: null,
  windows: new Map(), // "DRV|lap" -> state string
  drivers: [],
  maxLap: 0,
  maxWatermark: 1,
};

function setMessage(el, text, kind) {
  el.textContent = text;
  el.className = "msg" + (kind ? " " + kind : "");
}

async function api(path, options) {
  const response = await fetch(path, Object.assign({
    credentials: "same-origin",
    headers: { "Content-Type": "application/json" },
  }, options));
  const body = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(body.detail || body.message || `Request failed (${response.status})`);
  }
  return body;
}

async function checkHealth() {
  try {
    const health = await api("/api/health");
    $("conn-dot").className = "dot up";
    $("conn-text").textContent = health.database === "ok" ? "Connected" : "Database unreachable";
    if (health.authenticated) {
      showControls();
    }
  } catch {
    $("conn-dot").className = "dot down";
    $("conn-text").textContent = "Server unreachable";
  }
}

async function signIn() {
  const key = $("api-key").value.trim();
  if (!key) {
    setMessage($("auth-msg"), "Enter your API key.", "error");
    return;
  }
  try {
    await api("/api/auth/session", {
      method: "POST",
      body: JSON.stringify({ api_key: key }),
    });
    $("api-key").value = "";
    setMessage($("auth-msg"), "", null);
    showControls();
  } catch (err) {
    setMessage($("auth-msg"), err.message, "error");
  }
}

async function showControls() {
  $("panel-auth").classList.add("hidden");
  $("panel-control").classList.remove("hidden");
  await loadDatasets();
}

async function loadDatasets() {
  const select = $("dataset");
  select.textContent = "";
  try {
    const datasets = await api("/api/datasets");
    if (!datasets.length) {
      const option = document.createElement("option");
      option.textContent = "No datasets registered";
      option.disabled = true;
      select.appendChild(option);
      return;
    }
    datasets.forEach((d) => {
      const option = document.createElement("option");
      option.value = d.dataset_id;
      option.textContent = `${d.label} — ${d.event_count} events, ${d.ground_truth_laps} laps`;
      select.appendChild(option);
    });
  } catch (err) {
    setMessage($("run-msg"), err.message, "error");
  }
}

function resetBoard() {
  state.windows.clear();
  state.drivers = [];
  state.maxLap = 0;
  $("board").textContent = "";
  $("mismatch-table").classList.add("hidden");
  $("mismatch-body").textContent = "";
  $("verdict").textContent = "";
}

async function startRun() {
  const datasetId = $("dataset").value;
  if (!datasetId) return;

  $("btn-run").disabled = true;
  $("btn-reconcile").disabled = true;
  resetBoard();
  setMessage($("run-msg"), "Starting.", null);

  const payload = {
    dataset_id: datasetId,
    allowed_lateness_s: Number($("lateness").value),
    max_lateness_s: Number($("max-lateness").value),
    p_duplicate: Number($("dup").value) / 100,
    tail_delay_s: Number($("tail").value),
  };

  if (payload.max_lateness_s < payload.allowed_lateness_s) {
    setMessage($("run-msg"), "Correction horizon must be at least the allowed lateness.", "error");
    $("btn-run").disabled = false;
    return;
  }

  try {
    const started = await api("/api/runs", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    state.runId = started.run_id;
    $("panel-live").classList.remove("hidden");
    setMessage($("run-msg"), "Running.", null);
    openStream(started.run_id);
  } catch (err) {
    setMessage($("run-msg"), err.message, "error");
    $("btn-run").disabled = false;
  }
}

function openStream(runId) {
  if (state.source) state.source.close();
  state.maxWatermark = 1;

  const source = new EventSource(`/api/runs/${runId}/events`);
  state.source = source;

  source.addEventListener("progress", (event) => {
    applyProgress(JSON.parse(event.data));
  });

  source.addEventListener("done", (event) => {
    applyProgress(JSON.parse(event.data));
    source.close();
    state.source = null;
    setMessage($("run-msg"), "Run finished.", "ok");
    $("btn-run").disabled = false;
    $("btn-reconcile").disabled = false;
    loadWindows(runId);
  });

  source.addEventListener("error", () => {
    source.close();
    state.source = null;
    $("btn-run").disabled = false;
    $("btn-reconcile").disabled = false;
    loadWindows(runId);
  });
}

function applyProgress(data) {
  $("c-events").textContent = data.events.toLocaleString();
  $("c-closed").textContent = data.closed.toLocaleString();
  $("c-amended").textContent = data.amended.toLocaleString();
  $("c-dropped").textContent = data.dropped.toLocaleString();

  if (data.watermark != null) {
    if (data.watermark > state.maxWatermark) state.maxWatermark = data.watermark;
    $("wm-value").textContent = data.watermark.toFixed(1) + "s";
    const pct = Math.min(100, (data.watermark / state.maxWatermark) * 100);
    $("wm-fill").style.setProperty("width", pct + "%");
  }
}

async function loadWindows(runId) {
  try {
    const page = await api(`/api/runs/${runId}/windows?limit=2000`);
    page.windows.forEach((w) => {
      const cls = w.sectors_seen < 3 ? "incomplete" : w.state;
      state.windows.set(`${w.driver}|${w.lap}`, cls);
      if (!state.drivers.includes(w.driver)) state.drivers.push(w.driver);
      if (w.lap > state.maxLap) state.maxLap = w.lap;
    });
    renderBoard();
  } catch (err) {
    setMessage($("run-msg"), err.message, "error");
  }
}

function renderBoard() {
  const board = $("board");
  board.textContent = "";
  state.drivers.sort();

  state.drivers.forEach((driver) => {
    const row = document.createElement("div");
    row.className = "driver-row";

    const code = document.createElement("span");
    code.className = "driver-code";
    code.textContent = driver;
    row.appendChild(code);

    const cells = document.createElement("div");
    cells.className = "cells";
    for (let lap = 1; lap <= state.maxLap; lap++) {
      const cell = document.createElement("i");
      const cls = state.windows.get(`${driver}|${lap}`);
      cell.className = "cell" + (cls ? " " + cls : "");
      cell.title = `${driver} lap ${lap}${cls ? " — " + cls : ""}`;
      cells.appendChild(cell);
    }
    row.appendChild(cells);
    board.appendChild(row);
  });
}

async function reconcile() {
  if (!state.runId) return;
  $("btn-reconcile").disabled = true;
  try {
    const result = await api(`/api/runs/${state.runId}/reconcile`, { method: "POST" });
    $("panel-result").classList.remove("hidden");

    const clean = result.mismatched === 0;
    const verdict = $("verdict");
    verdict.textContent = "";

    const headline = document.createElement("span");
    headline.className = clean ? "pass" : "fail";
    headline.textContent = `${result.matched.toLocaleString()} of ` +
      `${(result.matched + result.mismatched + result.missing).toLocaleString()} ` +
      `laps match ground truth (${result.match_rate}%)`;
    verdict.appendChild(headline);

    const detail = document.createElement("span");
    detail.className = "detail";
    detail.textContent = clean
      ? "Every window the stream produced agrees with the offline aggregation."
      : `${result.mismatched} windows disagree. Each one has a row in the late-event log explaining why.`;
    verdict.appendChild(detail);

    if (!clean) {
      await showMismatches();
    } else {
      $("mismatch-table").classList.add("hidden");
    }
  } catch (err) {
    setMessage($("run-msg"), err.message, "error");
  } finally {
    $("btn-reconcile").disabled = false;
  }
}

async function showMismatches() {
  const data = await api(`/api/runs/${state.runId}/reconciliation`);
  const body = $("mismatch-body");
  body.textContent = "";
  const rows = (data.detail && data.detail.mismatches) || [];
  rows.slice(0, 15).forEach((m) => {
    const tr = document.createElement("tr");
    [
      m.driver,
      m.lap,
      m.expected_lap_time.toFixed(3),
      m.actual_lap_time.toFixed(3),
      `${m.expected_sectors} → ${m.actual_sectors}`,
    ].forEach((value) => {
      const td = document.createElement("td");
      td.textContent = value;
      tr.appendChild(td);
    });
    body.appendChild(tr);
  });
  $("mismatch-table").classList.toggle("hidden", rows.length === 0);
}

function bindDial(inputId, outputId, transform) {
  const input = $(inputId);
  const output = $(outputId);
  const update = () => {
    output.textContent = transform ? transform(input.value) : input.value;
  };
  input.addEventListener("input", update);
  update();
}

$("btn-signin").addEventListener("click", signIn);
$("api-key").addEventListener("keydown", (event) => {
  if (event.key === "Enter") signIn();
});
$("btn-run").addEventListener("click", startRun);
$("btn-reconcile").addEventListener("click", reconcile);

bindDial("lateness", "out-lateness");
bindDial("max-lateness", "out-max");
bindDial("dup", "out-dup");
bindDial("tail", "out-tail", (v) => Number(v).toFixed(1));

checkHealth();