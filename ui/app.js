// Relay console: a static admin UI for a relay server. No build step.
// It talks straight to the server's /api with the admin token, and follows
// /api/stream for live presence and messages.
"use strict";

const STORE_KEY = "relay.console";
const PAGE = 100;
const FEED_MAX = 500;
const NAME_PATTERN = "[A-Za-z0-9][A-Za-z0-9_.\\-]{0,63}";
const $ = (sel) => document.querySelector(sel);
const enc = encodeURIComponent;

const state = {
  url: "",
  token: "",
  workers: [],
  channels: [],
  online: new Set(),
  view: { kind: "channels" },
  feed: { channel: null, messages: [], hasOlder: false },
  unread: new Map(),
  stream: "down",
  streamCtrl: null,
};

// --- dom -------------------------------------------------------------------
function h(tag, attrs, ...children) {
  const el = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (v == null || v === false) continue;
    if (k === "class") el.className = v;
    else if (k.startsWith("on")) el.addEventListener(k.slice(2), v);
    else if (v === true) el.setAttribute(k, "");
    else el.setAttribute(k, v);
  }
  for (const c of children.flat(Infinity)) {
    if (c == null || c === false) continue;
    el.append(c instanceof Node ? c : String(c));
  }
  return el;
}

// replaceChildren() neither flattens arrays nor skips false, so conditional and
// mapped children go through here.
function fill(el, ...children) {
  el.replaceChildren(...children.flat(Infinity).filter((c) => c != null && c !== false));
}

const dot = (online) => h("span", { class: "dot" + (online ? " ok" : ""), title: online ? "online" : "offline" });
const empty = (title, text) => h("div", { class: "empty" }, h("strong", {}, title), text);
const brand = () => h("div", { class: "brand" }, h("span", { class: "mark" }), "relay");

function field(label, input, hint) {
  return h("label", { class: "field" }, h("span", { class: "field-label" }, label), input,
    hint && h("span", { class: "hint" }, hint));
}

function copyBlock(text) {
  const code = h("code", {}, text);
  const btn = h("button", {
    class: "btn small", type: "button",
    onclick: async () => {
      try {
        await navigator.clipboard.writeText(text);
        btn.textContent = "Copied";
      } catch {
        getSelection().selectAllChildren(code);
        btn.textContent = "Press Ctrl+C";
      }
      setTimeout(() => { btn.textContent = "Copy"; }, 1600);
    },
  }, "Copy");
  return h("div", { class: "copy" }, code, btn);
}

function toast(text, bad = false) {
  const el = h("div", { class: "toast" + (bad ? " bad" : ""), role: bad ? "alert" : "status" }, text);
  $("#toasts").append(el);
  setTimeout(() => el.remove(), bad ? 6000 : 3000);
}

function openDialog(title, content, actions, onsubmit) {
  const dlg = h("dialog", { class: "modal" });
  const form = h("form", {
    class: "modal-inner",
    onsubmit: async (e) => {
      e.preventDefault();
      if (!onsubmit) return dlg.close();
      const buttons = form.querySelectorAll("button");
      buttons.forEach((b) => { b.disabled = true; });
      try { await onsubmit(dlg); } finally { buttons.forEach((b) => { b.disabled = false; }); }
    },
  }, h("h2", {}, title), content, h("div", { class: "modal-actions" }, actions));
  dlg.append(form);
  dlg.addEventListener("close", () => dlg.remove());
  document.body.append(dlg);
  dlg.showModal();
  return dlg;
}

const cancel = () => h("button", { class: "btn", type: "button", onclick: (e) => e.target.closest("dialog").close() }, "Cancel");

function ago(ts) {
  if (!ts) return "never";
  const s = Math.max(0, Math.round(Date.now() / 1000 - ts));
  if (s < 60) return "just now";
  for (const [unit, size] of [["d", 86400], ["h", 3600], ["m", 60]]) {
    if (s >= size) return `${Math.floor(s / size)}${unit} ago`;
  }
  return "just now";
}

function clock(ts) {
  const d = new Date(ts * 1000);
  const time = d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
  return d.toDateString() === new Date().toDateString() ? time : `${d.toLocaleDateString()} ${time}`;
}

// --- storage ---------------------------------------------------------------
function storage(kind) {
  try { return kind === "local" ? window.localStorage : window.sessionStorage; } catch { return null; }
}

function loadSaved() {
  for (const s of [storage("local"), storage("session")]) {
    try {
      const v = JSON.parse(s?.getItem(STORE_KEY) || "null");
      if (v?.url && v?.token) return v;
    } catch { /* unreadable: ignore */ }
  }
  return null;
}

function saveCreds(remember) {
  forget();
  try { storage(remember ? "local" : "session")?.setItem(STORE_KEY, JSON.stringify({ url: state.url, token: state.token })); } catch { /* private mode */ }
}

function forget() {
  for (const s of [storage("local"), storage("session")]) {
    try { s?.removeItem(STORE_KEY); } catch { /* ignore */ }
  }
}

// --- api -------------------------------------------------------------------
class ApiError extends Error {
  constructor(status, message) { super(message); this.status = status; }
}

function normalizeUrl(raw) {
  let url = raw.trim().replace(/\/+$/, "");
  if (url && !/^https?:\/\//i.test(url)) url = "https://" + url;
  return url;
}

function unreachable() {
  const local = /^http:\/\/(localhost|127\.0\.0\.1)(:|\/|$)/.test(state.url);
  if (location.protocol === "https:" && state.url.startsWith("http://") && !local) {
    return "This console is served over HTTPS, so the browser blocks an http:// server. Put the relay behind HTTPS.";
  }
  return `Could not reach ${state.url}. Check the URL, and that the server was started with RELAY_CORS_ORIGINS=${location.origin}`;
}

async function api(method, path, body) {
  const headers = { Authorization: `Bearer ${state.token}` };
  if (body !== undefined) headers["Content-Type"] = "application/json";
  let res;
  try {
    res = await fetch(state.url + path, {
      method, headers, cache: "no-store", body: body === undefined ? undefined : JSON.stringify(body),
    });
  } catch {
    throw new ApiError(0, unreachable());
  }
  let data = null;
  try { data = await res.json(); } catch { /* empty body */ }
  if (!res.ok) {
    const detail = typeof data?.detail === "string" ? data.detail
      : Array.isArray(data?.detail) ? data.detail.map((d) => d.msg).join("; ") : "";
    throw new ApiError(res.status, res.status === 401 ? "The admin token was rejected." : detail || `${res.status} ${res.statusText}`);
  }
  return data;
}

function handleError(err) {
  if (err?.status === 401) return signOut(err.message);
  toast(err?.message || String(err), true);
}

// --- sign in ---------------------------------------------------------------
function showConnect({ url = "", error = "" } = {}) {
  stopStream();
  const urlIn = h("input", { type: "text", inputmode: "url", required: true, placeholder: "https://relay.example.com", value: url, autocomplete: "url", spellcheck: "false" });
  const tokenIn = h("input", { type: "password", required: true, placeholder: "RELAY_ADMIN_TOKEN", autocomplete: "current-password" });
  const remember = h("input", { type: "checkbox", checked: true });
  const err = h("p", { class: "error", role: "alert", hidden: !error }, error);
  const btn = h("button", { class: "btn primary block", type: "submit" }, "Connect");

  const form = h("form", {
    class: "connect-card",
    onsubmit: async (e) => {
      e.preventDefault();
      state.url = normalizeUrl(urlIn.value);
      state.token = tokenIn.value.trim();
      btn.disabled = true;
      btn.textContent = "Connecting…";
      try {
        await api("GET", "/api/workers");
        saveCreds(remember.checked);
        showApp();
      } catch (ex) {
        err.textContent = ex.message;
        err.hidden = false;
        btn.disabled = false;
        btn.textContent = "Connect";
      }
    },
  },
  brand(),
  h("h1", {}, "Connect to a relay"),
  h("p", {}, "Manage workers and channels, and watch messages move between them."),
  field("Server URL", urlIn),
  field("Admin token", tokenIn),
  h("label", { class: "check" }, remember, "Remember on this device"),
  err,
  btn,
  h("details", { class: "help" },
    h("summary", {}, "Server setup"),
    h("p", {}, "The relay must be reachable over HTTPS and must allow this page's origin. Start it with:"),
    copyBlock(`RELAY_CORS_ORIGINS=${location.origin}`)));

  $("#root").replaceChildren(h("div", { class: "connect" }, form));
  (url ? tokenIn : urlIn).focus();
}

function signOut(message) {
  forget();
  const url = state.url;
  state.token = "";
  state.workers = [];
  state.channels = [];
  state.unread.clear();
  showConnect({ url, error: typeof message === "string" ? message : "" });
}

// --- app shell -------------------------------------------------------------
async function showApp() {
  $("#root").replaceChildren(
    h("header", { class: "topbar" },
      brand(),
      h("span", { class: "server truncate", title: state.url }, state.url.replace(/^https?:\/\//, "")),
      h("span", { class: "spacer" }),
      h("span", { class: "pill", id: "online-pill" }),
      h("span", { class: "pill", id: "stream-pill" }),
      h("button", { class: "btn small", onclick: () => signOut() }, "Sign out")),
    h("div", { class: "shell" },
      h("nav", { class: "sidebar", id: "sidebar", "aria-label": "Workers and channels" }),
      h("main", { id: "main" })));
  renderStreamPill();
  startStream();
  await refreshAll();
  setView();
}

async function refreshAll() {
  try {
    const [workers, channels] = await Promise.all([api("GET", "/api/workers"), api("GET", "/api/channels")]);
    state.workers = workers;
    state.channels = channels;
    state.online = new Set(workers.filter((w) => w.online).map((w) => w.worker_id));
    renderSidebar();
    renderOnlinePill();
    if (state.view.kind === "workers") renderWorkersTable();
    else if (state.view.kind === "channels") renderChannelsList();
    else if (state.channels.some((c) => c.name === state.view.name)) { renderChannelHead(); renderMembers(); }
    else if ($("#feed")) renderMain();              // the open channel was deleted
  } catch (err) {
    handleError(err);
  }
}

let refreshTimer = 0;
function scheduleRefresh() {
  clearTimeout(refreshTimer);
  refreshTimer = setTimeout(refreshAll, 150);
}

function go(hash) {
  if (location.hash === hash) setView();
  else location.hash = hash;
}

function setView() {
  const m = location.hash.match(/^#\/channel\/(.+)$/);
  if (m) {
    const name = decodeURIComponent(m[1]);
    state.view = { kind: "channel", name };
    state.unread.delete(name);
  } else if (location.hash === "#/workers") {
    state.view = { kind: "workers" };
  } else {
    state.view = { kind: "channels" };
  }
  renderSidebar();
  renderMain();
}

const currentChannel = () => state.view.kind === "channel" ? state.channels.find((c) => c.name === state.view.name) : null;

function renderOnlinePill() {
  const el = $("#online-pill");
  if (!el) return;
  el.replaceChildren(dot(state.online.size > 0),
    h("span", {}, `${state.online.size}/${state.workers.length}`),
    h("span", { class: "online-text" }, "online"));
}

function renderStreamPill() {
  const el = $("#stream-pill");
  if (!el) return;
  const [cls, text, title] = {
    live: ["ok", "Live", "Receiving updates as they happen"],
    connecting: ["warn", "Connecting", "Opening the live event stream"],
    down: ["bad", "Reconnecting", "The live stream dropped; retrying"],
  }[state.stream];
  el.title = title;
  el.replaceChildren(h("span", { class: "dot " + cls }), text);
}

function renderSidebar() {
  const nav = $("#sidebar");
  if (!nav) return;
  const v = state.view;
  fill(nav,
    h("div", { class: "nav-title" }, "Channels",
      h("button", { class: "btn icon", title: "New channel", "aria-label": "New channel", onclick: openNewChannel }, "+")),
    h("button", { class: "nav-item" + (v.kind === "channels" ? " active" : ""), onclick: () => go("#/channels") },
      "All channels", h("span", { class: "count" }, state.channels.length)),
    state.channels.length
      ? state.channels.map((c) => {
        const unread = state.unread.get(c.name) || 0;
        const active = v.kind === "channel" && v.name === c.name;
        return h("button", { class: "nav-item" + (active ? " active" : ""), onclick: () => go("#/channel/" + enc(c.name)) },
          h("span", { class: "hash" }, "#"), h("span", { class: "truncate" }, c.name),
          unread ? h("span", { class: "badge", title: `${unread} new` }, unread > 99 ? "99+" : unread)
            : h("span", { class: "count", title: "messages retained" }, c.messages));
      })
      : h("p", { class: "nav-empty" }, "No channels yet."),
    h("div", { class: "nav-title" }, "Workers",
      h("button", { class: "btn icon", title: "New worker", "aria-label": "New worker", onclick: openNewWorker }, "+")),
    h("button", { class: "nav-item" + (v.kind === "workers" ? " active" : ""), onclick: () => go("#/workers") },
      "All workers", h("span", { class: "count" }, state.workers.length)));
}

// --- channels --------------------------------------------------------------
function renderChannelsView() {
  $("#main").replaceChildren(
    h("div", { class: "view-head" },
      h("div", { class: "grow" },
        h("h1", {}, "Channels"),
        h("p", {}, "A channel is the unit of delivery. Create one, then give workers membership of it.")),
      h("button", { class: "btn primary", onclick: openNewChannel }, "New channel")),
    h("div", { class: "scroll pad", id: "channels-list" }));
  renderChannelsList();
}

function renderChannelsList() {
  const box = $("#channels-list");
  if (!box) return;
  if (!state.channels.length) {
    // First run: a worker cannot do anything until a channel exists, so this
    // is the one action worth offering.
    box.replaceChildren(h("div", { class: "empty" },
      h("strong", {}, "Start with a channel"),
      h("p", {}, "Workers post to channels and receive from them. Nothing can be sent until one exists."),
      h("button", { class: "btn primary", onclick: openNewChannel }, "Create a channel")));
    return;
  }
  box.replaceChildren(h("div", { class: "table-wrap" }, h("table", {},
    h("thead", {}, h("tr", {}, ["Channel", "Members", "Messages", ""].map((t) => h("th", {}, t)))),
    h("tbody", {}, state.channels.map((c) => {
      const online = c.members.filter((m) => state.online.has(m)).length;
      return h("tr", {},
        h("td", {}, h("div", {},
          h("div", { class: "strong" }, h("span", { class: "hash" }, "#"), c.name),
          c.description && h("div", { class: "muted small" }, c.description))),
        h("td", {}, c.members.length
          ? h("div", { class: "chips" }, c.members.map((m) => h("span", { class: "chip" }, dot(state.online.has(m)), m)))
          : h("span", { class: "muted" }, "none")),
        h("td", { class: "muted nowrap" }, `${c.messages} retained`,
          c.members.length ? h("div", { class: "small" }, `${online}/${c.members.length} online`) : false),
        h("td", { class: "actions" },
          h("button", { class: "btn small", onclick: () => go("#/channel/" + enc(c.name)) }, "Open")));
    })))));
}

function renderMain() {
  if (state.view.kind === "workers") renderWorkersView();
  else if (state.view.kind === "channels") renderChannelsView();
  else renderChannelView(state.view.name);
}

// --- workers ---------------------------------------------------------------
function renderWorkersView() {
  $("#main").replaceChildren(
    h("div", { class: "view-head" },
      h("div", { class: "grow" },
        h("h1", {}, "Workers"),
        h("p", {}, "Each worker signs in with its own token and can only use the channels it belongs to.")),
      h("button", { class: "btn primary", onclick: openNewWorker }, "New worker")),
    h("div", { class: "scroll pad", id: "workers-table" }));
  renderWorkersTable();
}

function renderWorkersTable() {
  const box = $("#workers-table");
  if (!box) return;
  if (!state.workers.length) {
    box.replaceChildren(empty("No workers yet", "Register a worker to get the token it connects with."));
    return;
  }
  box.replaceChildren(h("div", { class: "table-wrap" }, h("table", {},
    h("thead", {}, h("tr", {}, ["Worker", "Channels", "Last seen", ""].map((t) => h("th", {}, t)))),
    h("tbody", {}, state.workers.map((w) => {
      const online = state.online.has(w.worker_id);
      return h("tr", {},
        h("td", {}, h("div", { class: "who" }, dot(online),
          h("div", {}, h("div", { class: "mono strong" }, w.worker_id), w.label && h("div", { class: "muted small" }, w.label)))),
        h("td", {}, w.channels.length
          ? h("div", { class: "chips" }, w.channels.map((c) => h("a", { class: "chip", href: "#/channel/" + enc(c) }, "#" + c)))
          : h("span", { class: "muted" }, "none")),
        h("td", { class: "muted nowrap" }, online ? "online now" : ago(w.last_seen)),
        h("td", { class: "actions" },
          h("button", { class: "btn small", onclick: () => rotateToken(w.worker_id) }, "New token"),
          h("button", { class: "btn small danger", onclick: () => removeWorker(w.worker_id) }, "Remove")));
    })))));
}

function openNewWorker() {
  const id = h("input", { type: "text", required: true, pattern: NAME_PATTERN, placeholder: "scout-1", autocomplete: "off", spellcheck: "false" });
  const label = h("input", { type: "text", placeholder: "Office PC, 2nd floor" });
  const err = h("p", { class: "error", hidden: true });
  openDialog("New worker",
    [field("Worker ID", id, "Letters, digits, _ . - (up to 64)."), field("Label", label, "Optional note for yourself."), err],
    [cancel(), h("button", { class: "btn primary", type: "submit" }, "Create")],
    async (dlg) => {
      try {
        const res = await api("POST", "/api/workers", { worker_id: id.value.trim(), label: label.value.trim() });
        dlg.close();
        showToken(res.worker_id, res.token, false);
        refreshAll();
      } catch (ex) {
        if (ex.status === 401) return handleError(ex);
        err.textContent = ex.message;
        err.hidden = false;
      }
    });
  id.focus();
}

function showToken(workerId, token, rotated) {
  openDialog(rotated ? `New token for ${workerId}` : `${workerId} is registered`, [
    h("p", {}, "Copy the token now. The server keeps only a hash of it, so it cannot be shown again."),
    copyBlock(token),
    rotated && h("p", { class: "muted small" }, "The old token has stopped working and the worker was disconnected."),
    h("h3", {}, "Connect a worker with it"),
    copyBlock(`client = RelayClient("${state.url}", "${token}")`),
  ], [h("button", { class: "btn primary", type: "submit" }, "Done")]);
}

async function rotateToken(workerId) {
  if (!confirm(`Issue a new token for ${workerId}?\n\nThe current token stops working immediately and the worker is disconnected until it uses the new one.`)) return;
  try {
    const res = await api("POST", `/api/workers/${enc(workerId)}/token`);
    showToken(workerId, res.token, true);
  } catch (err) { handleError(err); }
}

async function removeWorker(workerId) {
  if (!confirm(`Remove worker ${workerId}?\n\nIts token stops working, it is disconnected, and it leaves every channel.`)) return;
  try {
    await api("DELETE", `/api/workers/${enc(workerId)}`);
    toast(`Removed ${workerId}`);
    refreshAll();
  } catch (err) { handleError(err); }
}

// --- channels --------------------------------------------------------------
function openNewChannel() {
  const name = h("input", { type: "text", required: true, pattern: NAME_PATTERN, placeholder: "jobs", autocomplete: "off", spellcheck: "false" });
  const desc = h("input", { type: "text", placeholder: "What gets posted here" });
  const err = h("p", { class: "error", hidden: true });
  openDialog("New channel",
    [field("Name", name, "Letters, digits, _ . - (up to 64)."), field("Description", desc), err],
    [cancel(), h("button", { class: "btn primary", type: "submit" }, "Create")],
    async (dlg) => {
      try {
        const res = await api("POST", "/api/channels", { name: name.value.trim(), description: desc.value.trim() });
        dlg.close();
        await refreshAll();
        go("#/channel/" + enc(res.name));
      } catch (ex) {
        if (ex.status === 401) return handleError(ex);
        err.textContent = ex.message;
        err.hidden = false;
      }
    });
  name.focus();
}

async function renderChannelView(name) {
  const main = $("#main");
  if (!state.channels.some((c) => c.name === name)) {
    main.replaceChildren(empty("Channel not found", `There is no channel named #${name}. It may have been deleted.`));
    return;
  }
  main.replaceChildren(
    h("div", { class: "view-head", id: "channel-head" }),
    h("div", { class: "channel-body" },
      h("section", { class: "feed-col", "aria-label": "Messages" },
        h("div", { class: "feed", id: "feed", "aria-live": "polite" }, h("p", { class: "muted center" }, "Loading messages…")),
        composer(name)),
      h("aside", { class: "members", id: "members" })));
  renderChannelHead();
  renderMembers();

  state.feed = { channel: name, messages: [], hasOlder: false };
  try {
    const page = await api("GET", `/api/channels/${enc(name)}/messages?limit=${PAGE}`);
    if (state.feed.channel !== name) return;             // navigated away meanwhile
    mergeMessages(page);
    state.feed.hasOlder = page.length === PAGE;
    renderFeed(true);
  } catch (err) {
    handleError(err);
    $("#feed")?.replaceChildren(empty("Could not load messages", err.message));
  }
}

function renderChannelHead() {
  const el = $("#channel-head");
  const ch = currentChannel();
  if (!el || !ch) return;
  const online = ch.members.filter((w) => state.online.has(w)).length;
  el.replaceChildren(
    h("div", { class: "grow" },
      h("h1", {}, h("span", { class: "hash" }, "#"), ch.name),
      h("p", {}, ch.description || "No description", ` · ${ch.members.length} member${ch.members.length === 1 ? "" : "s"}, ${online} online`)),
    h("button", { class: "btn danger", onclick: () => removeChannel(ch.name) }, "Delete channel"));
}

function renderMembers() {
  const el = $("#members");
  const ch = currentChannel();
  if (!el || !ch) return;
  const others = state.workers.filter((w) => !ch.members.includes(w.worker_id));
  const select = h("select", { "aria-label": "Worker to add" }, others.map((w) => h("option", { value: w.worker_id }, w.worker_id)));
  const history = h("input", { type: "checkbox" });

  el.replaceChildren(
    h("h2", {}, "Members"),
    h("div", { id: "member-list" }),
    h("h2", {}, "Add member"),
    others.length
      ? h("form", { class: "stack", onsubmit: (e) => { e.preventDefault(); addMember(ch.name, select.value, history.checked); } },
        select,
        h("label", { class: "check small" }, history, "Also deliver retained history"),
        h("button", { class: "btn", type: "submit" }, "Add to channel"))
      : h("p", { class: "muted small" }, state.workers.length ? "Every worker is already a member." : h("a", { href: "#/workers" }, "Register a worker first.")));
  renderMemberList();
}

function renderMemberList() {
  const el = $("#member-list");
  const ch = currentChannel();
  if (!el || !ch) return;
  el.replaceChildren(ch.members.length
    ? h("ul", { class: "member-list" }, ch.members.map((id) => h("li", {},
      dot(state.online.has(id)),
      h("span", { class: "mono truncate", title: id }, id),
      h("button", { class: "btn icon", title: `Remove ${id} from #${ch.name}`, "aria-label": `Remove ${id} from #${ch.name}`, onclick: () => removeMember(ch.name, id) }, "×"))))
    : h("p", { class: "muted small" }, "No members yet. Only members can post here and receive what is posted."));
}

async function addMember(channel, workerId, fromStart) {
  if (!workerId) return;
  try {
    await api("PUT", `/api/channels/${enc(channel)}/members/${enc(workerId)}`, { from_start: fromStart });
    toast(`${workerId} joined #${channel}`);
    refreshAll();
  } catch (err) { handleError(err); }
}

async function removeMember(channel, workerId) {
  try {
    await api("DELETE", `/api/channels/${enc(channel)}/members/${enc(workerId)}`);
    toast(`${workerId} left #${channel}`);
    refreshAll();
  } catch (err) { handleError(err); }
}

async function removeChannel(name) {
  if (!confirm(`Delete #${name}?\n\nEvery message in it is deleted and its members are removed. This cannot be undone.`)) return;
  try {
    await api("DELETE", `/api/channels/${enc(name)}`);
    toast(`Deleted #${name}`);
    await refreshAll();
    go("#/workers");
  } catch (err) { handleError(err); }
}

// --- messages --------------------------------------------------------------
function composer(name) {
  const ta = h("textarea", { rows: "3", "aria-label": "Message body", placeholder: '{"url": "https://example.com/job/1"}   or plain text' });
  const btn = h("button", { class: "btn primary", type: "submit" }, "Send");
  const form = h("form", {
    class: "composer",
    onsubmit: async (e) => {
      e.preventDefault();
      const raw = ta.value.trim();
      if (!raw) return;
      let body;
      try { body = JSON.parse(raw); } catch { body = { text: raw }; }
      btn.disabled = true;
      try {
        const res = await api("POST", `/api/channels/${enc(name)}/messages`, { body });
        ta.value = "";
        // The stream normally delivers our own post; add it directly if the stream is down.
        if (state.stream !== "live") addMessage({ channel: name, seq: res.seq, sender: "@server", body, ts: Date.now() / 1000 });
      } catch (err) {
        handleError(err);
      } finally {
        btn.disabled = false;
        ta.focus();
      }
    },
  }, ta, h("div", { class: "composer-foot" },
    h("span", { class: "muted small" }, "Posts as @server to every member. JSON, or text sent as {\"text\": …}. Ctrl+Enter sends."),
    btn));
  ta.addEventListener("keydown", (e) => {
    if (e.key === "Enter" && (e.ctrlKey || e.metaKey)) { e.preventDefault(); form.requestSubmit(); }
  });
  return form;
}

function mergeMessages(list) {
  const bySeq = new Map(state.feed.messages.map((m) => [m.seq, m]));
  for (const m of list) bySeq.set(m.seq, m);
  let merged = [...bySeq.values()].sort((a, b) => a.seq - b.seq);
  if (merged.length > FEED_MAX) {
    merged = merged.slice(-FEED_MAX);
    state.feed.hasOlder = true;
  }
  state.feed.messages = merged;
}

function addMessage(m) {
  if (state.feed.channel !== m.channel || state.feed.messages.some((x) => x.seq === m.seq)) return;
  mergeMessages([m]);
  renderFeed(false);
}

async function loadOlder() {
  const { channel, messages } = state.feed;
  if (!messages.length) return;
  try {
    const page = await api("GET", `/api/channels/${enc(channel)}/messages?before=${messages[0].seq}&limit=${PAGE}`);
    if (state.feed.channel !== channel) return;
    const feed = $("#feed");
    const fromBottom = feed.scrollHeight - feed.scrollTop;
    state.feed.messages = [...page, ...messages];      // older pages are not capped: the user asked for them
    state.feed.hasOlder = page.length === PAGE;
    renderFeed(false);
    feed.scrollTop = feed.scrollHeight - fromBottom;
  } catch (err) { handleError(err); }
}

function renderFeed(stick) {
  const feed = $("#feed");
  if (!feed) return;
  const nearBottom = feed.scrollHeight - feed.scrollTop - feed.clientHeight < 80;
  const { messages, hasOlder } = state.feed;
  fill(feed,
    hasOlder && h("div", { class: "older" }, h("button", { class: "btn small", onclick: loadOlder }, "Load older messages")),
    messages.length ? messages.map(messageRow)
      : empty("No messages yet", "What members post appears here as it happens. You can also post below."));
  if (stick || nearBottom) feed.scrollTop = feed.scrollHeight;
}

function messageRow(m) {
  const body = m.body;
  const plain = body && typeof body === "object" && !Array.isArray(body)
    && Object.keys(body).length === 1 && typeof body.text === "string";
  return h("article", { class: "msg" },
    h("div", { class: "msg-meta" },
      h("span", { class: "msg-sender" + (m.sender === "@server" ? " server" : "") }, m.sender),
      h("span", {}, "#" + m.seq),
      h("time", { datetime: new Date(m.ts * 1000).toISOString(), title: new Date(m.ts * 1000).toLocaleString() }, clock(m.ts))),
    plain ? h("p", { class: "msg-text" }, body.text) : h("pre", {}, JSON.stringify(body, null, 2)));
}

// --- live stream -----------------------------------------------------------
function setStream(status) {
  state.stream = status;
  renderStreamPill();
}

function stopStream() {
  state.streamCtrl?.abort();
  state.streamCtrl = null;
}

const sleep = (ms, signal) => new Promise((resolve) => {
  const t = setTimeout(resolve, ms);
  signal.addEventListener("abort", () => { clearTimeout(t); resolve(); }, { once: true });
});

async function startStream() {
  stopStream();
  const ctrl = new AbortController();
  state.streamCtrl = ctrl;
  let backoff = 1000;
  let connectedBefore = false;

  while (!ctrl.signal.aborted) {
    setStream(connectedBefore ? "down" : "connecting");
    // One controller per attempt, so the watchdog can drop a silent connection
    // (a proxy that buffers, a laptop that slept) without stopping the loop.
    const attempt = new AbortController();
    const onAbort = () => attempt.abort();
    ctrl.signal.addEventListener("abort", onAbort, { once: true });
    let lastByte = Date.now();
    const watchdog = setInterval(() => { if (Date.now() - lastByte > 45000) attempt.abort(); }, 5000);
    try {
      const res = await fetch(state.url + "/api/stream", {
        headers: { Authorization: `Bearer ${state.token}` }, signal: attempt.signal, cache: "no-store",
      });
      if (res.status === 401) { signOut("The admin token was rejected."); return; }
      if (!res.ok || !res.body) throw new Error(`stream answered ${res.status}`);
      setStream("live");
      backoff = 1000;
      if (connectedBefore) resync();          // catch up on whatever happened while disconnected
      connectedBefore = true;
      const reader = res.body.pipeThrough(new TextDecoderStream()).getReader();
      let buf = "";
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        lastByte = Date.now();
        buf += value.replace(/\r\n/g, "\n");
        let i;
        while ((i = buf.indexOf("\n\n")) >= 0) {
          const block = buf.slice(0, i);
          buf = buf.slice(i + 2);
          const data = block.split("\n").filter((l) => l.startsWith("data:")).map((l) => l.slice(5).replace(/^ /, "")).join("\n");
          if (!data) continue;
          try { onEvent(JSON.parse(data)); } catch (err) { console.warn("bad stream event", err); }
        }
      }
    } catch {
      /* network error or watchdog: retry below */
    } finally {
      clearInterval(watchdog);
      ctrl.signal.removeEventListener("abort", onAbort);
    }
    if (ctrl.signal.aborted) return;
    setStream("down");
    await sleep(backoff, ctrl.signal);
    backoff = Math.min(backoff * 2, 30000);
  }
}

function resync() {
  refreshAll();
  if (state.view.kind === "channel") renderChannelView(state.view.name);
}

function renderPresence() {
  renderOnlinePill();
  if (state.view.kind === "workers") renderWorkersTable();
  else if (state.view.kind === "channels") renderChannelsList();
  else { renderChannelHead(); renderMemberList(); }
}

function onEvent(ev) {
  switch (ev.kind) {
    case "hello":
      state.online = new Set(ev.online);
      renderPresence();
      break;
    case "worker":
      if (ev.online) state.online.add(ev.worker_id);
      else {
        state.online.delete(ev.worker_id);
        const w = state.workers.find((x) => x.worker_id === ev.worker_id);
        if (w) w.last_seen = ev.ts;
      }
      renderPresence();
      break;
    case "message": {
      const ch = state.channels.find((c) => c.name === ev.channel);
      if (ch) ch.messages += 1;
      if (state.view.kind === "channel" && state.view.name === ev.channel) addMessage(ev);
      else state.unread.set(ev.channel, (state.unread.get(ev.channel) || 0) + 1);
      renderSidebar();
      break;
    }
    case "changed":
      scheduleRefresh();
      break;
    case "resync":
      resync();
      break;
  }
}

// --- boot ------------------------------------------------------------------
window.addEventListener("hashchange", () => { if ($("#main")) setView(); });
setInterval(() => { if (state.view.kind === "workers") renderWorkersTable(); }, 30000);   // keep "last seen" honest

(function boot() {
  const saved = loadSaved();
  const server = new URLSearchParams(location.search).get("server");
  if (saved && (!server || normalizeUrl(server) === saved.url)) {
    state.url = saved.url;
    state.token = saved.token;
    showApp();
  } else {
    showConnect({ url: server || window.RELAY_DEFAULT_SERVER || "" });
  }
})();
