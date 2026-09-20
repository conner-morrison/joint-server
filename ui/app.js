// Relay console: a static admin UI for a relay server. No build step.
// It talks straight to the server's /api with the admin token, and follows
// /api/stream for live presence and messages.
"use strict";

const STORE_KEY = "relay.console";
const WORKSPACES_KEY = "relay.workspaces";
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
  workspace: "",
  pending: [],
  bots: [],
  delivery: [],
  view: { kind: "channels" },
  feed: { channel: null, messages: [], hasOlder: false },
  unread: new Map(),
  stream: "down",
  streamCtrl: null,
  pollTimer: 0,
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
// A workspace is a named relay: its address and admin token under a name you
// choose, so the console can be opened straight at /<name> instead of being
// told where to connect every time.
function loadWorkspaces() {
  try {
    const all = JSON.parse(storage("local")?.getItem(WORKSPACES_KEY) || "{}");
    return all && typeof all === "object" ? all : {};
  } catch { return {}; }
}

function saveWorkspace(name, entry) {
  const all = loadWorkspaces();
  all[name] = entry;
  try { storage("local")?.setItem(WORKSPACES_KEY, JSON.stringify(all)); } catch { /* private mode */ }
}

function removeWorkspace(name) {
  const all = loadWorkspaces();
  delete all[name];
  try { storage("local")?.setItem(WORKSPACES_KEY, JSON.stringify(all)); } catch { /* private mode */ }
}

// The path the console is served from, so it works at a domain root and under
// a sub-path alike.
function slugify(name) {
  return name.trim().toLowerCase().replace(/\s+/g, "").replace(/[^a-z0-9_.-]/g, "");
}

// The relay a workspace talks to, derived from where the console is served
// and the workspace's own slug.
function derivedUrl(slug) {
  const base = deploymentRoot();
  return slug ? `${base}/${slug}` : base;
}

// Files the console is itself made of, so a path ending in one is not a
// workspace. Everything else in the last segment is a workspace name, whether
// or not this device has heard of it.
const FILE_SUFFIX = /\.(html?|js|mjs|css|json|map|png|jpe?g|gif|svg|ico|webp|txt|xml|woff2?)$/i;

function workspaceFromUrl() {
  const seg = location.pathname.replace(/\/+$/, "").split("/").pop() || "";
  if (!seg || FILE_SUFFIX.test(seg)) return "";
  try { return decodeURIComponent(seg); } catch { return seg; }
}

// Where the deployment itself is served from, derived from the address alone.
// Working it out from what is saved locally meant that a workspace this
// browser had never opened sent the console looking for the deployment inside
// that workspace, where it found nothing.
function basePath() {
  let path = location.pathname.replace(/\/+$/, "");
  const last = path.split("/").pop() || "";
  if (last && FILE_SUFFIX.test(last)) path = path.slice(0, -(last.length + 1));
  const slug = workspaceFromUrl();
  if (slug && path.endsWith("/" + slug)) path = path.slice(0, -(slug.length + 1));
  return path || "/";
}

const deploymentRoot = () => (location.origin + basePath()).replace(/\/+$/, "");

function goToWorkspace(name) {
  const base = basePath().replace(/\/+$/, "");
  history.pushState({}, "", `${base}/${encodeURIComponent(name)}${location.hash}`);
  openWorkspace(name);
}

function goHome() {
  history.pushState({}, "", basePath().replace(/\/+$/, "") || "/");
  showHome();
}

// A workspace address is a place, not a request to be recognised. If this
// device knows the password, open it; if the server has it, ask for the
// password; only if it is nowhere does the question become whether to make it.
async function enterWorkspace(slug) {
  const saved = loadWorkspaces()[slug];
  if (saved?.token) return openWorkspace(slug);
  if (saved) return showWorkspaceSignIn(slug, saved.name || slug);
  let found = null;
  try {
    const res = await fetch(`${deploymentRoot()}/api/workspaces/${enc(slug)}`, { cache: "no-store" });
    if ((res.headers.get("content-type") || "").includes("json")) found = await res.json();
  } catch {
    /* offline, or not served by a relay */
  }
  if (found?.exists) return showWorkspaceSignIn(slug, found.name || slug);
  // The server answered and said no: this address is empty, which is a
  // different thing from a workspace this browser happens not to know.
  if (found) return showNotFound(slug);
  showHome({ error: `No workspace called "${slug}" on this device.`, prefill: slug });
}

// An address with nothing at it. Says so plainly, and offers the two things
// worth doing next rather than dropping the visitor at the front door.
function showNotFound(slug) {
  stopStream();
  state.workspace = "";
  $("#root").replaceChildren(h("div", { class: "connect" },
    h("div", { class: "connect-card" },
      brand(),
      h("p", { class: "notfound-code" }, "404"),
      h("h1", {}, "No workspace here"),
      h("p", {}, "This server has nothing at ", h("code", {}, "/" + slug), "."),
      h("p", { class: "muted small" },
        "Check the address, or make this one. Workspace names are chosen when "
        + "they are created, and the address is a lowercase form of the name."),
      h("button", { class: "btn primary block", onclick: () => openNewWorkspace(slug) },
        `Create /${slug}`),
      h("button", { class: "btn block", type: "button", onclick: () => goHome() }, "All workspaces"))));
}

// The front door of one workspace: its own page, asking only for its password.
function showWorkspaceSignIn(slug, name, message = "") {
  stopStream();
  state.workspace = "";
  const pass = h("input", { type: "password", required: true, autocomplete: "current-password" });
  const err = h("p", { class: "error", role: "alert", hidden: !message }, message);
  const btn = h("button", { class: "btn primary block", type: "submit" }, "Open");

  const form = h("form", {
    class: "connect-card",
    onsubmit: async (e) => {
      e.preventDefault();
      state.url = `${deploymentRoot()}/${enc(slug)}`;
      state.token = pass.value;
      btn.disabled = true;
      btn.textContent = "Opening…";
      try {
        await api("GET", "/api/channels");
      } catch (ex) {
        err.textContent = ex.status === 401 ? "That password does not open this workspace." : ex.message;
        err.hidden = false;
        btn.disabled = false;
        btn.textContent = "Open";
        return;
      }
      saveWorkspace(slug, { name, url: state.url, token: state.token });
      openWorkspace(slug);
    },
  },
    brand(),
    h("h1", {}, name),
    h("p", {}, "This workspace is on this server. Enter its password to open it here."),
    field("Password", pass),
    err,
    btn,
    h("button", { class: "btn block", type: "button", onclick: () => goHome() }, "All workspaces"));

  $("#root").replaceChildren(h("div", { class: "connect" }, form));
  pass.focus();
}

function openWorkspace(name) {
  const ws = loadWorkspaces()[name];
  if (!ws) return showHome({ error: `No workspace called "${name}" on this device.`, prefill: name });
  if (!ws.token) return showWorkspaceSignIn(name, ws.name || name);
  state.workspace = name;
  state.url = ws.url;
  state.token = ws.token;
  showApp();
}

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

const api = (method, path, body) => apiAt(state.url, method, path, body);

async function apiAt(base, method, path, body) {
  const headers = { Authorization: `Bearer ${state.token}` };
  if (body !== undefined) headers["Content-Type"] = "application/json";
  let res;
  try {
    res = await fetch(base + path, {
      method, headers, cache: "no-store", body: body === undefined ? undefined : JSON.stringify(body),
    });
  } catch {
    throw new ApiError(0, unreachable());
  }
  let data = null;
  try { data = await res.json(); } catch { /* not JSON, or an empty body */ }

  // Every relay answer is JSON. Anything else means this address is not a
  // relay: static hosting serves the console for unknown paths, or a 404 page.
  // Reading it as data puts nulls into the app and breaks it far from here.
  const isJson = (res.headers.get("content-type") || "").includes("json");
  if (!isJson || data === null) {
    throw new ApiError(res.status, `${base} answered with a page, not a relay. `
      + "Check the URL points at the relay server itself.");
  }
  if (!res.ok) {
    const detail = typeof data?.message === "string" ? data.message
      : typeof data?.detail === "string" ? data.detail
      : Array.isArray(data?.detail) ? data.detail.map((d) => d.msg).join("; ") : "";
    throw new ApiError(res.status, res.status === 401 ? "The password was rejected." : detail || `${res.status} ${res.statusText}`);
  }
  return data;
}

function handleError(err) {
  if (err?.status === 401) return signOut(err.message);
  toast(err?.message || String(err), true);
}

// --- home ------------------------------------------------------------------
// The first page: the workspaces on this device, and the one button that
// makes another.
function showHome({ error = "", prefill = "" } = {}) {
  stopStream();
  state.workspace = "";
  const err = h("p", { class: "error", role: "alert", hidden: !error }, error);
  const list = h("div", { class: "ws-list" });

  const card = h("div", { class: "connect-card" },
    brand(),
    h("h1", {}, "Workspaces"),
    h("p", { class: "muted" }, "A workspace is a relay you can open by name."),
    err,
    list,
    h("button", { class: "btn primary block", onclick: () => openNewWorkspace(prefill) }, "Create a workspace"));

  $("#root").replaceChildren(h("div", { class: "connect" }, card));
  renderWorkspaceList(list, card, list);
}

// What this device has a password for, and what the deployment says it holds.
// The two overlap: a workspace can be on the server and remembered here, on
// the server and not remembered, or remembered from a relay somewhere else.
// What the deployment said when asked for its workspaces. A relay with no
// database answers every request with the reason, so the list is where that
// shows up: nothing has to ask separately.
function troubleNotice(answer) {
  if (!answer || answer.status !== "unconfigured" && answer.status !== "database_unreachable") return null;
  const unset = answer.status === "unconfigured";
  return h("div", { class: "notice", role: "alert" },
    h("strong", {}, unset ? "This deployment has no database yet"
      : "This deployment cannot reach its database"),
    h("span", {}, unset
      ? "Set DATABASE_URL to a Postgres connection string in this project's environment variables, then redeploy."
      : "The database is configured but is not answering."),
    // Only when it says something this does not: a driver's complaint is
    // worth reading, a restatement of the sentence above is not.
    !unset && answer.message ? h("code", {}, answer.message) : false);
}

async function renderWorkspaceList(list, card, errEl) {
  const saved = loadWorkspaces();
  const draw = (onServer) => {
    const slugs = [...new Set([...Object.keys(saved), ...onServer.map((w) => w.slug)])].sort();
    if (!slugs.length) {
      fill(list, h("p", { class: "muted small" }, "None yet. Make one to begin."));
      return;
    }
    const names = new Map(onServer.map((w) => [w.slug, w.name]));
    fill(list, slugs.map((slug) => {
      const known = saved[slug];
      const here = known?.token ? known : null;        // known, but can we open it?
      const label = known?.name || names.get(slug) || slug;
      return h("div", { class: "ws-row" },
        h("button", {
          class: "ws-open",
          onclick: () => (here ? goToWorkspace(slug) : openExistingWorkspace(slug, names.get(slug) || slug)),
        },
          h("span", { class: "strong" }, label),
          h("span", { class: "muted small truncate" },
            "/" + slug, here ? "" : " · password needed on this device")),
        known ? h("button", {
          class: "btn small danger", title: `Forget ${label} on this device`,
          onclick: () => { removeWorkspace(slug); showHome(); },
        }, "Forget") : false);
    }));
  };

  draw([]);                                   // what is known here, immediately
  try {
    const res = await fetch(deploymentRoot() + "/api/workspaces", { cache: "no-store" });
    if (!(res.headers.get("content-type") || "").includes("json")) return;
    const answer = await res.json();
    if (!list.isConnected) return;
    if (Array.isArray(answer)) return draw(answer);
    const notice = troubleNotice(answer);
    if (notice && card?.isConnected) card.insertBefore(notice, errEl);
  } catch {
    /* not a relay, or offline: what is saved here is the whole answer */
  }
}

// A workspace this deployment has, that this browser has never opened.
function openExistingWorkspace(slug, name) {
  const pass = h("input", { type: "password", required: true, autocomplete: "current-password" });
  const err = h("p", { class: "error", hidden: true });
  openDialog(`Open ${name}`,
    [h("p", { class: "muted small" }, `This workspace is on this server at /${slug}. `
      + "Enter its password to open it on this device."),
     field("Password", pass), err],
    [cancel(), h("button", { class: "btn primary", type: "submit" }, "Open")],
    async (dlg) => {
      state.url = `${deploymentRoot()}/${enc(slug)}`;
      state.token = pass.value;
      try {
        await api("GET", "/api/channels");
      } catch (ex) {
        err.textContent = ex.status === 401 ? "That password does not open this workspace." : ex.message;
        err.hidden = false;
        return;
      }
      saveWorkspace(slug, { name, url: state.url, token: state.token });
      dlg.close();
      goToWorkspace(slug);
    });
  pass.focus();
}


function openNewWorkspace(prefill = "") {
  const name = h("input", {
    type: "text", required: true, value: prefill,
    placeholder: "Acme jobs", autocomplete: "off", spellcheck: "false",
  });
  const urlIn = h("input", {
    type: "text", inputmode: "url", required: true,
    autocomplete: "url", spellcheck: "false", value: derivedUrl(slugify(prefill)),
  });
  const pass = h("input", { type: "password", required: true, autocomplete: "new-password" });
  const again = h("input", { type: "password", required: true, autocomplete: "new-password" });
  const err = h("p", { class: "error", hidden: true });
  const urlHint = h("span", {});

  // The address follows the name until the address is edited by hand, after
  // which it is left alone: a typed URL should not be overwritten by typing.
  let urlTouched = false;
  urlIn.addEventListener("input", () => { urlTouched = true; });
  const syncUrl = () => {
    const slug = slugify(name.value);
    if (!urlTouched) urlIn.value = derivedUrl(slug);
    urlHint.textContent = slug ? `Opens at /${slug}` : "";
  };
  name.addEventListener("input", syncUrl);
  syncUrl();

  openDialog("Create a workspace",
    [field("Workspace name", name, "Spaces are fine; the address uses a lowercase form of it."),
     field("Relay URL", urlIn, urlHint),
     field("Password", pass, "The relay's admin password."),
     field("Confirm password", again),
     err],
    [cancel(), h("button", { class: "btn primary", type: "submit" }, "Create")],
    async (dlg) => {
      const fail = (text, focus) => { err.textContent = text; err.hidden = false; focus?.focus(); };
      const slug = slugify(name.value);
      if (!slug) return fail("Give the workspace a name with at least one letter or digit.", name);
      if (loadWorkspaces()[slug]) return fail(`A workspace at /${slug} already exists on this device.`, name);
      if (!pass.value) return fail("Set a password.", pass);
      if (pass.value !== again.value) return fail("The passwords do not match.", again);

      state.url = normalizeUrl(urlIn.value);
      state.token = pass.value;
      // The deployment is whatever the workspace address sits under.
      const root = state.url.endsWith("/" + slug) ? state.url.slice(0, -(slug.length + 1)) : state.url;
      try {
        await apiAt(root, "POST", "/api/workspaces", { name: name.value.trim(), password: pass.value });
      } catch (ex) {
        if (ex.status !== 409) return fail(ex.message, urlIn);
        // It is already there. If this password opens it, this is someone
        // adding a workspace they already have to another device.
        try {
          await api("GET", "/api/channels");
        } catch (inner) {
          return fail(inner.status === 401
            ? `A workspace at /${slug} already exists and that password does not open it.`
            : inner.message, pass);
        }
      }
      saveWorkspace(slug, { name: name.value.trim(), url: state.url, token: state.token });
      dlg.close();
      goToWorkspace(slug);
    });
  name.focus();
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

// Signing out, or being turned away, drops the password and keeps the
// workspace. Only the Forget button removes one: a password that stopped
// working is a reason to ask for it again, never to lose the address, and a
// workspace that vanishes from the list looks like one that was never made.
function signOut(message) {
  const slug = state.workspace;
  stopStream();
  forget();
  const ws = slug ? loadWorkspaces()[slug] : null;
  if (ws) saveWorkspace(slug, { ...ws, token: "" });
  state.token = "";
  state.workers = [];
  state.channels = [];
  state.pending = [];
  state.unread.clear();
  const said = typeof message === "string" ? message : "";
  if (slug && ws) {
    history.pushState({}, "", `${basePath().replace(/\/+$/, "")}/${enc(slug)}`);
    return showWorkspaceSignIn(slug, ws.name || slug, said);
  }
  history.pushState({}, "", basePath().replace(/\/+$/, "") || "/");
  showHome({ error: said });
}

// --- app shell -------------------------------------------------------------
async function showApp() {
  $("#root").replaceChildren(
    h("header", { class: "topbar" },
      h("button", { class: "brand-home", title: "All workspaces", onclick: () => goHome() }, brand()),
      state.workspace && h("span", { class: "ws-tag" },
        (loadWorkspaces()[state.workspace] || {}).name || state.workspace),
      h("span", { class: "server truncate", title: state.url }, state.url.replace(/^https?:\/\//, "")),
      h("span", { class: "spacer" }),
      h("span", { class: "pill", id: "online-pill" }),
      h("span", { class: "pill", id: "stream-pill" }),
      h("button", { class: "btn small", onclick: () => signOut() }, "Sign out")),
    h("div", { class: "shell" },
      h("nav", { class: "sidebar", id: "sidebar", "aria-label": "Workers and channels" }),
      h("main", { id: "main" })));
  renderStreamPill();
  // A first load that fails leaves nothing to show, so go back to the list
  // with the reason rather than rendering an empty console.
  try {
    await refreshAll({ rethrow: true });
  } catch (err) {
    stopStream();
    return showHome({ error: err?.message || String(err) });
  }
  startStream();
  setView();
}

async function refreshAll({ rethrow = false } = {}) {
  try {
    const [workers, channels, pending, bots] = await Promise.all([
      api("GET", "/api/workers"), api("GET", "/api/channels"),
      // Older relays have neither a waiting list nor bots; empty is the right
      // answer for both.
      api("GET", "/api/pending").catch(() => []),
      api("GET", "/api/bots").catch(() => []),
    ]);
    state.workers = workers;
    state.channels = channels;
    state.pending = Array.isArray(pending) ? pending : [];
    state.bots = Array.isArray(bots) ? bots : [];
    state.online = new Set(workers.filter((w) => w.online).map((w) => w.worker_id));
    renderSidebar();
    renderOnlinePill();
    if (state.view.kind === "workers") renderWorkersTable();
    else if (state.view.kind === "channels") renderChannelsList();
    else if (state.channels.some((c) => c.name === state.view.name)) { renderChannelHead(); renderMembers(); }
    else if ($("#feed")) renderMain();              // the open channel was deleted
  } catch (err) {
    if (rethrow) throw err;
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
    polling: ["ok", "Polling", "This relay has no event stream, so the console checks every few seconds"],
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
    state.pending.length ? h("button", {
      class: "nav-item waiting" + (v.kind === "workers" ? " active" : ""),
      onclick: () => go("#/workers"),
    }, `${state.pending.length} waiting to join`) : false,
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
      h("button", { class: "btn icon", title: "Add a Telegram bot", "aria-label": "Add a Telegram bot", onclick: openNewBot }, "+")),
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
      h("button", { class: "btn primary", onclick: openNewBot }, "+ Telegram bot")),
    h("div", { class: "scroll pad", id: "workers-table" }));
  renderWorkersTable();
}

function renderWorkersTable() {
  const box = $("#workers-table");
  if (!box) return;
  const waiting = renderPending();
  if (!state.workers.length && !state.bots.length) {
    box.replaceChildren(waiting || empty("Nothing here yet",
      "Add a Telegram bot to be sent alerts, or point a worker at this workspace "
      + "and approve it when it asks to join."));
    return;
  }
  box.replaceChildren(waiting || "", h("div", { class: "table-wrap" }, h("table", {},
    h("thead", {}, h("tr", {}, ["Worker", "Channels", "Last seen", ""].map((t) => h("th", {}, t)))),
    h("tbody", {},
      // Bots first: nothing about them changes minute to minute, and they are
      // the thing a person here adds by hand.
      state.bots.map((b) => h("tr", {},
        h("td", {}, h("div", { class: "who" }, h("span", { class: "bot-dot" }),
          h("div", {}, h("div", { class: "mono strong" }, b.name),
            h("div", { class: "muted small" }, "Telegram"
              + (b.label ? " \u00b7 @" + b.label : "") + " \u00b7 chat " + b.chat_id)))),
        h("td", {}, (b.channels || []).length
          ? h("div", { class: "chips" }, b.channels.map((c) => h("a", { class: "chip", href: "#/channel/" + enc(c) }, "#" + c)))
          : h("span", { class: "muted" }, "none")),
        h("td", { class: "muted nowrap" }, "sent to by the server"),
        h("td", { class: "actions" },
          h("button", { class: "btn small danger", onclick: () => removeBot(b.name) }, "Remove")))),
      state.workers.map((w) => {
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

// Workers that turned up on their own and are waiting for a person. Approving
// keeps the token the worker chose, so it carries on with what it was doing.
function renderPending() {
  if (!state.pending.length) return null;
  return h("div", { class: "pending" },
    h("div", { class: "pending-head" },
      h("strong", {}, state.pending.length === 1 ? "A worker is asking to join"
        : `${state.pending.length} workers are asking to join`),
      h("span", { class: "muted small" },
        "Approve only what you recognise: the code below is the start of the token's fingerprint, "
        + "which the worker can print too.")),
    state.pending.map((p) => h("div", { class: "pending-row" },
      h("div", { class: "grow" },
        h("div", { class: "mono strong" }, p.worker_id),
        h("div", { class: "muted small" },
          p.label ? p.label + " · " : "", "fingerprint ", h("code", {}, p.fingerprint),
          " · asked ", ago(p.requested_at))),
      h("button", { class: "btn small primary", onclick: () => approveWorker(p.worker_id) }, "Approve"),
      h("button", { class: "btn small danger", onclick: () => rejectWorker(p.worker_id) }, "Reject"))));
}

async function approveWorker(workerId) {
  try {
    await api("POST", "/api/pending/" + enc(workerId));
    toast(`${workerId} approved. Add it to the channels it should use.`);
    await refreshAll();
  } catch (err) { handleError(err); }
}

async function rejectWorker(workerId) {
  if (!confirm(`Refuse ${workerId}?\n\nIts request is discarded and the token it chose stops working. `
    + "It can ask again.")) return;
  try {
    await api("DELETE", "/api/pending/" + enc(workerId));
    await refreshAll();
  } catch (err) { handleError(err); }
}

// A bot is somewhere the server sends to, not something that connects. It is
// registered here with the workers because that is what it is: a recipient the
// workspace knows, added to whichever channels should reach it.
function openNewBot() {
  const name = h("input", { type: "text", required: true, pattern: NAME_PATTERN, placeholder: "my-phone", autocomplete: "off", spellcheck: "false" });
  const chat = h("input", { type: "text", required: true, placeholder: "8401438560", autocomplete: "off", spellcheck: "false" });
  const token = h("input", { type: "password", required: true, placeholder: "123456:AA...", autocomplete: "off" });
  const err = h("p", { class: "error", hidden: true });
  const create = h("button", { class: "btn primary", type: "submit" }, "Create");

  openDialog("Add a Telegram bot",
    [h("p", { class: "muted small" },
      "A welcome message is sent to prove the ID and token work. The bot is only "
      + "created if it arrives."),
     field("Name", name, "What to call it here."),
     field("Bot ID", chat, "The chat the message goes to."),
     field("Bot token", token, "From @BotFather."),
     err],
    [cancel(), create],
    async (dlg) => {
      err.hidden = true;
      create.disabled = true;
      create.textContent = "Sending a test\u2026";
      try {
        const made = await api("POST", "/api/bots", {
          name: name.value.trim(), chat_id: chat.value.trim(), token: token.value.trim(),
        });
        dlg.close();
        toast(`${made.name} is connected. Add it to a channel to start receiving.`);
        await refreshAll();
        go("#/workers");
      } catch (ex) {
        if (ex.status === 401) return handleError(ex);
        err.textContent = ex.status === 400
          ? "Invalid ID and token: Telegram would not deliver a message with them."
          : ex.message;
        err.hidden = false;
      } finally {
        create.disabled = false;
        create.textContent = "Create";
      }
    });
  name.focus();
}

async function removeBot(name) {
  if (!confirm(`Remove ${name}?\n\nIt stops receiving from every channel it is in.`)) return;
  try {
    await api("DELETE", "/api/bots/" + enc(name));
    toast(`Removed ${name}`);
    await refreshAll();
  } catch (err) { handleError(err); }
}

async function addBotMember(channel, bot, fromStart) {
  try {
    await api("PUT", `/api/channels/${enc(channel)}/bots/${enc(bot)}`, { from_start: fromStart });
    await refreshAll();
    renderMembers();
  } catch (err) { handleError(err); }
}

async function removeBotMember(channel, bot) {
  try {
    await api("DELETE", `/api/channels/${enc(channel)}/bots/${enc(bot)}`);
    await refreshAll();
    renderMembers();
  } catch (err) { handleError(err); }
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
  state.delivery = [];
  loadDelivery(name);
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
  const inChannel = state.bots.filter((b) => (b.channels || []).includes(ch.name));
  const freeBots = state.bots.filter((b) => !(b.channels || []).includes(ch.name));

  // Rebuilding this while it is being used throws away a chosen worker and a
  // ticked box, so when nothing about it has changed only the list is redrawn.
  const shape = `${ch.name}|${others.map((w) => w.worker_id).join(",")}`
    + `|${state.bots.map((b) => b.name + ":" + (b.channels || []).join("+")).join(",")}`;
  if (el.dataset.shape === shape) return renderMemberList();
  el.dataset.shape = shape;
  const select = h("select", { "aria-label": "Worker to add" }, others.map((w) => h("option", { value: w.worker_id }, w.worker_id)));
  const history = h("input", { type: "checkbox" });

  fill(el,
    h("h2", {}, "Members"),
    h("div", { id: "member-list" }),

    // A bot in a channel is a member of it, so it is listed as one.
    inChannel.length ? h("h2", {}, "Telegram bots") : false,
    inChannel.length
      ? h("div", { id: "bot-list" }, inChannel.map((b) => h("div", { class: "member" },
        h("span", { class: "bot-dot", title: "The server sends to this chat" }),
        h("div", { class: "grow" },
          h("div", { class: "strong truncate" }, b.name),
          h("div", { class: "muted small truncate" }, "chat " + b.chat_id)),
        waitingFor(b.name)
          ? h("button", {
            class: "behind",
            title: `${waitingFor(b.name)} waiting. Click to skip them and start from now.`,
            onclick: () => skipBacklog(ch.name, b.name, waitingFor(b.name), true),
          }, waitingFor(b.name))
          : false,
        h("button", {
          class: "btn icon", title: `Remove ${b.name} from this channel`,
          "aria-label": `Remove ${b.name} from this channel`,
          onclick: () => removeBotMember(ch.name, b.name),
        }, "\u00d7"))))
      : false,
    freeBots.length ? h("h2", {}, "Add a bot") : false,
    freeBots.length
      ? h("div", { class: "stack" }, freeBots.map((b) => h("button", {
        class: "btn small", onclick: () => addBotMember(ch.name, b.name, false),
      }, `Add ${b.name}`)))
      : false,

    h("h2", {}, "Add member"),
    others.length
      ? h("form", { class: "stack", onsubmit: (e) => { e.preventDefault(); addMember(ch.name, select.value, history.checked); } },
        select,
        // A new member starts from now. This is the one way to ask for what it
        // missed, so it says how much that is: "retained history" is easy to
        // tick without realising it means four hundred old messages.
        h("label", { class: "check small" }, history,
          ch.messages ? `Also send the ${ch.messages} already here` : "Also send what is already here"),
        h("button", { class: "btn", type: "submit" }, "Add to channel"))
      : h("p", { class: "muted small" }, state.workers.length
        ? "Every worker is already a member."
        : h("a", { href: "#/workers" }, "Point a worker at this workspace, or add a bot.")));
  renderMemberList();
}

// How far behind each member is. Without it, a worker that never joined and a
// worker that is merely asleep look identical from here: both silent, both
// apparently fine, and only one of them will ever receive anything.
async function loadDelivery(channel) {
  try {
    const rows = await api("GET", `/api/channels/${enc(channel)}/delivery`);
    if (state.feed.channel !== channel) return;          // navigated away meanwhile
    state.delivery = Array.isArray(rows) ? rows : [];
  } catch {
    state.delivery = [];             // an older relay cannot say; claim nothing
  }
  renderMemberList();
}

// A backlog is kept so a worker that was away misses nothing. For one that
// was away long enough, that is a stack of stale news it will read in order
// before reaching anything current, which is rarely what anyone wants.
async function skipBacklog(channel, name, waiting, isBot) {
  if (!confirm(`Skip ${waiting} message${waiting === 1 ? "" : "s"} waiting for ${name}?\n\n`
    + "They stay in the channel and stay visible here. "
    + `${name} will not receive them, and starts from what is posted next.`)) return;
  try {
    const done = await api("POST",
      `/api/channels/${enc(channel)}/skip/${enc(name)}${isBot ? "?bot=true" : ""}`);
    toast(`Skipped ${done.skipped} for ${name}`);
    await loadDelivery(channel);
  } catch (err) { handleError(err); }
}

// Sending the last message again, to answer "did that actually arrive".
// Nothing is republished: the member's place is put back, so the relay sends
// it what it already had. Everyone else is untouched.
async function resendLast(channel, name, isBot) {
  if (!confirm(`Send ${name} the last message in #${channel} again?`)) return;
  try {
    const done = await api("POST",
      `/api/channels/${enc(channel)}/resend/${enc(name)}${isBot ? "?bot=true" : ""}`);
    toast(done.resending ? `Resending 1 to ${name}` : "Nothing to send again");
    await loadDelivery(channel);
  } catch (err) { handleError(err); }
}

function waitingFor(name) {
  const row = state.delivery.find((r) => r.name === name);
  return row ? Number(row.waiting) || 0 : null;
}

function renderMemberList() {
  const el = $("#member-list");
  const ch = currentChannel();
  if (!el || !ch) return;
  el.replaceChildren(ch.members.length
    ? h("ul", { class: "member-list" }, ch.members.map((id) => {
      const waiting = waitingFor(id);
      return h("li", {},
        dot(state.online.has(id)),
        h("span", { class: "mono truncate", title: id }, id),
        waiting
          ? h("button", {
            class: "behind", title: `${waiting} waiting. Click to skip them and start from now.`,
            onclick: () => skipBacklog(ch.name, id, waiting, false),
          }, waiting)
          : waiting === 0
            ? h("button", {
              class: "caught-up",
              title: "Has everything posted here. Click to send the last one again.",
              onclick: () => resendLast(ch.name, id, false),
            }, "\u2713")
            : false,
        h("button", { class: "btn icon", title: `Remove ${id} from #${ch.name}`, "aria-label": `Remove ${id} from #${ch.name}`, onclick: () => removeMember(ch.name, id) }, "×"));
    }))
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

// A job id is what ties one worker's answer to another worker's request.
// Whoever mentions it first is the thing being answered.
const JOB_ID_KEYS = ["jobId", "job_id", "job", "proposalId", "proposal_id"];

function jobIdOf(body) {
  if (!body || typeof body !== "object" || Array.isArray(body)) return null;
  for (const key of JOB_ID_KEYS) {
    const value = body[key];
    if (typeof value === "string" && value.trim()) return value.trim();
    if (typeof value === "number") return String(value);
  }
  return null;
}

// Messages arranged as what happened and what came back. A later message
// carrying a job id already seen belongs under the one that introduced it,
// because that is what it is: an answer, not a new event.
function thread(messages) {
  const sources = new Map();          // job id -> the message that introduced it
  const replies = new Map();          // seq of that message -> answers to it
  const top = [];
  for (const m of messages) {
    const id = jobIdOf(m.body);
    const source = id ? sources.get(id) : null;
    if (!source) {
      if (id) sources.set(id, m);
      top.push(m);
      continue;
    }
    if (!replies.has(source.seq)) replies.set(source.seq, []);
    replies.get(source.seq).push(m);
  }
  return { top, replies };
}

function renderFeed(stick) {
  const feed = $("#feed");
  if (!feed) return;
  const nearBottom = feed.scrollHeight - feed.scrollTop - feed.clientHeight < 80;
  const { messages, hasOlder } = state.feed;
  const { top, replies } = thread(messages);
  fill(feed,
    hasOlder && h("div", { class: "older" }, h("button", { class: "btn small", onclick: loadOlder }, "Load older messages")),
    top.length ? top.map((m) => messageRow(m, replies.get(m.seq)))
      : empty("No messages yet", "What members post appears here as it happens. You can also post below."));
  if (stick || nearBottom) feed.scrollTop = feed.scrollHeight;
}

// Bodies are written by workers and the server never looks inside them, so a
// link made from one is a link chosen by whoever published it. Only http and
// https become links: anything else, `javascript:` above all, stays as text.
function httpUrl(value) {
  if (typeof value !== "string" || !value.trim()) return null;
  try {
    const url = new URL(value, location.href);
    return url.protocol === "http:" || url.protocol === "https:" ? url.href : null;
  } catch {
    return null;
  }
}

const FACTS = [["Budget", "budget"], ["Terms", "terms"], ["Published", "published"],
               ["Posted", "posted"], ["Skills", "skills"]];

// Every field a publisher might put a link in. An invitation carries its own,
// and `links` holds however many an item has.
// Named links, with what to call each. A reply from a worker says where the
// work was published and where its source is; both are worth their names.
const NAMED_LINKS = [["published", "Published"], ["deployed", "Published"], ["live", "Published"],
                     ["demo", "Demo"], ["source", "Source"], ["repo", "Source"],
                     ["repository", "Source"], ["homepage", "Homepage"],
                     ["upworkUrl", "Upwork"], ["inviteUrl", "Invitation"],
                     ["html_url", "Link"], ["htmlUrl", "Link"], ["url", "Link"], ["link", "Link"]];
const LINK_KEYS = NAMED_LINKS.map(([key]) => key);

// What to call a link when the publisher did not say. The end of the path
// usually names the thing; the host is a decent fallback for a bare domain.
function labelFor(url) {
  try {
    const at = new URL(url);
    const tail = at.pathname.split("/").filter(Boolean).slice(-2).join("/");
    return tail ? `${at.hostname.replace(/^www\./, "")}/${tail}` : at.hostname.replace(/^www\./, "");
  } catch {
    return url;
  }
}

// An item may carry no link, one, or several. Collected from wherever they
// were put, http and https only, in the order they were given and without
// repeats: the same URL under two field names is one link, not two.
function linksOf(body) {
  if (!body || typeof body !== "object" || Array.isArray(body)) return [];
  const found = [];
  const seen = new Set();
  const take = (value, label) => {
    if (Array.isArray(value)) return value.forEach((v) => take(v, label));
    if (value && typeof value === "object") {
      return take(value.url || value.href || value.link,
        value.label || value.title || value.name || label);
    }
    const url = httpUrl(value);
    if (!url || seen.has(url)) return;
    seen.add(url);
    // A named field is called by its name; a bare URL by where it points.
    found.push({ url, label: label && label !== "Link" ? `${label}: ${labelFor(url)}` : labelFor(url) });
  };
  for (const [key, label] of NAMED_LINKS) take(body[key], label);
  take(body.links);
  take(body.urls);
  return found;
}

// What is worth knowing about whoever posted the job, in the order it is worth
// reading. A field the publisher did not send is simply absent: none of this
// is invented for a message that does not carry it.
const CLIENT_FIELDS = [
  ["Rank", "rank"],
  ["Rating", "rating"],
  ["Payment", "paymentVerified"],
  ["Location", "location"],
  ["Reviews", "reviews"],
  ["Jobs posted", "jobsPosted"],
  ["Hire rate", "hireRate"],
  ["Spent", "spent"],
  ["Registered", "registered"],
];

// Where a job description might be called home, most specific first.
const JD_KEYS = ["description", "jobDescription", "jd", "snippet", "summary", "details", "text"];

const SHOWN = new Set([...FACTS.map(([, key]) => key), ...JD_KEYS, ...LINK_KEYS,
                       ...JOB_ID_KEYS, "links", "urls", "title",
                       "type", "source", "index", "emailId", "receivedAt",
                       "client", ...CLIENT_FIELDS.map(([, key]) => "client" + key[0].toUpperCase() + key.slice(1))]);

function jdOf(body) {
  for (const key of JD_KEYS) {
    if (typeof body[key] === "string" && body[key].trim()) return body[key].trim();
  }
  return null;
}

// Accepts either a nested object or flat clientRank-style keys, so a worker
// can send whichever is natural to it.
function clientOf(body) {
  const nested = body.client && typeof body.client === "object" && !Array.isArray(body.client)
    ? body.client : {};
  const found = {};
  for (const [, key] of CLIENT_FIELDS) {
    const flat = body["client" + key[0].toUpperCase() + key.slice(1)];
    const value = nested[key] !== undefined ? nested[key] : flat;
    if (value !== undefined && value !== null && value !== "") found[key] = value;
  }
  // A plain string under `client` is a name, which is still worth showing.
  if (typeof body.client === "string" && body.client.trim()) found.name = body.client.trim();
  return found;
}

function clientValue(key, value) {
  if (typeof value === "boolean") {
    return h("span", { class: value ? "yes" : "no" }, value ? "Verified" : "Not verified");
  }
  if (key === "paymentVerified") {
    const said = String(value).toLowerCase();
    if (["true", "yes", "verified"].includes(said)) return h("span", { class: "yes" }, "Verified");
    if (["false", "no", "unverified"].includes(said)) return h("span", { class: "no" }, "Not verified");
  }
  if (key === "rating" && typeof value === "number") return `${value.toFixed(2)} / 5`;
  return String(Array.isArray(value) ? value.join(", ") : value);
}

function clientBlock(body) {
  const client = clientOf(body);
  const rows = CLIENT_FIELDS
    .filter(([, key]) => client[key] !== undefined)
    .map(([label, key]) => h("div", { class: "client-row" },
      h("dt", {}, label), h("dd", {}, clientValue(key, client[key]))));
  if (!rows.length && !client.name) return null;
  return h("div", { class: "client" },
    h("div", { class: "client-head" }, "Client", client.name ? h("span", { class: "client-name" }, client.name) : false),
    rows.length ? h("dl", { class: "client-facts" }, rows) : false);
}

// A title is what makes something worth showing as itself rather than as
// JSON. Anything more is a bonus: requiring a known shape meant a message
// arranged even slightly differently - an invitation, say - arrived as
// punctuation, which is the one presentation nobody wants.
function looksLikeJob(body) {
  return Boolean(body && typeof body === "object" && !Array.isArray(body)
    && typeof body.title === "string" && body.title.trim());
}

function when(value) {
  const at = new Date(value);
  return Number.isNaN(at.getTime()) ? String(value) : at.toLocaleString();
}

function jobCard(body) {
  const links = linksOf(body);
  const jobId = jobIdOf(body);
  // When an item carries a job id and links, the two are about different
  // things: the title names the work, the links point at where it was done.
  // Linking the title to the first of them would say they were the same.
  const separate = Boolean(jobId && links.length);
  const link = separate ? null : (links.length ? links[0].url : null);
  const listed = separate ? links : links.slice(1);
  // The description is the longest thing here and the least often wanted, so
  // it is behind a button: a list of jobs should stay a list.
  const jd = jdOf(body);
  const shortJd = jd && jd.length <= 240 && !jd.includes("\n\n") ? jd : null;
  const jdBox = jd && !shortJd ? h("div", { class: "jd", hidden: true }, jd) : null;
  const jdButton = jdBox ? h("button", {
    class: "btn small jd-toggle", type: "button",
    onclick: () => {
      jdBox.hidden = !jdBox.hidden;
      jdButton.textContent = jdBox.hidden ? "View JD" : "Hide JD";
    },
  }, "View JD") : false;
  const facts = FACTS
    // `published` is a time on a job and a URL on a reply; a URL is a link,
    // already listed as one, not a fact to repeat.
    .filter(([, key]) => body[key] !== undefined && body[key] !== null && body[key] !== ""
      && !httpUrl(body[key]))
    .map(([label, key]) => h("span", { class: "fact" },
      h("b", {}, label), String(Array.isArray(body[key]) ? body[key].join(", ") : body[key])));
  if (body.receivedAt) {
    facts.push(h("span", { class: "fact" }, h("b", {}, "Received"), when(body.receivedAt)));
  }
  if (jobId) {
    facts.unshift(h("span", { class: "fact" }, h("b", {}, "Job"), h("code", {}, jobId)));
  }
  // Whatever the publisher sent that this does not have a place for. Hidden,
  // but never dropped: a body is the worker's, not the console's, to decide.
  const rest = Object.fromEntries(Object.entries(body).filter(([key]) => !SHOWN.has(key)));

  return h("div", { class: "job" },
    link
      ? h("a", { class: "job-title", href: link, target: "_blank", rel: "noopener noreferrer" }, body.title)
      : h("div", { class: "job-title" }, body.title),
    facts.length ? h("div", { class: "job-facts" }, facts) : false,
    shortJd ? h("p", { class: "msg-text" }, shortJd) : false,
    listed.length
      ? h("div", { class: "job-links" }, listed.map((l) =>
        h("a", { class: "link-chip", href: l.url, target: "_blank", rel: "noopener noreferrer",
                 title: l.url }, l.label)))
      : false,
    jdButton ? h("div", { class: "job-actions" }, jdButton) : false,
    jdBox || false,
    clientBlock(body),
    Object.keys(rest).length
      ? h("details", { class: "job-more" }, h("summary", {}, "Everything else"),
          h("pre", {}, JSON.stringify(rest, null, 2)))
      : false);
}

// Where an alert came from decides how much attention it deserves, so it is
// said on every message rather than left inside the body for someone to open.
// An invitation is the one worth interrupting someone for: it means a client
// asked, rather than that a search matched.
const INVITATION_RE = /\binvit|\binterview\b|asked you to apply|wants to interview/i;

// What the sender said it was, and failing that what it says it is. Messages
// already stored were labelled by whatever rule was current when they arrived,
// so reading the subject as well means a rule fixed later still applies to
// them: nothing has to be re-sent to be classified correctly.
function looksLikeInvitation(body) {
  // Only where nothing better is known. A parsed job is a job however it is
  // worded, and "Build an interview scheduling app" is a job title, not an
  // invitation to interview.
  const type = String(body.type || "").toLowerCase();
  const source = httpUrl(body.source) ? "" : String(body.source || "").toLowerCase();
  if (type === "job" || source.startsWith("vollna")) return false;
  return INVITATION_RE.test(`${body.title || ""} ${body.emailSubject || ""}`);
}

function sourceTag(body) {
  if (!body || typeof body !== "object" || Array.isArray(body)) return null;
  const source = httpUrl(body.source) ? "" : String(body.source || "").toLowerCase();
  const type = String(body.type || "").toLowerCase();

  if (type === "invitation" || source.includes("invit") || looksLikeInvitation(body)) {
    return h("span", { class: "src invite", title: "A client invited you to apply" }, "Invitation");
  }
  if (source.startsWith("vollna")) return h("span", { class: "src" }, "Vollna");
  if (source.startsWith("upwork")) return h("span", { class: "src" }, "Upwork");
  if (!source) return null;
  return h("span", { class: "src" }, source.replace(/[-_]+/g, " "));
}

function messageBody(body) {
  if (looksLikeJob(body)) return jobCard(body);
  const isPlain = body && typeof body === "object" && !Array.isArray(body)
    && typeof body.text === "string";
  if (isPlain) {
    // An alert that could not be split into jobs still has a subject, and
    // dropping it leaves a paragraph with nothing saying what it is about.
    const heading = body.title || body.emailSubject;
    const plainLinks = linksOf(body);
    const link = plainLinks.length ? plainLinks[0].url : null;
    return h("div", { class: "job" },
      heading
        ? (link
          ? h("a", { class: "job-title", href: link, target: "_blank", rel: "noopener noreferrer" }, heading)
          : h("div", { class: "job-title" }, heading))
        : false,
      h("p", { class: "msg-text" }, body.text),
      plainLinks.length > 1
        ? h("div", { class: "job-links" }, plainLinks.map((l) =>
          h("a", { class: "link-chip", href: l.url, target: "_blank", rel: "noopener noreferrer",
                   title: l.url }, l.label)))
        : false);
  }
  return h("pre", {}, JSON.stringify(body, null, 2));
}

function messageRow(m, replies) {
  return h("article", { class: "msg" },
    h("div", { class: "msg-meta" },
      h("span", { class: "msg-sender" + (m.sender === "@server" ? " server" : "") }, m.sender),
      h("span", {}, "#" + m.seq),
      h("time", { datetime: new Date(m.ts * 1000).toISOString(), title: new Date(m.ts * 1000).toLocaleString() }, clock(m.ts)),
      sourceTag(m.body)),
    messageBody(m.body),
    replies && replies.length
      ? h("div", { class: "replies" }, replies.map(replyRow))
      : false);
}

// An answer to something above it. Short by nature: who, when, and what they
// said, which is usually one word.
function replyRow(m) {
  const body = m.body;
  const said = body && typeof body === "object" && !Array.isArray(body)
    ? String(body.status || body.state || body.result || body.text || body.message || "")
    : String(body ?? "");
  const done = /^(done|ok|complete|completed|success|succeeded|finished|merged)$/i.test(said.trim());
  const links = linksOf(body);
  return h("div", { class: "reply" },
    h("span", { class: "reply-mark" + (done ? " done" : "") }, done ? "\u2713" : "\u203a"),
    h("div", { class: "grow" },
      h("div", { class: "reply-said" },
        said ? h("span", { class: done ? "strong" : "" }, said) : h("span", { class: "muted" }, "replied"),
        h("span", { class: "muted small" }, "\u00b7 " + m.sender),
        h("time", { class: "muted small", title: new Date(m.ts * 1000).toLocaleString() },
          "\u00b7 " + clock(m.ts))),
      links.length
        ? h("div", { class: "job-links" }, links.map((l) =>
          h("a", { class: "link-chip", href: l.url, target: "_blank", rel: "noopener noreferrer",
                   title: l.url }, l.label)))
        : false));
}

// --- live stream -----------------------------------------------------------
function setStream(status) {
  state.stream = status;
  renderStreamPill();
}

function stopStream() {
  state.streamCtrl?.abort();
  state.streamCtrl = null;
  clearInterval(state.pollTimer);
  state.pollTimer = 0;
}

// Without a stream the console asks instead. Slower than being told, but the
// difference only shows as a few seconds' delay on a count or a new message.
function startPolling(every = 4000) {
  clearInterval(state.pollTimer);
  setStream("polling");
  state.pollTimer = setInterval(() => {
    refreshAll();
    pollFeed();
  }, every);
}

// Ask only for what is new and append it, the way an event would have
// arrived. Re-rendering the channel instead replaces the whole view every few
// seconds: the feed blinks through "Loading messages...", the scroll position
// jumps, and anything half-typed in the composer is thrown away.
async function pollFeed() {
  if (state.view.kind !== "channel") return;
  const name = state.view.name;
  const last = state.feed.channel === name && state.feed.messages.length
    ? state.feed.messages[state.feed.messages.length - 1].seq
    : 0;
  try {
    const newer = await api("GET", `/api/channels/${enc(name)}/messages?after=${last}&limit=${PAGE}`);
    // Moved on, or the feed was replaced while the request was in flight.
    if (state.view.kind !== "channel" || state.view.name !== name || state.feed.channel !== name) return;
    const seen = new Set(state.feed.messages.map((m) => m.seq));
    const fresh = newer.filter((m) => !seen.has(m.seq));
    if (!fresh.length) return;
    mergeMessages(fresh);
    renderFeed(false);
  } catch {
    /* the next tick tries again; refreshAll reports anything that matters */
  }
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
      if (res.status === 401) { signOut("The password was rejected."); return; }
      if (res.status === 404 || res.status === 405) { startPolling(); return; }
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

// The workspace is in the path and the view is in the hash, so Back moves
// between workspaces as well as between views.
window.addEventListener("popstate", () => {
  const name = workspaceFromUrl();
  if (name) enterWorkspace(name);
  else showHome();
});

(function boot() {
  const name = workspaceFromUrl();
  if (name) return enterWorkspace(name);

  // Anyone arriving with credentials from before workspaces existed keeps
  // working; their relay becomes a workspace named after its host.
  const saved = loadSaved();
  if (saved && !Object.keys(loadWorkspaces()).length) {
    const guess = (saved.url.replace(/^https?:\/\//, "").split(/[:/]/)[0] || "relay")
      .replace(/[^A-Za-z0-9_.-]/g, "-").slice(0, 64);
    saveWorkspace(guess, { name: guess, url: saved.url, token: saved.token });
    forget();
    return goToWorkspace(guess);
  }
  showHome();
})();
