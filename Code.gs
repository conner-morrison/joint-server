/**
 * Vollna job alerts, Upwork invitations and Upwork messages (Gmail) -> Relay workspace,
 * with a control panel web app.
 * Script properties: RELAY_URL, RELAY_ID, RELAY_TOKEN, RELAY_CHANNEL (default channel),
 *                     CHANNEL_JOB / CHANNEL_ALERT / CHANNEL_INVITE / CHANNEL_MESSAGE (optional, per kind)
 * Internal properties: PAUSED, ONLY_VERIFIED, SEND_JOB, SEND_ALERT, SEND_INVITE, SEND_MESSAGE,
 *                      SENT_IDS, FAILED_IDS, LAST_SENT, LAST_SKIPPED, LAST_ERROR, INTERVAL_MINUTES
 */
// A 2-day window costs nothing extra (messages are fetched in one batch) and leaves room to catch
// up after an outage, such as Gmail's daily call limit being reached.
const QUERY = 'newer_than:2d (from:info@vollna.com'
  + ' OR (from:upwork@t.upwork.com subject:"Invitation to Interview")'
  + ' OR (from:donotreply@upwork.com subject:"Upwork Enterprise job")'
  + ' OR (from:donotreply@upwork.com subject:"New job alert")'
  + ' OR (from:email.upwork.com subject:"sent you a message"))';
const MAX_IDS = 300;
// Minutes between checks. Gmail caps how many calls a script may make per day (20,000 on a
// personal account), so checking every minute can run out; the values Apps Script allows are fixed.
const INTERVALS = [1, 5, 10, 15, 30];
const DEFAULT_INTERVAL = 5;
const NAME_PATTERN = /^[A-Za-z0-9][A-Za-z0-9_.\-]{0,63}$/;
const KINDS = {
  job: 'Vollna job alerts', alert: 'Upwork job alerts', invite: 'Upwork invitations', message: 'Upwork messages',
};

function props_() {
  return PropertiesService.getScriptProperties();
}

function isSending_(kind) {
  return props_().getProperty('SEND_' + kind.toUpperCase()) !== 'false';
}

// ---------------------------------------------------------------------------
// Editor actions (Run dropdown)
// ---------------------------------------------------------------------------

// Run once: creates the trigger and skips emails that already exist.
function setup() {
  installTrigger_(intervalMinutes_());
  markExistingSent_();
}

// How often the script checks Gmail, as chosen in the control panel.
function intervalMinutes_() {
  const saved = Number(props_().getProperty('INTERVAL_MINUTES'));
  return INTERVALS.indexOf(saved) >= 0 ? saved : DEFAULT_INTERVAL;
}

function installTrigger_(minutes) {
  ScriptApp.getProjectTriggers()
    .filter(t => t.getHandlerFunction() === 'checkVollna')
    .forEach(t => ScriptApp.deleteTrigger(t));
  ScriptApp.newTrigger('checkVollna').timeBased().everyMinutes(minutes).create();
}

// Confirms the relay accepts messages from this worker on every channel it sends to.
function testRelay() {
  const results = sendTests_();
  if (!results.length) Logger.log('FAILED: no channel is set');
  results.forEach(r => Logger.log(`${r.channel}: ${r.ok ? 'OK' : 'FAILED: ' + r.error}`));
}

// Asks the relay to register RELAY_ID with RELAY_TOKEN (then approve it in the console).
function enrol() {
  const r = enrol_();
  Logger.log(r.code + ' ' + r.text);
}

// Stop sending. Emails that arrive meanwhile are not lost; resume() sends them (up to 2 days old).
function pause() {
  props_().setProperty('PAUSED', 'true');
  Logger.log('Paused');
}

// Start sending again, including emails that arrived while paused.
function resume() {
  props_().deleteProperty('PAUSED');
  Logger.log('Resumed');
}

// Start sending again, but skip emails that arrived while paused.
function resumeSkipMissed() {
  markExistingSent_();
  resume();
}

// Only send Vollna jobs whose client has a verified payment method (the default).
function requireVerifiedPayment() {
  props_().deleteProperty('ONLY_VERIFIED');
  Logger.log('Only jobs with a verified payment method will be sent');
}

// Send Vollna jobs regardless of the client's payment method.
function allowUnverifiedPayment() {
  props_().setProperty('ONLY_VERIFIED', 'false');
  Logger.log('Jobs will be sent regardless of payment verification');
}

function status() {
  const s = getState();
  const kinds = Object.keys(KINDS).map(k => `${KINDS[k]}: ${s.send[k] ? 'on' : 'off'}`).join(', ');
  Logger.log(`Sending: ${s.paused ? 'PAUSED' : 'ON'} | trigger: ${s.trigger ? `every ${s.interval} min` : 'missing (run setup)'}`
    + ` | ${kinds} | Verified payment only: ${s.onlyVerified ? 'yes' : 'no'}`);
}

// ---------------------------------------------------------------------------
// Control panel (web app)
// ---------------------------------------------------------------------------

function doGet() {
  return HtmlService.createHtmlOutputFromFile('Index')
    .setTitle('Vollna Alert Worker')
    .addMetaTag('viewport', 'width=device-width, initial-scale=1');
}

function getState() {
  migrate_();
  const p = props_();
  const token = p.getProperty('RELAY_TOKEN') || '';
  const send = {};
  const channels = {};
  Object.keys(KINDS).forEach(k => {
    send[k] = isSending_(k);
    channels[k] = p.getProperty('CHANNEL_' + k.toUpperCase()) || '';
  });
  return {
    paused: p.getProperty('PAUSED') === 'true',
    onlyVerified: p.getProperty('ONLY_VERIFIED') !== 'false',
    interval: intervalMinutes_(),
    intervals: INTERVALS,
    send,
    trigger: ScriptApp.getProjectTriggers().some(t => t.getHandlerFunction() === 'checkVollna'),
    url: p.getProperty('RELAY_URL') || '',
    id: p.getProperty('RELAY_ID') || '',
    channel: p.getProperty('RELAY_CHANNEL') || '',
    channels,
    tokenHint: token ? `set, ends in "${token.slice(-2)}"` : 'not set',
    lastSent: JSON.parse(p.getProperty('LAST_SENT') || 'null'),
    lastSkipped: JSON.parse(p.getProperty('LAST_SKIPPED') || 'null'),
    lastError: JSON.parse(p.getProperty('LAST_ERROR') || 'null'),
  };
}

function panelSetPaused(paused, skipMissed) {
  if (paused) pause();
  else if (skipMissed) resumeSkipMissed();
  else resume();
  return getState();
}

function panelSetOnlyVerified(on) {
  if (on) requireVerifiedPayment();
  else allowUnverifiedPayment();
  return getState();
}

// Changes how often Gmail is checked, and re-creates the trigger when one is installed.
function panelSetInterval(minutes) {
  const value = Number(minutes);
  if (INTERVALS.indexOf(value) < 0) throw new Error('Interval must be one of: ' + INTERVALS.join(', ') + ' minutes.');
  props_().setProperty('INTERVAL_MINUTES', String(value));
  if (ScriptApp.getProjectTriggers().some(t => t.getHandlerFunction() === 'checkVollna')) installTrigger_(value);
  return getState();
}

function panelSetSending(kind, on) {
  if (!KINDS[kind]) throw new Error('Unknown kind: ' + kind);
  if (on) props_().deleteProperty('SEND_' + kind.toUpperCase());
  else props_().setProperty('SEND_' + kind.toUpperCase(), 'false');
  return getState();
}

function panelInstallTrigger() {
  setup();
  return getState();
}

function panelSaveSettings(s) {
  const url = String(s.url || '').trim().replace(/\/+$/, '');
  const id = String(s.id || '').trim();
  const channel = String(s.channel || '').trim();
  const token = String(s.token || '').trim();
  if (!/^https:\/\/\S+$/.test(url)) throw new Error('Relay URL must start with https://');
  if (!NAME_PATTERN.test(id)) throw new Error('Worker ID may use letters, digits, _ . - (up to 64 characters).');
  if (!NAME_PATTERN.test(channel)) throw new Error('Default channel may use letters, digits, _ . - (up to 64 characters).');

  // Validate every per-kind channel before writing anything.
  const perKind = {};
  Object.keys(KINDS).forEach(k => {
    const value = String((s.channels || {})[k] || '').trim();
    if (value && !NAME_PATTERN.test(value)) {
      throw new Error(`${KINDS[k]} channel may use letters, digits, _ . - (up to 64 characters).`);
    }
    perKind[k] = value;
  });

  const p = props_();
  p.setProperties({ RELAY_URL: url, RELAY_ID: id, RELAY_CHANNEL: channel });
  Object.keys(perKind).forEach(k => {
    if (perKind[k]) p.setProperty('CHANNEL_' + k.toUpperCase(), perKind[k]);
    else p.deleteProperty('CHANNEL_' + k.toUpperCase());
  });
  if (token) p.setProperty('RELAY_TOKEN', token);
  return getState();
}

function panelEnrol() {
  return Object.assign(enrol_(), { state: getState() });
}

function panelTest() {
  const results = sendTests_();
  return { results, state: getState() };
}

// ---------------------------------------------------------------------------
// Worker
// ---------------------------------------------------------------------------

function checkVollna() {
  migrate_();
  const p = props_();
  if (p.getProperty('PAUSED') === 'true') return;
  const onlyVerified = p.getProperty('ONLY_VERIFIED') !== 'false';
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(5000)) return;
  try {
    const sent = new Set(loadIds_());
    const messages = [];
    // One search plus one batched fetch: Gmail calls are capped per day, so keep them few.
    GmailApp.getMessagesForThreads(GmailApp.search(QUERY, 0, 25)).forEach(thread =>
      thread.forEach(m => { if (!sent.has(m.getId())) messages.push(m); }));
    messages.sort((a, b) => a.getDate() - b.getDate());

    const deadline = Date.now() + 4 * 60 * 1000; // stop well before Apps Script's 6-minute limit
    for (const m of messages) {
      if (Date.now() > deadline) return; // whatever is left is picked up next run
      let handled;
      try {
        handled = handleMessage_(m, onlyVerified);
      } catch (e) {
        noteFailure_(m, e, sent); // one bad email must not fail the whole run
        return;
      }
      if (!handled) return; // relay down or not approved: retry next run
      clearFailure_(m.getId());
      sent.add(m.getId());
      saveIds_([...sent]);
    }
  } catch (e) {
    // e.g. Gmail's daily call limit. Record it for the panel instead of failing the run.
    const message = e && e.message ? e.message : String(e);
    console.error('Run failed: ' + message);
    p.setProperty('LAST_ERROR', JSON.stringify({ message: `Run failed: ${message}`, at: new Date().toISOString() }));
  } finally {
    lock.releaseLock();
  }
}

// Publishes one email. Returns false when the relay refused it, so the run can retry later.
function handleMessage_(m, onlyVerified) {
  const p = props_();
  const kind = classify_(m.getFrom(), m.getSubject());
  const base = { emailId: m.getId(), emailSubject: m.getSubject(), receivedAt: m.getDate().toISOString() };
  let bodies = [];
  let skipped = null;

  if (kind && !isSending_(kind)) {
    skipped = { title: m.getSubject(), count: 1, reason: `${KINDS[kind]} are turned off` };
  } else if (kind === 'job' || kind === 'alert') {
    const jobs = kind === 'job'
      ? parseJobs_(m.getBody()).map((j, i) => ({ source: 'vollna', type: 'job', index: i, ...j, ...base }))
      : [{ source: 'upwork', type: 'alert', index: 0, ...parseAlert_(m.getBody(), m.getSubject()), ...base }];
    // A job without client details (e.g. the Vollna results-table email) counts as not verified.
    const keep = j => !onlyVerified || (j.client && j.client.paymentVerified === true);
    const dropped = jobs.filter(j => !keep(j));
    bodies = jobs.length
      ? jobs.filter(keep)
      : [{ source: 'vollna', type: 'email', index: 0, text: m.getPlainBody().slice(0, 2000), ...base }];
    if (dropped.length) {
      skipped = { title: dropped[dropped.length - 1].title, count: dropped.length, reason: 'payment method not verified' };
    }
  } else if (kind === 'invite') {
    bodies = [{ source: 'upwork', type: 'invite', index: 0, ...parseInvite_(m.getBody(), m.getSubject()), ...base }];
  } else if (kind === 'message') {
    bodies = [{ source: 'upwork', type: 'message', index: 0, ...parseMessage_(m.getBody(), m.getFrom()), ...base }];
  }

  // One job reaches this inbox more than once, and not always in the same
  // shape: a Vollna card with the client on it, a digest row with only the
  // money, an email nothing could be read from. Sent under three ids those
  // are three notifications about one job. Sent under the job's own id the
  // relay keeps the first and drops the rest.
  const mentioned = bodies.length === 1 ? jobIdsIn_(m.getBody()) : [];
  for (const body of bodies) {
    const named = body.type === 'email' && mentioned.length === 1 ? 'job:' + mentioned[0] : '';
    const id = jobKey_(body) || named || `${m.getId()}-${body.index}`;
    if (!publish_(id, body, channelFor_(body))) return false;
  }

  const now = new Date().toISOString();
  if (bodies.length) {
    p.setProperty('LAST_SENT', JSON.stringify({ title: label_(bodies[bodies.length - 1]), count: bodies.length, at: now }));
  }
  if (skipped) {
    p.setProperty('LAST_SKIPPED', JSON.stringify({ ...skipped, at: now }));
  }
  return true;
}

// Records an unexpected error, and gives up on an email after three failed attempts.
function noteFailure_(m, e, sent) {
  const p = props_();
  const id = m.getId();
  const message = e && e.message ? e.message : String(e);
  const at = new Date().toISOString();
  console.error(`Failed on "${m.getSubject()}" (${id}): ${message}`);

  const fails = JSON.parse(p.getProperty('FAILED_IDS') || '{}');
  fails[id] = (fails[id] || 0) + 1;
  if (fails[id] >= 3) {
    delete fails[id];
    sent.add(id);
    saveIds_([...sent]);
    p.setProperty('LAST_SKIPPED', JSON.stringify({ title: m.getSubject(), count: 1, reason: `could not be read: ${message}`, at }));
  }
  p.setProperty('FAILED_IDS', JSON.stringify(fails));
  p.setProperty('LAST_ERROR', JSON.stringify({ message: `Could not handle "${m.getSubject()}": ${message}`, at }));
}

function clearFailure_(id) {
  const p = props_();
  const fails = JSON.parse(p.getProperty('FAILED_IDS') || '{}');
  if (!(id in fails)) return;
  delete fails[id];
  p.setProperty('FAILED_IDS', JSON.stringify(fails));
}

// Which kind of email this is, or null for any other email the search matched.
function classify_(from, subject) {
  if (/\binfo@vollna\.com\b/i.test(from)) return 'job';
  if (/\bupwork@t\.upwork\.com\b/i.test(from) && /^Invitation to Interview for:/i.test(subject)) return 'invite';
  if (/\bdonotreply@upwork\.com\b/i.test(from) && /invited to an Upwork Enterprise job/i.test(subject)) return 'invite';
  if (/\bdonotreply@upwork\.com\b/i.test(from) && /^New job alerts?:/i.test(subject)) return 'alert';
  if (/ via Upwork"?\s*<room_[0-9a-f]+@email\.upwork\.com>/i.test(from) && /sent you a message$/i.test(subject)) return 'message';
  return null;
}

function label_(body) {
  if (body.type === 'alert') return `Job alert: ${body.title || body.emailSubject}`;
  if (body.type === 'invite') return `Invitation: ${body.title || body.emailSubject}`;
  if (body.type === 'message') return `Message from ${body.from || '?'}${body.jobTitle ? ` (${body.jobTitle})` : ''}`;
  return body.title || body.emailSubject;
}

// Each kind uses its own channel when one is set in the control panel; everything else uses RELAY_CHANNEL.
function channelFor_(body) {
  const p = props_();
  const own = KINDS[body.type] ? p.getProperty('CHANNEL_' + body.type.toUpperCase()) : null;
  return own || p.getProperty('RELAY_CHANNEL');
}

// Moves the earlier single RELAY_UPWORK_CHANNEL setting onto the per-kind messages channel.
function migrate_() {
  const p = props_();
  const old = p.getProperty('RELAY_UPWORK_CHANNEL');
  if (!old) return;
  if (!p.getProperty('CHANNEL_MESSAGE')) p.setProperty('CHANNEL_MESSAGE', old);
  p.deleteProperty('RELAY_UPWORK_CHANNEL');
}

function markExistingSent_() {
  const ids = loadIds_();
  GmailApp.getMessagesForThreads(GmailApp.search(QUERY, 0, 100))
    .forEach(thread => thread.forEach(m => ids.push(m.getId())));
  saveIds_([...new Set(ids)]);
}

// Where a job's text stops and the email's own footer begins.
const FOOTER = /^(Open results|Show live feed|View on Vollna|View on Upwork|You can pause|Your Vollna trial)/i;
// Link text that is a button, not the name of the job.
const NOT_A_TITLE = /^(view on|view job|view details|open results|show live feed|see more|apply now)\b/i;

// Reads jobs from both Vollna formats: the results table and the single "New Job" card.
//
// One job is linked more than once in these emails - the title, a "view on
// Upwork" button, a tracking wrapper - and each link starts a new slice of the
// text that follows it. Read one link at a time, that is three jobs: one
// carrying the money, one holding only a title, one holding the description.
// The link says which job it belongs to, so the slices are put back together
// before anything is sent. Left apart, one job arrives as three notifications.
function parseJobs_(html) {
  const re = /<a\b[^>]*href="([^"]*place(?:=|%3D)title[^"]*)"[^>]*>([\s\S]*?)<\/a>/gi;
  const matches = [...html.matchAll(re)];
  const byKey = {};
  const order = [];

  matches.forEach((mt, i) => {
    const href = mt[1];
    const text = lines_(mt[2]).join(' ');
    // Upwork's ids are not only digits: ~021987abc cut down to ~021987 is a
    // link that does not open, and a key two different jobs could share.
    const jobId = (href.match(/jobs(?:\/|%2F|%252F|%25252F)(~[0-9a-z]+)/i) || [])[1];
    const pid = (href.match(/pid(?:=|%3D)(\d+)/i) || [])[1];
    // Without an id there is nothing to say two links are the same job, so
    // such a link stands alone rather than being merged into its neighbour.
    const key = jobId || (pid ? 'pid:' + pid : 'at:' + mt.index);

    const start = mt.index + mt[0].length;
    const end = i + 1 < matches.length ? matches[i + 1].index : html.length;
    const cells = [];
    for (const c of lines_(html.slice(start, end))) {
      if (FOOTER.test(c)) break;
      cells.push(c);
    }

    let job = byKey[key];
    if (!job) {
      job = { title: '', upworkUrl: null, vollnaProjectId: null, cells: [] };
      byKey[key] = job;
      order.push(job);
    }
    if (!job.title && text && !NOT_A_TITLE.test(text)) job.title = text;
    if (!job.upworkUrl && jobId) job.upworkUrl = `https://www.upwork.com/jobs/${jobId}`;
    if (!job.vollnaProjectId && pid) job.vollnaProjectId = pid;
    // The wrapper link repeats what the title link already said, so a line
    // that is already held is not held twice.
    cells.forEach(c => { if (job.cells.indexOf(c) < 0) job.cells.push(c); });
  });

  // A fragment with neither a name nor a link is markup, not a job.
  return order.filter(j => j.title || j.upworkUrl).map(j => {
    const job = { title: j.title || null, upworkUrl: j.upworkUrl, vollnaProjectId: j.vollnaProjectId };
    return j.cells.some(c => /^Published:/i.test(c))
      ? Object.assign(job, labeled_(j.cells))
      : Object.assign(job, { budget: j.cells[0] || null, published: j.cells[1] || null });
  });
}

// The same job reaches this inbox more than once: a Vollna digest and an
// Upwork job alert can both carry it, and two digests an hour apart often
// repeat it. The relay stores one message per id, so giving a job the id of
// the job itself makes a repeat a no-op there - once, for every reader at
// once, rather than each of them working out separately that they have seen
// it. Anything without an Upwork job id keeps the id of the email it came in.
function jobKey_(body) {
  const id = (String(body.upworkUrl || '').match(/\/jobs\/(~[0-9a-z]+)/i) || [])[1];
  return id ? 'job:' + id : '';
}

// Every Upwork job id an email mentions, without repeats. Used for the email
// that could not be read as jobs at all: when it names exactly one job, it is
// about that job, and it can be given the same id as the reading of that job
// that arrived in a different email. Invitations are deliberately left out of
// this - being asked for by name is news even about a job already seen.
function jobIdsIn_(html) {
  const ids = [...String(html).matchAll(/jobs(?:\/|%2F|%252F|%25252F)(~[0-9a-z]{6,})/gi)]
    .map(m => m[1].toLowerCase());
  return [...new Set(ids)];
}

// Fields of a "New Job" card, read by their labels.
function labeled_(cells) {
  const out = { budget: null, published: null, filter: null, description: null, client: {}, aiQualification: null, aiReason: null };
  const desc = [];
  let section = 'head';
  for (const c of cells) {
    let m;
    if (section !== 'ai' && (m = c.match(/^Client rank:\s*(.*)$/i))) { out.client.rank = m[1] || null; section = 'client'; continue; }
    if ((m = c.match(/^AI Qualification:\s*(.*)$/i))) { out.aiQualification = m[1] || null; section = 'ai'; continue; }
    if (section === 'head') {
      if ((m = c.match(/^Budget\b:?\s*(.*)$/i))) out.budget = m[1] || null;
      else if ((m = c.match(/^(Hourly Rate|Fixed[- ]Price)\b:?\s*(.*)$/i))) out.budget = `${m[1]}: ${m[2]}`;
      else if ((m = c.match(/^Published:\s*(.*)$/i))) out.published = m[1] || null;
      else if ((m = c.match(/^Filter:\s*(.*)$/i))) { out.filter = m[1] || null; section = 'desc'; }
      else { desc.push(c); if (out.published) section = 'desc'; }
    } else if (section === 'desc') {
      desc.push(c);
    } else if (section === 'client') {
      if ((m = c.match(/^Registered:\s*(.*)$/i))) out.client.registered = m[1];
      else if (/payment method/i.test(c)) out.client.paymentVerified = !/not verified|unverified/i.test(c);
      else if (/review/i.test(c)) out.client.reviews = c;
      else out.client.location = c;
    } else if ((m = c.match(/^Reason:\s*(.*)$/i))) {
      out.aiReason = m[1] || null;
    } else if (!out.aiQualification) {
      out.aiQualification = c;
    }
  }
  out.description = desc.join('\n').replace(/^["“]\s*|\s*["”]$/g, '') || null;
  return out;
}

// Upwork "New job alert: …" emails, which carry one job each.
function parseAlert_(html, subject) {
  const L = lines_(html);
  const out = { title: null, upworkUrl: null, jobType: null, budget: null, description: null, skills: [], client: {} };
  const anchors = [...html.matchAll(/<a\b[^>]*href\s*=\s*["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi)]
    .map(a => ({ href: cleanUrl_(a[1]), text: lines_(a[2]).join(' ') }));
  const titleLink = anchors.find(a => /upwork\.com\/jobs\/~/i.test(a.href) && a.text && !/^more$/i.test(a.text));
  if (titleLink) out.upworkUrl = titleLink.href;
  out.title = (subject.match(/^New job alerts?:\s*(.+)$/i) || [])[1] || (titleLink ? titleLink.text : null);
  out.skills = anchors
    .filter(a => /\/nx\/search\/jobs/i.test(a.href) && a.text && !/^\+\d+$/.test(a.text))
    .map(a => a.text);

  let i = L.findIndex(c => /^Posted\b/i.test(c));
  i = i < 0 ? 0 : i + 1;
  if (L[i] && (L[i] === out.title || (titleLink && L[i] === titleLink.text))) i++;
  if (/^(Hourly|Fixed)/i.test(L[i] || '')) out.jobType = L[i++];
  while (L[i] && /^[•·|–—-]+$/.test(L[i])) i++; // separator between job type and budget
  if (/^\$/.test(L[i] || '')) out.budget = L[i++];

  const desc = [];
  const STOP = /^(Payment (verified|unverified)|View job details|You received this email)/i;
  while (i < L.length && !STOP.test(L[i])) desc.push(L[i++]);
  if (desc.length && out.skills.length && desc[desc.length - 1].startsWith(out.skills[0])) desc.pop(); // the skills row
  out.description = desc.join('\n').replace(/\s*(?:\.{3}|…)?\s*more$/i, '…') || null;

  for (; i < L.length && !/^(View job details|You received this email)/i.test(L[i]); i++) {
    const c = L[i];
    if (/^Payment/i.test(c)) out.client.paymentVerified = /^Payment verified/i.test(c);
    else if (/^\d+(\.\d+)?$/.test(c)) out.client.rating = c;
    else if (/spent$/i.test(c)) out.client.spent = c;
    else out.client.location = c;
  }
  return out;
}

// Upwork "Invitation to Interview for: …" and "You have been invited to an Upwork Enterprise job!" emails.
function parseInvite_(html, subject) {
  const L = lines_(html);
  const enterprise = /Upwork Enterprise/i.test(subject);
  const out = { title: null, enterprise, terms: null, description: null, clientNote: null, clientName: null, client: null, inviteUrl: null };
  const END = /^(Personal note from client|View invite|Submit a Proposal|Decline)\b/i;

  let i = L.findIndex(c => /(good fit\.?|Enterprise Client:)$/i.test(c)) + 1;
  out.title = (subject.match(/^Invitation to Interview for:\s*(.+)$/i) || [])[1] || L[i] || null;
  i++;

  const terms = [];
  while (i < L.length && /^(Hourly|Fixed|•)/i.test(L[i])) terms.push(L[i++].replace(/^•\s*/, ''));
  out.terms = terms.join(' • ') || null;

  const desc = [];
  while (i < L.length && !END.test(L[i])) desc.push(L[i++]);
  out.description = desc.join('\n').replace(/\s*(\.{3}|…)\s*more$/i, '…') || null;

  if (/^Personal note from client$/i.test(L[i] || '')) {
    const note = [];
    for (i++; i < L.length && !END.test(L[i]); i++) note.push(L[i]);
    out.clientNote = note.join('\n') || null;
    const last = note[note.length - 1] || '';
    if (last.length <= 40 && /^\p{Lu}[\p{L}'’-]*(?: \p{Lu}[\p{L}'’-]*\.?)+$/u.test(last)) out.clientName = last;
  }

  const more = L.findIndex(c => /^More about this Upwork Enterprise Client$/i.test(c));
  if (more >= 0) {
    const info = [];
    for (let k = more + 1; k < L.length && !/^(Privacy Policy|Mobile app|Follow us|©|You are receiving)/i.test(L[k]); k++) {
      if (!/^(Learn more|Upwork Enterprise Client)$/i.test(L[k])) info.push(L[k]);
    }
    out.client = info.join(' ').replace(/,\s*$/, '') || null;
  }

  const link = [...html.matchAll(/<a\b[^>]*href\s*=\s*["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi)]
    .find(a => /^(View invite|Submit a Proposal)\b/i.test(lines_(a[2]).join(' ')));
  if (link) out.inviteUrl = cleanUrl_(link[1]);
  return out;
}

// Upwork "<name> sent you a message" emails from "<name> via Upwork".
function parseMessage_(html, from) {
  const L = lines_(html);
  const out = { from: (from.match(/^"?(.+?) via Upwork/i) || [])[1] || null, jobTitle: null, messages: [], text: null, roomUrl: null };
  const room = (from.match(/(room_[0-9a-f]+)@/i) || html.match(/messages\/rooms\/(room_[0-9a-f]+)/i) || [])[1];
  if (room) out.roomUrl = `https://www.upwork.com/ab/messages/rooms/${room}`;

  const head = L.findIndex(c => /^Unread messages? from .+ about /i.test(c));
  if (head >= 0) out.jobTitle = (L[head].match(/ about (.+)$/i) || [])[1] || null;
  const start = head + 1;
  let end = L.findIndex((c, k) => k >= start && /^View on Upwork$/i.test(c));
  if (end < 0) end = L.length;

  const TIME = /^\d{1,2}:\d{2}\s?[AP]M\b.*\d{4}$/i;
  const raw = L.slice(start, end);
  // Avatar initials ("RK") sit just before the author line and just after the time line.
  const body = raw.filter((c, k) => !(/^[A-Z]{1,3}$/.test(c) && (TIME.test(raw[k - 1] || '') || TIME.test(raw[k + 2] || ''))));
  const times = body.map((c, k) => (TIME.test(c) ? k : -1)).filter(k => k >= 0);
  times.forEach((t, n) => {
    const stop = n + 1 < times.length ? times[n + 1] - 1 : body.length; // the line before the next time is its author
    out.messages.push({ author: body[t - 1] || null, time: body[t], text: body.slice(t + 1, stop).join('\n') || null });
  });
  out.text = (times.length ? out.messages.map(x => x.text).filter(Boolean).join('\n\n') : body.join('\n')) || null;
  return out;
}

function cleanUrl_(url) {
  const u = url.replace(/&amp;/g, '&');
  return /^https:\/\/www\.upwork\.com\//i.test(u) ? u.replace(/[?#].*$/, '') : u;
}

const ENTITIES = {
  nbsp: ' ', amp: '&', quot: '"', apos: "'", lt: '<', gt: '>', raquo: '»', laquo: '«', bull: '•', hellip: '…',
  rsquo: '’', lsquo: '‘', rdquo: '”', ldquo: '“', mdash: '—', ndash: '–', trade: '™', copy: '©', reg: '®',
  zwj: '', zwnj: '', shy: '',
};

function lines_(s) {
  return s
    .replace(/<(style|script|head)[^>]*>[\s\S]*?<\/\1>/gi, '')
    .replace(/<br\s*\/?>|<\/(td|th|tr|p|div|h\d|li)>/gi, '\n')
    .replace(/<[^>]+>/g, '')
    .replace(/&(#x[0-9a-f]+|#\d+|[a-z]+);/gi, (e, n) => {
      if (n[0] !== '#') return ENTITIES[n.toLowerCase()] ?? e;
      const cp = /^#x/i.test(n) ? parseInt(n.slice(2), 16) : Number(n.slice(1));
      return cp > 0 && cp <= 0x10ffff ? String.fromCodePoint(cp) : '';
    })
    // Invisible spacer characters that emails use as preview-text padding.
    .replace(/[\u00AD\u034F\u115F\u1160\u17B4\u17B5\u180E\u2000-\u200F\u2028-\u202F\u205F-\u206F\u3000\u3164\uFEFF]/g, ' ')
    .split('\n').map(x => x.trim()).filter(Boolean);
}

// ---------------------------------------------------------------------------
// Relay
// ---------------------------------------------------------------------------

function relayUrl_(path) {
  return (props_().getProperty('RELAY_URL') || '').replace(/\/+$/, '') + path;
}

function publish_(id, body, channel) {
  const p = props_();
  let error;
  try {
    const res = UrlFetchApp.fetch(relayUrl_('/publish'), {
      method: 'post',
      contentType: 'application/json',
      headers: { Authorization: 'Bearer ' + p.getProperty('RELAY_TOKEN') },
      payload: JSON.stringify({ channel, id, body }),
      muteHttpExceptions: true,
    });
    const code = res.getResponseCode();
    if (code >= 200 && code < 300) {
      // The relay says when it has seen this id before: nothing was stored and
      // nobody was told, which is the point. Worth a line while watching.
      try {
        if (JSON.parse(res.getContentText() || '{}').duplicate) {
          console.log('already posted, skipped: ' + id);
        }
      } catch (ignored) { /* a body that is not JSON changes nothing here */ }
      p.deleteProperty('LAST_ERROR');
      return true;
    }
    error = `Relay answered ${code} for channel "${channel}": ${res.getContentText().slice(0, 300)}`;
  } catch (e) {
    error = 'Relay unreachable: ' + e;
  }
  console.error(error);
  p.setProperty('LAST_ERROR', JSON.stringify({ message: error, at: new Date().toISOString() }));
  return false;
}

// Sends a test message to each channel in use; returns [{ channel, ok, error }].
function sendTests_() {
  const p = props_();
  const perKind = Object.keys(KINDS).map(k => p.getProperty('CHANNEL_' + k.toUpperCase()));
  const channels = [...new Set([p.getProperty('RELAY_CHANNEL'), ...perKind].filter(Boolean))];
  return channels.map(channel => {
    const ok = publish_('test-' + Date.now(), {
      source: 'apps-script', type: 'test', text: 'Test from Apps Script', ts: new Date().toISOString(),
    }, channel);
    return { channel, ok, error: ok ? null : (JSON.parse(p.getProperty('LAST_ERROR') || '{}').message || 'unknown error') };
  });
}

function enrol_() {
  const p = props_();
  const res = UrlFetchApp.fetch(relayUrl_('/enrol'), {
    method: 'post',
    contentType: 'application/json',
    payload: JSON.stringify({
      worker_id: p.getProperty('RELAY_ID'),
      token: p.getProperty('RELAY_TOKEN'),
      label: 'Vollna Gmail alerts (Apps Script)',
    }),
    muteHttpExceptions: true,
  });
  return { code: res.getResponseCode(), text: res.getContentText().slice(0, 300) };
}

// ---------------------------------------------------------------------------
// Sent-email bookkeeping
// ---------------------------------------------------------------------------

function loadIds_() {
  return JSON.parse(props_().getProperty('SENT_IDS') || '[]');
}

function saveIds_(ids) {
  props_().setProperty('SENT_IDS', JSON.stringify(ids.slice(-MAX_IDS)));
}
