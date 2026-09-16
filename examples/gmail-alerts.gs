/**
 * Job alerts from Gmail into a relay channel.
 *
 * Script properties (Project Settings -> Script properties):
 *   RELAY_URL      https://relay-xxx.up.railway.app/upwork   (the workspace, not the console)
 *   RELAY_TOKEN    this worker's own token; invent one, or let enrol() make it
 *   RELAY_CHANNEL  jobs
 *   RELAY_WORKER   gmail-bot
 *
 * Run enrol() once, approve it in the console, then run setup() to install the
 * trigger. checkMail() runs every minute after that.
 *
 * Mail arrives from more than one place and in more than one shape. Every
 * source is read; what cannot be parsed into separate jobs is still sent, as
 * the email it was, because a message nobody can parse is worth more than a
 * message nobody sees.
 */

// Each source is searched separately, so one sender changing its format or
// stopping cannot quietly take the others with it.
const SOURCES = [
  { source: 'vollna', query: 'from:info@vollna.com newer_than:2d' },
  { source: 'upwork-invitation', query: 'from:upwork.com newer_than:2d subject:(invit OR interview)' },
  { source: 'upwork-alert', query: 'from:upwork.com newer_than:2d -subject:(invit OR interview)' },
];

const MAX_IDS = 600;          // remembered message ids, across all sources
const MAX_TEXT = 4000;        // characters of an email body worth sending on

function setup() {
  ScriptApp.getProjectTriggers()
    .filter(t => t.getHandlerFunction() === 'checkMail')
    .forEach(t => ScriptApp.deleteTrigger(t));
  ScriptApp.newTrigger('checkMail').timeBased().everyMinutes(1).create();

  // Everything already in the inbox is history, not news.
  const ids = [];
  SOURCES.forEach(s => GmailApp.search(s.query).forEach(
    th => th.getMessages().forEach(m => ids.push(m.getId()))));
  saveIds_(ids);
  Logger.log(`trigger installed; ${ids.length} existing messages marked as seen`);
}

/** Ask the relay to let this worker in. Run once; approve it in the console. */
function enrol() {
  const p = PropertiesService.getScriptProperties();
  let token = p.getProperty('RELAY_TOKEN');
  if (!token) {
    token = Utilities.getUuid() + Utilities.getUuid().replace(/-/g, '');
    p.setProperty('RELAY_TOKEN', token);
  }
  const res = UrlFetchApp.fetch(base_() + '/enrol', {
    method: 'post', contentType: 'application/json', muteHttpExceptions: true,
    payload: JSON.stringify({
      worker_id: p.getProperty('RELAY_WORKER') || 'gmail-bot',
      token: token,
      label: 'Gmail alerts',
    }),
  });
  Logger.log(res.getContentText());
}

/** Send one message, to check the relay accepts this worker. */
function testRelay() {
  Logger.log(post_({ source: 'test', type: 'email', emailSubject: 'Test from Apps Script',
                     text: 'If you can read this, the relay is reachable.' },
                   'test:' + Date.now()) ? 'OK' : 'FAILED');
}

function checkMail() {
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(5000)) return;
  try {
    const sent = new Set(loadIds_());
    for (const { source, query } of SOURCES) {
      const fresh = [];
      GmailApp.search(query, 0, 50).forEach(
        th => th.getMessages().forEach(m => { if (!sent.has(m.getId())) fresh.push(m); }));
      fresh.sort((a, b) => a.getDate() - b.getDate());

      for (const m of fresh) {
        if (!sendEmail_(source, m)) return;      // relay down: try again next run
        sent.add(m.getId());
        saveIds_([...sent]);
      }
    }
  } finally {
    lock.releaseLock();
  }
}

/** One email: as separate jobs where they can be found, otherwise as itself. */
function sendEmail_(source, m) {
  const base = {
    source: source,
    emailId: m.getId(),
    emailSubject: m.getSubject(),
    receivedAt: m.getDate().toISOString(),
  };
  const jobs = source === 'vollna' ? parseJobs_(m.getBody()) : [];
  const bodies = jobs.length
    ? jobs.map((j, i) => Object.assign({}, base, { type: 'job', index: i }, j))
    : [Object.assign({}, base, {
        type: source === 'upwork-invitation' ? 'invitation' : 'email',
        title: m.getSubject(),
        text: m.getPlainBody().slice(0, MAX_TEXT),
      })];

  for (let i = 0; i < bodies.length; i++) {
    // The id is what makes a retry harmless: the relay stores one message per
    // id, so re-sending after a failure cannot duplicate what already arrived.
    if (!post_(bodies[i], m.getId() + ':' + i)) return false;
  }
  return true;
}

function parseJobs_(html) {
  const jobs = [];
  const re = /<a\b[^>]*href="([^"]*place(?:=|%3D)title[^"]*)"[^>]*>([\s\S]*?)<\/a>/gi;
  const matches = [...html.matchAll(re)];
  matches.forEach((mt, i) => {
    const href = mt[1];
    const end = i + 1 < matches.length ? matches[i + 1].index : mt.index + mt[0].length + 2000;
    const cells = lines_(html.slice(mt.index + mt[0].length, end));
    const jobId = (href.match(/jobs(?:\/|%2F|%252F|%25252F)(~\d+)/i) || [])[1];
    const pid = (href.match(/pid(?:=|%3D)(\d+)/i) || [])[1];
    jobs.push({
      title: lines_(mt[2]).join(' '),
      budget: cells[0] || null,
      published: cells[1] || null,
      upworkUrl: jobId ? `https://www.upwork.com/jobs/${jobId}` : null,
      vollnaProjectId: pid || null,
      // Everything found between this job and the next. Sent as it is until
      // somebody has read enough of them to say which cell means what.
      cells: cells,
    });
  });
  return jobs;
}

function lines_(s) {
  return s
    .replace(/<br\s*\/?>|<\/(td|th|tr|p|div|h\d|li)>/gi, '\n')
    .replace(/<[^>]+>/g, '')
    .replace(/&nbsp;/g, ' ').replace(/&amp;/g, '&').replace(/&quot;/g, '"')
    .replace(/&#0?39;/g, "'").replace(/&lt;/g, '<').replace(/&gt;/g, '>')
    .replace(/&zwj;|&#\d+;/g, '')
    .split('\n').map(x => x.trim()).filter(Boolean);
}

function base_() {
  return PropertiesService.getScriptProperties().getProperty('RELAY_URL').replace(/\/+$/, '');
}

function post_(body, id) {
  const p = PropertiesService.getScriptProperties();
  const payload = { channel: p.getProperty('RELAY_CHANNEL') || 'jobs', body: body };
  if (id) payload.id = id;
  try {
    const res = UrlFetchApp.fetch(base_() + '/publish', {
      method: 'post',
      contentType: 'application/json',
      headers: { Authorization: 'Bearer ' + p.getProperty('RELAY_TOKEN') },
      payload: JSON.stringify(payload),
      muteHttpExceptions: true,
    });
    const code = res.getResponseCode();
    if (code >= 200 && code < 300) return true;
    // 401 is "who are you", 403 is "not yet approved" or "not in that channel".
    console.error(`Relay answered ${code}: ${res.getContentText().slice(0, 300)}`);
  } catch (e) {
    console.error('Relay unreachable: ' + e);
  }
  return false;
}

function loadIds_() {
  return JSON.parse(PropertiesService.getScriptProperties().getProperty('SENT_IDS') || '[]');
}

function saveIds_(ids) {
  PropertiesService.getScriptProperties()
    .setProperty('SENT_IDS', JSON.stringify(ids.slice(-MAX_IDS)));
}
