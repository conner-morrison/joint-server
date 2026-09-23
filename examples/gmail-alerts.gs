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
// stopping cannot quietly take the others with it. Upwork mail is fetched in
// one search and sorted afterwards: Gmail matches whole words, so a search for
// `invit` finds neither "invitation" nor "invited", and splitting the two
// kinds by search terms quietly mislabelled every invitation as a digest.
const SOURCES = [
  { source: 'vollna', query: 'from:info@vollna.com newer_than:2d' },
  { source: 'upwork', query: 'from:upwork.com newer_than:2d' },
];

// An invitation says someone asked for you. A digest says a search matched.
// Matched against the subject as a prefix, so invite, invited and invitation
// are all one rule.
const INVITATION_RE = /\binvit|\binterview\b|asked you to apply|wants to interview/i;

// The same job reaches this inbox more than once: a Vollna digest and an
// Upwork alert can both carry it, and two digests an hour apart often repeat
// it. The relay stores one message per id, so giving a job the id of the job
// itself makes a repeat a no-op there - once, for every reader at once,
// rather than each of them working out separately that they have seen it.
function jobKey_(job) {
  const url = job.upworkUrl || job.url || '';
  const id = (url.match(/~[0-9a-z]+/i) || [])[0];       // Upwork's own job id
  if (id) return 'job:' + id;
  if (url) return 'job:' + url;
  return '';                                            // nothing stable to go on
}

// Every Upwork job id a mail mentions, without repeats. Used for the mail that
// could not be read as jobs at all: when it names exactly one job it is about
// that job, and can carry the same id as the reading of it that arrived
// elsewhere. Invitations are left out - being asked for by name is news even
// about a job already seen.
function jobIdsIn_(html) {
  const ids = [...String(html).matchAll(/jobs(?:\/|%2F|%252F|%25252F)(~[0-9a-z]{6,})/gi)]
    .map(m => m[1].toLowerCase());
  return [...new Set(ids)];
}

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
  const subject = m.getSubject() || '';
  const invitation = source !== 'vollna' && INVITATION_RE.test(subject);
  const base = {
    source: invitation ? 'upwork-invitation' : source === 'upwork' ? 'upwork-alert' : source,
    emailId: m.getId(),
    emailSubject: subject,
    receivedAt: m.getDate().toISOString(),
  };
  const jobs = source === 'vollna' ? parseJobs_(m.getBody()) : [];
  const bodies = jobs.length
    ? jobs.map((j, i) => Object.assign({}, base, { type: 'job', index: i }, j))
    : [Object.assign({}, base, {
        type: invitation ? 'invitation' : 'email',
        title: subject,
        text: m.getPlainBody().slice(0, MAX_TEXT),
      })];

  const mentioned = bodies.length === 1 ? jobIdsIn_(m.getBody()) : [];
  for (let i = 0; i < bodies.length; i++) {
    // A job is identified by the job; anything else by the mail it came in.
    // Either way the id is what makes a retry harmless, because the relay
    // stores one message per id.
    const named = bodies[i].type === 'email' && mentioned.length === 1 ? 'job:' + mentioned[0] : '';
    const id = jobKey_(bodies[i]) || named || (m.getId() + ':' + i);
    if (!post_(bodies[i], id)) return false;
  }
  return true;
}

function parseJobs_(html) {
  // One job is linked more than once in these emails - the title, a view
  // link, a tracking wrapper - and each link starts a new slice of the text
  // after it. Taken one link at a time that is three jobs: one with the money
  // in it, one with only a title, one holding the description. They are the
  // same job, and the link says which, so they are put back together.
  const re = /<a\b[^>]*href="([^"]*place(?:=|%3D)title[^"]*)"[^>]*>([\s\S]*?)<\/a>/gi;
  const matches = [...html.matchAll(re)];
  const byJob = new Map();
  const order = [];

  matches.forEach((mt, i) => {
    const href = mt[1];
    const end = i + 1 < matches.length ? matches[i + 1].index : mt.index + mt[0].length + 2000;
    const cells = lines_(html.slice(mt.index + mt[0].length, end));
    // Upwork's ids are not only digits: ~021987abc truncated to ~021987 is a
    // link that does not open, and a key two different jobs could share.
    const jobId = (href.match(/jobs(?:\/|%2F|%252F|%25252F)(~[0-9a-z]+)/i) || [])[1];
    const pid = (href.match(/pid(?:=|%3D)(\d+)/i) || [])[1];
    const title = lines_(mt[2]).join(' ');

    // Without an id there is nothing to say two links are one job, so each
    // stands alone rather than being merged into whatever came before it.
    const key = jobId || (pid && 'pid:' + pid) || ('at:' + mt.index);
    let job = byJob.get(key);
    if (!job) {
      job = {
        title: '',
        budget: null,
        published: null,
        upworkUrl: jobId ? `https://www.upwork.com/jobs/${jobId}` : null,
        vollnaProjectId: pid || null,
        cells: [],
      };
      byJob.set(key, job);
      order.push(job);
      // The first slice is the one laid out as a row: money, then when.
      job.budget = cells[0] || null;
      job.published = cells[1] || null;
    }
    if (!job.title && title) job.title = title;
    if (!job.upworkUrl && jobId) job.upworkUrl = `https://www.upwork.com/jobs/${jobId}`;
    if (!job.vollnaProjectId && pid) job.vollnaProjectId = pid;
    for (const cell of cells) if (!job.cells.includes(cell)) job.cells.push(cell);
  });

  // The longest thing said about a job is its description, wherever in the
  // email it turned up.
  for (const job of order) {
    const longest = job.cells.reduce((a, b) => (b.length > a.length ? b : a), '');
    if (longest.length >= 80) job.description = longest;
  }
  // A fragment with neither a title nor a link is markup, not a job.
  return order.filter(j => j.title || j.upworkUrl);
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
    if (code >= 200 && code < 300) {
      // The relay says when it has seen this id before. Nothing was stored and
      // nobody was told, which is the point; worth a line while watching.
      const said = JSON.parse(res.getContentText() || '{}');
      if (said.duplicate) console.log('already posted, skipped: ' + id);
      return true;
    }
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
