/**
 * Job alerts from Gmail into a relay channel.
 *
 * Script properties (Project Settings -> Script properties):
 *   RELAY_URL      https://relay-xxx.up.railway.app/upwork   (the workspace, not the console)
 *   RELAY_CHANNEL  jobs
 *   RELAY_NAME     what to call this worker in the console, e.g. Gmail alerts
 *
 * Written by the script itself, not by you:
 *   RELAY_TICKET   while a request is waiting to be approved
 *   RELAY_ID       the id the relay gave this worker. It is the whole of this
 *                  worker's identity and its way in, so treat it as a password.
 *
 * Run enrol() once and approve it in the console. The id arrives by itself;
 * nothing is copied from the console to here. Then run setup() to install the
 * trigger, and checkMail() runs every minute after that.
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

/**
 * Ask the relay to let this worker in. Run once; approve it in the console.
 *
 * All this sends is a name. There is nothing to invent here and nothing to
 * carry back from the console: the relay answers with a ticket, and once a
 * person has said yes, that ticket is exchanged for the id this worker uses
 * from then on. collect() does the exchange, and publishing does it too, so in
 * practice there is nothing to run but this.
 */
function enrol() {
  const p = PropertiesService.getScriptProperties();
  const res = UrlFetchApp.fetch(base_() + '/enrol', {
    method: 'post', contentType: 'application/json', muteHttpExceptions: true,
    payload: JSON.stringify({ name: p.getProperty('RELAY_NAME') || 'Gmail alerts',
                              label: 'Gmail alerts (Apps Script)' }),
  });
  const said = JSON.parse(res.getContentText() || '{}');
  if (said.ticket) {
    p.setProperty('RELAY_TICKET', said.ticket);
    p.deleteProperty('RELAY_ID');
    Logger.log('asked to join as "' + said.name + '". Approve it in the console; '
               + 'the id arrives on its own.');
    return;
  }
  Logger.log('could not ask to join: ' + res.getResponseCode() + ' ' + res.getContentText());
}

/**
 * Collect the id, if a person has approved the request by now.
 *
 * Returns the id, or an empty string while there is still nothing to collect.
 * Safe to call as often as you like: once the id is here it does nothing.
 */
function collect_() {
  const p = PropertiesService.getScriptProperties();
  const known = p.getProperty('RELAY_ID');
  if (known) return known;
  const ticket = p.getProperty('RELAY_TICKET');
  if (!ticket) return '';
  const res = UrlFetchApp.fetch(base_() + '/enrol/' + encodeURIComponent(ticket),
                                { muteHttpExceptions: true });
  const code = res.getResponseCode();
  const said = JSON.parse(res.getContentText() || '{}');
  if (code === 200 && said.worker_id) {
    p.setProperty('RELAY_ID', said.worker_id);
    p.deleteProperty('RELAY_TICKET');
    console.log('registered as "' + said.name + '"');
    return said.worker_id;
  }
  if (code === 404) {
    // Declined, or waited so long the relay forgot it. Asking again is the
    // only thing that can help, and it costs one request.
    p.deleteProperty('RELAY_TICKET');
    console.log('the request is gone; asking again');
    enrol();
  }
  return '';
}

/** What came of the request, in words, for the Run dropdown. */
function registration() {
  const p = PropertiesService.getScriptProperties();
  const id = collect_();
  if (id) Logger.log('registered. The id is in RELAY_ID; treat it as a password.');
  else if (p.getProperty('RELAY_TICKET')) Logger.log('waiting to be approved in the console');
  else Logger.log('not registered and not waiting: run enrol()');
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
        slices: [],
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
    // A wrapper link repeats the whole slice the title link already gave, so
    // an identical slice is dropped whole. Not line by line: a description
    // may say the same short thing twice, and both times are part of it.
    const slice = cells.join('\n');
    if (slice && job.slices.indexOf(slice) < 0) {
      job.slices.push(slice);
      job.cells.push(...cells);
    }
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
  // Nothing can be published until this worker has been given an id, and the
  // moment it has, everything waiting goes. Checking here is what makes
  // approval take effect by itself.
  const worker = collect_();
  if (!worker) {
    console.log('not registered yet; nothing published. Approve it in the console.');
    return false;
  }
  const payload = { channel: p.getProperty('RELAY_CHANNEL') || 'jobs', body: body };
  if (id) payload.id = id;
  try {
    const res = UrlFetchApp.fetch(base_() + '/publish', {
      method: 'post',
      contentType: 'application/json',
      headers: { Authorization: 'Bearer ' + worker },
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
    if (code === 401) {
      // The id this worker held means nothing to the relay any more: it was
      // removed, or given a new one. Asking again is how it gets back in.
      p.deleteProperty('RELAY_ID');
      console.error('the relay does not know this id any more; asking to join again');
      enrol();
      return false;
    }
    // 403 is "not in that channel".
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
