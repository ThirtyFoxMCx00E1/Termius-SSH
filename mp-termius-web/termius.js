// MP Termius: phone-friendly console for the MP Multiplayer servers (2.21).
// Reads regions.txt (the one file with every region's broker), connects to every region over
// secure WebSocket, shows the 24/7 status beacon of each HiveMQ server (mp-status), the live server
// list, ping, and embeds each server's web terminal (ttyd, https://DOMAIN:7681). No secrets here:
// only the public read-only "web" login from regions.txt is used.
(function () {
  'use strict';
  var REG = ['Asia', 'Europe', 'NA', 'Oceania'];
  var PUBLIC = ['wss://broker.hivemq.com:8884/mqtt', 'wss://broker.emqx.io:8084/mqtt', 'wss://test.mosquitto.org:8081/'];
  var ROOT = 'thirtyfox/mp4/';
  var FRESH = 15000, BEACON_FRESH = 45;
  var $ = function (id) { return document.getElementById(id); };
  var regions = [null, null, null, null], ping = [null, null, null, null], beacon = [null, null, null, null];
  var conn = [null, null, null, null], servers = new Map(), pub = null, pubName = '', pubPing = null;
  var hist = [], hp = 0;

  function esc(x) { return String(x).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); }
  function okWss(u) { return /^wss:\/\/[^\s/]+/i.test(u || ''); }
  function okHttps(u) { return /^https:\/\/[^\s/]+/i.test(u || ''); }

  // ---------------------------------------------------------------- output
  var out = $('out');
  function print(html) {
    var d = document.createElement('div'); d.innerHTML = html; out.appendChild(d);
    while (out.childNodes.length > 600) out.removeChild(out.firstChild);
    out.scrollTop = out.scrollHeight;
  }
  function p(cls, t) { print('<span class="' + cls + '">' + esc(t) + '</span>'); }

  // ---------------------------------------------------------------- regions.txt
  function parseRegions(txt) {
    var cur = -2;
    txt.split(/\r?\n/).forEach(function (raw) {
      var line = raw.replace(/(^|\s)#.*/, '').trim();
      if (!line) return;
      var m = line.match(/^\[(.+)\]$/);
      if (m) {
        var n = m[1].trim().toLowerCase();
        cur = REG.findIndex(function (r) { return r.toLowerCase() === n; });
        if (n === 'north america' || n === 'us') cur = 2;
        if (n === 'eu') cur = 1;
        if (n === 'au' || n === 'oc') cur = 3;
        if (cur >= 0 && !regions[cur]) regions[cur] = {};
        return;
      }
      if (cur < 0) return;
      var eq = line.indexOf('=');
      if (eq < 0) return;
      var k = line.slice(0, eq).trim().toLowerCase();
      if (!(k in regions[cur])) regions[cur][k] = line.slice(eq + 1).trim();
    });
    regions = regions.map(function (r) { return r && (r.host || r.web) ? r : null; });
  }

  // ---------------------------------------------------------------- ping
  function wsPing(url) {
    return new Promise(function (res) {
      var t0 = performance.now(), done = false, ws;
      var fin = function (v) { if (done) return; done = true; try { ws.close(); } catch (e) {} res(v); };
      try { ws = new WebSocket(url, 'mqtt'); } catch (e) { return res(-1); }
      ws.onopen = function () { fin(Math.round(performance.now() - t0)); };
      ws.onerror = function () { fin(-1); };
      setTimeout(function () { fin(-1); }, 6000);
    });
  }
  function best2(url) { return wsPing(url).then(function (a) { return wsPing(url).then(function (b) { var v = [a, b].filter(function (x) { return x >= 0; }); return v.length ? Math.min.apply(null, v) : -1; }); }); }
  function measure() {
    return Promise.all(regions.map(function (r, i) {
      if (!r || !okWss(r.web)) return null;
      return best2(r.web).then(function (v) { ping[i] = v; });
    })).then(function () {
      if (pub) return best2(pub).then(function (v) { pubPing = v; });
    }).then(render);
  }

  // ---------------------------------------------------------------- MQTT
  function onList(topic, payload, region) {
    var tok = topic.split('/').pop(), s = payload.toString();
    if (!s) { servers.delete(tok); return; }
    var f = s.split('|');
    if (f[0] !== 'v1' || f.length < 7) return;
    var r = +f[4];
    servers.set(tok, { port: +f[1] || 0, players: +f[2] || 0, max: +f[3] || 0, region: isNaN(r) ? region : r, name: f.slice(6).join('|').slice(0, 64), seen: Date.now() });
  }
  function opts(user, pass) {
    var o = { clientId: 'webterm' + Math.random().toString(16).slice(2, 12), clean: true, connectTimeout: 8000, reconnectPeriod: 3000, keepalive: 30, protocolVersion: 4 };
    if (user) { o.username = user; o.password = pass || ''; }
    return o;
  }
  function connectRegion(i) {
    var r = regions[i];
    if (!r || !okWss(r.web) || typeof mqtt === 'undefined') return;
    var c = mqtt.connect(r.web, opts(r.web_user, r.web_pass));
    conn[i] = c;
    c.on('connect', function () { c.subscribe([ROOT + 'l/+', ROOT + 'st/+']); render(); });
    c.on('close', render);
    c.on('error', function () {});
    c.on('message', function (topic, payload) {
      if (topic.indexOf(ROOT + 'st/') === 0) {
        var name = topic.split('/').pop(), idx = REG.findIndex(function (x) { return x.toLowerCase() === name.toLowerCase(); });
        if (idx < 0) return;
        try { beacon[idx] = JSON.parse(payload.toString() || '{}'); beacon[idx].rx = Date.now(); } catch (e) {}
      } else onList(topic, payload, i);
      render();
    });
  }
  var pi = 0;
  function connectPublic() {
    if (typeof mqtt === 'undefined') { p('r', 'MQTT library did not load. Reload the page.'); return; }
    var url = PUBLIC[pi % PUBLIC.length], c = mqtt.connect(url, Object.assign(opts(), { reconnectPeriod: 0 })), gone = false;
    pub = url; pubName = new URL(url).hostname;
    c.on('connect', function () { c.subscribe(ROOT + 'l/+'); p('m', 'public lobby: connected to ' + pubName); render(); });
    c.on('message', function (t, pl) { onList(t, pl, -1); render(); });
    var fail = function () { if (gone) return; gone = true; try { c.end(true); } catch (e) {} pi++; setTimeout(connectPublic, 2500); render(); };
    c.on('error', fail); c.on('close', fail);
    conn.pub = c;
  }

  // ---------------------------------------------------------------- state
  function bState(i) {
    var b = beacon[i], c = conn[i];
    if (!regions[i]) return { cls: '', t: 'not set up (public lobby)' };
    if (!okWss(regions[i].web)) return { cls: 'warn', t: 'set up, no wss endpoint' };
    if (!c || !c.connected) return { cls: 'bad', t: ping[i] === -1 ? 'broker unreachable' : 'connecting…' };
    if (!b) return { cls: 'warn', t: 'broker online (no status beacon)' };
    var age = Date.now() / 1000 - (b.t || 0);
    if (b.online === false) return { cls: 'bad', t: 'server went offline' };
    if (age > BEACON_FRESH && Date.now() - b.rx > BEACON_FRESH * 1000) return { cls: 'warn', t: 'broker online, beacon silent ' + Math.round(age) + 's' };
    return { cls: 'ok', t: 'online 24/7' };
  }
  function up(sec) { sec = +sec || 0; var d = Math.floor(sec / 86400), h = Math.floor(sec % 86400 / 3600), m = Math.floor(sec % 3600 / 60); return (d ? d + 'd ' : '') + h + 'h ' + m + 'm'; }
  function bestRegion() { var b = -1; ping.forEach(function (v, i) { if (v != null && v >= 0 && (b < 0 || v < ping[b])) b = i; }); return b; }
  function prune() { var now = Date.now(); servers.forEach(function (s, k) { if (now - s.seen > FRESH) servers.delete(k); }); }

  function render() {
    prune();
    var counts = [0, 0, 0, 0], players = 0, online = 0, b = bestRegion();
    servers.forEach(function (s) { if (s.region >= 0 && s.region < 4) counts[s.region]++; players += s.players; });
    $('hosts').innerHTML = REG.map(function (name, i) {
      var st = bState(i), be = beacon[i], r = regions[i];
      if (st.cls === 'ok') online++;
      var pill = function (on, t) { return '<span class="pill ' + (on ? 'on' : 'off') + '">' + t + '</span>'; };
      var info = r ? esc(r.host || new URL(r.web).hostname) : 'add a block in regions.txt';
      var extra = '';
      if (be && be.online) {
        extra = '<span class="s">up ' + up(be.up) + ' · ' + (be.clients >= 0 ? be.clients + ' MQTT clients' : '') + ' · load ' + (+be.load || 0).toFixed(2) + (be.mem ? ' · ' + be.mem[0] + '/' + be.mem[1] + ' MB' : '') + '</span>' +
          '<span class="row">' + pill(be.broker, 'HiveMQ') + pill(be.server, '24/7 server') + pill(be.bridge, 'bridge') + pill(!!be.console, 'console') + '</span>';
      }
      return '<article class="host"><span class="h ' + st.cls + '"><span class="dot"></span><b>' + name + (i === b ? ' · best' : '') + '</b>' +
        '<span class="ms">' + (ping[i] > 0 ? ping[i] + ' ms' : '') + '</span></span>' +
        '<span class="s">' + esc(st.t) + ' · ' + info + '</span><span class="s">' + counts[i] + ' server' + (counts[i] === 1 ? '' : 's') + ' listed</span>' + extra + '</article>';
    }).join('');
    var anyRegion = regions.some(function (r) { return r && okWss(r.web); });
    var lv = $('live'), pubOk = conn.pub && conn.pub.connected;
    lv.className = 'live ' + (online ? 'ok' : (pubOk || anyRegion ? 'warn' : 'bad'));
    $('live-t').textContent = (anyRegion ? online + '/4 regions online' : (pubOk ? 'public lobby ' + pubName : 'connecting…')) + ' · ' + servers.size + ' servers · ' + players + ' players';
    fillConsoles();
  }

  // ---------------------------------------------------------------- server console tab
  function consoleUrl(i) {
    var r = regions[i], be = beacon[i];
    if (be && okHttps(be.console)) return be.console;
    if (r && okHttps(r.console)) return r.console;
    return '';
  }
  var lastSel = '';
  function fillConsoles() {
    var sel = $('csel'), opts = [];
    REG.forEach(function (n, i) { var u = consoleUrl(i); if (u) opts.push([i, n + ' – ' + new URL(u).host]); });
    var key = JSON.stringify(opts);
    if (key === lastSel) return;
    lastSel = key;
    sel.innerHTML = opts.map(function (o) { return '<option value="' + o[0] + '">' + esc(o[1]) + '</option>'; }).join('');
    $('cnote').hidden = opts.length > 0; sel.hidden = !opts.length; $('copen').hidden = !opts.length; $('cframe').hidden = !opts.length; document.querySelector('.webbar').hidden = !opts.length;
    if (opts.length) showConsole(+sel.value, false);
  }
  function showConsole(i, load) {
    var u = consoleUrl(i);
    if (!u) return false;
    $('copen').href = u;
    if (load || $('cframe').dataset.u) { if ($('cframe').dataset.u !== u) { $('cframe').src = u; $('cframe').dataset.u = u; } }
    return true;
  }
  $('csel').addEventListener('change', function () { showConsole(+this.value, true); });
  function tab(v) {
    document.querySelectorAll('.tab').forEach(function (t) { t.classList.toggle('on', t.dataset.v === v); });
    $('v-shell').hidden = v !== 'shell'; $('v-web').hidden = v !== 'web';
    if (v === 'web' && $('csel').value !== '') showConsole(+$('csel').value, true);
    if (v === 'shell') $('in').focus();
  }
  document.querySelectorAll('.tab').forEach(function (t) { t.addEventListener('click', function () { tab(t.dataset.v); }); });

  // ---------------------------------------------------------------- commands
  function regionArg(a) {
    if (!a) return bestRegion() >= 0 ? bestRegion() : REG.findIndex(function (_, i) { return !!regions[i]; });
    a = a.toLowerCase();
    var i = REG.findIndex(function (r) { return r.toLowerCase() === a || r.toLowerCase().slice(0, 2) === a.slice(0, 2); });
    if (a === 'eu') i = 1; if (a === 'us') i = 2; if (a === 'au' || a === 'oc') i = 3;
    return i;
  }
  var CMDS = {
    help: function () {
      p('a', 'MP Termius 2.21 - commands');
      [['status', 'health of every region (broker, 24/7 server, bridge, uptime)'], ['servers', 'live server list (all regions)'],
       ['ping', 'measure ping to every region now'], ['regions', 'what regions.txt contains (no passwords shown)'],
       ['console [region]', 'open the server terminal of a region (login admin)'], ['ssh [region]', 'SSH command / Termius link for a region'],
       ['broker', 'connection details for the mod / MQTT apps'], ['clear', 'clear the screen']].forEach(function (c) {
        print('  <span class="g">' + esc(c[0]) + '</span>' + ' '.repeat(Math.max(1, 18 - c[0].length)) + '<span class="m">' + esc(c[1]) + '</span>');
      });
    },
    status: function () {
      REG.forEach(function (n, i) {
        var st = bState(i), be = beacon[i];
        print('<span class="' + (st.cls === 'ok' ? 'g' : st.cls === 'bad' ? 'r' : st.cls === 'warn' ? 'y' : 'm') + '">' + esc((n + '        ').slice(0, 8)) + '</span> ' + esc(st.t) +
          (ping[i] > 0 ? ' <span class="a">' + ping[i] + ' ms</span>' : '') +
          (be && be.online ? '<span class="m"> · up ' + up(be.up) + ' · ' + be.clients + ' clients · server ' + (be.server ? 'running' : 'stopped') + ' · ' + (be.slots || 100) + ' slots</span>' : ''));
      });
      if (!regions.some(Boolean)) p('m', 'No region server yet: everybody uses the free public lobby (' + (pubName || '…') + (pubPing > 0 ? ', ' + pubPing + ' ms' : '') + '). Set one up with the HiveMQ kit (cloud-init/user-data.yaml).');
    },
    servers: function () {
      prune();
      var all = Array.from(servers.values()).sort(function (a, b) { return b.players - a.players || a.name.localeCompare(b.name); });
      if (!all.length) { p('m', 'No servers online right now. Host one from the game and it shows up here.'); return; }
      p('a', all.length + ' server' + (all.length === 1 ? '' : 's') + ' online');
      all.forEach(function (s) {
        print('<span class="' + (s.max && s.players >= s.max ? 'r' : 'g') + '">' + s.players + '/' + s.max + '</span>  ' + esc(s.name || 'Unnamed server') + ' <span class="m">· ' + esc(REG[s.region] || '?') + ' · port ' + s.port + '</span>' + (s.port === 1922 && /mp lan server/i.test(s.name) ? ' <span class="c">24/7</span>' : ''));
      });
    },
    ping: function () { p('m', 'measuring…'); measure().then(function () { CMDS.status(); }); },
    regions: function () {
      var any = false;
      regions.forEach(function (r, i) {
        if (!r) return; any = true;
        print('<span class="g">[' + REG[i] + ']</span> host=' + esc(r.host || '-') + ' port=' + esc(r.port || '1883') + ' tls=' + esc(r.tls || '0') + ' web=' + esc(r.web || '-') + (r.console ? ' console=' + esc(r.console) : ''));
      });
      if (!any) p('m', 'regions.txt has no region yet (all blocks are commented out).');
    },
    console: function (a) {
      var i = regionArg(a);
      if (i < 0 || !consoleUrl(i)) { p('y', 'No web console for ' + (REG[i] || 'that region') + '. Install the region with the HiveMQ kit and a domain (console runs on https://DOMAIN:7681).'); return; }
      $('csel').value = String(i); showConsole(i, true); tab('web');
    },
    ssh: function (a) {
      var i = regionArg(a), r = regions[i];
      if (!r || !r.host) { p('y', 'No host for that region in regions.txt yet.'); return; }
      var user = (r.ssh && r.ssh.indexOf('@') > 0) ? r.ssh.split('@')[0] : 'ubuntu';
      print('ssh ' + esc(user) + '@' + esc(r.host) + '   <span class="m">(then: sudo /opt/mp/console/mp-console.sh)</span>');
      print('<a href="ssh://' + encodeURIComponent(user) + '@' + esc(r.host) + '">open in an SSH app</a> <span class="m">· no app? use "console ' + REG[i].toLowerCase() + '" (browser terminal)</span>');
    },
    broker: function () {
      var i = bestRegion(), r = regions[i];
      if (!r) { p('m', 'Mod default: public lobby (' + PUBLIC.map(function (u) { return new URL(u).hostname; }).join(', ') + '), topic root ' + ROOT); return; }
      print('best region <span class="g">' + REG[i] + '</span>: mqtt://' + esc(r.host) + ':' + esc(r.port || 1883) + (r.web ? ' · ' + esc(r.web) : '') + ' · topic root ' + ROOT);
      p('m', 'The mod reads regions.txt by itself and picks the lowest ping; every phone shares the public "mp" login with its own client id.');
    },
    clear: function () { out.innerHTML = ''; }
  };
  CMDS.ls = CMDS.help; CMDS['?'] = CMDS.help; CMDS.cls = CMDS.clear; CMDS.list = CMDS.servers; CMDS.top = CMDS.status; CMDS.uptime = CMDS.status;
  function run(line) {
    line = line.trim();
    print('<span class="g">mp@lobby:~$</span> ' + esc(line));
    if (!line) return;
    hist.push(line); hp = hist.length;
    var parts = line.split(/\s+/), c = parts[0].toLowerCase().replace(/^\//, '');
    if (CMDS[c]) CMDS[c](parts[1]); else p('r', c + ': command not found (type help)');
  }
  $('f').addEventListener('submit', function (e) { e.preventDefault(); var v = $('in').value; $('in').value = ''; run(v); });
  $('in').addEventListener('keydown', function (e) {
    if (e.key === 'ArrowUp' && hp > 0) { hp--; this.value = hist[hp]; e.preventDefault(); }
    else if (e.key === 'ArrowDown') { hp = Math.min(hist.length, hp + 1); this.value = hist[hp] || ''; e.preventDefault(); }
    else if (e.key === 'Tab') { var v = this.value.toLowerCase(), m = Object.keys(CMDS).filter(function (k) { return k.indexOf(v) === 0; }); if (m.length === 1) this.value = m[0] + ' '; e.preventDefault(); }
  });
  $('keys').addEventListener('click', function (e) { var b = e.target.closest('button'); if (b) run(b.dataset.c); });

  // ---------------------------------------------------------------- start
  p('g', 'MP Termius 2.21 - server console for MP Multiplayer');
  p('m', 'Reading regions.txt…');
  render();
  fetch('regions.txt', { cache: 'no-cache' }).then(function (r) { return r.ok ? r.text() : ''; }).catch(function () { return ''; })
    .then(function (t) {
      parseRegions(t || '');
      var n = regions.filter(function (r) { return r && okWss(r.web); }).length;
      p('m', n ? n + ' region broker(s) found, connecting…' : 'No region broker in regions.txt yet, using the public lobby.');
      regions.forEach(function (_, i) { connectRegion(i); });
      if (!n) connectPublic();
      render();
      return measure();
    }).then(function () { p('m', 'Type help, or tap a key below.'); CMDS.status(); });
  setInterval(render, 3000);
  setInterval(measure, 60000);
})();
