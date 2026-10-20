# MP Termius (web)

Our own Termius for MP Multiplayer, made for phones: no app, no computer. Live at
**https://thirtyfoxmcx00e1.github.io/Sa-mp/termius.html**

- 24/7 status of every region (Asia, Europe, NA, Oceania): HiveMQ broker, 24/7 server, bridge, uptime, clients, ping
- live server list of all regions
- `MP shell`: `status`, `servers`, `ping`, `regions`, `console`, `ssh`, `broker`, `help`
- `Server console`: the real terminal of each HiveMQ server in the browser (ttyd on https://DOMAIN:7681, password login), with a menu for logs, restarts, updates and a bash shell

Everything comes from one file, `regions.txt` (the same file the mod reads). A region shows up as soon as its
server was installed with the [HiveMQ kit](https://github.com/ThirtyFoxMCx00E1/HiveMQ) (`cloud-init/user-data.yaml`
for a phone-only setup) and its block is in `regions.txt`.

This folder is a copy of `docs/termius.*` from [Sa-mp](https://github.com/ThirtyFoxMCx00E1/Sa-mp) so it can be
hosted anywhere static (GitHub Pages, Netlify ...). Put your `regions.txt` next to `termius.html`.
