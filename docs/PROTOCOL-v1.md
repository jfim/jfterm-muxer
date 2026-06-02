# jftermd wire protocol — v1

Canonical contract between **jftermd** (the Rust daemon, this repo) and any client
(notably JFTerm's Python/GTK `RemotePtyProxy`). This document is authoritative for
`proto_version = 1`; the canonical source of truth in code is
[`jftermd/src/protocol.rs`](../jftermd/src/protocol.rs).

The client codes against this document and against `proto_version`, never against
the daemon's internals. The coupling is deliberately narrow: the daemon owns PTYs
and parsing; the client is a pure transport + a terminal widget (VTE).

---

## 1. Transport

- **Unix domain socket**, one daemon per user, at
  `$XDG_RUNTIME_DIR/jfterm/muxer.sock`. If `$XDG_RUNTIME_DIR` is unset, the daemon
  falls back to `/tmp/jfterm-<uid>/muxer.sock`.
- The parent directory is created `0700`; the socket file is `0600`. Single-user;
  filesystem permissions are the only access control (no in-band auth).
- A client opens **two kinds of connection**, each a separate socket:
  - **One control connection** per client — launch-time reconciliation
    (`HELLO`, `LIST`).
  - **One session connection per session** — bound to a single shell by its first
    frame. Carries the hot path (keystrokes in, output out). One connection per
    session gives natural per-session backpressure and keeps a session id off
    every frame.
- The **first frame** on a connection classifies it (see §4/§5). If the first
  frame is neither `HELLO` nor `ATTACH_OR_OPEN`, the daemon closes the connection.

---

## 2. Frame format (TLV)

Every message is exactly one frame:

```
+--------+------------------+-------------------------+
| type   | length           | value                   |
| u8     | u32 (big-endian) | `length` bytes          |
+--------+------------------+-------------------------+
```

- **`type`** — 1 byte, one of the tags in §3.
- **`length`** — 4 bytes, **big-endian** unsigned. The number of bytes in `value`.
- **`value`** — `length` bytes. Either **raw terminal bytes** (`DATA`, `INPUT`) or
  a **UTF-8 JSON** document (all control frames). Some frames have an **empty**
  value (`length = 0`).

The 5-byte header is fixed. `length` **must not exceed `16 * 1024 * 1024`
(16 MiB)**; a frame declaring a larger length is a protocol violation and the
receiver closes the connection. The high bits of `length` are therefore always
zero today (16 MiB is `0x0100_0000`); there is **no continuation/“more” flag** in
the length field — see "Payloads larger than the cap" below for why one isn't
needed.

### Reading frames (the framing mechanism)

A frame's bytes may arrive across several `read()`s, and one `read()` may deliver
several frames (or a frame and a half). So a reader buffers incoming bytes and
extracts frames with a fixed loop:

1. If the buffer holds fewer than 5 bytes, wait for more.
2. Parse `type` (byte 0) and `length` (bytes 1–4, big-endian). If `length`
   exceeds 16 MiB, the stream is malformed — close the connection.
3. If the buffer holds fewer than `5 + length` bytes, wait for more.
4. Otherwise, a complete frame is `buffer[0 .. 5 + length]`; remove those bytes
   and dispatch it. Repeat from step 1 (more frames may already be buffered).

This is the entire framing contract. A reader **never reassembles a payload across
multiple frames** — each frame is processed on its own as soon as it completes.

### Payloads larger than the cap

There is no per-frame continuation flag because the only payloads that can grow
large are **byte streams**, where frame boundaries carry no meaning:

- **`DATA` (output) and `INPUT` (keystrokes/paste) are streams.** When output
  exceeds the cap, the daemon simply emits it as **several consecutive `DATA`
  frames**; the client concatenates their values in arrival order and feeds them to
  the terminal (VTE), which reassembles any escape sequence split across a frame
  boundary. Splitting at an arbitrary byte offset is lossless and invisible to the
  consumer — multiple `DATA` frames *are* the continuation, implicitly. The same
  applies to a large `INPUT`: send it as multiple `INPUT` frames; the daemon writes
  each to the PTY in order. Neither side allocates a reassembly buffer, so the
  16 MiB cap stays a hard per-read bound (no unbounded-growth path).
- **Control (JSON) frames are atomic and small.** A JSON document must arrive in a
  single frame (you can't parse half of one), but the largest control message,
  `SESSIONS`, is on the order of a couple hundred bytes per session — orders of
  magnitude under the cap. Control frames are never split.

(If some future *atomic* payload ever needed to exceed the cap, the always-zero top
bit of `length` could be claimed as a continuation flag in a later `proto_version`,
paired with a total-reassembled-size cap. v1 does not need it.)

### Byte-layout examples

A `DATA` frame carrying the two bytes `hi`:

```
09  00 00 00 02  68 69
^   ^^^^^^^^^^^   ^^^^^
|   length = 2    "hi"
type = DATA (9)
```

An empty `LIST` request (no value):

```
03  00 00 00 00
^   ^^^^^^^^^^^
|   length = 0
type = LIST (3)
```

A `HELLO` control frame whose value is the JSON `{"proto_version":1,"daemon_version":"0.1.0"}`
(42 bytes):

```
01  00 00 00 2A  7B 22 70 72 6F 74 6F ...   (42 JSON bytes)
^   ^^^^^^^^^^^   ^^^^^^^^^^^^^^^^^^^^^^^
|   length = 42  {"proto_version":1,...}
type = HELLO (1)
```

---

## 3. Frame types

| Tag | Name             | Dir  | Connection | Value         |
|----:|------------------|------|------------|---------------|
| 1   | `HELLO`          | C→D  | control    | JSON          |
| 2   | `HELLO_OK`       | D→C  | control    | JSON          |
| 3   | `LIST`           | C→D  | control    | empty         |
| 4   | `SESSIONS`       | D→C  | control    | JSON (array)  |
| 5   | `ATTACH_OR_OPEN` | C→D  | session    | JSON          |
| 6   | `INPUT`          | C→D  | session    | raw bytes     |
| 7   | `RESIZE`         | C→D  | session    | JSON          |
| 8   | `CLOSE`          | C→D  | session    | JSON          |
| 9   | `DATA`           | D→C  | session    | raw bytes     |
| 10  | `STATUS`         | D→C  | session    | JSON          |
| 11  | `EXIT`           | D→C  | session    | JSON          |

`C→D` = client to daemon, `D→C` = daemon to client. An unknown tag byte is a
protocol violation (connection closed).

All JSON objects use the exact field names below (snake_case). Unknown extra
fields should be ignored by readers; absent optional fields take their documented
default. JSON numbers are integers unless noted.

---

## 4. Control connection

Used once at launch to greet the daemon and enumerate live sessions.

### `HELLO` (C→D) / `HELLO_OK` (D→C)

The client's **first** control frame must be `HELLO`:

```json
{ "proto_version": 1, "daemon_version": "0.1.0" }
```

- `proto_version` — u32. Must be `1`. If it does not match the daemon's version,
  the daemon **closes the connection without replying** (treat a closed control
  socket right after `HELLO` as a version mismatch).
- `daemon_version` — string. Informational on the client→daemon `HELLO` (the
  daemon ignores the client's value); meaningful on the daemon→client `HELLO_OK`,
  where it carries the daemon's own version string.

On a matching version the daemon replies `HELLO_OK` with the same shape, e.g.
`{ "proto_version": 1, "daemon_version": "0.1.0" }`.

### `LIST` (C→D) / `SESSIONS` (D→C)

After `HELLO_OK`, the client may send `LIST` (empty value) any number of times.
The daemon replies `SESSIONS`, a JSON **array** of session descriptors:

```json
[
  {
    "session_id": "9b1f…",
    "argv": ["bash", "-l"],
    "cwd": "/home/u/project",
    "running": true,
    "has_client": false,
    "created_at": 1717286400
  }
]
```

- `session_id` — string. The client-assigned key for the session.
- `argv` — array of strings. The shell command the session was opened with.
- `cwd` — string. The cwd the session was opened with (its *initial* cwd; live cwd
  is tracked client-side from OSC 7 in the output stream, not from this field).
- `running` — bool. Whether a foreground command is active (the status dot). See
  §6 `STATUS` for semantics.
- `has_client` — bool. Whether a viewer is currently attached.
- `created_at` — u64. Unix epoch seconds when the session was opened.

The control connection stays open for the client's lifetime; it may be polled with
further `LIST`s. An unexpected frame type on the control connection is ignored.

---

## 5. Session connection

One socket per shell. The **first** frame must be `ATTACH_OR_OPEN`, which binds the
connection to a session (attaching to an existing one or opening a new one). There
is **no separate bind acknowledgement** — after `ATTACH_OR_OPEN` the daemon simply
begins sending `DATA`/`STATUS`/`EXIT` (see §7 for the exact ordering).

### `ATTACH_OR_OPEN` (C→D)

```json
{
  "session_id": "9b1f…",
  "cwd": "/home/u/project",
  "argv": ["bash", "-l"],
  "want_chunks": 0,
  "cols": 80,
  "rows": 24
}
```

- `session_id` — string. If a session with this id exists → **attach** (replay,
  then live). If not → **open** a fresh shell with `argv`/`cwd`. Race-free: a
  simultaneous attach-or-open for the same unknown id resolves to exactly one
  shell, and the loser attaches to it.
- `cwd` — string. Working directory for an **open**; ignored on attach.
- `argv` — array of strings. The shell command for an **open**; ignored on attach.
  `argv[0]` is resolved via `PATH`.
- `want_chunks` — integer ≥ 0. Replay depth on **attach**. `0` = full available
  scrollback (back to the last screen-clear boundary). `N > 0` = cap to the most
  recent `N` ring chunks (a memory/scrollback dial; modes/cwd/title stay correct,
  top scrollback is trimmed).
- `cols`, `rows` — u16. Terminal size; the daemon sets the PTY winsize and
  SIGWINCHes the shell.

The opened shell's environment has `TERM=xterm-256color` and
`COLORTERM=truecolor` forced (emulator-capability hints); all other environment is
inherited from the daemon. (Per-tab custom env is not part of v1.)

### `INPUT` (C→D)

Raw keystroke bytes in the value (no JSON). Forwarded verbatim to the shell's PTY.

### `RESIZE` (C→D)

```json
{ "cols": 80, "rows": 24 }
```

`cols`/`rows` are u16. The daemon updates the winsize and SIGWINCHes the shell's
process group. While detached the winsize holds its last value; the client should
send a `RESIZE` on attach if its size differs.

### `CLOSE` (C→D)

```json
{ "grace_ms": 0 }
```

- `grace_ms` — u32. Kill the shell and drop the session.
  - The daemon sends **SIGHUP** to the shell's process group immediately and stops
    accepting new attaches for the session.
  - If `grace_ms > 0` and the shell has not exited after `grace_ms`, the daemon
    escalates to **SIGKILL** (process group). The escalation lives in the daemon
    because only it owns the child and can observe its death; the client cannot.
  - `grace_ms = 0` → SIGHUP only, no escalation.
- Typical values: normal tab/window close → `{ "grace_ms": 0 }`; **restart**
  (replace the shell, keep the tab) → `{ "grace_ms": 1500 }`, after which the
  client mints a **new** `session_id` for the replacement shell so it never
  collides with the still-reaping old one.

### `DATA` (D→C)

Raw output bytes in the value (no JSON). Feed verbatim into the terminal widget
(VTE). The daemon has already stripped replay-unsafe sequences from **scrollback**
(see §7); live `DATA` is byte-exact shell output. A single logical burst may arrive
as several `DATA` frames — just feed them in order.

### `STATUS` (D→C)

```json
{ "running": true, "progress": null }
```

- `running` — bool. Whether a foreground command is active (drives the status dot).
  Derived from OSC 133 prompt markers when the shell emits them; otherwise from a
  daemon-side foreground-process check. The client never computes this.
- `progress` — integer 0–100, or `null`. OSC 9;4 progress percentage, or `null`
  when not in a progress state.

`STATUS` is pushed by the daemon (the client never polls). cwd is intentionally
**not** in `STATUS`: it rides the output stream as OSC 7 and the client's VTE
emits its directory-changed signal from the replayed/live bytes.

### `EXIT` (D→C)

```json
{ "status": 0 }
```

- `status` — integer. The shell child's exit status: `0–255` for a normal exit,
  or `128 + N` for death by signal `N`. After `EXIT` the shell is gone; the client
  should surface "child exited" (e.g. offer restart for command tabs).

### Detach

Closing the session socket **is** detach — no frame required. The session keeps
running on the daemon and its ring keeps filling; a later `ATTACH_OR_OPEN` with the
same `session_id` reattaches and replays.

---

## 6. Status semantics (`running`)

`running` reflects whether a foreground command is executing (dot on) vs. sitting
at the prompt (dot off):

- If the shell emits **OSC 133** prompt markers, `running` follows them
  (`133;C` → command started → `true`; `133;D` → command finished → `false`), and
  that source is authoritative for the rest of the session.
- For shells **without** OSC 133, the daemon falls back to polling the PTY's
  foreground process group while a client is attached; the first OSC 133 marker of
  any kind permanently disables that fallback for the session.

Either way the client just receives `STATUS` frames — there is no new wire message
and the client never polls.

---

## 7. Lifecycle flows

### Open (unknown `session_id`)

```
C → ATTACH_OR_OPEN{session_id=new, argv, cwd, want_chunks, cols, rows}
D    forkpty(shell), TERM/COLORTERM set, drain begins
D → DATA …            (live shell output as it is produced)
D → STATUS{…}         (on first/each status change)
```

There is no explicit "opened" ack; output flows as the shell produces it.

### Attach (known `session_id`) — replay then live

The daemon snapshots the ring, then sends, **in order**:

```
(winsize set + SIGWINCH to the shell group)
D → DATA  (synthesized state prologue: SGR/modes/scroll region/charset/OSC7/title)
D → DATA  (sanitized scrollback bytes, one or more frames, up to want_chunks)
D → STATUS{running, progress}
…then live:
D → DATA / STATUS / EXIT   (verbatim live output from the snapshot point onward)
```

There is **no in-band "end of replay" marker** — by design. Replay bytes are
already sanitized (no BEL, OSC 52, notifications, or input-generating queries), so
the client feeds every `DATA` frame into VTE identically; the boundary is enforced
by *what the bytes contain*, not by a signal. A bell only rings from a live frame.

### Detach / reattach

Close the socket to detach; the shell keeps running. Reattach with another
`ATTACH_OR_OPEN{same session_id}` to replay and resume.

### Takeover (second attach to a live session)

A second `ATTACH_OR_OPEN` for an already-attached session is **most-recent-wins**:
the daemon binds the new connection and **drops the previous one** (its socket sees
EOF). A client should treat an unexpected EOF on a session socket as "taken over /
detached," not as the shell dying (no `EXIT` is sent for a takeover).

### Shell exits while detached

The session enters a `dead` state and retains its ring. The next reattach replays
the final output and an `EXIT`, then the session is dropped:

```
C → ATTACH_OR_OPEN{session_id=dead}
D → DATA …        (final output replay)
D → STATUS{…}
D → EXIT{status}
(socket then closes)
```

### Close

```
C → CLOSE{grace_ms}
D    SIGHUP shell group; stop accepting attaches; reap in background
D    (if grace_ms>0 and still alive after grace_ms) SIGKILL group; reap
```

The closing client's socket is detached; no `EXIT` replay is guaranteed to the
caller of `CLOSE` (the tab is going away).

---

## 8. Error handling

| Situation | Client-observable behavior |
|---|---|
| `proto_version` mismatch | Control socket closes right after `HELLO` with no `HELLO_OK`. |
| Malformed frame (unknown tag, or `length` > 16 MiB) | The daemon closes that connection. Reattach to recover. |
| Slow/stuck client (not draining output) | The daemon drops the client (forced detach, socket EOF) rather than stalling the shell. Reattach and replay. The shell is unaffected. |
| Daemon crash / not running | All session sockets EOF; control connect fails. The client self-spawns the daemon (see below) and reconciles via `LIST`. |
| Resize while detached | Winsize holds the last value; send `RESIZE` on reattach to correct any TUI that queried a stale size. |

### Spawning the daemon

If the socket is absent or `connect()` gives `ECONNREFUSED` (stale socket), the
client `exec`s `jftermd` (a single static binary on `PATH`). The daemon
double-forks, resolves spawn races with an `flock` lockfile + atomic `bind()`, and
unlinks a stale socket before rebinding, so concurrent launches converge on one
daemon. The client then connects normally. The daemon self-exits a short grace
period after its last session ends.

---

## 9. JSON field reference

| Message | Field | Type | Notes |
|---|---|---|---|
| `HELLO` / `HELLO_OK` | `proto_version` | u32 | must be `1` |
| | `daemon_version` | string | daemon's version on `HELLO_OK` |
| `SESSIONS[]` | `session_id` | string | |
| | `argv` | string[] | |
| | `cwd` | string | initial cwd |
| | `running` | bool | |
| | `has_client` | bool | |
| | `created_at` | u64 | unix epoch seconds |
| `ATTACH_OR_OPEN` | `session_id` | string | |
| | `cwd` | string | open only |
| | `argv` | string[] | open only |
| | `want_chunks` | uint | `0` = full scrollback |
| | `cols` / `rows` | u16 | |
| `RESIZE` | `cols` / `rows` | u16 | |
| `CLOSE` | `grace_ms` | u32 | `0` = SIGHUP only |
| `STATUS` | `running` | bool | |
| | `progress` | u8 \| null | 0–100 or `null` |
| `EXIT` | `status` | i32 | `0–255`, or `128+signal` |

`INPUT` and `DATA` carry **raw bytes** (no JSON). `LIST` carries an **empty** value.

---

## 10. Implementation status vs. this document

This document describes protocol **v1** as the agreed contract. As of writing, the
daemon implements all of it except the following, which are in active development
and do not change the wire shape the client codes against:

- **`CLOSE.grace_ms` escalation** — the current daemon honors `CLOSE` (SIGHUP) but
  the `grace_ms`→SIGKILL escalation is landing now. A client may send
  `{ "grace_ms": N }` today; older daemon builds simply close with SIGHUP and
  ignore the field.
- **`running` fallback for non-OSC-133 shells** — the daemon-side
  foreground-process poll (§6) is landing now. Until then, `running` reflects only
  OSC 133 markers (shells without them report `running: false`). No wire change.

`proto_version` stays `1` across these; they are additive daemon behaviors, not
wire-format changes. The canonical encoding is
[`jftermd/src/protocol.rs`](../jftermd/src/protocol.rs).
