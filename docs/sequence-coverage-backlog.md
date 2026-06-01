# Sequence-coverage backlog

Deferred escape/control-sequence handling refinements for the `jftermd-core`
replay engine, surfaced by the final code review of the core-engine branch
(2026-06-01). None are data-loss bugs (the 8-bit C1 `ST` byte-loss bug was
fixed before merge); these are **classification and prologue-fidelity gaps** to
work through in a dedicated sequence-coverage pass.

Two failure modes to keep in mind while triaging:
- **Replay re-fires a side effect / injects input.** A query sequence kept in
  the ring will, on replay into VTE, make VTE generate a response that is routed
  back up the input stream → spurious bytes to the shell. These must be
  classified as drop-actions.
- **Capped/cut replay renders wrong.** A visual mode that affects rendering but
  is not re-asserted in the per-chunk prologue (`StickyState::serialize`) will
  render incorrectly when replay starts at a chunk boundary rather than from the
  very beginning. The full-replay oracle does not catch these because its test
  inputs are small (single chunk) — add **capped-replay** oracle cases that cut
  mid-state.

## Group A — queries that must be DROPPED (input-injection risk)

Currently kept verbatim (or passed through DCS), so replay would re-fire them.

| Sequence | Form | Risk | Fix |
|---|---|---|---|
| DECRQM | `\x1b[?<n>$p` | Medium | classify CSI with `$` intermediate + `p` as drop |
| XTVERSION | `\x1b[>q` | Medium | drop CSI `>`-private `q` |
| Other DSR variants | `\x1b[?6n`, `\x1b[?<n>n` | Medium | the `n` arm already drops; confirm private `?...n` also drops |
| DCS queries | XTGETTCAP `\x1bP+q…`, DECRQSS `\x1bP$q…` | Medium | DCS is currently always Keep (`hook`/`put`/`unhook`); classify request DCS as drop |
| OSC color queries | OSC `4;<i>;?`, `10;?`, `11;?`, `12;?` | Medium | drop OSC color sequences whose payload is `?` |

## Group B — modes re-asserted in the prologue that maybe should NOT be

These are currently sticky-tracked and re-emitted; decide per-mode whether
re-asserting on reattach is desired or spurious.

| Mode | Code | Consideration |
|---|---|---|
| Mouse reporting | `?1000/1002/1003/1006` | Re-enabling on reattach is arguably correct for a live TUI, but re-asserting blindly may produce spurious client input. Decide. |
| Focus reporting | `?1004` | Same as mouse. |
| Alt screen | `?1049` (`?47`/`?1047`) | A cut landing mid-alt-screen makes a *capped* replay enter the alt screen without reconstructing its drawn content. Options: (a) don't sticky-track alt-screen; (b) treat alt-screen enter as a purge-like boundary; (c) stop buffering alt-screen churn and rely on SIGWINCH repaint (per the spec's alt-screen note). |

### Design note: the state-prologue model has limits for side-effecting modes

The per-chunk state prologue (an initial-state snapshot re-asserted at the start
of a replayed chunk) is correct for **inert, value-only properties** — SGR color,
charset, scroll region, cwd, title. Re-asserting the latest value is harmless
because the property has no side effect beyond "this is the current value," and
in-data transitions carry it forward.

It is **not** sufficient for **modes with side effects / client behavior** —
mouse reporting, focus reporting, bracketed paste. For these the property isn't a
passive value: having it enabled changes what the client sends back, so a
transient enable during replay actually matters.

Two distinct cases, and neither a plain prologue nor a prologue-diff fully solves:

- **Stale prologue.** A chunk's frozen prologue captures a mode as ON, but it was
  turned OFF later. A diff of the prologue snapshot against the chunk's end (or
  the live state for the open chunk) can drop the stale entry. This is the
  cheap, prologue-only fix.
- **In-buffer span (the hard case).** A mode is enabled and disabled *within the
  buffered byte stream* — e.g. mouse reporting on from offset 1000 to 8000 inside
  a chunk's `data`. Those toggles live in the data as verbatim bytes; a
  prologue-level diff cannot remove them, so replaying the data still re-creates a
  transient enable window. Properly avoiding this needs **design**, not a tweak.

Likely direction (to be designed in this pass): split sticky state into
**positional/rendering** state (journey matters → keep inline in data + frozen
prologue, as today) vs **input-behavior** modes (non-positional, only the current
value matters → drop their toggles from the ring data entirely, track the current
value, and emit it once at replay time from the live state). The second bucket
sidesteps both cases above but is more invasive than the prologue, so it warrants
explicit design before implementation.

## Group C — visual modes NOT tracked in the prologue (capped replay renders wrong)

Kept inline in the data stream but not re-asserted by `StickyState::serialize`,
so a capped replay starting after they were set renders incorrectly.

| Mode | Sequence | Risk |
|---|---|---|
| Autowrap (DECAWM) | `\x1b[?7h/l` | Medium |
| Origin mode (DECOM) | `\x1b[?6h/l` | Low–Medium |
| Cursor visibility | `\x1b[?25h/l` | Low |
| G0/G1 charset designation | `\x1b(0`, `\x1b(B`, etc. | Medium (line-drawing renders as letters) |
| Color palette set | OSC `4;<i>;<spec>`, `10`, `11`, `12` set forms | Medium (wrong colors) |
| OSC 8 hyperlinks | `\x1b]8;;<uri>\x07` | Low (link state lost across a cut; harmless visually) |

## Group D — other 8-bit C1 controls

The fixed bug handled only the 8-bit `ST` (`0x9c`). vte 0.15 is 7-bit-only, so
other 8-bit C1 controls are also unrecognized: `0x9b` (CSI), `0x90` (DCS),
`0x9d` (OSC), `0x9e`/`0x9f` (PM/APC), etc. Decide whether to translate the full
C1 range to their 7-bit `ESC <Fe>` forms in `Scanner::feed` (the same technique
used for `0x9c`) or to document that the daemon assumes 7-bit/UTF-8 input.
Risk: Low (modern terminals emit 7-bit forms in UTF-8 streams).

## Suggested approach for the pass

1. Add **capped-replay oracle cases** (cut mid-state via a tiny watermark) for
   each Group C mode — these will fail today and define the work.
2. Extend the scanner's classification table for Group A (drop) and Group D
   (translate), with a regression test per sequence.
3. Make the Group B decisions explicitly (they are policy, not bugs) and encode
   the chosen behavior with tests.
