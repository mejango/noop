# Divergences

**History: PUT_SPIKE_PRICE_FLAT on the 2h window marked already-expanded skew, not cheapness (Apr-05, Aug-11, Oct-06). Type E (all windows confirm cheap puts) has never been confirmed in about 90 journal-reported checks. No divergence is confirmed now.** [2026-11-13]

*Metadata: changed=2026-11-13 | reviewed=2026-11-13*

## Active Divergences

> ⚠️ **Staleness notice:** live values come from ticks [tick:#210117]–[tick:#210122] (2026-11-13T19:00–19:05 UTC) and the surface computed at ~19:06Z. The divergence feed is not in the packet; the readings below are journal-reported and unverified.

**Put-price divergence: none confirmed.** Raw scores are not reported.

- **Type B (PUT_SPIKE_PRICE_FLAT): NOT confirmed.** The journal shows a 0.5h flag (put value about `+32.5%` against spot `+0.09%`) with 1h and 2h neutral. The 2h flag seen at ~11:02Z has cleared. The rule needs the 2h raw score above `50`. With the Jan-29 put wing at pctl 89 a spike would mark expensiveness anyway.
- **Type C (IV compression on rising spot): NOT observed.** Spot is lower over the session and put ask quotes are not in the packet, so the `>8%` ask-decline test cannot be run.
- **Type A (OI expansion with skew compression): NOT met.** Journal OI about `14,705` (`+8.7%` 24h, unverified) clears the OI leg, but the put wing widened `+2.4` over 24h instead of compressing.
- **Type D (OI crash with spot rally): NOT met.** OI is rising and spot is lower over the session.
- **Type E (PUT_CHEAP_PRICE_STABLE): NOT met.** Jan-29 put ATM pctl 55, 10Δ pctl 75, wing pctl 89 (smile 7d/30d `95/99`). Put VRP `+0.96` is in the bottom decile of its 7d and 30d ranges (smile `7/2`) and ATM−RV30d is `+1.0`: level insurance is priced near realized vol, while the tail wing is rich but not at a recorded maximum (all-history pctl 89). See protection/pricing.md.
- **Cross-surface (not a feed signal):** the Nov-20 call gap `−10.2` (pctl 11) is a realized-vol effect with the call wing at pctl 5; see revenue/pricing.md.
- **Anchor:** spot `$1,939.70–$1,942.60`, latest `$1,942.60` ([tick:#210122]); put_score `0.0040–0.0041`, best_put Jan-29 1000-P; best_call Nov-20 2100-C/2125-C, call_score `68.33–73.70`, DTE `6.5`. [2026-11-13]

## Historical Divergence Episodes

Evidence tier: analyst journal notes unless a row cites a tick or an order. The tracker compares put-value change with spot change over 0.5h, 1h and 2h windows. Reviewed outcomes exist only for hypotheses that cited a divergence signal, and no divergence signal has a realized-P&L test. The three single-date rows are the episodes the page records as Type B (2h PUT_SPIKE_PRICE_FLAT).

| Date | Type | Magnitude | Duration | Resolution |
|------|------|-----------|----------|------------|
| 2026-02-27 to 03-05 | Put-spike clusters; PUT_CHEAP_PRICE_STABLE | +14% to +198.6%; 03-01 +166.9–200.1% was a null-to-live artifact | Drops within 11–21 ticks | Spot -0.54–0.64% after spikes, $1925→$1861; 03-01 rally to $2189; tracker then NEUTRAL about 100 hours (03-02 to 03-05) through a +11% rally |
| 2026-03-06 | Five spike-then-drop flags | +199.6% to +504.7% | 7–24 ticks | -0.53–1.57%, during, not before, the cascade to $1958.37 |
| 2026-03-07 to 03-31 | All-NEUTRAL 03-07 to 03-12; five spikes 03-13 to 03-15; clusters 03-19 to 03-31 | Best-put score frozen at 0.0021527, then 0.00246 | Not recorded | No signal before $1917.5, the 03-09 rally or the break to $2144; 03-15 reversal call wrong (rally to $2378.8); cluster drops 0.50–2.21% |
| 2026-04-01 to 04-29 (04-05 listed below) | Spikes 04-03, 04-19 (three) and 04-29 (2h); otherwise NEUTRAL | 04-03 +503%; 04-29 2h +14.09% | Not recorded | 04-03 -0.74%; 04-29 reviewed $2333→$2263 (-3%), credited, nothing bought; no flag before the 04-11 breakout or the high at $2463.4 |
| 2026-04-05T09:07 | PUT_SPIKE_PRICE_FLAT | +65.98 divergence (notes: 2h +66.06% on 04-05/06) | ~2h signal | OI surged +55.7%; fear-loading confirmed; notes record spot then rallying $2039→$2262 |
| 2026-05-01 to 06-12 | Spike flags; one PRICE_DROPPING flag (06-05); zero valid puts | +5.7% to +21.2%; 05-20 +65.7%; 06-05 +420% | Not recorded | 05-04 rise to $2378; slides to $2117, $2025, $1881, $1759 and $1511 were credited to flags, but no put was bought; later flags preceded rises, ranges or ~1% dips |
| 2026-06-21 to 07-21 | About 45 spike clusters | +10% to +112%; 07-16 +99.2% and +100.1% | Drops within 3–24 ticks | Drops -0.5% to -1.72%; the 06-29, 07-03, 07-08, 07-11, 07-15 and 07-18 flags preceded rises |
| 2026-07-22 to 08-15 (08-11 listed below) | Spikes; 07-26 one-tick artifact | +12–18% | Not recorded | Drops about -0.5%; 07-26 was followed by a rally; no flag before $1,973.9→$1,860, $1,822.9 or the skew floors |
| 2026-08-11 | PUT_SPIKE_PRICE_FLAT | put value +5.69%, price –0.27%, 2h window | ~2h signal | Type B framework; escalation CLOSED; notes attribute the flag to an instrument switch |
| 2026-08-16 to 08-28 | 08-16 2h flag; 08-19 0.5h flag; clusters 08-20 to 08-22 (~17); 08-28 five spikes | 08-16 +5.0 (price +0.10%); 08-19 +16.74%; 08-28 +17.4% to +23.5% | Not recorded | 08-16 falsified; 08-19 flag arrived inside the August breakout; cluster drops -0.51% to -1.30%, then new highs; 08-28 drops -0.81% to -1.11%, spot -2.8% |
| 2026-08-29 to 09-15 | PUT_CHEAP_PRICE_STABLE 08-29 and 09-15 (at a rotation); flags 08-31 (2h) and 09-11 | 08-31 +6.07%; 09-11 +13.1%; 09-15 -23.35% | Not recorded | 08-29 preceded a +$34 bounce; 08-31 a rise; no flag before the 09-04 rejection or the -4.7% leg to $2,368 (put IV 63.9%→74.5%); 09-15 was a rotation artifact |
| 2026-09-16 to 09-24 | Spike clusters; flags 09-17 (2h) and 09-18 (1h) | Clusters +10.5–22.9%; 09-17 +8.49%; 09-18 +7.95% | Not recorded | Drops -0.52% to -1.17%, then recovery; 09-17 ETH rose; 09-18 flag falsified by the rally to $2,616; no flag in the $2,437→$2,801 rally |
| 2026-09-25 to 10-05 | 09-25 1h flag; NEUTRAL through the 09-28 break and 09-29 probe; 10-01 spike | 09-25 +12.43%; 10-01 +14.2% | Not recorded | 09-25 graded noise; 10-01 -0.59% |
| 2026-10-06T22:41–23:06 | 2h PUT_SPIKE_PRICE_FLAT; others NEUTRAL | 2h: put value +73.18%, spot +0.09% | ~8h; resolved NEUTRAL | Artifact (fired at a rotation); two 1900-P fills [order:#4008], [order:#4014]; notes record a ~$80 flush and about -4.7% afterwards |
| 2026-10-07 to 11-04 | NEUTRAL on nearly all windows; sub-threshold flags 10-18 (1h), 10-20 (2h) and 10-31 (1h) | 10-18 +9.5%; 10-20 +5.4% vs +0.3% spot; 10-31 +8.2% (2h -4.8%); raw score never above 50 | Each resolved | Spot broke $2,604 (10-07), $2,408.2 (10-08), $2,323.2 (10-15), $2,286.6 (10-24) and $2,202 (10-30) with the tracker NEUTRAL; put value repriced with, not ahead of, spot |
| 2026-11-05 to 11-13 | NEUTRAL through the 11-05, 11-09, 11-10 and 11-12 falls; one-window flags 11-07 (0.5h) and 11-13 (0.5h, 2h, 0.5h; neighbors neutral each time) | 11-13: +199% vs spot +0.4%; +55.6% vs flat; +32.5% vs +0.09%; 2h raw score not in packet | Flags did not persist across windows | Journal-reported, unverified; put surface repriced with the falls, wing pctl `82→86→89` on 11-13; ticks [tick:#209219]–[tick:#209224], [tick:#209770]–[tick:#209775], [tick:#210113]–[tick:#210118] |

Compiled from the episodes (02-27 to 11-13):

- **Sample.** Roughly 250 logged spike episodes (02-27 to 10-31; cluster count on indicators/leading.md) and about 95 PUT_SPIKE_PRICE_FLAT or PRICE_DROPPING firings, plus PUT_CHEAP_PRICE_STABLE firings (n≈40). From 10-07 no cluster formed.
- **Late-year checks.** About 90 journal-reported checks from 10-07 to 11-13 added only sub-threshold flags: the raw score was never above 50 and Type E was never confirmed. Ticks: [tick:#187120]–[tick:#187125], [tick:#193046]–[tick:#193051], [tick:#202950]–[tick:#202955], [tick:#208945]–[tick:#208950].
- **Spike-then-drop is small, late and uncalibrated.** Most clusters through 10-01 were followed by a drop within ~6–24 ticks, mostly under 1.5% and short-lived, often simultaneous with the move. Spike size did not scale the drop: +99% and +100% gave -1.72% and -1.20%; +14.2% gave -0.59%. Put-spike clusters did not time the larger falls; see indicators/leading.md for the tally.
- **A silent tracker does not mean a quiet market.** The tracker read NEUTRAL for ~100 hours through a +11% rally (03-02 to 03-05), through $1942.7, the highs to $2463.4, the 06-02 breakdown, a +12% rebound and the $1,973.9→$1,851 slide. From 09-01 to 11-13 it raised no flag ahead of the 09-15 leg, the run to $2,801, or any break from 10-07 to 11-12, or the rebounds between them.
- **What the tracker missed in those stretches (09-01 to 11-13).** Skew swung -8.4% to +21.8%, OI moved +114.8%, -69% and -29%, and surface percentiles flipped extremes within hours. Spot levels and regime arcs are on regimes/history.md.
- **PUT_CHEAP_PRICE_STABLE has shown no predictive value (n≈40, mixed).** It fired before rallies (after 03-01; 03-14/15 breakouts; 07-03 to 07-18 rises; 08-19 inside the August breakout; 08-29 before a +$34 bounce), at the 09-15 rotation artifact, and in lulls that ended in a break (09-26 to 10-06, flat ~10 days; 10-09 to 10-11, flat ~40h, then $2,323).
- **Mechanical flags.** Percentage flags are unreliable when the best-put score is null, frozen or rebased, or when the instrument migrates (the June rotation and the Aug-28, Sep-25, Oct-30, Nov-27, Dec-25 and Jan-29 rotations). One-tick asks, -100% readings, +166–200% (03-01), +497% and +1289% (03-15), the 08-11 switch flag, OI resets, and put avg_spread of 773%–939% are mechanical.
- **Single-put chains.** A chain with one quote-eligible put makes put-value flags noise. On 11-13 only the 1000-P was eligible; put_score read 0.0000 on two ticks and 0.0041 on four with the same instrument named. Check the absolute ask and a repeat quote first.
- **Algorithm limitation.** The tracker uses put_dv change, not absolute ask. Asks moved with delta, spot and momentum under all-NEUTRAL readings: the Dec-25 1800-P rose to ~$39 on the 10-24 slide, the 1600-P ~$16–18→$26–27 at the 10-30 break, and the 1500-P ~$32→~$38 on 11-12 as the wing re-bid. Skew, OI and percentile moves were not captured.
- **Percentile moves are not divergences.** Put wing and ATM percentiles also moved with expiry-zone rolls (Nov-27→Dec-25 on 10-13, Oct-30→Nov-06 about 10-25, Dec-25→Jan-29 from about 11-10) and with the rolling RV7d window. How the put surface repriced around falls is on protection/convexity.md.
- **Narratives built on divergences.** "Institutional accumulation", "coiled spring", "record OI forces repricing", "bifurcation inversion normalizes within 24–48h" and similar theses (03-27 to 11-13) were mostly unmet or wrong on timing; falls lifted the put wing contemporaneously, never ahead. The forecast record is on indicators/leading.md.
- **Trade outcomes** for the campaigns that ran alongside these flags are on protection/convexity.md (puts) and revenue/efficiency.md (short calls).

## Divergence Playbook

No type has a realized-P&L test. The type definitions are analyst hypotheses; the actions below are how to treat each flag, not proven edges.

**Type A: OI-expansion / skew-compression**
- Signal: OI growing `>3%` 24h while skew compresses `>1.5 points` simultaneously
- Action: Proposed as an accumulation signal that resolves via skew re-expansion within `24–72 hours`. Not confirmed: only the 04-05 episode (OI +55.7%) supports it, and OI expansion failed to suppress or lead put repricing in later sessions (09-26 to 11-13).
- Sizing: Unproven. One supporting episode does not justify treating it as a high-conviction signal; do not size up on Type A alone.
- Stop: Exit thesis if spot breaks above prior session high with OI contracting
- **Status [2026-11-13]:** not met (OI leg only).

**Type B: PUT_SPIKE_PRICE_FLAT**
- Signal: 2h divergence raw score `>50` with spot move `<0.5%`
- Action: [lesson:process.decision_quality] Do NOT accumulate; put skew has ALREADY re-expanded at signal time.
- Key lesson: the signal marks expensiveness, not cheapness.
- **Threshold clarification:** the `>50` threshold applies to the 2h window only. A 1h-only or 0.5h-only spike does not meet it; such flags (09-25, 10-18, 10-31, 11-13) were graded noise.
- **Interaction with Type E:** Type B takes precedence if the 2h raw score is above `50`.
- Record: observed on 04-05, 08-11 (instrument switch) and 10-06 (rotation artifact).
- **Status [2026-11-13]:** not confirmed; 0.5h flag only, journal-reported.

**Type C: IV compression on rising spot**
- Signal: Put ask declining `>8%` while spot rises `>0.5%` in the same session
- Action: Treat as potential best convexity entry; deploy at `50%` of normal size pending confirmation
- Note: the Apr 7 episode is the only documented instance; use with caution.
- **Status [2026-11-13]:** not met; put ask quotes are not in the packet.

**Type D: OI-crash / spot-rally**
- Signal: OI contracts `>20%` while spot rallies `>5%` in the same session
- Action: Wait `8–24 hours` for vol suppression to stabilize; then evaluate ask vs. prior floor
- Record: not observed in a tested form.
- **Status [2026-11-13]:** not met.

**Type E: PUT_CHEAP_PRICE_STABLE**
- Signal: All three divergence windows (0.5h, 1h, 2h) confirm put cheapness: no spike, no compression race
- Action: [lesson:process.decision_quality] Optimal accumulation when gate requirements are met (put wing pctl at or near floor, put_score at or below cycle norms, budget available). In bifurcation inversion (ATM expensive, wing cheap), target deep OTM instruments scoring high on put_score rather than ATM-adjacent strikes.
- **Dependency check:** confirm the 2h divergence raw score is not simultaneously showing Type B (`>50`).
- Record: never confirmed in about 90 checks. PUT_CHEAP_PRICE_STABLE firings did not predict a break (n≈40, mixed), and a chain with a single eligible put makes the flag noise.
- **Status [2026-11-13]:** not met; the wing is at all-history pctl 89 and the divergence feed is journal-only.

**General readings from the record** (analyst notes and reviewed hypotheses, 02-27 to 11-13):

- **A put-value spike is context, not a trigger.** It is clustered, often simultaneous with the move, uncalibrated to size, and short-lived inside uptrends or ranges. Many flags preceded rises, ranges or ~1% dips.
- **One-window flags are noise.** A flag on one window while neighboring windows read NEUTRAL (09-25 and 10-18, 10-31: 1h only; 10-06, 10-20: 2h only; 11-13: 0.5h or 2h only, three times) was graded noise or a rotation artifact.
- **Sub-threshold divergences** (div 3.7–4.83) preceded a ~2.5% slide once and ranges or rises otherwise; treat as description.
- **Mechanical readings.** Flags from null scores, instrument migration, frozen or rebased bests, empty books, one-tick asks, stale quotes, single-eligible-put chains or OI resets are mechanical. A one-tick ask jump of $10–$20 under flat NEUTRAL divergences has self-corrected every time (~7 of 7).
- **No signal is not safety.** Long NEUTRAL stretches coexisted with ±3–20% spot swings and skew swings from -9.6% to 55%. The -4.7% leg after the 09-11 to 09-14 skew floor, the 09-18 to 09-21 rally and every break from 10-07 to 11-13 came with no divergence flag.
- **Percentile swings are description.** The put wing went pctl 5→95 within ~16h on a $70 fall and back to pctl 4 within a day; such swings track spot, expiry-zone rolls and the RV7d window. Both "inversion resolves" and "inversion persists" calls failed on timing.
- **Reversal calls built on a flag near a ceiling were reviewed wrong** when the ceiling broke (03-15/16, 08-21).
- **Buying protection on a flag was graded chasing.** A flag during a breakdown with skew above ~18% argued for not chasing (06-24 gate reviewed correct); a flag after the ask had repriced was graded chasing (07-08).
- **Waiting after a repricing was graded correct** (09-15, 10-29 to 10-31, 11-05 to 11-13) over buying into elevated put IV. Protection bought at $28 on 10-23 cost ~$39 a day later; the 1600-P rose ~$16–18→$26–27 into the 10-30 break and gave back ~$4 within ~16h.
- **Narratives are description, not signal.** Divergence, convergence, "false security", "coiled spring", bifurcation-resolution and efficiency-threshold narratives produced no reliable skew, efficiency or direction call.
