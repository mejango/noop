# Leading Indicators

**History (2026-02-26 to 11-14): no confirmed leading indicator. Flow, skew, OI, momentum, put-spike, vol-surface and efficiency signals worked sometimes and failed repeatedly. Falls re-bid the put tail before ATM; RV7d, not call IV, drives most call-gap moves.** [2026-11-14]

Current readings are on regimes/current.md, protection/pricing.md and revenue/pricing.md. Unless a tick or an order is cited, every record below comes from analyst journal notes and hypothesis reviews, the lowest evidence tier: counts are approximate, as the notes recorded them, and none is verified.

## Confirmed Leading Indicators

None. No lead-lag relationship has been validated across independent periods, and no hypothesis-style rule has a consistent reviewed record (2026-02-26 to 11-13). Numbered entries (1 to 34, E and F series) are filed below under the verdict their record supports. Entries 1 and 8 describe what happens during a move; neither gives advance notice.

## Experimental Indicators

All entries are unconfirmed: mixed record or still open. None sets a trading rule.

### Open hypothesis

**E67. Skew stays elevated and the back stays anchored over 24h from about 2026-11-14T03:02Z: OPEN [2026-11-14].** Journal hypothesis, unconfirmed.
- Bands: put-zone 10Δ above `63.0`, wing above `+10.5`, Nov-20 ATM − RV7d between `−13` and `−2`, back anchored (no numeric band).
- Start: 10Δ `67.3`, wing `+13.9`, gap `−6.9`, 30d ATM `52.7`, 90d ATM `53.2`.
- Falsified at about 2026-11-15T03:00Z if any leg breaks. Bands sit near the starting prints, so a hold is weak evidence; an RV7d rollback like the Nov-12/13 one (gap `−1.2`) would breach the `−2` ceiling.

### Described regularities (coincident, not leads)

**1. Falls re-bid the put tail before ATM (with F11, spot decline and put skew widening: horizon- and leg-dependent).** [2026-06-15; updated 2026-11-14]
- Oct-24: spot `−7.5%` ([tick:#190990] to [tick:#194665]) came with Dec-25 10Δ `+4.5` and ATM `+2.1` (arithmetic).
- 11-12 to 11-14, Jan-29 zone: five consecutive lower-spot legs each re-bid the wing (`+0.6` to `+1.9`) while ATM stayed within about `0.7` in total and the 42d+ curve stayed at 52–54. One expiry, small sample.
- Limits: a `−1.9%` leg on a pin (11-11 to 11-12) came with wing `−0.3`, and the notes record falls the surface ignored (11-09, 11-10). The link holds on large declines and new-low legs, not on small legs.
- The repricing follows the fall, so it is no warning. Record: protection/convexity.md.

**8, 13, 32. RV7d, not call IV, moves the front call gap (ATM − RV7d).** [2026-09-28 to 2026-11-14]
- Compression: the gap percentile read 98–99 while RV7d compressed (from 09-28), then 32 and 27 when RV7d jumped 32→44 (10-08), and 0→68 within ~8h on 10-15.
- November: RV7d 51→67 after the 11-08/09 break took the gap to `−6.4` to `−14.6` (pctl 0–16). An RV7d fall of `61.8→52.4` lifted it to pctl 32–35, and a rise back to `60.0` (RV30d `52.5`) returned it to pctl 11–16. The 11-14 gain to pctl 16 came from call ATM on the dip, not RV7d.
- As a forecast it failed. "RV7d compression expands the spread durably" failed 2026-10-08: the improvement was mechanical and reversed mechanically. "A single-session RV7d surge drives the gap to the cycle floor" failed as stated 2026-10-13, then was supported on most later spikes (count on indicators/correlations.md). Small samples; not validated.

**16. Spot decline with call IV surge, recovery with compression: SUPPORTED ON LARGE DECLINES, MIXED ON SMALL LEGS; descriptive only.** [2026-10-16; updated 2026-11-14]
- Record: four large legs supportive (for example Oct-30, spot `−6.2%`, call ATM `+16.6`; the 11-05 drop, `+6.7`), about eight small legs mixed. Nov-04 (spot `+2.6%`, call ATM lower) matched on the recovery side.
- Against: from 18:46Z Nov-08 spot fell `14.5%` (`$2,232.40→$1,909.20`; [tick:#205243]→[tick:#210589]) while Nov-20 call-zone ATM went `54.1→53.1` (`−1.0`), flat-to-opposite, though the last leg (spot `−1.6%`, call ATM `+3.8`) matched in sign.
- The front-call jumps on the 10-22, 10-24 and 10-30 falls unwound within ~8–16h (three of three), so a surge is not a lasting premium window.
- **Falsification:** a spot recovery above 3% in an RV7d-elevated regime not compressing call ATM within the session; not yet tested.

**30. Call harvest gate near-miss reverses; recoveries can re-close.** [2026-10-16; updated 2026-11-13]
- On the Nov-20 chain the all-history ATM − RV7d percentile spent most readings at 8–16. The single pctl 53 reading was followed within eight hours by pctl 16, and the pctl 32–35 lift was RV7d-led and re-closed.
- A single gate-open reading is not a window. Gate episodes: revenue/windows.md.
- **Falsification:** holding above pctl 50 for two consecutive sessions weakens the pattern; not met.

### Mixed-record signals (worked sometimes, failed repeatedly)

**Open-interest steps as a reversal, continuation or skew signal (03-13 to 11-13).**
- Large steps recurred around expiries and rolls: for example `−76%/−81%` (03-27), `+146%` to `+194%` (03-29/30), `−82%` (05-25/26), `+640%` in August (cause unestablished), `−69%` (10-12).
- Builds preceded a fall twice in November (a `+27%` rebuild into the 11-05 high; `+13%` on a flat tape before the 11-09 break), but the `+114.8%` build of 10-04 preceded a 3-day range.
- April's record build (`+24%`) coincided with skew `16.61%`; both unwound within ~24h: contemporaneous, not leading.
- The 10-18/20 and 10-26 feeds were measurement breaks. Aggregates are not comparable across resets or rolls.

**"OI up while skew compresses, then re-expansion in 24–72h".** Fit once (04-05). Failed 04-16: OI `−7.3%` with skew `5.69%` saw skew reach only `7.8%`, then `6.1–6.7%` for 16h+; the 10% call was falsified 04-17. [2026-04-21]

**OI contraction, then skew compression.** Supported 03-23 to 03-26; about 19 hits and 9 misses through 09-15; the 09-21 and 09-28/29 contractions came with skew `−6.7%` or rising. The 11-09 `−29%` flush coincided with skew easing `21.75%→12.4%` (one fit), as much roll clearance as signal. Mixed. [2026-11-13]

**Onchain flow (03-05 to 11-13).**
- Weekly flow as a veto or floor: inflow was a floor through 03-17 (`+7.42%`), then eroded as ETH fell `11.6%`. Mid-April inflow was called a structural floor while spot slid `$2463→$2296`; it "held" only because no deep break followed.
- Outflow arrived with the moves, not before: June outflow `−2.2%→−11.7%` with the drop, 09-15 outflow (conf 0.95) with the `−4.7%` leg. July inflow persisted through rallies and slides alike; October and November outflow labels reverted within ~2h.
- Daily outflow compression as a bounce signal: six hits, one failure (03-05 to 06-18), including the `$1683` low on 06-18.
- Tri-timeframe outflow co-fire: coincided with five March moves and the 09-15 leg. Lead unverified.
- Outflow for ~4 days (08-05 to 08-12) preceded no drop and no skew rise. Verdict: coincident at best.

**Medium-momentum labels (March to November).**
- Deceleration as a reversal signal: about 8 right and 8 wrong in March–April. "Deceleration at a high, then pullback": ~14 of 19 over the year, the best momentum record, still unreviewed as a rule.
- Fresh transitions: notes claimed "100% vs ~40% in consolidation"; reviews never tested it. The "upward/accelerating" flip scored 4/4 in the 03-05 to 03-09 notes, but reviewed outcomes were wrong three times, and later flips mostly reverted within ~2–16h (04-17 in ~7h; the 08-19 flip arrived after the move).
- Three September labels preceded moves (09-02, 09-04, 09-10). August labels cycled within 6–16h; October and November downward labels coincided with slides and lows and flipped neutral within hours.
- "Neutral reset precedes a lower low" held through 03-03, then rallies followed. Verdict: labels lag price.

**Put spike-then-drop clusters (late June to 11-13).**
- Record: the notes count ~100 of ~136 clusters followed by a dip, nearly all under ~1.5%, which is noise-sized. Late-June (~15) and July (~45) clusters each preceded `−0.5%` to `−1.7%` within ~6–24 ticks.
- Misses in both directions. Flags on 07-03, 07-08, 07-15, 07-18, 07-26, 08-16, 08-19, 08-31 and 09-16 to 09-24 were followed by rallies or nothing (~17 August flags sat inside a rising trend). No flag preceded the 09-15 leg, the 09-21 run, the October falls (10-12/15, 10-22/24, 10-30), the 11-05 drop or the 11-09 break.
- Size did not calibrate: magnitude mapped to drop size only on 07-16 (`+99%/+100%`); `+96–103%` (08-21) gave `−1.0%` to `−1.3%` then a new high.
- Verdict: a cluster marks small noise-sized dips, not falls.

**5. PUT_SPIKE_PRICE_FLAT flags: scoring artifacts and noise.** The `+166–200%` spike of 03-01 and the 2h flag of 2026-10-06 were scoring artifacts (the latter still preceded a ~$80 flush). The journal reports a 2h flag at the 11-14 low (put value `+118%`, spot `+0.3%`) with 0.5h/1h neutral, attributed to zero-ask ticks on a thin chain; unverified. [2026-11-14]

**Cheap put skew as a "cheap protection" or direction signal (late March to 11-04).**
- Late-March compressions were followed by 4–8% drops in 5 of 5 (notes; not a timing rule), but reversed within 8–16h.
- April floors failed as timing: four skew floors of `3.1%` to `9.7%` (04-04 to 04-25) sat in the "aggressive accumulation band" while spot ranged or rallied to `$2,463`; only 04-18/19 saw a ~`−2.5%` slide.
- 06-10 to 06-13 skew fell `29.85%→7.86%` ahead of a rally. September 1–15: of ~18 sub-3% readings, ~8 preceded 1.5%–5% pullbacks.
- 10-01 to 10-04 skew `5.41%→0.43%` preceded a ~`−12%` slide with days of lag; 11-02 to 11-04 a wing of `+8.3` to `+8.9` preceded a rally first, then the 11-05 drop repriced puts (10Δ `+4.4`).
- Verdict: reviews credited identifying cheapness; direction and timing were unstable, and capital deployed was zero or tiny. Windows are on protection/windows.md.

**Skew floor, then re-expansion within ~48h.** Held 12 of 12 through 07-30, then broke from 08-05 (notes disagree on the dates). Forecasts built on its timing failed throughout (see the tally). Fastest noted move: `−6.72` pts in ~8h (to `7.36%` by 04-21). [2026-09-30]

**Skew regime bands as accumulation gates** (9%, 13%, 16–18% and several named floors; applied 04-11 to 04-26 and 06-16 to 10-04). Refusals at 11–34% skew cost nothing, but "gate within 2–3 sessions" calls failed on timing, and April's "three-cycle fear-peak compression" calls were right in direction and missed the 13%/24h timing. Bands describe, not predict. [2026-10-04]

**6, E3. Skew normalization from inversion as a put-IV reset or bid-gap predictor.** Not a stable lead: the tail that eased from the Oct-30 peak re-firmed past it by 11-14 (wing `+13.9` against `+12.4`, different expiries). Observation only. [2026-11-14]

**Put-efficiency ("% of historical best") as an accumulation timer (04-16 to 11-13).** Readings of 59%–99.6% were narrated as "peak", "floor" or "last window" while the reference was rebased and instruments rotated. The score held `0.0027–0.0054` across skew `−9.6%` to `10.75%` and spot `$1,823–$2,801`, so it timed nothing. [2026-11-13]

**11. Post-expiry OI collapse does not lift put_score.** put_score read `0.0000` while the put zone was unlisted and `0.0037–0.0046` once Jan-29 listed; on [tick:#210113]–[tick:#210118] it flipped between the two with the same best put named. A flat scan output, not a signal. [2026-10-12; updated 2026-11-14]

**Call-efficiency (best_call_dv % of reference), with 7 and E6 (04-16 to 11-13).** Scores tracked spot delta, then call IV in the breakout, then expiry theta. Chain rolls lifted the score `+20` to `+35` points with spot flat (the notes counted 11 of 11 through 10-04; 7, expiry-pool entry as a reset catalyst, confirmed 2026-10-18), a mechanical reset. No standalone content. [2026-11-13]

**Put-surface percentile pockets (09-28 to 11-13).** Falls followed cheap pockets (pctl ≤20) within 0–5 days in four episodes, a rally in one, a flat range in one. Percentiles are basis-sensitive (a 7d pctl 99 was all-history pctl 43): name the basis. Unconfirmed. [2026-11-13]

**Exhaustion score and put-candidate count.** Exhaustion activated once (`0.4`, 02-26); the predicted escalation to 0.6–0.8 never occurred. Zero-best stretches (05-13 to 05-15, 05-19 to 05-21, 06-03/04, 06-17 to 06-22) coincided with a slide, ranges and a low alike; later ones were artifacts or universe coverage. [2026-08-15]

**E4. Post-expiry call IV compression as a mean-reversion indicator.** The deflation recurred on Nov-20, then reversed on the 11-14 dip (call ATM `49.3→53.1`). One episode; not validated. [2026-11-14]

**E7–E66: resolved, falsified, superseded or unsupported (condensed).** E7–E31 were falsified, unsupported or held without establishing a lead. Many later ones were superseded before their 24h deadline with no verdict; E35 completed (weak hold); E43, E49, E50, E51, E53 and E64 broke on one leg; E44 reversed within about 16 hours; E52 held at 24h; E55–E66 each held on every surviving leg at ~8–16h (E66 on all bands at ~8h) and were superseded by the next band, which was centered on current prints, so only a break was informative. [2026-11-14]

## Failed Indicators

### Failed-forecast tally (home of this count)

The compiled notes put the total at about 600 forecasts that failed review between 2026-02-26 and 11-13. They do not say what it sums. The family counts below add to more than 600, so families overlap or the total is narrower: unresolved, not additive.

| Forecast family | Period | Reviewed sample | Outcome |
|---|---|---|---|
| Efficiency-threshold breach, recovery or persistence (put or call efficiency, percentile gates, 2–48h) | 05-04 to 11-13 | ~490 | All unmet except one directional hit (05-06), a few sale-based credits, the 09-13/14 and 10-04 call-score credits and cheap-protection identifications |
| Put-efficiency thresholds | 04-16 to 11-13 | 24 through 05-23, ~200 later | Unmet in ~22 of 24, then nearly all |
| Call-efficiency thresholds | 04-16 to October | ~58 through 07-31, then ~32 (August) and ~40 (September–October) | Unmet in ~48 of ~58, then the later ones |
| Momentum alignment, "equilibrium", divergence and convergence states | 04-25 to 09-30 | ~150 | Unmet in all but three |
| Directional calls under the 4/4 checklist | 03-05 to 03-27 | ~100 | About half missed direction |
| Level-threshold persistence (10Δ, ATM or wing caps and floors; call gap or slope bands; includes F13) | 10-16 to 11-13 | ~90 | Bounded: held on flat spot, broke within a day of falls |
| Skew compression and re-expansion timing | 03-27 to 09-30 | ~14 (late March), 12 (April), ~16 (06-16 to 06-30) | None, ~11 of 12 and all ~16 unmet; July–September thresholds failed |
| OI-repricing and put-score forecasts | 09-10 to 10-08 | ~18, plus seven (09-19 to 09-24) | Unmet |
| Put ATM/wing bifurcation resolution (F3–F4) | 10-01 to 10-14 | at least 14 | Ten unmet or wrong in direction |

### Entries

**F1. Call-dv and put-dv leads (r about `−0.11` to `−0.80`, 6–24h; 12-hour rule lagged_r=`−0.21`).** Cited 04-11 to 10-15 with drifting offsets; forecasts inert, failed or unresolved. The 12-hour rule was retired 2026-08-10. [2026-10-15]

**F2. OI→IV lead-lag as a put IV repricing predictor: FAILED BIDIRECTIONALLY [2026-09-24].** r ran `−0.44` to `−0.61` (03-02 to 03-08), `+0.56` to `+0.59` by late March, weak in April, `−0.12` to `−0.42` July–August, `+0.65` to `+0.68` (09-19 to 09-24). From 09-25 to 11-13, OI swings of `+4%` to `+115%` and `−16%` to `−70%` moved neither put IV nor put score. The related best_put_dv→IV thesis (08-14 to 09-15) fit once, on 09-15. No stable sign or offset.

**F3–F4, F9–F10, 9, 10, 12. Put wing extremes, ATM/wing bifurcation and synchronized fear peaks as state or direction signals: FAILED [2026-10-05; 2026-10-09].** Overnight wing compression and bifurcation direction calls failed. The 7d smile put wing swung `27→85` in eight hours and `85→30→60→55→32` within about 36 hours; ATM pctl 0–4 beside wing pctl 71–100 flipped within hours (10-09 to 10-14). Bifurcation and synchronized peaks (four instances, 99/95 to 97/97) recur and are transient. Observation only.

**F5. 20+ consecutive put resting-order failures as a closed put gate: FAILED [2026-10-06], FAILED AGAIN [2026-11-13].**
- A Dec-25 cancellation run ([order:#4586]–[order:#4617]) preceded fills on 1800-P ([order:#4618], [order:#4625]).
- Jan-29 resting buy bids on the 1500-P and then the 1000-P cancelled at zero fill for hours (for example [order:#6017], [order:#6086], [order:#6085]) while the scan named the leading put on every tick; an IOC buy on the 1000-P then filled ([order:#6087]). A Nov-20 2200-C buyback run also ended in a fill ([order:#6073]).
- Scan leadership is not executability, and a failure run is not a closed gate. Execution handling is Learning-owned ([lesson:execution.order_hygiene]).

**F12. RV7d roll-off as a call-gate opener; episode-aging model: FALSIFIED [2026-10-16].** The 10-09 to 10-18 roll-off call was right only in direction and about 10 days late, and the Oct-22 gate recovery forecast failed. The gap does not normalize because a break ages; treat it as roll and RV7d mechanics (entry 8). [2026-11-13]

**F13, 34. Term-slope band as a 24h forecast target: FAILED [2026-10-28; updated 2026-11-14].** The slope swung `±3–4` within sessions, led by the call-zone leg, expiry rolls and weekend front deflation, while the 60d–90d curve stayed anchored. The wider family of level bands (tally above) held through the 11-02 to 11-04 sag and the 11-08/09 break and failed on spot or call-gap legs. A hold is weak evidence because bands sit near prints; wins were small call buybacks, never a captured protection payoff.

**3, 33. Terminal-chain (sub-8 DTE) call_score and call wing extremes: proximity mechanics.** [2026-09-24; updated 2026-11-13] Scores read `63.8–103.5` while the gap percentile ranged 8 to 98, and the leader rotated 2150-C→2125-C→2100-C as spot eased (April rotations likewise). A wing at pctl 99 unwound within about a day; the Nov-06 and Nov-20 wings sat at pctl 0–5 without bid. The score carries no gate information; a non-terminal chain with IV above realized vol is the quality signal.

**Call-score "DTE compression lifts it through the gate" (08-17 to 08-19; 08-30).** The score peaked at `54.17` and declined; the 08-30 version failed likewise. [2026-08-30]

**Spike-flag continuation (a flat-price put spike predicts repricing higher).** Ten flags from 07-03 to 09-18 did not propagate or were followed by rallies; the 09-25 1h flag faded. [2026-09-25]

**Rally-fade, ceiling-rejection and reversal-at-high rules.** The "$2085 triple ceiling" was mixed; July ceilings broke; `$1,970.6` broke 08-19; `$2,544.2` and `$2,634.5` rejected or reversed, then both broke 09-18 to 09-21 (high `$2,801`). Unreliable. Levels are on regimes/history.md. [2026-10-02]

**"Bearish call is safe because puts are owned" and the 4/4 checklist.** Beyond the March sample in the tally, July–August high scores preceded non-events or rallies, and April scores of 1–3/4 (04-16 to 04-24) led to no put purchase of size. [2026-09-26]

**Delta dilution read as cheapening.** Falling put asks on rallies are not an IV signal; asks rose on falls (Dec-25 1800-P `$28.4→$38` on 10-14/15). [2026-11-13]

**Post_only and instrument-pivot theses for call selling (04-16/17).** 30+ post_only rejections over ~20h were read as structural and an IOC pivot proposed; never executed, spot hit the stated falsifier (`$2,463`) on 04-17, reviewed outcome `$0`. [2026-04-17]

**Early liquidity-flow and delta-value leads (02-26 to 03-01).** Flow→put delta-value (6h, r `−0.59` to `−0.63`) was replaced by a `+0.85` 24h lead as the sample grew; flow→depth (r `+0.82` to `+0.93`) was gone by 03-01; the call-dv lead flipped sign; "put delta-value leads spot by ~1h" (r `0.54`) was retracted the same day. All dropped. [2026-03-01]
