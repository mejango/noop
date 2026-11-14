# Correlations

**No cross-series pair gave a tradable lead, February to November 2026. Spot return against call ATM change kept its negative sign across windows; flow, open-interest and score coefficients flipped sign between regimes. Every coefficient is descriptive.** [2026-11-14]

How to read this page:

- A contemporaneous r gives no lead time, and no reviewed outcome has tested a coefficient. Most values below come from analyst journal notes on one dataset; only entries that give n come from a structured packet.
- The packet labels some 30d pairs "unstable_across_halves" even where both halves share a sign; treat all of these as descriptive.
- The last packet (early 2026-11-14) carried only a few pairs. Other pairs were not recomputed; their last reading is dated in the entry.

## Strong Correlations

Nothing predictive is established. The two entries here are the only pairs whose sign held in every window quoted, and the second is true by construction.

**46/52. spot_return ↔ call_atm_change (negative): sign held, magnitude varies.** r_7d `−0.552` (n=167); 30d `−0.445` (n=716); halves `−0.422`/`−0.467`, same sign [2026-11-14]. The 7d reading a day earlier was `−0.462`. Journal notes quoted the pair at `−0.26` to `−0.58` (period not stated), and one October note gave `−0.64`. In the leg record, large declines lifted call ATM, small legs were mixed, and call ATM also moved on flat spot, so the size of the response is not predictable.

| Leg | Spot | Call ATM | Fit |
|---|---|---|---|
| 10-14 | `−5.2%` | front call IV `+3.3` (notes) | yes |
| 10-22 to 10-24 | `−3.9%` | front call IV `+9.2` (notes) | yes |
| Oct-30 | `−6.2%` | call-zone ATM `+16.6` (arithmetic; notes recorded `+14.4`) | yes |
| 11-04 | `+2.6%` | `−2.0` (notes) | yes |
| Nov-05 | `−7.0%` | Nov-13 call ATM `+6.7` | yes |
| Nov-09, two legs | `−3.6%`, `−2.2%` | `+1.2`, `+0.9` | yes, small |
| Later small legs | `+1.0%`, `−1.3%`, `−0.6%`, `−1.4%`, `−1.1%`, `−0.5%` | `−0.7`, `−3.3`, `+0.8`, `+1.9`, `+0.2`, `+0.9` | mixed |
| Early Nov-14 | `−1.6%` | Nov-20 call ATM `+3.8` | yes, the strongest of the recent small legs |
| 10-18, 10-23, 11-08 | flat | front moves of 4 to 7 vols either way (notes) | no |

- Confidence medium-low. Expiry rolls from 11-06 to 11-13 changed the call-zone basis, so 24h call comparisons across a roll are unreliable.
- **Falsification (coefficient):** 7d r above `−0.30` on two consecutive windows (`−0.552` on 2026-11-14, not met).
- **Falsification (legs):** a spot recovery above 3% in an RV7d-elevated regime not compressing call ATM within the session. Still not run. [2026-11-14]

**59. Call ATM change ↔ term-slope change (negative): mechanical.** Slope is put-zone ATM minus call-zone ATM, so it contains the call-zone leg and a strongly negative coefficient is expected by construction. Notes quoted r `−0.92` to `−0.98` on 7d and 30d windows, both halves, daily from 10-20 to 11-13. [2026-11-13]

## Weakening Correlations

Pairs that fit in some periods and broke or flipped sign in others. Entries 1, 5, 6 and 7 were once listed as strong; the year's record does not support that.

**2. ETH spot decline ↔ put skew expansion: fits sharp breaks, fails on small legs and when call IV moves alone.**

| Period | What happened | Fit |
|---|---|---|
| Late April | 6 of 9 legs fit | mostly |
| June and July | Broke in both directions | no |
| 08-19 to 08-25 | Fit four days, then broke | partly |
| September | Fit unevenly | partly |
| 10-14, 10-22 to 10-24, 10-30 | Fit on each fall (notes) | yes |
| 10-18, 10-23, 11-08 | Front vol moved with spot flat | no |
| 11-05 drop | Put ATM `+3.2`, 10Δ `+4.4`, contemporaneous (notes) | yes |
| 11-09 break | Skew sentiment `15.95%→21.75%` in the 1 to 4 day expiries; Dec-25 wing flat at `−0.1` (notes) | front only |
| Nov-12 to Nov-14, Jan-29 zone | Wing `+8.6` at `$2,054.60`, `+9.0` at `$2,043.20`, `+10.9` at `$2,014.30` ([tick:#208950]), `+11.9` at `$1,991.80` ([tick:#209224]), `+13.2` at `$1,940.00` ([tick:#210118]), `+13.9` at `$1,909.20` ([tick:#210589]); ATM `+0.5` on spot `−6.6%` | yes, one expiry |

- Across expiries the direction also held: Dec-25 wing `+7.2` at `$2,503.80` ([tick:#190990]) and `+12.4` at `$2,201.10` ([tick:#198573]). Dec-25 and Jan-29 reads are not like-for-like.
- The journal's `$1,898.2` low for 11-13 is unverified and is not used here.
- On sharp breaks the wing and 10Δ moved while ATM and the 42d-plus curve stayed near `52–54`; small legs on a pinned tape did not re-bid the tail.
- **Falsification:** the relationship breaks for three or more consecutive sessions (not verifiable in the last packet). [2026-11-14]

**56. spot_return ↔ call_rr25_change (positive): sign stable, 7d size swings.** r_7d `+0.348` (n=167), up from `+0.258` a day earlier; 30d `+0.358` (halves `+0.320`/`+0.387`, same sign) [2026-11-14]. Not weakening on this pass; retained here pending a further read. Notes quoted `+0.26` to `+0.45` (period not stated). Call RR swung across its whole 7d percentile range (0 to 100 and back) during October, so the level carries little information.

**60. spot_return ↔ put ATM and put wing change (negative): weak across halves.** spot_return/put_atm_change r_7d `−0.514`, 30d `−0.311`, halves `−0.266`/`−0.343` (same sign, labelled unstable by the packet) [2026-11-14]. The wing-change pair last read r_7d `−0.303`, 30d `−0.228` [2026-11-12]. October notes quoted put_atm_change against put_wing_premium_change at `−0.43` to `−0.71` with 30d halves `−0.86` and `−0.18`. **Falsification:** r_7d above `−0.20` on two consecutive windows.

**8. Spot rally ↔ put premium decline: mixed, thin.** One clean leg in the notes: the put wing fell `+12.4→+8.3` over 11-01 to 11-03 on flat-to-rising spot. The comparison across the fall from `$2,503.80` (10Δ `+5.8`, ATM `+3.8`, wing `+2.0` on spot `−15.3%`, read 02:54Z Nov-10) tests the decline side, not the rally side, and later reads are on a different expiry. Not tested since. [2026-11-13]

**6. best_put_dv ↔ implied_vol (contemporaneous): sign and size flipped between regimes.**

| Period | r quoted | Note |
|---|---|---|
| 04-10 to 04-15 | `−0.52` to `−0.89` | |
| 06-26 to 06-30 | `+0.59` to `+0.70` | sign reversed |
| 08-04 to 08-07 | `−0.55` to `−0.85` | |
| 08-16 to 08-31 | `−0.20` to `−0.86` | wide |
| 09-01 to 09-15 | `−0.53` to `−0.69` | quoted as a 24h lead |
| 09-26 to 09-28 | 7d `−0.74` to `−0.79` | contemporaneous `−0.21` in the same notes |
| 2026-09-26 | r_7d `−0.304` (n=168) | last packet reading |

- The notes and the packet disagree for late September (`−0.74` to `−0.79` against `−0.304`); the packet value has n and takes precedence.
- Late-September notes read the pair as "structural put mispricing" while put IV sat flat; that reading was never reviewed.
- put_score read a flat `0.0044–0.0046` on [tick:#210584]–[tick:#210589], a constant scan output, so the pair gives no signal while the score does not move.
- **Falsification:** 7d r above `−0.30` on two consecutive windows. Not recomputed since 2026-09-26.

**7. best_put_dv ↔ best_call_dv (inverse): unstable across windows.** Notes quoted 7d `−0.79` (08-23/24); `+0.004` on 7d against `−0.61` on 30d (09-04); `−0.56→−0.70` (09-26 to 09-28); `−0.41` (10-07). Lagged versions (best_call_dv leading best_put_dv, options_spread and options_depth by 12 to 24h) ranged `−0.11` to `−0.65` from 04-09 to 10-15. Confidence low; not in a packet since. **Falsification:** 7d r above `−0.40` on two consecutive windows.

**9. Score improvement ↔ directional put hypothesis success: not testable, the score barely moves.** Notes record put score at `0.0029–0.0037` across skew from `−9.6%` to `+7.07%` and spot up to `$2,801`, then `0.0030–0.0046` in November through a `$2,441` high and the lows that followed. From May to 08-16 put score stayed at 62% to 99% of best while call score moved `9.7–88`. A series this flat cannot rank hypotheses. The record of score-threshold hypotheses is on indicators/leading.md. [2026-11-13]

**1. OI ↔ skew and OI ↔ implied vol: no stable sign.**

- OI expansion with skew compression rests on one episode: April 2026, skew `9.68%→8.12%→6.24%→3.29%` while OI expanded. Never re-evaluated; journal OI readings are inconsistent across feeds and carry no weight.
- OI→implied_vol: r `−0.56` to `−0.60` was used to explain a put_dv floor near `0.00275` through 03-05 and broke on 03-06. Later quotes ran `−0.24` to `−0.42`, then `+0.65` to `+0.86` (09-14 to 09-24), then r_7d `−0.42` to `−0.55` (10-01 to 10-05).
- OI→best_put_score at 24h `+0.41`/`+0.42` (09-19 to 09-23); best_call_dv↔OI `+0.33` to `+0.57`.
- From 09-26 to 11-13 large OI swings in both directions came with little movement in put score, put ATM IV or call IV. Aggregate OI feeds disagreed with each other on occasion, read as coverage or roll artifacts, so OI levels are not comparable across swings.
- Whether OI steps predict anything: they did not; see indicators/leading.md. [2026-11-13]

**5. Skew compression rate ↔ fear-exhaustion recovery speed: no coefficient, no sample.** The skew path from 03-19 to 04-26 (`18.6%→7.7%→24.65%→4.16%→3.1%→16.61%`) never led spot or flow; October ran `+5.4%→0.43%`. Put percentile paths cannot be chained across the Dec-25 to Jan-29 relisting: different expiry, not like-for-like. Skew-floor and skew mean-reversion records are on indicators/leading.md. [2026-11-13]

**61–63. liquidity_flow ↔ best_put_dv and best_call_dv: sign flips between windows, unusable as a rule.**

- 02-26: a 6h r of `−0.59` to `−0.63` was replaced the same day by a 24h lead of `+0.85`, which was gone by 03-01. Re-cited 03-16 to 03-25, 08-05/06, 09-30 (7d `−0.72` to `−0.78`), 10-01 and 10-02.
- The best_call_dv lead flipped `−0.76`/`−0.81` (12h) to `+0.70` to `+0.50`.
- Last packet: liquidity_flow/best_put_dv r_7d `+0.456` (n_7d 133), 30d `−0.230`, halves `+0.127`/`+0.272`, best lag 24h r `−0.293`. The 7d and 30d signs disagree. [2026-11-14]
- spot_return/put_rr25_change and the call_dv pair last read unstable or opposite-signed [2026-11-08 to 2026-11-09]; not recomputed.
- USD TVL change reflects token prices and swaps; it is not net flow. Confidence very low.

**Other leads quoted and not sustained.**

- Early leads, 02-26 to 03-18: flow→options_depth (24h, `+0.93→+0.82`, gone 03-01); flow→spot_return (`−0.40`); flow→implied_vol (`+0.24` to `+0.55`); options_depth→implied_vol (`−0.63` to `−0.69`); put_dv against options_spread (`−0.65`, then `+0.73`).
- best_call_dv→implied_vol `+0.37` to `+0.94`: the 7d rose `+0.84→+0.94` (08-20 to 08-23) as call IV went `36%→101.8%`, a single breakout window; 30d `+0.72` to `+0.85` (08-26 to 09-24).
- Weekly flow labels coincided with breaks and reverted within hours; they did not lead. See indicators/leading.md.

## Retired Correlations

**3. Medium momentum neutral state ↔ regime transition inflection.** Retired; momentum labels are no longer tracked. [2026-10-19]

**14. Post-expiry OI surge/collapse ↔ put IV repricing.** Confirmed retired in both directions; the notes retired the OI lead as direction-dependent. [2026-09-24]

## New Correlations

Unreplicated, or observed over one regime only. None is a rule.

**17, 36, 49, 51/54. Call ATM IV−RV7d gap ↔ the RV7d window (10-06 to 11-14, about 27 swings in the notes).** The gap percentile tracks the realized-vol denominator more than call richness. Calls were never paid above realized during the break; each time the gap closed, it closed from the realized side.

| Episode | RV7d | Gap response | Reading |
|---|---|---|---|
| 10-08 | level `32→44` | pctl `83→32` | spike, gap falls |
| 10-13 to 10-15 | n/a | pctl 0, then 68 within about 8h | window effect |
| 10-20 to 10-28 | level `53.2→39.6` | gap `−9.0→+12.4` | decline, gap recovers |
| 10-30 | level `41.2→49.2` | call ATM jumped with it (entry 46/52) | both legs moved |
| 11-04 | level to `51.6` | no collapse in the packet record (notes: gap `+4.5→−4.4`) | disputed |
| Nov-05 | change `+9.2` | call VRP all-history pctl 18 | spike, collapse |
| Nov-06 | change `−6.8` | gap `+8.7` | decline, recovery |
| Nov-09 | change `+10.7` | Nov-20 gap pctl 16 | spike, collapse |
| Nov-10 to Nov-12 | level `66.9`, held about 72 hours | pctl 8, about two days at the floor | spike, collapse |
| Nov-11 | change `−4.0` | gap `+3.4` | decline, recovery |
| Nov-12 | change `−8.8` to level `53.0` | gap `+10.6`, pctl 32 then 35 | decline, recovery |

- **49, spike ↔ collapse:** four November spikes through Nov-13 each drove the gap to pctl 18 or lower (the last, RV7d `52.4→59.5`, to pctl 11); the Nov-04 spike did not. **Falsification:** an RV7d spike above 2 vol pts within 8 hours not driving call VRP below pctl 20.
- **51/54, decline ↔ recovery:** supported on the RV7d-led legs above; the 10:30Z Nov-06 leg did not fit, Nov-09 and Nov-10 legs ran the other way, and four call-ATM-led legs (Nov-11, Nov-12, Nov-14) do not test it. **Falsification:** an RV7d decline above 2 vol pts not producing a gap improvement of at least `1.0` in the same session; last tested 18:59Z Nov-12 and held.
- **36, single-session extreme realized vol ↔ reversal of RV7d compression:** the first November surge rolled back as the volatile hours aged out, even as spot made new lows, consistent with earlier surges; a second surge followed the Nov-13 low and was still elevated on 2026-11-14 (RV7d `60.0`, RV30d `52.5`). Notes add that each RV7d-led recovery reversed when the next low entered the window, and that forecasts of when a shock would age out were wrong on timing or driver.
- **17, gap as a call-richness signal:** the gate needs a gap clearly positive and a wing together at two consecutive checkpoints on a non-terminal chain. A terminal (sub-8 DTE) chain carries no gate information whatever its score. Gate episodes are on revenue/windows.md. [2026-11-14]

**55. Front-end vs back-end IV: the curve is front-led, the back barely moves (10-16 to 11-13).**

- After each shock a hump formed at 1 to 4 days and deflated within about 16 to 36 hours to a flat `50–54` plateau beyond about 10 days. Examples in the notes: 10-30 (`88` at 0.3d, slope `−10.8`, pctl 1), 11-05 (front `61–62`, slope `−0.4`), 11-09 (front `62` at 0.9d, slope `−1.2`, RR25 `−17.8`).
- The 42d-plus back stayed within about 1.5 points through every November episode: 60d to 90d fixed ATM `52.4–53.3` across about ten checks while spot fell `8.6%`.
- Slope followed the front, not spot: `+11.2` (10-16), `+0.4` (10-24), `−10.8` (10-30), `+3.4` (11-04, pctl 100) on a calm tape.
- spot_return/term_slope_change: r_7d `+0.485`, 30d `+0.408`, same sign [2026-11-14]; notes quoted `+0.36` to `+0.54`, with the 30d halves flipping sign on 10-24.
- Skew fear stayed short-dated: RR25 `−8` to `−18` at 1 to 4 days against `−3` to `−4` at 45 days.
- Confidence low. **Falsification:** back-end (30d to 90d) ATM above `56.0` within 24h; `52.7–53.2` on 2026-11-14, not met.

**44/50. Put ATM percentile with wing percentile ↔ bifurcation and inversion (09-29 to 11-13).**

- ATM at pctl 0 to 20 sat beside wing pctl 71 to 100 from 09-29 to 10-09, then inverted within hours on moves under one vol point (wing pctl `5→19→95` over 16h on 10-12).
- The wing held 7d/30d pctl 95 to 100 for about four days through the 10-30 break; it eased to `+8.3` by 11-03, re-bid to `+10.5` to `+11.8` after the 11-05 drop, eased to `+9.2` by 11-09/10 and re-bid again on the Nov-12 to Nov-14 lows, to 7d pctl 98 with ATM at pctl 35 (03:00Z Nov-14).
- ATM low with wing high is a bifurcated surface (tail bid, level flat).
- The 7d, 30d and all-history bases disagree on ATM, so name the basis.
- Not independent of spot: the wing re-bid on each new low. Reviews credited cheap-wing identification with zero or small capital. [2026-11-14]

**47. Synchronized fear peak (ATM and wing both near the top of the 7d range): recurrent, four instances.** Day 37 evening (99/95), Day 39 evening (100/90), Day 55 early (99/100), Day 55 evening (97/97). A wing-only elevation with a flat back curve is not an instance. [2026-11-13]

**Put/call IV gap and quote quality (May to 11-13, notes).** The gap changed sign with the regime: call below put by 2 to 24pp before 08-19, call above by 7 to 39.5pp after it, put mostly above call by 4.6 to 16.5pp from 08-28, near parity 10-24 to 10-29. In November call spreads ran 12% to 45% against 5% to 15% for puts, a 1.4x to 5.1x crossing penalty, so a call score did not mean an executable quote. [2026-11-13]

**Call efficiency ↔ momentum state (05-04 to 11-13, notes): no relationship.** Call efficiency swung 23% to 78% across every momentum state. Chain roll, delta, DTE, spot distance and, in the breakout, call IV explained most jumps; terminal chains fell to `45–66` with spreads of 33% to 66%. Roll effects are recorded on indicators/leading.md. [2026-11-13]

**57/58. Call VRP and put VRP changes ↔ spot return: exploratory.** Last reading spot_return/call_vrp_change r_7d `−0.373`, 30d `−0.324` [2026-11-08]. Confidence very low; no causality claimed. **Falsification:** halves flip sign, or 7d r above `−0.20` on two consecutive windows.

**24. Daily flow reversal ↔ spot grind higher: thin evidence.** USD TVL change reflects token prices and swaps, not net flow, and 24h volume was unavailable on incomplete hourly coverage until 2026-11-14 (v3 `$384.8M`, v4 `$1.82M`, GeckoTerminal OHLCV). **Falsification:** a flow reversal coinciding with put_score above `0.0045` or a directional impulse above `$20` in one tick.

**Flow series are window and feed artifacts in places.** The weekly figure jumped `+1.88%→+5.19%` in one tick (03-05) as old data rolled out; an hourly `+22.4%` beside a weekly `−4.0%` (06-27) was a DEX-liquidity artifact; the DEX TVL feeds were restated on 10-11, with legacy v3/v4 TVL quarantined as unverified. Any coefficient on liquidity_flow inherits these breaks. [2026-11-13]

**41. Resting order failure ↔ episodic liquidity gap.** Scan leadership is not executability: resting bids cancelled at zero fill in episodic, strike-dependent gaps ([order:#4586]–[order:#4617], [order:#5998]–[order:#6017], [order:#6068]–[order:#6086]). The execution record is on revenue/efficiency.md and protection/convexity.md.

**Recorded elsewhere.**

- Range-lock then breakout: no pair flagged any break beforehand, and the November breaks arrived with flat back-end IV. Episodes are on regimes/history.md.
- Flow leading price, deceleration at highs, and compression-precondition stacks: none held as a rule; see indicators/leading.md.
