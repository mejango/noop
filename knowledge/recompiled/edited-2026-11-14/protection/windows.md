# Protection Windows

**Put window CLOSED (2026-11-14): tail rich, level fair. Since February, cheap-looking put windows recurred in about 210 sessions and timed no drop; two paid, repricing followed falls, and empty budgets or unfilled resting bids missed most.** [2026-11-14]

## Active Windows

> ⚠️ **Staleness notice:** values come from ticks [tick:#210584]–[tick:#210589] (2026-11-14T02:58–03:02 UTC), spot `$1,909.20`. The smile is as of 03:00Z. Re-anchor before acting.

**Put window: closed.** The tail legs are in the upper range of history, ATM is mid-range, and nothing indicates cheapness.
- Jan-29 (76.2d): wing `+13.9` (pctl 92 of 5,134h; Δ24h `+2.0`), 10Δ `67.3` (pctl 76; Δ24h `+2.4`), ATM `53.3` (pctl 54; Δ24h `+0.4`).
- Name the basis: smile put ATM is pctl `35` on 7d and `80` on 30d against `54` on all-history, and the wing reads `98/99`. A 7d low is not an all-history low.
- ATM sits `6.7` below RV7d `60.0` and `0.8` above RV30d `52.5` (arithmetic), so level insurance is priced near 30d realized.
- The zone is the 2027-01-29 expiry, different from the earlier Dec-25 series, so comparisons with Dec-25 percentiles are indicative only.
- The divergence feed is not in the packet; see indicators/divergences.md.
- The short front is deflated and lifted only at 7d, over an anchored 60d–90d back; see protection/pricing.md for the shape and fixed-maturity moves.

**Call-side cross-reference:** the Nov-20 chain is terminal (DTE `6.2`) with wing pctl 17 and gap pctl 16; no call window (revenue/windows.md). [2026-11-14]

## Historical Windows

One row per window episode, 2026-02 to 2026-11-14. Rows before 09-22 come from analyst journal notes and campaign reviews (the lowest evidence tiers) and are not tick-verified; rows from 09-22 carry tick citations. Percentiles are "7d smile / all-history zone" where both are given. [2026-11-14]

| Period | Spot | What marked put pricing | Capital and fills | What followed |
|------|------|------|------|------|
| 2026-02 | `~$2,737–2,740` at entry | Feb-27 2000-P (about 27% OTM) staged at `~$12.90` | Bought | Spot troughed `~$1,938–1,940` (`-29%`) within weeks; sold `~$194–200`, about 15x [review:#24] [review:#25] [review:#26]. The about 45%-OTM 1500-P bought in the same window expired worthless through a low of `~$1,809` |
| 03-05 to 03-26 | moved `$350` on 03-05 | Put pricing stayed in a narrow band; after the 03-06 spike cluster the score rose to `~0.0031` and put_valid fell to 0; skew compressed `18.6%` (03-19) to `4.16%` (03-26) | None recorded | Notes recorded drops after compressions; not a timing signal (indicators/leading.md). Compressions re-expanded within 8–16h |
| 03-27 to 03-31 (expensive) | n/a | Skew `13–21%`, put_valid 0 | None; not buying at `19–21%` was graded sound | "Cheap within 24–48h" calls failed |
| 04-02 to 04-25 | `~$2,320` on 04-25 | Skew `12.43%→3.29%` (04-02 to 04-11), then `≤9%` for about 3.5 days (04-15 to 04-25) | Not buying at `11.33%–15.91%` endorsed; first purchases 04-24/25 tiny: Jun-26 1800-P `~$53` (about 22% OTM) | April compressions were followed by rallies or ranges. The 1800-P was sold near intrinsic `~$300` after a fall to `~$2,015` (size on regimes/history.md), about 5–6x [review:#102] [review:#103] [review:#112] |
| 04-25 to 06-30 | `$2,129→$1,976`, June low near `$1,511` | Jun 1500-P ask fell `$16.5–$21→$8.4–$10.4` by delta shrinkage, then rose `$18→$30` as spot fell | Budget near empty (`~$0.90`); small hedges closed 05-28 to 06-05; first fill 06-25 | The June drop came days after those closes and met a near-empty budget; the 06-24 `$15.5` window preceded a break to `$1,553` by about 30 minutes |
| 07-01 to 07-25 | lows to `$1,838` | Skew `26.28%` (06-30) to `1.11%` (07-25), with a rebound to `16.33%` on 07-17; best score `0.0038–0.0050`; Sep-25 1400-P ask `~$20.5–$30.3`; put IV `~60–72%` | Almost all zero capital; only fills 2.58 Sep-25 1400-P at `$22.80` (07-22/23) | Cheap identification credited as correct but bounded; compression timing mostly unmet; skew `1%–5.5%` preceded a 2–4% dip about as often as flat or rising spot; the 1400-P expired worthless |
| 07-26 to 08-31 | `$1,822.9` low (08-01), `$1,916→$2,543` (08-19 to 08-27) | Skew `8.08%–8.36%` called cheap; from 08-04 it fell to `1.87%` and as low as `-9.61%`; score `0.0045–0.0051`; Oct-30 1400-P ask `$22.30–$28.10` | Budget `~$0.90→$0.19`; Sep-25 1500-P at `$19.5` (08-05/07); Oct-30 1900-P `~$34` (08-28); eighteen buy_put rejections 08-22 to 08-25 while ITM short calls held margin | The 08-01 break lifted skew to `10.75%` for about 8h only; cheapness persisted through the breakout; both purchases were later salvaged at a loss as spot stayed above strikes |
| 09-01 to 09-30 | `$2,356.3–$2,544.2`, then a rally to `$2,801` | Score `0.00328–0.00370` on Oct-30 1500–1900-P (`$7.3–$23.6`); the 09-15 fall took put IV `63.9%→74.5%` and score to `0.002685`, deflating within about 8h | Budget `$0.02` through 09-05, `$75.20` from 09-06; zero fills 09-01 to 09-10; about 35 resting Nov-27 bids cancelled unfilled; then 1800-P 2.19 at `$22.8` (09-18) and 2000-P 3.51 at `$28.2–$29.3` (09-20 to 09-22) [order:#3693] [order:#3695] | Fills went into the rally and were graded bounded bleed; score drifted `0.00316→0.00287` by delta dilution with put IV flat `~60–64%`; patience at `74.5%` was credited |
| 09-22 to 10-14 | `$2,765.2→$2,408.2` (`-12.9%`), slides to `$2,323.2` | Put OPEN (Type E) on 09-27, 10-04, 10-07/08; Nov-27 put ATM pctl 0–1 from 10-02 to 10-07 beside wing pctl 71–100; put_score `0.0029–0.0034` | 50+ resting bids unfilled until IOC fills of Nov-27 1900-P at `$17.6`/`$17.7` on 10-07 [order:#4008] [order:#4014], after the about `$80` flush | ATM pctl went `3→99–100` on the slides, with fear peaks 10-12/10-14; "bifurcation resolves" forecasts were unmet or wrong |
| 10-15 to 10-29 (about 48 checks) | `$2,305.60–$2,505.90` | CHEAP on 10-23 morning ([tick:#193046]–[tick:#193051]: 10Δ pctl 8, ATM pctl 12); otherwise not open; wing pctl 0–100 / 13–64; put_score `0.0031–0.0034` | Two Dec-25 1800-P fills on 10-23, about `$75` [order:#4618] [order:#4625] | The 10-24 fall (`-3.9%` to `$2,286.6`) came a day later and lifted the wing to pctl 100; the fills were credited as an asymmetric setup with bounded bleed; all-history cheapness did not return |
| 10-30 to 11-02 (ten passes) | `$2,201.10–$2,293.50` | Closed to half open; wing pctl 43–100 / 59–83, ATM pctl 13–99 / 29–57; slope `−10.8` to `+1.0`; [tick:#198568]–[tick:#200864] | None beyond the earlier `$75` and the 1900-P roll | The 10-30 break lifted 10Δ `60.2→66.5` and ATM `+3.4`; ATM and 10Δ retraced within about 16h while the wing held `+11.3` to `+12.4` for about 2 days: fair on level, rich on skew |
| 11-03 to 11-05 morning (eight passes) | `$2,252.30–$2,432.20` | Cheap-to-fair; open on level 11-04 midday (ATM pctl 11–28); wing pctl 1–25 / 31–41; put_score `0.0029–0.0035`; [tick:#201685], [tick:#202555]–[tick:#202560] | About `$50` of 1500-P tranches, filled at `$11.7` against asks near `$7` (hypothesis review only) | Spot `-7.0%` then repriced the surface `+3.0` to `+4.3` in eight hours; "stays cheap" failed, the action was bounded |
| 11-05 evening to 11-14 03:03Z (about 25 passes) | `$2,250.50→$1,909.20` | CLOSED: 10Δ pctl 60–71 to 11-10; put zone unlisted 11-10 to 11-11 (Dec-25 below the 45d floor), then Jan-29 with level fair (ATM pctl 51–55) and the tail going fair to expensive (wing pctl 35–43, then 92); [tick:#204346]–[tick:#204351], [tick:#207301]–[tick:#207306], [tick:#208001]–[tick:#208009], [tick:#210113]–[tick:#210118], [tick:#210584]–[tick:#210589] | Resting bids unfilled; one deep-tail 1000-P IOC fill (see below) | New lows bid the tail, not the level; patience at wing pctl 65–82 was credited as zero bleed |

What the episodes add up to:
- **Cheap-looking windows were the normal state, not a signal.** From 02-26 through 11-13 the notes called cheap protection open 500+ times (1000-P to 2100-P, asks `~$7–$60`, delta `-0.02` to `-0.12`, score `~0.0020–0.0054`), in about 210 sessions, across spot from the June low near `$1,511` to `$2,801`. They did not by themselves time a drop. [2026-11-13]
- **Budget, not identification, was the binding limit.** Put budget was zero or near zero for most of those calls (`$0.90` from 06-26 falling to `~$0.02` by 08-29; `$75.20` from 09-06; `~$82.56` from about 10-06; `~$0.21` after 10-23; `~$22.93` on 11-07) or put_valid was 0. [2026-11-13]
- **The two windows that paid** (2026-02 and 04-25) were puts about 22–27% OTM bought before a fall that arrived within days to weeks and sold into the decline. Analyst reading from campaign reviews, not a tested rule. Campaign ledger, strike distance and exit grading: protection/convexity.md. [2026-06-07]
- **Expensive phases were handled correctly by not buying** (03-27 to 03-31 at skew `19–21%`; April at `11.33%–15.91%`; 09-15 at put IV `74.5%`), each credited in review notes. Three phases. [2026-09-30]
- **Repricing arrived after falls began, never before**, and usually faded within 8–16h on level while the wing held longer. Sizes and the episode count: protection/convexity.md. [2026-11-13]

Call-gap near-opens at 17:10 UTC Oct-22 (pctl 58) and 17:12 UTC Oct-24 (pctl 57) faded within about 8–16h; the pctl 98 reading at 01:40Z Oct-30 faded to pctl 66 within about eight hours. See revenue/windows.md. [2026-11-13]

**Dated execution examples (historical):**
- Dec-25 1800-P buys filled at order values `$39.76` ([order:#4618]) and `$35.00` ([order:#4625]) after multi-hour runs of unfilled resting orders; journal notes record them as 1.42 and 1.25 contracts at `$28`. Fills confirm those prices were executable then, not that protection was cheap. [2026-10-25]
- Dec-25 1400-P, 1500-P and 1600-P resting buys kept cancelling at zero fill through 08:02Z Nov-10 (for example [order:#5770], [order:#5763]).
- On the Jan-29 zone, resting 1500-P buys cancelled at zero fill through 02:59Z Nov-13 (for example [order:#6017]) and resting 1000-P buys did the same (cumulative `0/1.46–1.87`) for about eighteen orders ([order:#6068]–[order:#6086]) before an IOC buy on the 1000-P filled at order value `$11.89` at 17:49Z ([order:#6087]). A post_only rejection occurred earlier on the same strike ([order:#6056]). The 1000-P was about 48% OTM, a deep tail and not a cheap window. Scan leadership is not executability, and an IOC fill shows executability at that price, not cheapness. [2026-11-13]
- The same pattern ran all year in the notes: about 35 resting Nov-27 bids cancelled unfilled 09-16 to 09-18 and 50+ before the 10-07 IOC fills. Resting bids rarely filled; fills came by IOC, after a flush or into a rally. [2026-10-07]

## Window Indicators

No confirmed precursor of cheap protection exists in the record from February to November. Everything below is journal-level evidence unless a tick is cited; predictive value is unverified. [2026-11-13]

Signals observed **preceding** cheap protection pricing:
- **Front-end vol washout with a flat-to-contango curve:** Day 41 evening call ATM Δ24h `-5.0` vs put ATM `-1.0` and slope `+11.2`; put cheapness widened as the front deflated (ATM pctl 39→28→12). The Nov-02 instance reversed within 8 hours; the Nov-03 instance held, reached pctl 11 at 10:11Z Nov-04 and was gone by 18:26Z Nov-05. Through Nov-10 to Nov-14 the front deflated repeatedly with put ATM at pctl 51–55 and the wing re-bidding, so deflation did not cheapen protection. Fifteen-plus episodes, mixed outcomes; not a validated lead. [2026-11-14]
- **Put surface anchored through a spot rise (experimental, failed on reversal):** from 02:11Z Nov-04 to 10:25Z Nov-05 spot rose `6.9%` with the surface up only about a vol point; the reversal then repriced it by `+3.0` to `+4.3` in eight hours on spot `-7.0%`. One episode. [2026-11-05]
- **Back end anchored through new lows (experimental):** from 02:54Z Nov-10 to 03:03Z Nov-14 spot fell `10.0%` (`$2,121.70→$1,909.20`) while 60d–90d fixed ATM stayed at `52.4–53.3`. About a dozen consecutive checks; not a lead. [2026-11-14]
- **Skew-only re-bid on new lows (experimental):** five consecutive legs since 10:58Z Nov-12 (spot `−1.4%`, `−1.1%`, `−2.1%`, `−0.5%`, `−1.6%`) each bid the tail (10Δ `+0.6` to `+2.1`, wing `+0.6` to `+1.9`) while ATM held within `−0.2` to `+0.3`. The last two legs were small, so leg size did not matter. Five legs; not a lead. [2026-11-14]
- **Put protection repricing on a decline:** against `$2,503.80` the surface was `+5.8`/`+3.8`/`+2.0` (10Δ/ATM/wing) at the 02:54Z Nov-10 read on `-15.3%`; the Oct-30 peak builds were mostly given back on the wing and 10Δ. Descriptive only. [2026-11-10]
- **Wing compression on a modest decline, then reversal, then draining (experimental):** from 18:46Z Nov-08 spot `-3.6%` came with the wing `-1.7`; the next leg `-2.2%` with `+1.2`; later legs gave back `-0.4`/`-1.0` and `-0.8`/`-1.4`. Four legs; not a lead. [2026-11-10]
- **Held repricing that unwound on a flat tape (experimental):** from 18:26Z Nov-05 to 18:46Z Nov-08 spot moved about `-0.7%`; the wing pctl rose `65→80`, fell to `54`, re-firmed to `68–71`, then drained to `42` as spot pinned. One episode of about six days. [2026-11-10]
- **Low surface percentile ahead of a fall (experimental, mixed):** Nov-27 put ATM at pctl 0–1 (10-02 to 10-07) preceded the `-12.9%` fall by 0–5 days and the 10-23 pocket preceded the 10-24 fall by about a day; but pctl 14–39 (09-27 to 09-30) and pctl 3 (10-10) preceded flat tape, and the 11-03/04 lows a rally first. Not a lead. [2026-11-05]

Signals confirmed **not predictive** or unreliable:
- A low put-value score: put pricing stayed in a narrow band while spot moved, and cheapness persisted through rallies and declines alike (about 210 sessions, February to November), so a low score timed nothing. PUT_CHEAP_PRICE_STABLE had no predictive value. [2026-11-13]
- The "% of best" score reading: instrument rolls and a rebased reference best (for example `0.003738→0.005569`) moved it between 40% and 99% with no relation to later spot moves. [2026-09-25]
- Single-tick readings: Sep-25 1400-P asks of `$35.70–$44.60` against a `$21–$29` band self-corrected within hours, and thin call chains (spreads 33–273%) gave scores of 9.72 to 67. The put score tracked spot delta and the momentum derivative, not IV; strikes drifting further OTM in a rally lower it by delta dilution alone. July and September. [2026-09-30]
- Put wing percentile as a stable state: the 7d wing pctl ranged 0 to 100 across the tabulated checks and swung `5→95` within about 16h. Percentiles are relative to their window (an ATM 7d pctl of 99 was 43 on all-history), so neither marks durable cheapness or richness. [2026-11-13]
- A rich wing as a warning: wing pctl 99–100 with ATM at all-history pctl 22–29 (10-28/29) preceded the 10-30 `-6.2%` break by about a day, but the same wing at pctl 95–99 on 10-31 and 11-06 to 11-08 preceded only ranges. [2026-11-08]
- Best_put chain leadership as confirmation of cheapness: the lead has moved among 1000-P, 1400-P, 1500-P, 1600-P, 1800-P and 1900-P across ATM pctl 11 to 63. [2026-11-13]
- Call wing at an extreme percentile as a durable signal: pctl 99 unwound within about 24h earlier in the cycle; the Nov-20 wing then fell to the floor (pctl 5) before partly recovering. [2026-11-14]
- Call VRP gate-open readings as multi-session conditions: all-history ATM−RV7d percentile has ranged `8→98`. RV7d roll-off, not call IV, moved the call gap (10-20 to 10-28). [2026-11-13]
- Term slope as a persistent signal: from `-2.0` at 02:25Z Nov-06 it changed sign repeatedly to `+0.2` (pctl 16) by Nov-14, mostly via call-zone ATM, expiry rolls and short-expiry roll-offs. Compression to a floor (`+0.1` to `-1.4`, 10-24 to 10-27) accompanied a flat tape and a bounce, not a further leg. [2026-11-14]
- Realized vol as a lead: RV7d rose after breaks while back-end IV stayed `52–54`. [2026-11-13]

Signals whose record is kept on another page (conclusion only):
- Put-spike clusters did not time falls; no flag preceded the main October and November breaks. See indicators/leading.md.
- Skew level, skew floors and skew mean-reversion forecasts did not time drops or re-expansion reliably. See indicators/leading.md.
- Flow was at best coincident with falls, never a lead; open-interest swings and momentum states moved neither put score nor skew. See indicators/leading.md.
- Cross-correlation leads cited to claim structural put underpricing produced no IV rise ahead of a fall. See indicators/correlations.md.
