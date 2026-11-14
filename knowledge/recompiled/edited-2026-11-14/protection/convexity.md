# Convexity Map

**Strike distance from the trough decided put payoff: two clear wins, most reviewed campaigns bled. The put surface repriced after falls, never before. Scan leader now: Jan-29 1000-P, about `47.6%` below spot; scan leadership is not executability.** [2026-11-14]

*Metadata: reviewed=2026-11-14 | snapshot_expiry=2026-11-15T03:00Z | latest_ticks=[tick:#210588]–[tick:#210593]. Percentiles are rolling-zone comparisons. Campaign outcomes come from reviews; entries marked "notes" come from analyst journal notes, the lowest evidence tier.*

## Current Convexity Map

**[2026-11-14T03:01–03:06 UTC], 2027-01-29 (~76.2 DTE)**

- **put_score:** `0.0043–0.0046` on [tick:#210588]–[tick:#210593] with the same instrument named; a candidate screen, not a gate.
- **Spot:** `$1,907.60–$1,909.20`; latest `$1,907.60` ([tick:#210593]).
- **Leading instrument:** ETH-20270129-1000-P on 6 of 6 ticks; buffer about `47.6%` at `$1,907.60` (arithmetic). The leader moved from the 1500-P (about `24.7%`) to this far deeper strike on Nov-13 and has held it.
- **Surface:** the put zone is skew-heavy, with 10Δ pctl 76 and wing pctl 92 against ATM pctl 54 (levels in protection/pricing.md). Put skew is concentrated short-dated: 25Δ RR `−4.0` at 76.2d against `−9.0` at 6.2d and `−9.3` at 0.2d.
- Option-level quotes are not in the packet, so convexity per dollar cannot be confirmed. A journal-quoted 1000-P ask near `6.3` with delta about `−0.024` is unverified; the journal also reports roughly `0x` shock multiples at `−30%/−40%` spot and about `3.5x` at `−50%`, so payoff is deep-tail only (unverified). [2026-11-14]

## Strike-Delta Sweet Spots

**2027-01-29 (~76.2 DTE):** no single sweet spot is confirmed. The scan led with the 1500-P on its first five passes on this expiry and has held the 1000-P on the three most recent passes (Nov-13 10:48Z to Nov-14 03:02Z), so persistence is unknown. Dec-25 leadership alternated earlier between the 1500-P, 1400-P and 1600-P ([tick:#206497], [tick:#206199]). Leadership is a score, not executability (dated example below). Historical sweet spots are in the reference table. [2026-11-14]

No general delta or distance rule is established. The reviewed campaigns below (2026-01 to 11-02, plus unreviewed later fills) are the evidence. The 1d/3d/7d reviews of a campaign are windows on one position, not independent episodes.

| Instrument | Entry spot | Strike OTM | Premium | Drawdown seen | Outcome |
|---|---|---|---|---|---|
| ETH-20260227-2000-P | ~$2737–2740 | ~27% | ~$12.90 | -29% to ~$1938 | sold ~$194–200, ~15x; disciplined_win [review:#24] [review:#25] [review:#26] [2026-02-05] |
| ETH-20260227-1500-P | ~$2737 | ~45% | ~$26 | -34% to $1809 | expired worthless; disciplined_loss [review:#27] [review:#28] [review:#29] [2026-02-27] |
| ETH-20260220-1600-P | ~$2016 | ~20% | ~$186 | none | expired worthless; disciplined_loss [review:#39] [review:#40] [review:#41] [2026-02-20] |
| ETH-20260327-2200-P | ~$3378 | ITM at trough | n/a | to $1809 | part sold ~$371.9; disciplined_win [review:#21] [review:#22] [review:#23] [2026-03-27] |
| ETH-20260327-1400-P / -1000-P | accumulated Feb | ~35% / ~50% | ~$1242 / ~$3784 total | low $1809 | expired worthless; 1400-P loss ×3 [review:#33] [review:#34] [review:#35], 1000-P win/loss/loss [review:#30] [review:#31] [review:#32] [2026-03-27] |
| ETH-20260424-1000-P | Feb–Mar | ~50% | ~$556 net loss | $1809–$2378 | closed ~$0.10, 16 DTE; loss ×3 [review:#45] [review:#46] [review:#47] [2026-04-08] |
| ETH-20260410-1600-P | ~$1990 | ~20% | small | rally to $2088 | closed $0, 3 DTE; execution_mistake 3d/7d, loss 1d [review:#82] [review:#83] [review:#84] [2026-04-07] |
| ETH-20260424-1400/1500/1600-P | ~$1940–2070 | ~27–32% | ~$465 (1500-P) | low ~$1840 | expired worthless; loss ×9 [review:#51] [review:#52] [review:#53] [review:#54] [review:#55] [review:#56] [review:#85] [review:#86] [review:#87] [2026-04-24] |
| ETH-20260529-1500-P | ~$1987–2190 | ~25–31% | ~$1.65k total | low $1942.7 | closed 05-17 ~$0.90; loss ×8, win ×1 [review:#78] [review:#89] [review:#100] [review:#164] [review:#165] [review:#166] [review:#168] [review:#169] [review:#170] [2026-05-17] |
| ETH-20260529-1000-P | ~$2000+ | ~50% | ~$380 | none near strike | expired worthless; loss ×3 [review:#107] [review:#111] [review:#119] [2026-05-29] |
| ETH-20260626-1800-P | ~$2320 | ~22% | ~$53 | to ~$2015 (-13%) | sold near intrinsic ~$300 at $2113; win ×3 [review:#102] [review:#103] [review:#112] [2026-05-25] |
| ETH-20260731-1500-P | ~$1976 | ~24% | ~$190 total | low $1510.9 | $2.30 exit execution_mistake ×3 [review:#106] [review:#110] [review:#117]; residual closed 07-08, loss ×3 [review:#156] [review:#159] [review:#162] [2026-07-08] |
| ETH-20260626-1500-P / -1000-P | ~$1945–2000 | ~24% / ~49% | small / ~$26.9 | to ~$1510–1630 after close | closed ~$3.43 loss ×3 [review:#113] [review:#120] [review:#128]; 1000-P mistake 3d/7d [review:#125] [review:#130] [review:#139] [2026-06-07] |
| ETH-20260731-1200-P | ~$1630 | ~27% | ~$49.10 | low $1511 | closed 07-08 $3.40; loss ×3 [review:#157] [review:#160] [review:#163] [2026-07-08] |
| ETH-20260828-1000-P / -1300-P | $1,561.50 / ~$1,735 | ~36% / ~25% | $51.99 / $54.94 | rally to $1,973.9 | 1000-P expired, loss ×3 [review:#186] [review:#189] [review:#193]; 1300-P closed 08-03, ~$49.23 loss ×3 [review:#176] [review:#177] [review:#179] [2026-08-28] |
| ETH-20260925-1400-P / -1500-P | ~$1,897–1,926 | ~21–27% | $58.82 / $58.50 | low ~$1,848 | 1400-P expired, -$56.84 [review:#211] [review:#213] [review:#215]; 1500-P sold 08-31/09-01 at $1.40–$1.90, -$53.40 [review:#192] [review:#196] [review:#197]; loss ×3 each [2026-09-25] |
| ETH-20261030-1900-P | ~$2,445 | ~22% | $73.78 | $2,323–$2,801 | sold 10-18 at $6.90 despite IV 56.9%→77.9%; -$58.81; loss ×5 [review:#254] [review:#247] [review:#242] [review:#301] [review:#300] [2026-10-18] |
| ETH-20261127-1800-P / -2000-P | ~$2,400–2,740 (09-16 to 09-22) | ~27–33% | 1800-P $53.71; 2000-P 3.51 at $28.2–$29.3 | high $2,801, low $2,175 | 1800-P closed 11-02 at $11, -$28.52; disciplined_loss ×3 [review:#279] [review:#270] [review:#266]; 2000-P hypothesis reviews only [2026-11-02] |
| ETH-20261127-1900-P | ~$2,610–2,620 (10-07) | ~27% | 2.31 at $17.6 + 2.31 at $17.7 (~$81.54) | low $2,408 by 10-08 | first fills after ~50 zero-fill bids [order:#4008] [order:#4014]; no campaign review [2026-10-07] |
| ETH-20261225-1800-P | ~$2,374–2,382 (10-23) | ~24% | 1.42 and 1.25 at $28 (~$75) | low $2,286.6 | [order:#4618] [order:#4625]; no campaign review; hypothesis reviews: bounded bleed [2026-10-24] |
| ETH-20261027-2275-P | ~$2,302 | ~1.2% | $5.60 ×1.1 | flat | closed 23s later, -$1.19; execution_mistake ×3 [review:#265] [review:#258] [review:#257] [2026-10-26] |
| ETH-20270129-1000-P | ~$1,940–1,950 (11-13) | ~48% | IOC fill, order value `$11.89` (journal: ~1.83 at ~$6.50, unverified) | none verified; a journal low near $1,898 is unverified | [order:#6087]; deep-tail lottery strike, no review [2026-11-13] |

What the campaigns show:

- **Strike distance from the trough, not drawdown percentage, decided payoff** (campaigns 2026-01 to 05-29). The ~45% OTM $1500 paid nothing through a -34% fall; the 35–50% OTM $1400 and $1000 repeated it; the $1600 needed a 20%+ crash; the 04-24 expiries (ETH never below ~$1840) were acceptable bleed. A 22%-OTM $1800 bought near highs returned ~5–6x into a ~9–13% fall. [2026-05-29]
- **Payoff is rare** (reviewed campaigns through 11-02): 2 clear wins of ~5–15x and 1 partial harvest; the reviews count 20 of 24 campaigns as bled or closed early. Near-zero closes and expiries were graded disciplined_loss, "cost of insurance not triggered" (18 campaigns). The supported claim is narrow: far-OTM puts bought cheaply before a sharp drop can return large multiples. [2026-11-02]
- **Exit timing is graded by the post-exit path** (reviews, Jun to Oct). Closing before a further fall drew execution_mistake (Jul 1500-P at $2.30, Jun 1500-P, Jun 1000-P); the 05-28 to 06-07 drop came after the 1500/1000 puts were closed. Selling the 1800-P into ~$2113 drew win ×3. A sub-minute round trip drew execution_mistake (10-26). [2026-11-02]
- **No delta range or payoff frequency can be stated.** Older purchase deltas are unrecorded. Oct–Nov Dec-25 and Jan-29 entries ran delta -0.03 to -0.12 (notes). Modelled payoff on a -40% shock was ~12–17x at asks $7–$10 and ~8–12x at $30–36, loss capped at premium; modelled, not realized. [2026-11-13]
- **Resting bids rarely filled; fills came on breaks** (notes, 09-13 to 11-13). Resting 1400/1500/1600-P bids failed at zero fill 90+ times (09-13 to 09-22), ~50+ times (10-05/06) and ~20 per session through 11-13. Fills came during breaks (10-07, 11-05, 11-13) or after a gtc change (10-23): ask-floor loosening as spot fell, a four-episode inference. A small Dec-25 1500-P tranche on 11-05 (~$50, fill ~$11.7 against ~$7 quoted) is known only from a hypothesis review.
- **Budget, not signal, usually bound** (notes, 03-13 to 11-13). Margin and budget were the binding constraint except around 08-05/07, 08-28, 09-18 to 09-22, 10-07, 10-23, 11-05 and 11-13; put budget was ~$0.21 after 10-23 and ~$22.93 on 11-07.
- **Earlier execution drift** (reviewed, 03-29 to 06-24): a 03-29 review argued against buying at 20.99% skew, yet 1600-P buys occurred; late-May Jul 1500-P fills ran $24.8–$27.5; five IOC buy_put failures (06-23/24) preceded a $12.9 fill. Failure series (20 buy_put on 04-08, 20 sell_put on 04-13, zero-fill put bids from 09-13) carried execution-layer fingerprints. Order execution is compiled on revenue/efficiency.md.
- **Ask spikes are book state** (notes, ~250 observations, 2026-04-06 to 10-09). Single-tick asks far above the floor reverted within a tick or hours; put avg_spread of ~939% (09-05) and ~773% (09-12) and a 09-15 put-value reading of -23.35% were rotation artifacts. Delta tracks spot, the score follows delta, the dollar ask drifts with decay. [2026-10-07]

## Convexity Shifts

**Asks move with delta, not implied volatility** (notes, 02-26 to 11-13; at least 4 cycles plus most May to July stretches).

- The 1500-P ask rose +17% as spot fell ~$2189 to ~$2077 while the score moved ~1% (02-26 to 03-05), and the reverse held in rallies. 08-01 to 08-15 were exceptions: score up, ask down as skew compressed. From 08-19 the dollar ask stayed ~$17–$37 only because strikes moved up with spot.
- On rallies asks fell by delta dilution (1800-P $29.9 to $14.7–$16 at $2,650–$2,720); on falls they rose (table below).
- The reference best rebased repeatedly, so "% of best" is not comparable across periods.

**Repricing followed falls and never led** (09-15 to 11-14). Eight episodes are listed in the notes; their own summary lines count eight in one place and nine in another, so the table is the count. Each was partly reversed on the rebound where tested.

| Date | Fall | Put-surface move | Afterwards |
|---|---|---|---|
| 09-15 | spot -4.7% | put IV 63.9%→74.5% | not recorded |
| 10-07/08 | ~$80, low $2,408 | Nov-27 ATM pctl 0→20; modest | not recorded |
| 10-12 to 10-15 | to $2,323 | Dec-25 ATM pctl 94→99–100; 1800-P ask $25.6→~$38 | cheapening 10-16 to 10-23 |
| 10-18 | not recorded | 10Δ 58.4→60.1 | reversed in ~16h |
| 10-24 | to $2,286.6 | 10Δ 59.1→61.7; 1800-P ask ~$31→~$39 | not recorded |
| 10-30 | -6.2% to $2,202, low $2,174.8 | 10Δ 60.2→66.5, ATM +3.4, wing to `+12.4` ([tick:#198573]); 1600-P ask ~$16–18→$26–27 | put 10Δ and wing eased ~1–2 pts in ~16h; unbroken cheapening 11-01 to 11-04 |
| 11-05 | $2,441.5 to ~$2,250 (size on regimes/history.md) | ATM 50.4→53.4, 10Δ 59.3→63.8; 1600-P ask $9.3→$18.2; wing +11.8 by 11-06 | eased by 11-07 |
| 11-12 to 11-14 | spot `6.6%` below `$2,043.20` | Jan-29 10Δ `61.8→67.3`, wing `+9.0→+13.9`, ATM `52.8→53.3` | not yet tested |

- **Two falls did not reprice puts:** 11-09 (`-5.7%`; wing -0.1 in 24h, ATM +1.4) and 11-10 (back end pinned; notes). A fall is not sufficient. [2026-11-10]
- **Between falls the surface cheapened without a break** (two stretches): 10-16 to 10-23 with 10Δ 56.9–60, and 11-01 to 11-04 with 10Δ 64.5→58.0, ATM 53.3→49.2 and wing +11.1→+8.3, even through the 11-04 rally. Dec-25 put 10Δ ranged ~56.9–61.7 across spot $2,287–$2,576 (10-09 to 10-28). [2026-11-04]
- **A rally gives the wing back** (one dated case): at `$2,503.80` ([tick:#190990]) the Dec-25 wing was `+7.2`.
- **A rich wing is not a lead** (two occurrences, 10-27 to 10-31). On 10-27/28 the Dec-25 wing rose +1.6 in 24h and +3.1 in 7d to +10.3 on spot -1.3%, with put ATM falling -0.4 and all-history 10Δ mid-range (pctl 41–50): level fair, skew rich, a disagreement between bases. The 10-30 break followed about a day later, but the same wing at pctl 95–99 on 10-31 preceded only a range.
- **Skew relief arrives on the call side first** (one episode, 10-28 to 10-31). In the calm tape the call zone deflated (call ATM -2.8, call wing to +0.9) beside a put wing held at pctl 95–99; after the break call ATM fell -12.3 in 24h while put 10Δ and wing eased only ~1–2 pts in ~16h.
- **Jan-29 tail re-bid, ATM flat** (11-12 to 11-14, one expiry). The tail re-bid on each lower-spot leg with the same sign while ATM stayed within about `0.7`. The notes give four leg-by-leg changes through 11-13 (10Δ +2.1/+1.0/+0.9/+0.9, wing +1.9/+1.0/+0.7/+0.6) but count five legs; the 11-14 leg (since 19:02Z Nov-13, spot `−1.6%`: 10Δ `+0.6`, wing `+0.7`, ATM `−0.2`) makes five listed against six counted; sources disagree: read it as five to six legs. The wing is `1.5` above the Oct-30 different-expiry peak. Small legs on a pin did not re-bid the tail. [2026-11-14]
- **Skew fear stays in strikes and short tenors; the back end stays anchored** (11-12 to 11-14). Skew re-bid was short-dated and in the 76d tail; the 41d+ skew moved at most `0.5` over the same window, and the notes record the 60d–90d curve holding 52.4–53.3 across ~10 checks as spot fell 8.6% (through 11-13). [2026-11-14]
- **Short-window percentiles are not a timing tool** (notes, 09-29 to 11-13). 7d percentiles flipped inside hours on sub-1-vol-pt moves (wing 5→95 over 16h), and all-history and 7d/30d bases disagreed repeatedly (ATM pctl 99 on 7d was 43 on all-history).
- **Zero valid candidates** appeared on scattered days from 05-13 to 07-10 and while Dec-25 sat below the 45d floor (11-10/11); cause unestablished (notes).

Facts whose home is another page (no counts here):

- Put-spike clusters did not time falls, and their size does not calibrate drops; see indicators/leading.md.
- Skew floors, skew cycles and compression forecasts; see indicators/leading.md.
- Open-interest steps tracked rolls, listings and aggregation changes and did not reprice puts; see indicators/leading.md.
- Cheap-put windows and what marked them; see protection/windows.md.

## Historical Reference

Scan leadership by period. Rows before 09-22 come from analyst notes, which did not record spot, buffer or DTE per period; across the whole run the notes say strikes stepped with spot to hold ~24–33% OTM until the last step.

| Session | Spot | Expiry | Leading Strike | Buffer | Put Score | DTE (row date) |
|---|---|---|---|---|---|---|
| 2026-03-13 to 03-31 (notes) | n/a | n/a | n/a | n/a | 0.00215 for 24–50+ ticks, then 0.00246 | n/a |
| 2026-04 (notes) | n/a | n/a | 1400-P, 1500-P | n/a | 0.0023–0.0033 | n/a |
| 2026-05 to 07 (notes) | n/a | n/a | 1500-P; 1200-P and 1000-P from 06-05 | n/a | 0.0024–0.0053 | n/a |
| 2026-08 to 09-14 (notes) | n/a | Aug 28, Sep 25, Oct 30 | 1000–1400-P, then 1400-P, then 1400–1900-P; Nov-27 1800-P from 09-10 | n/a | 0.0030–0.0039 (08-19 to 09-14) | n/a |
| 2026-09-22 to 10-08 | $2,519.60–$2,762.40 | Nov 27 / Dec 25 | 2000-P, 1800-P, 1900-P | ~23.7–31.2% | 0.0029–0.0035 | ~65→51 / ~77.7 |
| 2026-10-12 to 10-29 | $2,305.60–$2,576.10 | Dec 25 | 1800-P mostly; 1600-P, 1900-P rotating | ~22–33.8% | 0.0031–0.0034 | ~74→56.6 |
| 2026-10-30 to 11-11 | $2,038.30–$2,432.20 | Dec 25 | 1500-P, 1600-P, 1400-P; 1800-P at times; unnamed once the put zone emptied (11-10/11) | ~24.0–38.3% | 0.0000–0.0035 | ~56.3→43.9 |
| 2026-11-11 18:52 to 11-13 02:59 UTC (five passes) | $1,988.30–$2,082.90 | 2027-01-29 | 1500-P (6/6 each pass) | ~24.7–27.9% | 0.0037–0.0039 | ~78.5→77.2 |
| 2026-11-13 10:48 to 11-14 03:06 UTC (three passes) | $1,907.60–$1,951.00 | 2027-01-29 | **current:** 1000-P (6/6 each pass) | ~47.6–48.7% | 0.0000–0.0046 | ~76.9→76.2 |

*Buffer = (spot − strike) / spot.* The put score stayed inside `0.0027–0.0041` from 09-15 to 11-13 through every fall and rally; intraday swings came from ask marking, rotation and one-tick artifacts (notes). It read `0.0043–0.0046` on 11-14. The score is a candidate screen, not a gate.

**Dated put-price examples (historical):** Nov-27 2000-P order values `$59.48` and `$41.74` ([order:#3693], [order:#3695]) at ~65 DTE; Nov-27 1900-P `$40.89` ([order:#4008]) and `$40.66` ([order:#4014]) at ~51 DTE; Dec-25 1800-P `$39.76` ([order:#4618]) and `$35.00` ([order:#4625]) at ~63 DTE; Jan-29 1000-P IOC fill `$11.89` ([order:#6087]) at ~76.5 DTE, consistent with the journal's 1.83 contracts near `$6.50` (unverified). These show prices that were executable, not that protection was cheap. [2026-11-14]

**Dated leadership-versus-execution examples:** on the Jan-29 1500-P, twenty resting buy orders were cancelled at zero fill between 00:54Z and 02:59Z Nov-13 ([order:#5998]–[order:#6017]) while the scan named it on every tick. After the leader rotated to the 1000-P, eighteen further resting buys cancelled at zero fill between 14:42Z and 17:43Z (from [order:#6068] to [order:#6086]; cumulative `0/1.46–1.87`) while the scan named it every tick; an IOC buy then filled at 17:49Z ([order:#6087]). Resting-order failure ended only with an IOC; scan leadership is not executability. [2026-11-13]
