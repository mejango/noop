# Revenue: Windows

**No call-selling window is open (2026-11-14): chain, gap and wing legs are all unmet. Since 10-22 every above-median gap reading faded within about a day; earlier call-score peaks lasted hours to two days and tracked chain rolls, not richness.** [2026-11-14]

## Active Windows

> **Staleness notice:** values come from ticks [tick:#210584]–[tick:#210589] (2026-11-14T02:58–03:02 UTC) and the surface computed at ~03:03Z. Re-anchor before acting.

**No window. Chain, gap and wing legs are all unmet** (Nov-20 call zone, [tick:#210589]):
- **Chain:** DTE `6.2`, under the 8 DTE terminal line.
- **Gap:** ATM−RV7d `−6.9` (pctl 16 of 5,666h; Δ24h `−5.7`). Smile call VRP `−6.906` (7d/30d pctl `53/16`). Call ATM `53.1` against RV7d `60.0` and RV30d `52.5`.
- **Wing:** `+1.8` (pctl 17; smile 7d/30d `21/34`; Δ24h `+0.1`), in the bottom fifth of history. The Nov-06 chain wing sat lower, at pctl 0–1 for days (see the gate-episode table). Call RR25 `−9.0` (smile `4/11`).
- **Why the gap stays shut:** RV7d rose from `52.4` (03:00Z Nov-13) to `60.0`. Since 19:02Z Nov-13 call ATM lifted `+3.8` on the lower spot, narrowing the gap by `+3.3`, but it remains negative and the wing is thin.
- **Best-call scan:** the leader was the Nov-20 2100-C on all six ticks, call_score `73.02–76.68` ([tick:#210584]–[tick:#210589]). On a terminal chain this is proximity mechanics and carries no gate information.

**Confirmation test for an open window (inference from this wiki's inputs, all-history basis; unvalidated, never yet met):** ATM−RV7d at or above pctl 50 and wing at or above about pctl 40, at two consecutive checkpoints on a chain above 8 DTE, without a spot-driven IV spike and without RV7d as the main driver.
- Chain leg: NOT met. Gap leg: NOT met (pctl 16). Wing leg: NOT met (pctl 17).
- The next expiry, Nov-27 (13.2d), has curve ATM `51.5` and RR25 `−7.4` but no call-zone readings in the packet, so a successor-chain test cannot be run.

**Cross-reference:** the put window is closed; see protection/windows.md. Current readings beyond the deciding ones are on revenue/pricing.md. [2026-11-14]

## Historical Windows

Two records with different evidence. The premium-environment table (February to November) rests on analyst journal notes, the lowest tier: no reviewed campaign from 2026-02 to 09-25 records IV or a percentile at entry. The gate-episode table (October to November) uses the all-history percentile basis and cites ticks where the live record had them. Short-call outcomes, strike distance, buybacks and order execution are compiled on revenue/efficiency.md; the rows below name outcomes only to close a window episode.

### Premium environment and call-selling windows by period (journal notes)

| Period | Call IV as noted | Best call score | What the window did |
|---|---|---|---|
| 2026-02 | not recorded; on 02-26 the best March call candidates were ~2x the 7-day average premium (~`$4.06`) | not recorded | three weekly sales kept full premium (`$241`, `$247.20`, `$187.80`) [review:#36] [review:#42] [review:#48]; spot stayed below strike in all three, two within ~3.7% of `$2200`, so three wins do not show the windows were safe |
| 03-13 to 03-17 | ~`82–87%` | not recorded | highest noted call IV before August; no gate or percentile reading exists for it |
| 04-08 to 04-20 | ~`60–66%` (04-08 to 04-11), `71.5–78.1%` (04-12 to 04-20) | `57–72` | the 45% margin cap and order rejections bound capacity, so much of the window was not executable (see revenue/efficiency.md) |
| 06-16 to 06-30 | `51%–66.5%` | not recorded | a thin `23.3` reading on 06-14 preceded a +9% move: thin premium did not mark a safe window |
| 07-01 to 07-15 | `46.4%–57.5%`, then `42.7%–52.2%`; put IV `67–73%` | `34.8–67.1` against the 65 gate (0 on 07-08) | call IV fell `56.3%→46.4%`; scores tracked spot delta and chain rotation; three 1900-C sells on 07-08 were blocked by the margin cap |
| 07-16 to 07-24 | `44–64%` (put `60–72%`) | `36–49` on the terminal Jul-24 chain; `35–38→59–65` on the 07-19 roll to Jul-31 with spot flat | the roll lift faded as OI unwound; the put/call IV gap narrowed 22pp to ~4pp without lifting scores durably; a thin chain (spreads 273%) read `9.72` for hours on 07-21 |
| 07-25 to 07-31 | `50–62%` | `37–47` (07-25), `70–88` (07-26/27, peak `88.09`), then `55–67`, `46.8`, `33` (07-31) | peak lasted about two days and tracked chain rotation and spot delta; sales made at 65–88 met only a `$1,973.9` peak and closed at ~81–83%; the 07-28 2100-C sell at `$8.90` failed post-only at the peak |
| 08-15 to 08-23 | `35.7%–36.5%` (08-15, noted as the lowest), then `94.8%–101.3%` (08-21 peak, 39.5pp over put ~`62%`) | ~`48–51` on 08-18/19; `81.7–140` (08-19 to 08-23) | the 08-19 sales came after a 9-day neutral lock, into a breakout; both strikes expired ITM, graded risk_mistake [review:#187] [review:#190] [review:#194]. Richness arrived after the sale, not before it |
| 09-10 to 09-15 | `59.6–66%`, `68.5%` (09-13, 5.2pp over put at 5 DTE, read as an expiry artifact), `79.1%` (09-14, 15.8pp over put `63.3%`), `73–79%` (09-15) | spike peak `106` at `$2,619` (09-11); `96–107` after the Sep-18 chain cleared | the 09-11 spike made a rich window (2900-C `$12` bid) that closed within hours; after the roll, Sep-25 calls sold at `$8.1–$11.1` (~`$103` P&L), credited by reviews as real rich premium, yet met a rally to `$2,629–$2,801` and closed at 62–86% |
| 09-20 to 09-29 | `47%–57%` (09-20 to 09-26); put above call from 09-22 | ~`91–99` on the Oct-09 sales | IV−RV7d read pctl 98–99 on 09-28/29; reviews credited the sales at 59% IV; a probe to `$2,765.2` faded and the calls were bought back at 60–66% |
| 10-01 to 10-31 | `33.6` (10-03, Oct-09 ATM, pctl 1–2), `42.9` (10-04, Oct-16 ATM, pctl 27, first reading above RV7d), `38.8–44` (10-07/08); Nov-06 ATM `47–51` (10-28/29), `64.9` at the 10-30 spike, `53–56` by 10-31 | `54–129`; `129` at 02:14Z 10-07 faded to `75` within ~90 min; `62–107` across 10-28 to 10-31 | gate readings above the median were brief (next table); broad call spreads averaged `13.6–35.5%` against `8.6–12.7%` for puts across 10-28 to 10-31, so calls cost ~1.6x to 3.7x more to cross |
| 11-01 to 11-13 | ~`45–65%` | `57–103.5` | call IV never re-bid in the break lower; the gap closed on RV7d and the Nov-20 chain went terminal on 11-12 |

- **Put/call IV relation swung through the year without marking a window.** Call sat ~11–24pp below put in July and August, ~35–40pp above on 08-21, within ±1–5pp on 09-10 to 09-13, above put on 09-14/15 and below it from 09-22 through November. [2026-11-13]
- **IV−RV7d tracks the RV7d window, not IV.** It read pctl 98–99 on 09-28/29 and pctl 0 on 10-13 to 10-15 only because RV7d moved. [2026-10-15]
- **Credited sales were single outcomes.** Reviews credited the post-Sep-18 and Oct-09 sales as rich premium; the 08-19 sales, made in a similar score regime, were risk_mistake. One credited sale does not define a window. [2026-10-02]
- Spot levels and regime arcs behind these periods are on regimes/history.md.

### Dated call-entry pricing examples (outcomes are in revenue/efficiency.md)

| Date (UTC) | Chain | Pricing context at entry |
|---|---|---|
| 2026-09-27 to 10-15 | ETH-20261009 / 1016 / 1023, 2600–3200 strikes | ~9.6–18.5% OTM; several near ~11%, some with the harvest gate closed |
| 2026-10-21 / 10-26 | ETH-20261030-2800-C / 20261106-2650-C | ~11.5% OTM at `$2510` ([review:#261], sold [order:#4345]); ~14% OTM ([order:#4629]) |
| 2026-10-29/30 | ETH-20261106-2500-C / 2450-C / 2425-C | sold into a stress-driven gap with a negative wing ([order:#4652], [order:#4646], [order:#4653]) |
| 2026-11-01 to 11-06 | ETH-20261113-2550-C / 2700-C / 2425-C | 2550-C ([order:#4677]) with wing pctl 3; 2700-C ([order:#4699]) with ATM below RV7d; 2425-C ([order:#5237]) at ~6.4 DTE |
| closed 2026-11-08 / 11-09 | ETH-20261113-2525-C / 2500-C / 2450-C / 2475-C / 2425-C | 3d finals disciplined_win ([review:#288]–[review:#292]); entry-time gate readings not in the packet |

The OTM figures describe where past fills happened; they are not a minimum or a maximum. A profitable review does not validate a gate-closed or thin-buffer entry ([lesson:process.decision_quality], [lesson:short_call.strike_and_sizing]). [2026-11-14]

### Call-gate episodes (ATM−RV7d, all-history pctl)

Above pctl 50 since 2026-10-22: 2 readings on the Oct-30 chain, 21 on the Nov-06 chain, 9 on the Nov-13 chain, and one on the Nov-20 chain (pctl 53 at 18:46Z Nov-08, followed within eight hours by pctl 16). Every above-median reading faded within hours to about a day; no gate has held for two consecutive sessions. Sample: Oct-18 to Nov-14, with the early-October rows added from journal notes.

| Period | Chain | Gap (pctl) | Wing pctl | Driver and outcome |
|---|---|---|---|---|
| 2026-10-01 to 10-03 (notes) | Oct-09, terminal | IV below RV7d by `−5` to `−6`; call ATM pctl 1–15 | spike to 85–95 | OI surged +28–115% with no IV response; the wing spike and RR25 swinging `+3→−0.9` with spot flat were expiry gamma artifacts |
| 2026-10-04 to 10-09 (notes) | Oct-16 | pctl 83–91 (10-07/08), `+9.8` (97) at 10-08 midday, 32 by evening, 27 on 10-09 | 0–7 | opened as RV7d compressed to ~`32` (call ATM `40.1`); shut as the `$2,408` low lifted RV7d `32→44`. Notes read it as richest in weeks but uninvestable on a dying chain; sales on the Oct-16 and Oct-23 chains still earned `$24–$70` as spot fell |
| 2026-10-18 to 10-29 | Oct-30, then Nov-06 | `−6.6` to `+12.4` (17–92) | 8–42 | highs from RV7d roll-off; faded within hours |
| 2026-10-30 early | Nov-06 (7.3d) | `+15.8` (98) | `−3.4` (0) | spot `−6.1%`, call ATM `48.3→64.9` ([tick:#198576]); back to pctl 66 within about 8h |
| 2026-10-30 to 11-08 | Nov-06, then Nov-13 | `−5.7` (18) to `+7.5` (72) | 0–43 | spot spike, then RV7d `44.1→59.5` |
| 2026-11-08 to 11-12 midday | Nov-20 (11.9d–7.9d) | `+3.2` (53) to `−14.6` (8) | 17–43 | RV7d `51.0→67.1` after the channel break ([tick:#205243]); gap at pctl 8 for about two days |
| 2026-11-12 18:59Z to 11-14 03:03Z | Nov-20 (7.5d–6.2d) | `−1.9` (32) to `−10.2` (11), then `−6.9` (16) | 25 to 5, then 17 | RV7d-led recovery (`61.8→52.4`) reversed as RV7d rose back to `60.0` on new lows; the Nov-14 partial gain was call-ATM-led (`+3.8` on a lower spot) and left the gap negative; chain terminal |

- **Gap legs by driver.** RV7d-led: the early-October flip, the Nov-09 collapse, the Nov-10 deepening and the Nov-13 closures to 19:02Z. Implied-led: 10-28/29 (call ATM `−2.8`, gap `+12.4` to `+9.4`, pctl 79, RV7d steady near `39.6`), the 10-30 deflation (`+15.8→+6.3`), the Nov-11 02:54Z, Nov-12 02:59Z and 10:58Z legs, and the Nov-14 03:03Z leg. On 10-31 the gap widened to `+6.7` (pctl 68) only because RV7d rolled `49.7→47.7`. [2026-11-14]
- **Wide gap with a floor wing.** On 10-28/29 the call wing sat at `+0.9` to `+1.8` (smile pctl 1–10, the lowest of the sample) while the put wing stayed at pctl 95–100. A wide gap with a thin call wing (pctl 0–26) reflects sticky IV over realized, not richer upside premium. [2026-10-31]
- **Score did not track the gate.** best_call score read `63.8–103.5` across the November cycle and `73.02–76.68` at gap pctl 16; it read `97.26` at call VRP pctl 0 on Nov-09 ([tick:#205647]). [2026-11-14]
- **Gate readings and outcomes diverged.** Notes called wide gaps with a wing at pctl 0–15 and best bids of `$3–$8` unpaid, thin premium (10-28 to 10-31); reviews found that read contradicted by call sales at `$9.7–$10.6` that profited as spot fell, and credited waiting only as bounded. Profit there came from spot falling, not from an open gate ([lesson:process.decision_quality]). [2026-10-31]

### Put windows (cross-reference; canonical home is protection/windows.md)
Dec-25 puts were cheapest on 10Δ on the morning of 2026-10-23 (10Δ pctl 8, ATM pctl 12, [tick:#193046]–[tick:#193051]). The Jan-29 put zone is closed at the latest read. [2026-11-14]

## Window Indicators

Nothing is confirmed as a leading signal for a call-selling window: no reviewed campaign before October records IV or skew at open, and the gate test above is an inference that has never been met. The entries below are conditions to check, then signals that failed.

Signals observed **preceding** executable call-selling windows:
- **Chain roll with spot flat (best-supported driver of the score, not of richness).** Rolls lifted the best call score ~20–35 points on 07-19, 07-26, 09-13/14 and 10-04, and the lift decayed over 1–4 days. A roll reading is chain composition; confirm the gap and wing separately. The roll-lift tally is on indicators/leading.md. [2026-10-04]
- **Clearance of a crowded expiry.** One episode: the harvest gate opened only after the Sep-18 chain cleared (OI `−38.6%`/`−36.4%`, score `96–107`), faster than the hypothesized 24–36h. The October repeat was weak: the Oct-16 chain opened 10-04 at ATM pctl 27 with score ~`82–91`, and the realised IV−RV gap reversed within ~4 days on RV7d. [2026-10-08]
- **Call ATM above RV7d, checked on both legs.** Each widening came from a different mix of legs and several reversed within hours. The two RV7d-led closures on Nov-12 and early Nov-13 left the gap negative and the next leg undid them; the Nov-14 call-ATM-led narrowing is the same single-checkpoint kind. Check both legs at the entry checkpoint. [2026-11-14]
- **Non-terminal chain with a non-floor wing.** Across the late-October and November readings the wing oscillated at or below the median (between pctl 5 and 43), so one reading is not a second consecutive checkpoint; under 8 DTE the chain test is unmet whatever the wing reads. [2026-11-13]
- **Strike distance at fill.** Reviewed fills span roughly `9.6–18.5%` OTM; that range describes past fills and is not a minimum or a maximum, and a profitable fill near `11%` does not validate a thin-buffer entry. See revenue/efficiency.md and [lesson:short_call.strike_and_sizing]. [2026-10-24]
- **Spot/call-ATM relationship (experimental, descriptive only).** Spot `−6.1%` into 01:40Z Oct-30 came with call ATM `+16.6`, and `−7.0%` into Nov-05 evening with `+6.7`; from 18:46Z Nov-08 spot fell `−14.5%` while Nov-20 call ATM fell `−1.0`, so the sign did not hold over the window, though the latest leg (`−1.6%`, `+3.8`) matched. Coefficients are on indicators/correlations.md. [2026-11-14]

Signals confirmed **not predictive** or unreliable for window quality:
- **Call_score as a harvest signal.** It ran `0–67` in early July, `20–88` in July and August, `81.7–140` on 08-19 to 08-23, `22–117` in September, `54–129` in October and `57–103.5` in November while call VRP spanned its full range. Outside rolls it tracked spot delta, call IV in the August breakout, and expiry theta in the last ~6–8 DTE. [2026-11-10]
- **A high score or rich premium as safety.** Sales at rich readings (Apr-3; Aug-28 at a ~65 gate; Sep-25 at ~`87–96`) preceded moves through or toward the strike; the Oct-09 sales at ~`91–99` met a probe that faded. The 07-04 breakout took a ~14% buffer to ~0 in days: distance, not score, decided safety. [2026-10-02]
- **Thin premium as safety.** `23.3` on 06-14 preceded +9%, ~`48–51` on 08-18/19 preceded the August breakout, and call ATM ~`47` below RV7d on 11-04 preceded +5% in about an hour. Thin-premium waits in July cost nothing, but thinness says nothing about the next move. [2026-11-04]
- **Closer strikes outscoring buffered ones.** A closer-to-money strike outscored a buffered one by delta, not richness (the 3000-C, ~10% OTM, scored above the 3100-C on 10-05/06). Single-tick thin-chain prints (`9.72` on 07-21) are mechanical. [2026-10-06]
- **Call wing extremes.** Pctl 99 on a sub-8 DTE chain (or 95 on a 5-DTE chain) unwound within about a day; the Nov-06 wing sat at pctl 0–1 for days; the Nov-20 wing then fell to pctl 5 and rebounded to 17 with no durable level. A wing spike on a terminal chain is an expiry artifact. [2026-11-14]
- **Call-over-put IV inversion near expiry.** Record OI (`8.1k→11.1k`) and call IV `68.5%` against put `63.3%` at 5 DTE on the Sep-18 chain marked no harvest window until the roll; the score fell to 53–74% of its cycle best at 5–7 DTE. [2026-09-14]
- **Single gate-open gap readings as multi-session conditions.** One above-median Nov-20 reading was followed within eight hours by pctl 16; none of the October or November readings held two sessions. [2026-11-10]
- **RV7d roll-off as a timing forecast.** RV7d moved in both directions (`39.2→…→67.1→61.8→53.0→52.4→58.3→59.5`). Calls of 10-09 to 10-18 that RV7d would roll off and open the gate were right only in direction and ~10 days late. [2026-11-13]
- **"Gate closed, so a sale will lose."** November hypotheses that call ATM minus RV7d would stay below 0 held on the gap, yet reviewed sales then still profited as spot fell. This is not a licence: profit does not validate a gate-closed entry ([lesson:process.decision_quality]). [2026-11-09]
- **Low skew with a low score.** Sub-2% skew with a score of `37–47` (07-25) was a closed window; July scores hovered around the gate while OI swung ±15–170% and skew moved `26%→1.11%`. [2026-07-31]
- **Flow, open-interest, momentum and put-score-lead calls.** None timed a call-score peak or fade or protected a short call; see indicators/leading.md for the record. [2026-11-13]
- **Cross-series correlations as rules.** The spot/call-ATM relationship is exploratory; see indicators/correlations.md. [2026-11-10]
