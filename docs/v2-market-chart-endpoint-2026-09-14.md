# V2 latest option-score chart point

The main market chart mixed two different contract selections at its endpoint.
Historical PUT RAW comes from the maximum raw score in the option snapshots;
the tick summary contains the raw score of the contract that wins on PUT EDGE.
The live spot overlay replaced the last historical RAW point with that summary.

Read-only production responses captured on September 14, 2026 at 12:26 UTC
reproduced the screenshot:

| Selection | Contract | Raw score |
| --- | --- | --- |
| Snapshot maximum RAW | ETH-20261030-1900-P | 0.0037275362318840584 |
| Tick maximum EDGE | ETH-20261127-1800-P | 0.002735830618892508 |

The chart substitution manufactured a 26.6% drop. The latter contract's PUT EDGE
was approximately 0.003229118, consistent with the chart's EDGE telemetry.
Both contracts were inside the entry DTE window in this reproduction.

The chart now uses its snapshot RAW and telemetry EDGE series throughout, for
both puts and calls. A spot refresh updates only spot and momentum fields at
the matching last timestamp, or appends a spot-only point if it is newer. It
does not move older option observations to the new spot timestamp. Earlier
spot responses do not insert duplicate or out-of-order rows. Missing option
observations remain missing until the chart data refreshes.

This is a dashboard-only change. Trading selection, recorded observations,
schema, and historical data are unchanged. Current/best score cards continue
to use the tick summary.

Validation exercises the actual production merge callback with distinct RAW
and EDGE winners, missing scores, and independently refreshed timestamps.
The captured production data was also rendered in desktop and mobile previews;
the latest plotted PUT RAW was 0.0037275362318840584 with no artificial collapse.
The deployed pre-fix dashboard reproduced the drop with the identical captured
responses. All six merge regressions fail against that version and pass with
the fix. The existing 931 tests and the dashboard production build also pass.
