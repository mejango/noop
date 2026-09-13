# Options result including expiry settlements

The overview now keeps recorded gross cashflow and also plots an estimated options result including expiry settlements. Expired short calls subtract their remaining quantity times estimated intrinsic value; expired long puts add their estimated proceeds. Call buybacks reduce the quantity left to settle.

Recorded, valued exchange settlements already contribute to gross cashflow. They suppress the corresponding estimate, so the additional curve cannot count both. A recorded settlement without a USD value stays unknown rather than being replaced with an estimate. Estimates still use the existing bounded spot lookup within 15 minutes before expiry.

The cumulative estimate includes settlements before the selected window in its opening value. Expiry-only periods receive their own bucket even when there were no fills or account snapshots. Missing valuations before or within the window mark the displayed result as partial. Fees, unreconciled external fills and open-position P&L are not included; this is not a claim of complete realized P&L or portfolio return.

The chart shows separate estimated call-cost and put-proceeds bars, labels years and aggregation periods, and uses its own report dates. Recorded activity, counts and the existing gross cashflow fields retain their accounting basis. This is a read-only reporting change, with no schema migration or historical repair.

Validation covers actual API behavior, cumulative chart arithmetic, recorded-settlement precedence, missing values, partial closes, opening balances and date boundaries. The full existing suite and new chart tests passed; the Next.js production build passed TypeScript/lint checks. Desktop and mobile previews used an isolated browser and response fixtures from a read-only production report.
