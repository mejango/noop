'use strict';

// OI is a stock: repeated observations must not multiply an instrument's size.
// This is the observed contract population, not guaranteed exchange-wide OI.
const HOURLY_OPEN_INTEREST_SQL = `
  WITH observations AS (
    SELECT strftime('%Y-%m-%dT%H:00:00Z', timestamp) AS hour,
           open_interest,
           ROW_NUMBER() OVER (
             PARTITION BY strftime('%Y-%m-%dT%H:00:00Z', timestamp), instrument_name
             ORDER BY julianday(timestamp) DESC, id DESC
           ) AS recency
    FROM options_snapshots
    WHERE timestamp >= @start AND timestamp < @end
  )
  SELECT hour,
         CASE WHEN SUM(CASE WHEN open_interest IS NULL
           OR typeof(open_interest) NOT IN ('integer', 'real')
           OR open_interest < 0 OR open_interest > 1.7976931348623157e308
           THEN 1 ELSE 0 END) > 0
           THEN NULL ELSE SUM(open_interest) END AS value
  FROM observations
  WHERE recency = 1
  GROUP BY hour ORDER BY hour ASC
`;

function openInterestHourBounds(since, nowMs = Date.now()) {
  const sinceMs = Date.parse(since);
  if (!Number.isFinite(sinceMs) || !Number.isFinite(nowMs)) {
    throw new RangeError('Open-interest bounds require valid timestamps');
  }
  const hourMs = 60 * 60 * 1000;
  // Only complete hours inside the requested window. Suffix-free boundaries
  // include both :00Z and :00.000Z while retaining the raw timestamp index.
  return {
    start: new Date(Math.ceil(sinceMs / hourMs) * hourMs).toISOString().slice(0, 19),
    end: new Date(Math.floor(nowMs / hourMs) * hourMs).toISOString().slice(0, 19),
  };
}

module.exports = { HOURLY_OPEN_INTEREST_SQL, openInterestHourBounds };
