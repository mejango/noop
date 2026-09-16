'use strict';

// Checked-in extraction of explicitly selected PURE V2 helpers. See provenance.json.
// No live script import, environment, filesystem, API, DB, or timer is reachable.
// This compatibility layer deliberately preserves binary-number research math;
// bounded contract amounts are separately checked with exact decimal arithmetic.
function createLegacyPolicy(policy, researchState = {}) {
  const PUT_ROLL_DTE_THRESHOLD = Number(policy.put_roll_dte);
  const CALL_BUYBACK_PROFIT_THRESHOLD = Number(policy.call_profit_capture_pct);
  const PUT_MONETIZATION_PROFIT_THRESHOLD = Number(policy.put_monetization_profit_pct);
  const PUT_MONETIZATION_MAX_TRANCHE_FRACTION = Number(policy.put_monetization_tranche_ratio);
  const CALL_EXPOSURE_CAP_PCT = Number(policy.call_entry_margin_ratio);
  const CALL_BREAKOUT_OVERRIDE_CAP_PCT = Number(policy.call_breakout_margin_ratio);
  const CALL_EXPOSURE_BUFFER_PCT = Number(policy.call_execution_buffer_ratio);
  const CALL_ENTRY_CAP_PCT = Math.max(0, CALL_EXPOSURE_CAP_PCT - Number(policy.call_entry_caution_buffer_ratio));
  const getCallExposureLimitPct = target => Math.min(1, Math.max(0, Number(target) || 0) + CALL_EXPOSURE_BUFFER_PCT);
  const CALL_EXPOSURE_LIMIT_PCT = getCallExposureLimitPct(CALL_EXPOSURE_CAP_PCT);
  const CALL_BREAKOUT_OVERRIDE_LIMIT_PCT = getCallExposureLimitPct(CALL_BREAKOUT_OVERRIDE_CAP_PCT);
  const CALL_BREAKOUT_DERIVATIVES = new Set(['moving', 'slanted', 'steep']);
  const botData = structuredClone(researchState);
  const PUT_DELTA_RANGE = [Number(policy.put_min_delta), Number(policy.put_max_delta)];
  const BUY_PUT_ADVISORY_DTE_RANGE = [Number(policy.put_min_dte), Number(policy.put_max_dte)];
  const CALL_DELTA_RANGE = [Number(policy.call_min_delta), Number(policy.call_max_delta)];
  const CALL_EXPIRATION_RANGE = [Number(policy.call_min_dte), Number(policy.call_max_dte)];
  const SELL_CALL_FALLBACK_MIN_BID = Number(policy.call_fallback_min_bid);
  const SELL_CALL_FALLBACK_MIN_SCORE = Number(policy.call_fallback_min_score);
  const SELL_CALL_EDGE_REFERENCE_DTE = Number(policy.call_score_reference_dte);
  const SELL_CALL_EDGE_DTE_EXPONENT = Number(policy.call_score_dte_exponent);
  const getBuyPutDteNormalizationFactor = dte => Number(dte) > 0
    ? Math.pow(Number(dte) / Number(policy.put_score_reference_dte), Number(policy.put_score_dte_exponent)) : 0;
  const normalizeBuyPutScore = (rawScore, dte) => Number(rawScore) > 0 && getBuyPutDteNormalizationFactor(dte) > 0
    ? Number(rawScore) * getBuyPutDteNormalizationFactor(dte) : 0;
  const getBuyPutPriceForEdgeScore = (absDelta, edgeScore, dte) => Math.abs(Number(absDelta)) > 0 && Number(edgeScore) > 0 && getBuyPutDteNormalizationFactor(dte) > 0
    ? (Math.abs(Number(absDelta)) * getBuyPutDteNormalizationFactor(dte)) / Number(edgeScore) : null;
  const normalizeSellCallScore = (rawScore, dte) => Number(rawScore) > 0 && Number(dte) > 0
    ? Number(rawScore) * Math.pow(SELL_CALL_EDGE_REFERENCE_DTE / Number(dte), SELL_CALL_EDGE_DTE_EXPONENT) : 0;

  const floorOptionPriceCents = (value) => {
    const numeric = Number(value);
    if (!(numeric > 0)) return null;
    return Math.max(0.01, Math.floor((numeric + 1e-9) * 100) / 100);
  };

  const getBuyPutEntryPricing = ({ askPrice, absDelta, dte, minScore = null, targetScore = null }) => {
    const liveAsk = Number(askPrice) || 0;
    const delta = Math.abs(Number(absDelta) || 0);
    const liveRawScore = liveAsk > 0 && delta > 0 ? delta / liveAsk : 0;
    const liveScore = normalizeBuyPutScore(liveRawScore, dte);
    const min = Number(minScore ?? 0);
    const target = Number(targetScore ?? 0);
    const requiredScore = Math.max(min > 0 ? min : 0, target > 0 ? target : 0);
    const thresholdPrice = requiredScore > 0 && delta > 0
      ? floorOptionPriceCents(getBuyPutPriceForEdgeScore(delta, requiredScore, dte))
      : null;

    let limitPrice = liveAsk > 0 ? liveAsk : null;
    let priceSource = 'live_ask';
    if (thresholdPrice != null && (!(limitPrice > 0) || thresholdPrice < limitPrice)) {
      limitPrice = thresholdPrice;
      priceSource = 'score_threshold';
    }

    const plannedRawScore = limitPrice > 0 && delta > 0 ? delta / limitPrice : 0;
    const plannedScore = normalizeBuyPutScore(plannedRawScore, dte);
    return {
      liveRawScore,
      liveScore,
      plannedRawScore,
      plannedScore,
      limitPrice,
      thresholdPrice,
      requiredScore,
      priceSource,
    };
  };

  const parseAdvisoryOptionInstrument = (name) => {
    const parts = String(name || '').split('-');
    if (parts.length !== 4 || !/^\d{8}$/.test(parts[1])) return null;
    const expiry = new Date(`${parts[1].slice(0, 4)}-${parts[1].slice(4, 6)}-${parts[1].slice(6, 8)}T08:00:00Z`);
    const strike = Number(parts[2]);
    return {
      expiry,
      strike: Number.isFinite(strike) ? strike : null,
      optionType: parts[3],
    };
  };

  const BUY_PUT_VALUE_SIGNALS = new Set([
    'strict_fresh_best',
    'spot_drop_option_repricing_lag',
    'recent_relative_value',
    'any_actionable_buy_put',
  ]);

  const normalizeBuyPutValueSignal = (signal) => {
    const normalized = String(signal || '').trim().toLowerCase();
    if (!normalized) return null;
    if (normalized === 'fresh_best') return 'strict_fresh_best';
    if (normalized === 'repricing_lag' || normalized === 'spot_lag') return 'spot_drop_option_repricing_lag';
    if (normalized === 'relative_value' || normalized === 'recent_value') return 'recent_relative_value';
    if (BUY_PUT_VALUE_SIGNALS.has(normalized)) {
      return normalized;
    }
    return null;
  };

  const hasExplicitBuyPutValueSignal = (signal) => String(signal ?? '').trim().length > 0;

  const isKnownBuyPutValueSignal = (signal) => !hasExplicitBuyPutValueSignal(signal) || normalizeBuyPutValueSignal(signal) != null;

  const buildRulebookRequirements = ({
    putBudgetRemaining = 0,
    accountHealth = {},
    positionSnapshots = [],
  } = {}) => {
    const requirements = [];
    const putBudget = Number(putBudgetRemaining);
    if (putBudget > 1) {
      requirements.push({
        action: 'buy_put',
        type: 'entry',
        applies: 'put budget remains',
        instruction: 'Create a patient standing buy_put watcher with favorable min_score/target_score price criteria plus tight-spread, lower-IV/skew, OI-support, and crash-payoff edge context; do not chase higher delta by itself or require an immediately marketable buy.',
      });
    }

    const margin = accountHealth?.margin || {};
    const callDiscipline = accountHealth?.callMarginDiscipline || {};
    const utilizationPct = Number(callDiscipline.utilizationPct ?? margin.margin_usage_pct);
    const limitPct = Number(callDiscipline.bufferedLimitPct) * 100;
    const hasMarginState = Boolean(accountHealth?.margin);
    const marginAvailable = hasMarginState
      && !margin.is_under_liquidation
      && Number.isFinite(utilizationPct)
      && Number.isFinite(limitPct)
      && utilizationPct < limitPct;
    if (marginAvailable) {
      requirements.push({
        action: 'sell_call',
        type: 'entry',
        applies: 'call margin headroom remains',
        instruction: `Create a standing sell_call watcher only for favorable call premium. Encode value with min_score plus min_bid, DTE, delta, and margin criteria. CALL EDGE is raw bid / abs(delta), lightly normalized by (${SELL_CALL_EDGE_REFERENCE_DTE} / DTE)^${SELL_CALL_EDGE_DTE_EXPONENT} to reduce weekly expiry-roll artifacts.`,
      });
    }

    for (const snapshot of positionSnapshots || []) {
      if (snapshot?.direction === 'long' && snapshot?.option_type === 'P') {
        requirements.push({
          action: 'sell_put',
          type: 'exit',
          instrument_name: snapshot.instrument,
          applies: 'open long put',
          instruction: `Create a reduce-only sell_put watcher with put_exit_intent="roll_protection" only if DTE <= ${PUT_ROLL_DTE_THRESHOLD} and longer-dated put protection is already in the book; a roll may close the aging instrument fully. Use put_exit_intent="monetize_tail_win" only after executable PnL reaches >${PUT_MONETIZATION_PROFIT_THRESHOLD}%. Monetization must include min_exit_price/limit_price, be tranched, and retain downside protection after each sale.`,
        });
      }
      if (snapshot?.direction === 'short' && snapshot?.option_type === 'C') {
        requirements.push({
          action: 'buyback_call',
          type: 'exit',
          instrument_name: snapshot.instrument,
          applies: 'open short call',
          instruction: `Create a reduce-only buyback_call watcher with buyback_intent="profit_capture" for ${CALL_BUYBACK_PROFIT_THRESHOLD}%+ capture or patient target-capture bids, or buyback_intent="threat_management" only for genuine short-call danger. Price rising alone is not enough.`,
        });
      }
    }

    return requirements;
  };

  const formatRulebookRequirements = (requirements = []) => {
    if (!Array.isArray(requirements) || requirements.length === 0) {
      return 'No mandatory standing watchers: no put budget, no call margin headroom, and no open option positions requiring exit watchers.';
    }
    return requirements.map((req, index) => {
      const instrument = req.instrument_name ? ` ${req.instrument_name}` : '';
      return `${index + 1}. ${req.type}/${req.action}${instrument} — applies because ${req.applies}. ${req.instruction}`;
    }).join('\n');
  };

  const findMissingRulebookRequirements = (agenda = {}, requirements = []) => {
    const entryRules = Array.isArray(agenda?.entry_rules) ? agenda.entry_rules : [];
    const exitRules = Array.isArray(agenda?.exit_rules) ? agenda.exit_rules : [];
    return (requirements || []).filter((req) => {
      if (req.type === 'entry') {
        return !entryRules.some((rule) => rule?.action === req.action);
      }
      return !exitRules.some((rule) =>
        rule?.action === req.action && rule?.instrument_name === req.instrument_name
      );
    });
  };

  const buildAgendaFromValidatedRules = (rules = []) => ({
    entry_rules: (rules || []).filter((rule) => rule?.rule_type === 'entry'),
    exit_rules: (rules || []).filter((rule) => rule?.rule_type === 'exit'),
  });

  const buildCanonicalRequiredWatcherRule = (requirement, context = {}) => {
    if (requirement?.type === 'entry' && requirement?.action === 'sell_call') {
      return {
        rule_type: 'entry',
        action: 'sell_call',
        instrument_name: null,
        criteria: {
          option_type: 'C',
          delta_range: CALL_DELTA_RANGE,
          dte_range: CALL_EXPIRATION_RANGE,
          min_bid: SELL_CALL_FALLBACK_MIN_BID,
          min_score: SELL_CALL_FALLBACK_MIN_SCORE,
        },
        budget_limit: null,
        priority: 'low',
        reasoning: `Required sell-call coverage fallback: patient watcher for favorable short-dated call premium only; requires ${CALL_EXPIRATION_RANGE[0]}-${CALL_EXPIRATION_RANGE[1]} DTE, delta ${CALL_DELTA_RANGE[0]}-${CALL_DELTA_RANGE[1]}, bid >= $${SELL_CALL_FALLBACK_MIN_BID.toFixed(2)}, and DTE-normalized CALL EDGE >= ${SELL_CALL_FALLBACK_MIN_SCORE}.`,
        advisory_id: context.advisoryId || null,
        preferred_order_type: 'post_only',
      };
    }

    if (requirement?.type === 'exit' && requirement?.action === 'sell_put' && requirement.instrument_name) {
      const snapshot = (context.positionSnapshots || []).find((item) => item?.instrument === requirement.instrument_name);
      const canRoll = snapshot
        && Number(snapshot.dte) <= PUT_ROLL_DTE_THRESHOLD
        && hasLongerDatedPutProtectionSnapshot(snapshot, context.positionSnapshots || []);
      if (canRoll) {
        return {
          rule_type: 'exit',
          action: 'sell_put',
          instrument_name: requirement.instrument_name,
          criteria: {
            put_exit_intent: 'roll_protection',
            conditions: [
              { field: 'dte', op: 'lte', value: PUT_ROLL_DTE_THRESHOLD },
            ],
            condition_logic: 'all',
            requires_longer_dated_protection: true,
          },
          budget_limit: null,
          priority: 'low',
          reasoning: `Required long-put coverage fallback: reduce-only roll watcher because ${requirement.instrument_name} is inside the ${PUT_ROLL_DTE_THRESHOLD} DTE roll window and longer-dated put protection is already in the book.`,
          advisory_id: context.advisoryId || null,
          preferred_order_type: 'ioc',
        };
      }

      const entryPrice = Number(snapshot?.avg_entry_price);
      const minExitPrice = entryPrice > 0
        ? Number((entryPrice * (1 + PUT_MONETIZATION_PROFIT_THRESHOLD / 100) + 0.01).toFixed(2))
        : null;
      if (!(minExitPrice > 0)) return null;

      return {
        rule_type: 'exit',
        action: 'sell_put',
        instrument_name: requirement.instrument_name,
        criteria: {
          put_exit_intent: 'monetize_tail_win',
          conditions: [
            { field: 'unrealized_pnl_pct', op: 'gt', value: PUT_MONETIZATION_PROFIT_THRESHOLD },
          ],
          condition_logic: 'all',
          min_exit_price: minExitPrice,
          tranche_fraction: PUT_MONETIZATION_MAX_TRANCHE_FRACTION,
          retain_downside_protection: true,
        },
        budget_limit: null,
        priority: 'low',
        reasoning: `Required long-put coverage fallback: dormant tail-win monetization watcher; only sell up to ${(PUT_MONETIZATION_MAX_TRANCHE_FRACTION * 100).toFixed(0)}% at executable PnL > ${PUT_MONETIZATION_PROFIT_THRESHOLD}% with min exit price $${minExitPrice.toFixed(2)}, preserving remaining downside protection.`,
        advisory_id: context.advisoryId || null,
        preferred_order_type: 'post_only',
      };
    }

    if (requirement?.type !== 'exit' || requirement?.action !== 'buyback_call' || !requirement.instrument_name) {
      return null;
    }

    const snapshot = (context.positionSnapshots || []).find((item) => item?.instrument === requirement.instrument_name);
    const entryPrice = Number(snapshot?.avg_entry_price);
    const maxBuybackPrice = entryPrice > 0
      ? floorOptionPriceCents(entryPrice * (1 - CALL_BUYBACK_PROFIT_THRESHOLD / 100))
      : null;
    const criteria = {
      buyback_intent: 'profit_capture',
      conditions: [
        { field: 'unrealized_pnl_pct', op: 'gte', value: CALL_BUYBACK_PROFIT_THRESHOLD },
      ],
      condition_logic: 'all',
      target_capture_pct: CALL_BUYBACK_PROFIT_THRESHOLD,
    };
    if (maxBuybackPrice != null) {
      criteria.max_buyback_price = maxBuybackPrice;
    }

    return {
      rule_type: 'exit',
      action: 'buyback_call',
      instrument_name: requirement.instrument_name,
      criteria,
      budget_limit: null,
      priority: 'low',
      reasoning: maxBuybackPrice != null
        ? `Required short-call coverage fallback: patient synthetic reduce-only buyback watcher at ${CALL_BUYBACK_PROFIT_THRESHOLD}%+ capture; max bid $${maxBuybackPrice.toFixed(2)} from entry $${entryPrice.toFixed(2)}.`
        : `Required short-call coverage fallback: reduce-only buyback watcher only when executable capture reaches ${CALL_BUYBACK_PROFIT_THRESHOLD}%+.`,
      advisory_id: context.advisoryId || null,
      preferred_order_type: maxBuybackPrice != null ? 'post_only' : 'ioc',
    };
  };

  const isSellCallCandidateInStrategyRange = (dte, delta) => (
    Number.isFinite(dte)
    && dte >= CALL_EXPIRATION_RANGE[0]
    && dte <= CALL_EXPIRATION_RANGE[1]
    && Number.isFinite(delta)
    && delta >= CALL_DELTA_RANGE[0]
    && delta <= CALL_DELTA_RANGE[1]
  );

  const parseMaybeJsonObject = (value) => {
    if (!value) return null;
    if (typeof value === 'object' && !Array.isArray(value)) return value;
    if (typeof value !== 'string') return null;
    try {
      const parsed = JSON.parse(value);
      return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : null;
    } catch {
      return null;
    }
  };

  const isBuybackProfitCaptureCondition = (condition) => {
    if (!condition || condition.field !== 'unrealized_pnl_pct') return false;
    if (!['gte', 'gt'].includes(condition.op)) return false;
    return Number.isFinite(Number(condition.value ?? condition.threshold));
  };

  const REDUNDANT_BUYBACK_CAPTURE_FIELDS = new Set(['dte', 'mark_price']);

  const isThreatManagementBuybackCriteria = (criteria) => (
    criteria?.allow_below_profit_floor === true
    || criteria?.buyback_intent === 'threat_management'
  );

  const getBuybackProfitCaptureCondition = (criteria) => {
    const parsed = parseMaybeJsonObject(criteria);
    if (!parsed || !Array.isArray(parsed.conditions)) return null;
    const condition = parsed.conditions.find(isBuybackProfitCaptureCondition);
    if (!condition) return null;
    return {
      op: condition.op,
      threshold: Number(condition.value ?? condition.threshold),
    };
  };

  const conditionPasses = (actual, op, threshold) => {
    if (!Number.isFinite(actual) || !Number.isFinite(threshold)) return false;
    if (op === 'gt') return actual > threshold;
    if (op === 'gte') return actual >= threshold;
    if (op === 'lt') return actual < threshold;
    if (op === 'lte') return actual <= threshold;
    return false;
  };

  const getRuleIntent = (criteria, keys) => {
    for (const key of keys) {
      const value = String(criteria?.[key] || '').trim();
      if (value) return value;
    }
    return null;
  };

  const getPutExitIntent = (criteria) => getRuleIntent(criteria, ['put_exit_intent', 'exit_intent']);

  const getBuybackIntent = (criteria) => getRuleIntent(criteria, ['buyback_intent']);

  const isFiniteNumber = (value) => Number.isFinite(Number(value));

  const isRangeWithin = (range, min, max) => (
    Array.isArray(range)
    && range.length === 2
    && isFiniteNumber(range[0])
    && isFiniteNumber(range[1])
    && Number(range[0]) <= Number(range[1])
    && Number(range[0]) >= min
    && Number(range[1]) <= max
  );

  const getConditions = (criteria) => Array.isArray(criteria?.conditions) ? criteria.conditions : [];

  const hasCondition = (criteria, predicate) => getConditions(criteria).some(predicate);

  const hasThresholdCondition = (criteria, field, ops, threshold, mode = 'at_least') => (
    hasCondition(criteria, (condition) => {
      const value = Number(condition?.value ?? condition?.threshold);
      if (condition?.field !== field || !ops.includes(condition?.op) || !Number.isFinite(value)) return false;
      return mode === 'at_most' ? value <= threshold : value >= threshold;
    })
  );

  const hasLongerDatedPutProtectionSnapshot = (snapshot, snapshots = []) => {
    const currentDte = Number(snapshot?.dte);
    if (!Number.isFinite(currentDte)) return false;
    return (snapshots || []).some((candidate) =>
      candidate?.instrument !== snapshot?.instrument
      && candidate?.direction === 'long'
      && candidate?.option_type === 'P'
      && Number(candidate?.amount) > 0
      && Number(candidate?.dte) > currentDte
    );
  };

  const getTotalLongPutAmount = (positions = []) => (positions || [])
    .filter((position) => position?.direction === 'long' && position?.instrument_name?.endsWith('-P'))
    .reduce((total, position) => total + Math.max(0, Number(position.amount) || 0), 0);

  const leavesDownsideProtectionAfterSale = (position, positions = [], sellAmount = 0) => {
    const totalLongPutAmount = getTotalLongPutAmount(positions);
    const amount = Math.max(0, Number(sellAmount) || 0);
    return totalLongPutAmount - amount > 1e-9 && Number(position?.amount) - amount > 1e-9;
  };

  const getSellPutExitAmount = (rule, criteria, position, values) => {
    const fullAmount = Math.max(0, Number(position?.amount) || 0);
    if (fullAmount <= 0 || rule?.action !== 'sell_put') return fullAmount;

    const intent = getPutExitIntent(criteria);
    const dte = Number(values?.dte);
    const pnlPct = Number(values?.unrealized_pnl_pct);
    const isTailWin = intent === 'monetize_tail_win'
      || (Number.isFinite(dte) && dte > PUT_ROLL_DTE_THRESHOLD
        && Number.isFinite(pnlPct) && pnlPct > PUT_MONETIZATION_PROFIT_THRESHOLD);
    if (!isTailWin) return fullAmount;

    const requestedFraction = Number(criteria?.tranche_fraction ?? criteria?.max_tranche_fraction);
    const fraction = Number.isFinite(requestedFraction) && requestedFraction > 0
      ? Math.min(requestedFraction, PUT_MONETIZATION_MAX_TRANCHE_FRACTION)
      : PUT_MONETIZATION_MAX_TRANCHE_FRACTION;
    return Math.max(0, Math.min(fullAmount * fraction, fullAmount - 1e-9));
  };

  const getAdvisorSellPutLimitPrice = (criteria) => {
    const explicit = Number(criteria?.min_exit_price ?? criteria?.limit_price ?? criteria?.target_exit_price);
    return Number.isFinite(explicit) && explicit > 0 ? explicit : null;
  };

  const getLongPutFairValueProof = (position, values = {}) => {
    const entryPrice = Number(position?.avg_entry_price);
    if (!(entryPrice > 0)) return null;

    const parsed = parseAdvisoryOptionInstrument(position?.instrument_name);
    const spotPrice = Number(values?.spot_price);
    const intrinsicValue = parsed?.optionType === 'P' && Number.isFinite(parsed.strike) && spotPrice > 0
      ? Math.max(0, parsed.strike - spotPrice)
      : 0;
    const markPrice = Number(values?.mark_price);
    const normalizedMarkPrice = Number.isFinite(markPrice) && markPrice > 0 ? markPrice : 0;
    const fairValuePrice = Math.max(
      normalizedMarkPrice,
      intrinsicValue
    );
    if (!(fairValuePrice > 0)) return null;

    return {
      price: fairValuePrice,
      pnlPct: ((fairValuePrice - entryPrice) / entryPrice) * 100,
      source: intrinsicValue > normalizedMarkPrice ? 'intrinsic_value' : 'mark_price',
    };
  };

  const getPatientSellPutPlan = (rule, criteria, position, values = {}) => {
    if (!rule || rule.action !== 'sell_put') return null;
    if (getPutExitIntent(criteria) !== 'monetize_tail_win') return null;

    const limitPrice = getAdvisorSellPutLimitPrice(criteria);
    const entryPrice = Number(position?.avg_entry_price);
    if (!(limitPrice > 0) || !(entryPrice > 0)) return null;

    const pnlPct = ((limitPrice - entryPrice) / entryPrice) * 100;
    if (!(pnlPct > PUT_MONETIZATION_PROFIT_THRESHOLD)) return null;

    const fairValueProof = getLongPutFairValueProof(position, values);
    if (!(Number(fairValueProof?.pnlPct) > PUT_MONETIZATION_PROFIT_THRESHOLD)) return null;

    return {
      limitPrice,
      pnlPct,
      fairValuePrice: fairValueProof.price,
      fairValuePnlPct: fairValueProof.pnlPct,
      fairValueSource: fairValueProof.source,
      preferredOrderType: normalizePreferredOrderType(rule.action, rule.preferred_order_type) || 'post_only',
    };
  };

  const getBuybackTargetCapturePct = (criteria) => {
    const explicit = Number(criteria?.target_capture_pct ?? criteria?.capture_floor_pct);
    if (Number.isFinite(explicit) && explicit > 0) return explicit;
    const condition = getBuybackProfitCaptureCondition(criteria);
    return Number.isFinite(condition?.threshold) ? condition.threshold : null;
  };

  const getPatientBuybackPlan = (rule, criteria, position) => {
    if (!rule || rule.action !== 'buyback_call') return null;
    if (getBuybackIntent(criteria) !== 'profit_capture') return null;
    const preferredOrderType = normalizePreferredOrderType(rule.action, rule.preferred_order_type) || 'post_only';

    const targetCapturePct = Number(getBuybackTargetCapturePct(criteria));
    const entryPrice = Number(position?.avg_entry_price);
    if (!Number.isFinite(targetCapturePct) || targetCapturePct < CALL_BUYBACK_PROFIT_THRESHOLD || !(entryPrice > 0)) return null;

    const explicitLimit = Number(criteria?.max_buyback_price ?? criteria?.limit_price);
    const derivedLimit = entryPrice * (1 - targetCapturePct / 100);
    const limitPrice = Number.isFinite(explicitLimit) && explicitLimit > 0
      ? Math.min(explicitLimit, derivedLimit)
      : derivedLimit;
    if (!(limitPrice > 0)) return null;

    const capturePct = ((entryPrice - limitPrice) / entryPrice) * 100;
    if (capturePct + 1e-9 < targetCapturePct) return null;

    return {
      limitPrice,
      ceilingPrice: limitPrice,
      capturePct,
      entryPrice,
      targetCapturePct,
      preferredOrderType,
      priceReason: 'capture_floor_ceiling',
    };
  };

  const getBuybackCapturePctAtPrice = (entryPrice, buybackPrice) => {
    const entry = Number(entryPrice);
    const price = Number(buybackPrice);
    if (!(entry > 0) || !(price > 0)) return null;
    return ((entry - price) / entry) * 100;
  };

  const validateAdvisorRuleContract = (rule, context = {}) => {
    const criteria = parseMaybeJsonObject(rule?.criteria);
    if (!rule || !criteria) return { valid: false, reason: 'criteria must be a JSON object' };

    if (rule.rule_type === 'entry' && rule.action === 'buy_put') {
      const rawValueSignal = criteria.value_signal ?? criteria.buy_put_signal;
      if (criteria.option_type !== 'P') return { valid: false, reason: 'buy_put requires option_type P' };
      if (!isRangeWithin(criteria.delta_range, PUT_DELTA_RANGE[0], PUT_DELTA_RANGE[1])) return { valid: false, reason: `buy_put delta_range must stay within ${JSON.stringify(PUT_DELTA_RANGE)}` };
      if (!isRangeWithin(criteria.dte_range, BUY_PUT_ADVISORY_DTE_RANGE[0], BUY_PUT_ADVISORY_DTE_RANGE[1])) return { valid: false, reason: `buy_put dte_range must stay within ${JSON.stringify(BUY_PUT_ADVISORY_DTE_RANGE)}` };
      if (Object.prototype.hasOwnProperty.call(criteria, 'max_cost')) return { valid: false, reason: 'buy_put must use budget_limit/min_score/target_score, not max_cost' };
      if (!isKnownBuyPutValueSignal(rawValueSignal)) return { valid: false, reason: `unknown buy_put value_signal: ${rawValueSignal}` };
      if (!(Number(criteria.min_score) > 0)) return { valid: false, reason: 'buy_put requires min_score as a value gate' };
      return { valid: true };
    }

    if (rule.rule_type === 'entry' && rule.action === 'sell_call') {
      if (criteria.option_type !== 'C') return { valid: false, reason: 'sell_call requires option_type C' };
      if (!isRangeWithin(criteria.delta_range, CALL_DELTA_RANGE[0], CALL_DELTA_RANGE[1])) return { valid: false, reason: `sell_call delta_range must stay within ${JSON.stringify(CALL_DELTA_RANGE)}` };
      if (!isRangeWithin(criteria.dte_range, CALL_EXPIRATION_RANGE[0], CALL_EXPIRATION_RANGE[1])) return { valid: false, reason: `sell_call dte_range must stay within ${JSON.stringify(CALL_EXPIRATION_RANGE)}` };
      if (!(Number(criteria.min_score) > 0)) return { valid: false, reason: 'sell_call requires min_score as the DTE-normalized CALL EDGE gate' };
      if (!(Number(criteria.min_bid) > 0)) return { valid: false, reason: 'sell_call requires min_bid as the liquidity/premium floor' };
      const marketConditions = Array.isArray(criteria.market_conditions) ? criteria.market_conditions : [];
      if (marketConditions.some((condition) => condition?.field !== 'spot_price')) return { valid: false, reason: 'sell_call market_conditions may only use spot_price as supporting context' };
      return { valid: true };
    }

    if (rule.rule_type === 'exit' && rule.action === 'sell_put') {
      if (!Array.isArray(criteria.conditions) || criteria.conditions.length === 0) return { valid: false, reason: 'sell_put requires structured conditions' };
      const intent = getPutExitIntent(criteria);
      if (!['roll_protection', 'monetize_tail_win'].includes(intent)) return { valid: false, reason: 'sell_put requires put_exit_intent roll_protection or monetize_tail_win' };

      if (intent === 'roll_protection') {
        if (!hasThresholdCondition(criteria, 'dte', ['lt', 'lte'], PUT_ROLL_DTE_THRESHOLD, 'at_most')) return { valid: false, reason: `roll_protection requires dte <= ${PUT_ROLL_DTE_THRESHOLD}` };
        if (criteria.requires_longer_dated_protection !== true) return { valid: false, reason: 'roll_protection must require longer-dated protection in the book' };
        if (rule.preferred_order_type && rule.preferred_order_type !== 'ioc') return { valid: false, reason: 'roll_protection sell_put must use ioc/non-resting execution' };
        const snapshot = (context.positionSnapshots || []).find((item) => item?.instrument === rule.instrument_name);
        if (snapshot && !hasLongerDatedPutProtectionSnapshot(snapshot, context.positionSnapshots)) return { valid: false, reason: 'roll_protection rejected: no longer-dated long put currently in book' };
      }

      if (intent === 'monetize_tail_win') {
        if (!hasThresholdCondition(criteria, 'unrealized_pnl_pct', ['gt', 'gte'], PUT_MONETIZATION_PROFIT_THRESHOLD)) return { valid: false, reason: `monetize_tail_win requires executable unrealized_pnl_pct > ${PUT_MONETIZATION_PROFIT_THRESHOLD}` };
        if (!(getAdvisorSellPutLimitPrice(criteria) > 0)) return { valid: false, reason: 'monetize_tail_win requires min_exit_price/limit_price so sparse markets cannot force an undersell' };
        const fraction = Number(criteria.tranche_fraction ?? criteria.max_tranche_fraction ?? PUT_MONETIZATION_MAX_TRANCHE_FRACTION);
        if (!(fraction > 0) || fraction > PUT_MONETIZATION_MAX_TRANCHE_FRACTION) return { valid: false, reason: `monetize_tail_win tranche_fraction must be >0 and <=${PUT_MONETIZATION_MAX_TRANCHE_FRACTION}` };
        if (criteria.retain_downside_protection !== true) return { valid: false, reason: 'monetize_tail_win must require retained downside protection' };
      }
      return { valid: true };
    }

    if (rule.rule_type === 'exit' && rule.action === 'buyback_call') {
      if (!Array.isArray(criteria.conditions) || criteria.conditions.length === 0) return { valid: false, reason: 'buyback_call requires structured conditions' };
      const intent = getBuybackIntent(criteria);
      if (!['profit_capture', 'threat_management'].includes(intent)) return { valid: false, reason: 'buyback_call requires buyback_intent profit_capture or threat_management' };

      if (intent === 'profit_capture') {
        if (criteria.allow_below_profit_floor === true) return { valid: false, reason: 'profit_capture cannot allow below profit floor' };
        if ((criteria.condition_logic || 'all') !== 'all') return { valid: false, reason: 'profit_capture requires condition_logic all' };
        if (getConditions(criteria).some((condition) => REDUNDANT_BUYBACK_CAPTURE_FIELDS.has(condition?.field))) return { valid: false, reason: 'profit_capture buyback cannot use dte or mark_price blockers' };
        if (!hasThresholdCondition(criteria, 'unrealized_pnl_pct', ['gt', 'gte'], CALL_BUYBACK_PROFIT_THRESHOLD)) return { valid: false, reason: `profit_capture requires executable unrealized_pnl_pct >= ${CALL_BUYBACK_PROFIT_THRESHOLD}` };
      }

      if (intent === 'threat_management') {
        if (criteria.allow_below_profit_floor !== true) return { valid: false, reason: 'threat_management must set allow_below_profit_floor=true' };
        const fields = new Set(getConditions(criteria).map((condition) => condition?.field));
        if (!fields.has('delta') && !fields.has('spot_price')) return { valid: false, reason: 'threat_management requires delta or spot_price threat evidence' };
        if (!fields.has('dte')) return { valid: false, reason: 'threat_management requires remaining-DTE context' };
      }
      return { valid: true };
    }

    return { valid: false, reason: `unsupported rule contract ${rule.rule_type}/${rule.action}` };
  };

  const normalizeBuybackCaptureFloor = (rule) => {
    if (!rule || rule.rule_type !== 'exit' || rule.action !== 'buyback_call') return { rule, changed: false };

    const criteria = parseMaybeJsonObject(rule.criteria);
    if (!criteria || !Array.isArray(criteria.conditions)) return { rule, changed: false };
    if (isThreatManagementBuybackCriteria(criteria)) return { rule, changed: false };

    let changed = false;
    let previousFloor = null;
    let hasProfitCaptureCondition = false;
    const removedCaptureBlockers = new Set();
    const conditions = [];
    for (const condition of criteria.conditions) {
      if (REDUNDANT_BUYBACK_CAPTURE_FIELDS.has(condition?.field)) {
        removedCaptureBlockers.add(condition.field);
        changed = true;
        continue;
      }
      if (!isBuybackProfitCaptureCondition(condition)) {
        conditions.push(condition);
        continue;
      }
      hasProfitCaptureCondition = true;
      const threshold = Number(condition.value ?? condition.threshold);
      if (threshold >= CALL_BUYBACK_PROFIT_THRESHOLD) {
        conditions.push(condition);
        continue;
      }
      previousFloor = previousFloor == null ? threshold : Math.min(previousFloor, threshold);
      changed = true;
      conditions.push({ ...condition, value: CALL_BUYBACK_PROFIT_THRESHOLD });
    }

    if (!hasProfitCaptureCondition) {
      conditions.push({ field: 'unrealized_pnl_pct', op: 'gte', value: CALL_BUYBACK_PROFIT_THRESHOLD });
      changed = true;
    }

    const previousLogic = criteria.condition_logic || 'all';
    const conditionLogic = 'all';
    if (previousLogic !== conditionLogic) changed = true;

    if (!changed) return { rule, changed: false };

    const suffixParts = [];
    if (previousFloor != null) {
      suffixParts.push(`capture floor: ${previousFloor}% -> ${CALL_BUYBACK_PROFIT_THRESHOLD}%`);
    } else if (!hasProfitCaptureCondition) {
      suffixParts.push(`added capture floor: ${CALL_BUYBACK_PROFIT_THRESHOLD}%`);
    }
    if (previousLogic !== conditionLogic) {
      suffixParts.push(`condition_logic: ${previousLogic} -> ${conditionLogic}`);
    }
    if (removedCaptureBlockers.size > 0) {
      suffixParts.push(`removed redundant ${Array.from(removedCaptureBlockers).join('/')} capture blocker(s)`);
    }
    const suffix = `normalized buyback ${suffixParts.join(', ')}`;
    return {
      rule: {
        ...rule,
        criteria: { ...criteria, conditions, condition_logic: conditionLogic },
        reasoning: rule.reasoning ? `${rule.reasoning} [${suffix}]` : suffix,
      },
      changed: true,
      reason: suffix,
    };
  };

  const getBuybackCaptureGate = (rule, criteria, values) => {
    if (!rule || rule.rule_type !== 'exit' || rule.action !== 'buyback_call') {
      return { allowed: true };
    }
    if (isThreatManagementBuybackCriteria(criteria)) {
      return { allowed: true };
    }

    const condition = getBuybackProfitCaptureCondition(criteria);
    if (!condition) {
      return {
        allowed: false,
        reason: `missing executable unrealized_pnl_pct >= ${CALL_BUYBACK_PROFIT_THRESHOLD}% capture floor`,
      };
    }

    const actual = Number(values?.unrealized_pnl_pct);
    const patientCapture = Number(values?.patient_buyback_capture_pct);
    if (
      !conditionPasses(actual, condition.op, condition.threshold)
      && !conditionPasses(patientCapture, condition.op, condition.threshold)
    ) {
      const opText = condition.op === 'gt' ? '>' : condition.op === 'gte' ? '>=' : condition.op;
      return {
        allowed: false,
        reason: `executable capture ${Number.isFinite(actual) ? actual.toFixed(2) : 'N/A'}% does not satisfy ${opText} ${condition.threshold}%`,
      };
    }

    return { allowed: true };
  };

  const buildBuybackConfirmationContext = (action, triggerData) => {
    if (action?.action !== 'buyback_call') return null;

    const ruleCondition = getBuybackProfitCaptureCondition(action.rule_criteria);
    const triggerCondition = Array.isArray(triggerData?.conditions_met)
      ? triggerData.conditions_met.find(isBuybackProfitCaptureCondition)
      : null;

    const threshold = Number(ruleCondition?.threshold ?? triggerCondition?.threshold ?? triggerCondition?.value);
    const op = ruleCondition?.op || triggerCondition?.op || 'gte';
    const actual = Number(
      triggerCondition?.actual
      ?? triggerData?.current_values?.unrealized_pnl_pct
      ?? triggerData?.unrealized_pnl_pct
    );
    const executionPrice = Number(triggerData?.current_values?.execution_price);
    const patientLimitPrice = Number(triggerData?.advisor_limit_price ?? triggerData?.current_values?.patient_buyback_limit_price);
    const patientCeilingPrice = Number(triggerData?.patient_buyback_ceiling_price ?? triggerData?.current_values?.patient_buyback_ceiling_price);
    const patientCapturePct = Number(triggerData?.patient_buyback_capture_pct ?? triggerData?.current_values?.patient_buyback_capture_pct);

    if (!Number.isFinite(threshold)) return null;
    const actualSatisfied = conditionPasses(actual, op, threshold);
    const patientSatisfied = conditionPasses(patientCapturePct, op, threshold);

    return {
      threshold,
      op,
      actual: Number.isFinite(actual) ? actual : null,
      executionPrice: Number.isFinite(executionPrice) && executionPrice > 0 ? executionPrice : null,
      patientLimitPrice: Number.isFinite(patientLimitPrice) && patientLimitPrice > 0 ? patientLimitPrice : null,
      patientCeilingPrice: Number.isFinite(patientCeilingPrice) && patientCeilingPrice > 0 ? patientCeilingPrice : null,
      patientCapturePct: Number.isFinite(patientCapturePct) ? patientCapturePct : null,
      satisfied: actualSatisfied || patientSatisfied,
      actualSatisfied,
      patientSatisfied,
    };
  };

  const formatBuybackConfirmationContext = (context, liveMarketPrice) => {
    if (!context) return '';
    const actualText = Number.isFinite(context.actual) ? `${context.actual.toFixed(2)}%` : 'N/A';
    const opText = context.op === 'gt' ? '>' : '>=';
    const liveText = Number(liveMarketPrice) > 0 ? `$${Number(liveMarketPrice).toFixed(4)}` : 'unavailable';
    const executionText = context.executionPrice ? `$${context.executionPrice.toFixed(4)}` : 'N/A';
    const patientText = context.patientLimitPrice
      ? `; patient bid $${context.patientLimitPrice.toFixed(4)} would capture ${Number.isFinite(context.patientCapturePct) ? `${context.patientCapturePct.toFixed(2)}%` : 'N/A'}`
      : '';
    const ceilingText = context.patientCeilingPrice && context.patientCeilingPrice !== context.patientLimitPrice
      ? `; max buyback ceiling $${context.patientCeilingPrice.toFixed(4)}`
      : '';
    return [
      'Advisor-rule buyback context:',
      `- Active buyback_call rule threshold: executable unrealized_pnl_pct ${opText} ${context.threshold}%`,
      `- Current executable capture from trigger details: ${actualText}; live_rule_satisfied=${context.actualSatisfied ? 'yes' : 'no'}; patient_bid_satisfies_rule=${context.patientSatisfied ? 'yes' : 'no'}; rule_satisfied=${context.satisfied ? 'yes' : 'no'}`,
      `- Live buyback ask: ${liveText}; trigger execution_price=${executionText}${patientText}${ceilingText}`,
      `- If the rule names max_buyback_price, treat that cap as a ceiling, not a target: use a patient synthetic reduce-only gtc/post_only limit at or below the named price when profit_capture is the intent, and keep any extra edge available from lower live asks, lower visible bids, or sparse-book price improvement.`,
      `- Confirmation should validate live price, reduce-only semantics, and rule consistency. Do not reject solely because the call is OTM, delta is low, theta remains, or the rule is a profit-harvest/capacity-reset rather than a threat signal. The advisor rule is the source of strategic intent for this pending exit.`,
    ].join('\n');
  };

  const buildBuyPutPatientMakerContext = (action, triggerData = {}, advisorLimitPrice = null, liveMarketPrice = null, options = {}) => {
    if (action?.action !== 'buy_put') return { satisfied: false };

    const criteria = parseMaybeJsonObject(action.rule_criteria) || {};
    const plannedScore = Number(triggerData?.planned_score ?? triggerData?.score);
    const minScore = Number(criteria.min_score ?? triggerData?.min_score);
    const targetScore = Number(triggerData?.target_score ?? criteria.target_score);
    const requiredScore = Math.max(
      Number.isFinite(minScore) && minScore > 0 ? minScore : 0,
      Number.isFinite(targetScore) && targetScore > 0 ? targetScore : 0
    );
    const limitPrice = Number(advisorLimitPrice ?? triggerData?.advisor_limit_price);
    const liveAsk = Number(liveMarketPrice);
    const amount = Number(action?.amount);
    const plannedOutlay = Number.isFinite(amount) && amount > 0 && Number.isFinite(limitPrice) && limitPrice > 0
      ? amount * limitPrice
      : null;
    const putBudgetRemaining = Number(options.putBudgetRemaining);
    const hasBudgetCap = Number.isFinite(putBudgetRemaining) && putBudgetRemaining >= 0;
    const budgetSatisfied = !hasBudgetCap
      || (Number.isFinite(plannedOutlay) && plannedOutlay <= putBudgetRemaining + 0.01);
    const liquidationSafe = !options.marginState?.is_under_liquidation;

    const scoreSatisfied = Number.isFinite(plannedScore)
      && plannedScore > 0
      && requiredScore > 0
      && plannedScore + 1e-9 >= requiredScore;
    const patientBid = Number.isFinite(limitPrice)
      && limitPrice > 0
      && Number.isFinite(liveAsk)
      && liveAsk > limitPrice;

    return {
      satisfied: scoreSatisfied && patientBid && budgetSatisfied && liquidationSafe,
      plannedScore,
      requiredScore,
      limitPrice,
      liveAsk,
      plannedOutlay,
      putBudgetRemaining: hasBudgetCap ? putBudgetRemaining : null,
      budgetSatisfied,
      liquidationSafe,
    };
  };

  const getMarginCapacityBase = (marginState) => {
    const collateralMarginBase = Number(marginState?.collaterals_initial_margin ?? 0);
    if (collateralMarginBase > 0) return collateralMarginBase;
    const collateralValue = Number(marginState?.collaterals_value ?? 0);
    if (collateralValue > 0) return collateralValue;
    return Number(marginState?.subaccount_value ?? 0);
  };

  const getMarginUtilizationBase = (marginState) => {
    const aggregatedMaintenanceBase = Math.abs(Number(marginState?.aggregated_collaterals_maintenance_margin ?? 0));
    if (aggregatedMaintenanceBase > 0) return aggregatedMaintenanceBase;
    const maintenanceBase = Math.abs(Number(marginState?.collaterals_maintenance_margin ?? 0));
    if (maintenanceBase > 0) return maintenanceBase;
    return getMarginCapacityBase(marginState);
  };

  const normalizeMarginUtilizationValue = (value) => {
    if (!Number.isFinite(value)) return null;
    return Math.max(0, Math.min(1, value));
  };

  const estimateMarginUtilizationFromComponents = (marginState, additionalOpenOrdersMargin = 0) => {
    const base = getMarginUtilizationBase(marginState);
    if (!(base > 0)) return null;
    const usedMargin = Math.abs(Number(
        marginState?.aggregated_positions_initial_margin ??
        marginState?.positions_initial_margin ??
        0
      ))
      + Math.abs(Number(marginState?.open_orders_margin ?? 0))
      + Math.max(0, Number(additionalOpenOrdersMargin ?? 0));
    return normalizeMarginUtilizationValue(usedMargin / base);
  };

  const estimateMarginUtilization = (marginState, additionalOpenOrdersMargin = 0) => {
    const componentUtilization = estimateMarginUtilizationFromComponents(marginState, additionalOpenOrdersMargin);
    if (componentUtilization != null) return componentUtilization;

    const base = getMarginCapacityBase(marginState);
    if (!(base > 0)) return null;
    const availableInitialMargin = Number(marginState?.initial_margin ?? NaN);
    if (Number.isFinite(availableInitialMargin)) {
      const projectedAvailable = availableInitialMargin - Math.max(0, Number(additionalOpenOrdersMargin ?? 0));
      return normalizeMarginUtilizationValue(1 - (projectedAvailable / base));
    }

    const explicitMarginUsage = Number(
      marginState?.margin_usage_pct ??
      marginState?.margin_utilization_pct ??
      marginState?.margin_utilization ??
      NaN
    );
    const additionalRatio = Math.max(0, Number(additionalOpenOrdersMargin ?? 0)) / base;
    if (Number.isFinite(explicitMarginUsage)) {
      const normalized = explicitMarginUsage > 1 ? explicitMarginUsage / 100 : explicitMarginUsage;
      return normalizeMarginUtilizationValue(normalized + additionalRatio);
    }

    return estimateMarginUtilizationFromComponents(marginState, additionalOpenOrdersMargin);
  };

  const estimateDisplayedMarginUtilization = (marginState) => {
    if (!marginState) return null;
    const maintenanceBase = getMarginUtilizationBase(marginState);
    const maintenanceMargin = Number(marginState?.maintenance_margin ?? NaN);
    if (maintenanceBase > 0 && Number.isFinite(maintenanceMargin)) {
      return normalizeMarginUtilizationValue(1 - (maintenanceMargin / maintenanceBase));
    }
    return estimateMarginUtilization(marginState);
  };

  const estimateProjectedDisplayedMarginUtilization = (marginState, additionalMargin = 0) => {
    if (!marginState) return null;
    const currentDisplayed = estimateDisplayedMarginUtilization(marginState);
    const maintenanceBase = getMarginUtilizationBase(marginState);
    if (currentDisplayed != null && maintenanceBase > 0) {
      return normalizeMarginUtilizationValue(currentDisplayed + (Math.max(0, Number(additionalMargin ?? 0)) / maintenanceBase));
    }
    return estimateMarginUtilization(marginState, additionalMargin);
  };

  const getShortCallExposure = (positions = []) => positions
    .filter((p) => p.instrument_name?.endsWith('-C') && p.direction === 'short')
    .reduce((sum, p) => sum + Math.abs(Number(p.amount) || 0), 0);

  const isCallBreakoutAddWindow = (positions = [], spotPrice = 0) => {
    const currentShortExposure = getShortCallExposure(positions);
    if (!(currentShortExposure > 0) || !(spotPrice > 0)) return false;

    const short = botData.shortTermMomentum || {};
    const medium = botData.mediumTermMomentum || {};
    const threeDayHigh = Number(short.threeDayHigh || 0);
    const sevenDayHigh = Number(short.sevenDayHigh || 0);
    const nearRecentHigh = (threeDayHigh > 0 && spotPrice >= threeDayHigh * 0.997)
      || (sevenDayHigh > 0 && spotPrice >= sevenDayHigh * 0.995);

    return short.main === 'upward'
      && CALL_BREAKOUT_DERIVATIVES.has(short.derivative)
      && medium.main !== 'downward'
      && nearRecentHigh;
  };

  const getEffectiveCallExposureCapPct = (positions = [], spotPrice = 0) => (
    isCallBreakoutAddWindow(positions, spotPrice)
      ? CALL_BREAKOUT_OVERRIDE_CAP_PCT
      : CALL_EXPOSURE_CAP_PCT
  );

  const getEffectiveCallExposureLimitPct = (positions = [], spotPrice = 0) => (
    getCallExposureLimitPct(getEffectiveCallExposureCapPct(positions, spotPrice))
  );

  const getDisplayedMarginHeadroomAtCap = (marginState, capPct = CALL_EXPOSURE_CAP_PCT) => {
    if (!marginState) return null;
    const currentDisplayed = estimateDisplayedMarginUtilization(marginState);
    const utilizationBase = getMarginUtilizationBase(marginState);
    if (currentDisplayed == null || !(utilizationBase > 0)) return null;
    return Math.max(0, (capPct - currentDisplayed) * utilizationBase);
  };

  const ACTION_POLICY = Object.freeze({
    buy_put: Object.freeze({
      phase: 'entry',
      direction: 'buy',
      reduceOnly: false,
      allowedOrderTypes: Object.freeze(['ioc', 'gtc', 'post_only']),
      semantics: 'Entry action: buying a put for tail-risk insurance. Bounded premium outlay, long convexity.',
    }),
    sell_call: Object.freeze({
      phase: 'entry',
      direction: 'sell',
      reduceOnly: false,
      allowedOrderTypes: Object.freeze(['ioc', 'gtc', 'post_only']),
      semantics: 'Entry action: selling a call to open short call exposure against ETH-collateralized account capacity.',
    }),
    sell_put: Object.freeze({
      phase: 'exit',
      direction: 'sell',
      reduceOnly: true,
      allowedOrderTypes: Object.freeze(['ioc', 'gtc', 'post_only']),
      semantics: 'Exit-only action: selling an already-owned long put to close or trim it. This must be reduce-only in effect and cannot create a naked short put.',
    }),
    buyback_call: Object.freeze({
      phase: 'exit',
      direction: 'buy',
      reduceOnly: true,
      allowedOrderTypes: Object.freeze(['ioc', 'gtc', 'post_only']),
      semantics: 'Exit-only action: buying back an already-open short call to close or trim it. This must be reduce-only in effect and cannot create a new long call exposure beyond the short being closed.',
    }),
  });

  const getActionPolicy = (action) => ACTION_POLICY[action] || null;

  const getAllowedOrderTypesForAction = (action) => getActionPolicy(action)?.allowedOrderTypes || ['ioc', 'gtc', 'post_only'];

  const getStandingRulebookDisciplinePrompt = () => [
    'STANDING RULEBOOK DISCIPLINE:',
    '- The advisory output is a standing rulebook for tick-by-tick execution, not only a list of trades that should execute at the current tick.',
    '- Every REQUIRED STANDING RULEBOOK COVERAGE item must have a corresponding rule in the final agenda. If the favorable condition is not true now, encode the condition that would make it favorable later.',
    '- buy_put rules define a price/score where insurance is worth buying while put budget remains. Use min_score/target_score as the hard price contract, and use the PUT edge gate to decide whether raw score is actually good insurance value.',
    `- sell_call rules must include min_score for CALL EDGE, where CALL EDGE = (bid / abs(delta)) * (${SELL_CALL_EDGE_REFERENCE_DTE} / DTE)^${SELL_CALL_EDGE_DTE_EXPONENT}. This light correction reduces the mechanical Sunday jump when the available expiry range rolls forward.`,
    '- min_bid is the executable premium/liquidity floor. min_score is compared with CALL EDGE; do not emit min_edge_score for sell_call.',
    '- If the sell_call thesis is patience or margin is near its cap, encode that stance through a stricter min_score/min_bid, lower priority, or narrower delta/DTE range. The written reasoning and JSON trigger must not contradict each other.',
    '- sell_call market_conditions may only use spot_price as optional supporting context. Express other selectivity through min_score, min_bid, delta_range, dte_range, priority, and reasoning.',
    '- Do not use broad spot_price floors as a proxy for sell_call recovery, stability, or good premium. After a sharp drop, spot can remain above a floor while call selling is still unattractive.',
    `- sell_call rules must stay inside the normal call sale universe: ${CALL_EXPIRATION_RANGE[0]}-${CALL_EXPIRATION_RANGE[1]} DTE and ${CALL_DELTA_RANGE[0]}-${CALL_DELTA_RANGE[1]} delta.`,
    `- sell_put rules must declare put_exit_intent. Use "roll_protection" only when DTE <= ${PUT_ROLL_DTE_THRESHOLD} and the book already holds longer-dated long puts; roll_protection may close the aging instrument fully because the replacement protection is already on. Use "monetize_tail_win" only when executable unrealized_pnl_pct > ${PUT_MONETIZATION_PROFIT_THRESHOLD}; set retain_downside_protection=true, tranche_fraction <= ${PUT_MONETIZATION_MAX_TRANCHE_FRACTION}, and min_exit_price/limit_price. For monetization, never sell all downside protection at once or dump into sparse crash bids.`,
    `- buyback_call rules must declare buyback_intent. Use "profit_capture" for ${CALL_BUYBACK_PROFIT_THRESHOLD}%+ executable capture or a patient synthetic reduce-only resting limit whose price would achieve that capture. Use "threat_management" only for genuine short-call danger; price rising alone is not enough.`,
    '- Do not omit a required watcher just because it is not currently triggered. Tighten criteria instead.',
  ].join('\n');

  const getCallMarginDisciplinePrompt = () => `CALL DISCIPLINE: Short calls normally target ${(CALL_EXPOSURE_CAP_PCT * 100).toFixed(0)}% inferred Derive margin utilization. The ${(CALL_EXPOSURE_BUFFER_PCT * 100).toFixed(0)} percentage point buffer up to ${(CALL_EXPOSURE_LIMIT_PCT * 100).toFixed(0)}% is last-mile execution safety for estimate drift, not planned sell-call capacity. ${(CALL_ENTRY_CAP_PCT * 100).toFixed(0)}% is a caution threshold, not an automatic rejection line. New entries must stay at or below the active target cap after sizing; at-or-below means <= and equality at the cap is allowed. Do not create dust orders merely to use the execution buffer. When spot is breaking upward and short calls are already on, the active target can widen to ${(CALL_BREAKOUT_OVERRIDE_CAP_PCT * 100).toFixed(0)}% with a buffered limit of ${(CALL_BREAKOUT_OVERRIDE_LIMIT_PCT * 100).toFixed(0)}% only if margin context explicitly shows breakout_override=active. These are discipline limits for new entries, not margin-emergency thresholds. Reject call sells that exceed the active target cap, exceed the buffered limit, lack buying power, or are too small to matter.`;

  const getCallBuybackDisciplinePrompt = () => `CALL BUYBACK DISCIPLINE: For buyback_call, keep two intents separate. Intent 1 is profit/capacity reset while the short call is winning: use buyback_intent="profit_capture" and executable unrealized_pnl_pct >= ${CALL_BUYBACK_PROFIT_THRESHOLD}% as the economic trigger, or set a patient max_buyback_price/target_capture_pct where the bid would capture at least ${CALL_BUYBACK_PROFIT_THRESHOLD}% if filled. Do not add DTE or mark_price blockers for this intent; executable capture already uses the live buyback ask. Intent 2 is threat management when the short call is genuinely dangerous and time/range for recovery is running out: use buyback_intent="threat_management" with allow_below_profit_floor=true, and conditions on real threat facts such as delta, spot vs strike, and remaining DTE. Do not prematurely buy back just because price is rising; spot can come back down, and buying back fear premium can make us the sucker of the trade. The short call premium is already collected; mark expansion alone does not erase that. A buyback below strike is paying to remove tail risk of further upside continuation. Confirm or create buybacks only when the position is genuinely threatened, assignment risk is credible, the insurance cost is justified by actual breakout evidence, or an advisor-led take-profit rule names patient pricing. If live executable buyback price already implies strictly better capture than a profit-capture rule, do not bid back up to the threshold. Never confirm a threshold-style buyback when live market price is unavailable. Treat margin context as sizing/redeployment context, not a standalone buyback trigger.`;

  const getPutExitDisciplinePrompt = () => `PUT EXIT DISCIPLINE: For sell_put, judge whether rolling or monetizing an owned long hedge is sensible. Never treat sell_put as opening naked short put exposure. Selling an owned long put is capital-releasing: it returns cash/premium recovery, reduces the hedge position, and does not consume more margin. Use put_exit_intent="roll_protection" only when DTE <= ${PUT_ROLL_DTE_THRESHOLD} and the book already holds longer-dated long put protection. For roll_protection, a full close of the aging instrument is allowed, negative PnL is not a rejection reason, and monetize_tail_win tranche/profit thresholds do not apply because replacement protection is already in the book. Use put_exit_intent="monetize_tail_win" only when executable unrealized_pnl_pct is greater than ${PUT_MONETIZATION_PROFIT_THRESHOLD}; set retain_downside_protection=true, sell in tranches with tranche_fraction <= ${PUT_MONETIZATION_MAX_TRANCHE_FRACTION}, and name min_exit_price/limit_price as the minimum acceptable sell price. For monetization, never sell all protection at once. In severe crash markets, do not undersell a valuable put just because visible bids are sparse; if making the market, choose a responsible floor from intrinsic value, Greeks, IV/skew, spread/depth, DTE, and remaining hedge role. Even when the monetization hard trigger is satisfied, confirm only if the full market context says selling a tranche is wise rather than prematurely cutting convexity. If you reject a sell_put, do it because the typed intent's requirements fail or removing protection is strategically unwise, not because the exit itself uses more margin.`;

  const getConfirmationScopePrompt = () => 'CONFIRMATION SCOPE: This is a last-mile execution check, not a second scheduled advisory. Treat the active rule and trigger details as the strategic intent. Confirm only if fresh execution facts still satisfy the typed rule, the limit/order type can respect that intent, and hard safety checks pass. Reject for stale or moved-market facts that invalidate the rule, missing live pricing, margin/liquidation danger, reduce-only violations, or action-specific discipline failures; do not invent a new strategy thesis at confirmation time.';

  const normalizePreferredOrderType = (action, preferredOrderType) => {
    if (typeof preferredOrderType !== 'string') return null;
    const normalized = preferredOrderType.trim().toLowerCase();
    if (!normalized) return null;
    return getAllowedOrderTypesForAction(action).includes(normalized) ? normalized : null;
  };

  return Object.freeze({
    floorOptionPriceCents,
    getBuyPutEntryPricing,
    parseAdvisoryOptionInstrument,
    normalizeBuyPutValueSignal,
    hasExplicitBuyPutValueSignal,
    isKnownBuyPutValueSignal,
    buildRulebookRequirements,
    formatRulebookRequirements,
    findMissingRulebookRequirements,
    buildAgendaFromValidatedRules,
    buildCanonicalRequiredWatcherRule,
    isSellCallCandidateInStrategyRange,
    parseMaybeJsonObject,
    isBuybackProfitCaptureCondition,
    isThreatManagementBuybackCriteria,
    getBuybackProfitCaptureCondition,
    conditionPasses,
    getRuleIntent,
    getPutExitIntent,
    getBuybackIntent,
    isFiniteNumber,
    isRangeWithin,
    getConditions,
    hasCondition,
    hasThresholdCondition,
    hasLongerDatedPutProtectionSnapshot,
    getTotalLongPutAmount,
    leavesDownsideProtectionAfterSale,
    getSellPutExitAmount,
    getAdvisorSellPutLimitPrice,
    getLongPutFairValueProof,
    getPatientSellPutPlan,
    getBuybackTargetCapturePct,
    getPatientBuybackPlan,
    getBuybackCapturePctAtPrice,
    validateAdvisorRuleContract,
    normalizeBuybackCaptureFloor,
    getBuybackCaptureGate,
    buildBuybackConfirmationContext,
    formatBuybackConfirmationContext,
    buildBuyPutPatientMakerContext,
    getMarginCapacityBase,
    getMarginUtilizationBase,
    normalizeMarginUtilizationValue,
    estimateMarginUtilizationFromComponents,
    estimateMarginUtilization,
    estimateDisplayedMarginUtilization,
    estimateProjectedDisplayedMarginUtilization,
    getShortCallExposure,
    isCallBreakoutAddWindow,
    getEffectiveCallExposureCapPct,
    getEffectiveCallExposureLimitPct,
    getDisplayedMarginHeadroomAtCap,
    getActionPolicy,
    getAllowedOrderTypesForAction,
    getStandingRulebookDisciplinePrompt,
    getCallMarginDisciplinePrompt,
    getCallBuybackDisciplinePrompt,
    getPutExitDisciplinePrompt,
    getConfirmationScopePrompt,
    normalizePreferredOrderType,
    getCallExposureLimitPct,
    normalizeBuyPutScore,
    normalizeSellCallScore,
    getBuyPutDteNormalizationFactor,
    getBuyPutPriceForEdgeScore
  });
}

module.exports = { createLegacyPolicy };
