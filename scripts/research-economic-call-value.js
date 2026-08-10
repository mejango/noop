#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');
const Database = require('better-sqlite3');
const {
  buildEconomicExamples,
  buildEconomicPredictionTape,
  economicExampleSummary,
  economicUtility,
  loadHistoricalFrames,
  makeDteNormalizedRawPolicy,
  makeEconomicValuePolicy,
  makeRawScorePolicy,
  parseArgv,
  parseNumber,
  quantile,
  round,
  runBacktest,
  splitChronologicalFrames,
} = require('../research/sell-call-backtest');

function simulationConfig(args) {
  return {
    startingEth: parseNumber(args['starting-eth'], 5),
    startingCash: parseNumber(args['starting-cash'], 0),
    execution: args.execution || 'bid_ask',
    feeBps: parseNumber(args['fee-bps'], 0),
    settlementFeeBps: parseNumber(args['settlement-fee-bps'], parseNumber(args['fee-bps'], 0)),
    callExposureCap: parseNumber(args['exposure-cap'], 0.45),
    marginRate: parseNumber(args['margin-rate'], 0.15),
    marginBudgetPct: parseNumber(args['margin-budget-pct'], 0.45),
    profitCapturePct: parseNumber(args['profit-capture-pct'], 0.80),
    stopLossMultiple: parseNumber(args['stop-loss-multiple'], null),
    maxHoldHours: parseNumber(args['max-hold-hours'], null),
    entryCooldownHours: parseNumber(args['entry-cooldown-hours'], 0),
    maxOpenPositions: parseNumber(args['max-open-positions'], 1),
    maxContracts: parseNumber(args['max-contracts'], null),
    amountStep: parseNumber(args['amount-step'], 0.01),
    useQuotedDepth: args['ignore-depth'] !== true,
  };
}

function numberList(value, fallback) {
  if (value == null) return fallback;
  const parsed = String(value).split(',').map(Number);
  if (parsed.length === 0 || parsed.some((item) => !Number.isFinite(item))) {
    throw new Error(`invalid numeric list: ${value}`);
  }
  return parsed;
}

function totalMarginDays(result) {
  return result.trade_log.reduce((sum, trade) => (
    sum + Number(trade.margin_reserved || 0) * Math.max(Number(trade.holding_hours || 0) / 24, 1 / 24)
  ), 0);
}

function summarize(result) {
  const lossPnl = result.trade_log.reduce((sum, trade) => sum + Math.min(0, Number(trade.pnl || 0)), 0);
  const marginDays = totalMarginDays(result);
  const riskPenalty = Math.abs(lossPnl) * 2 + result.tail_losses * result.starting_nav * 0.02;
  return {
    overlay_pnl: result.overlay_pnl,
    realized_call_pnl: result.realized_call_pnl,
    trades: result.trades,
    wins: result.wins,
    win_rate: result.win_rate,
    loss_pnl: round(lossPnl, 6),
    tail_losses: result.tail_losses,
    max_drawdown: result.max_drawdown,
    max_margin_used: result.max_margin_used,
    margin_days: round(marginDays, 6),
    pnl_per_margin_day: marginDays > 0 ? round(result.realized_call_pnl / marginDays, 8) : null,
    risk_adjusted_pnl: round(result.overlay_pnl - riskPenalty, 6),
    average_holding_hours: result.average_holding_hours,
    trade_log: result.trade_log,
  };
}

function entryWindowPolicy(policy, entryEndMs) {
  return {
    name: policy.name,
    description: policy.description,
    onFrame(frame) {
      if (typeof policy.onFrame === 'function') policy.onFrame(frame);
    },
    select(context) {
      return context.frame.timestamp_ms <= entryEndMs ? policy.select(context) : null;
    },
    getArtifacts() {
      return typeof policy.getArtifacts === 'function' ? policy.getArtifacts() : [];
    },
  };
}

function evaluate(frames, policy, simulation, purgeHours = 12 * 24) {
  const entryEndMs = frames.at(-1).timestamp_ms - purgeHours * 60 * 60 * 1000;
  return summarize(runBacktest(frames, entryWindowPolicy(policy, entryEndMs), simulation));
}

function incumbentPolicy() {
  return makeDteNormalizedRawPolicy({
    minBid: 4,
    minScore: 65,
    minDte: 5,
    maxDte: 12,
    referenceDte: 8.5,
    exponent: 0.12,
  }, 'production_call_edge');
}

function utilitiesForFrames(tape, frames, riskPenalty, maxAdverseBreachProbability) {
  const timestamps = new Set(frames.map((frame) => frame.timestamp_ms));
  const values = [];
  for (const [timestampMs, framePredictions] of tape.predictions) {
    if (!timestamps.has(timestampMs)) continue;
    for (const prediction of framePredictions.values()) {
      if (prediction.adverse_breach_probability > maxAdverseBreachProbability) continue;
      const utility = economicUtility(prediction, riskPenalty);
      if (Number.isFinite(utility)) values.push(utility);
    }
  }
  return values.sort((a, b) => a - b);
}

function candidateConfigs(tape, trainFrames, args) {
  const riskPenalties = numberList(args['risk-penalties'], [0, 0.1, 0.25, 0.5, 1]);
  const adverseBreachProbabilityCaps = numberList(args['adverse-breach-probability-caps'], [0.25, 0.50, 0.75, 1]);
  const thresholdQuantiles = numberList(args['threshold-quantiles'], [0, 0.25, 0.5, 0.75]);
  const configs = [];
  const seen = new Set();
  for (const riskPenalty of riskPenalties) {
    for (const maxAdverseBreachProbability of adverseBreachProbabilityCaps) {
      const utilities = utilitiesForFrames(tape, trainFrames, riskPenalty, maxAdverseBreachProbability);
      if (utilities.length === 0) continue;
      const thresholds = [0, ...thresholdQuantiles.map((probability) => quantile(utilities, probability))];
      for (const minUtility of thresholds) {
        if (!Number.isFinite(minUtility)) continue;
        const config = {
          riskPenalty,
          maxLossProbability: 0.35,
          maxAdverseBreachProbability,
          minUtility: round(minUtility, 10),
          minExpectedProfitRate: 0,
        };
        const key = JSON.stringify(config);
        if (seen.has(key)) continue;
        seen.add(key);
        configs.push(config);
      }
    }
  }
  return configs;
}

function rankDevelopment(configs, tape, splits, simulation) {
  return configs.map((config, index) => {
    const train = evaluate(
      splits.train,
      makeEconomicValuePolicy(tape, config, `economic_value_${index}_train`),
      simulation,
    );
    const validation = evaluate(
      splits.validation,
      makeEconomicValuePolicy(tape, config, `economic_value_${index}_validation`),
      simulation,
    );
    const coverage = train.trades >= 3 && validation.trades >= 2;
    const objective = coverage && train.risk_adjusted_pnl > 0 && validation.risk_adjusted_pnl > 0
      ? validation.pnl_per_margin_day
      : -Infinity;
    return { config, train, validation, coverage, objective: round(objective, 8) };
  }).sort((a, b) => b.objective - a.objective
    || b.validation.overlay_pnl - a.validation.overlay_pnl
    || b.train.overlay_pnl - a.train.overlay_pnl
    || a.config.riskPenalty - b.config.riskPenalty);
}

function period(frames) {
  return {
    from: frames[0]?.timestamp || null,
    to: frames.at(-1)?.timestamp || null,
    frames: frames.length,
  };
}

function money(value) {
  return `$${Number(value || 0).toFixed(2)}`;
}

function standardizedFeatureEffects(artifact, modelName, limit = 8) {
  const model = artifact?.[modelName];
  if (!model?.coefficients || !artifact?.feature_names) return [];
  return artifact.feature_names.map((feature, index) => ({
    feature,
    effect: round(model.coefficients[index + 1], 10),
  })).sort((a, b) => Math.abs(b.effect) - Math.abs(a.effect)).slice(0, limit);
}

function effectText(rows) {
  return rows.map((row) => `${row.feature} ${row.effect >= 0 ? '+' : ''}${row.effect}`).join('; ');
}

function renderMarkdown(report) {
  const winner = report.selected_challenger;
  const rows = [
    ['Production EDGE', report.incumbent.train, report.incumbent.validation, report.incumbent.holdout, report.incumbent.full],
    ['Raw bid/delta', report.raw_score.train, report.raw_score.validation, report.raw_score.holdout, report.raw_score.full],
    ['Economic value', winner.train, winner.validation, winner.holdout, winner.full],
  ];
  return [
    '# Economic Call-Value Study',
    '',
    `Computed: ${report.computed_at}`,
    `History: ${report.historical_window.from} through ${report.historical_window.to}`,
    `Path labels: ${report.labels.examples} across ${report.labels.independent_frames} decision frames; loss rate ${(Number(report.labels.loss_rate || 0) * 100).toFixed(2)}%.`,
    `Selected challenger: risk penalty ${winner.config.riskPenalty}, adverse-breach cap ${winner.config.maxAdverseBreachProbability}, minimum utility ${winner.config.minUtility}.`,
    '',
    '| Policy | Train P&L | Validation P&L | Holdout P&L | Full P&L | Holdout trades | Holdout loss P&L | Holdout P&L/margin-day |',
    '|---|---:|---:|---:|---:|---:|---:|---:|',
    ...rows.map(([name, train, validation, holdout, full]) => `| ${name} | ${money(train.overlay_pnl)} | ${money(validation.overlay_pnl)} | ${money(holdout.overlay_pnl)} | ${money(full.overlay_pnl)} | ${holdout.trades} | ${money(holdout.loss_pnl)} | ${holdout.pnl_per_margin_day == null ? 'n/a' : holdout.pnl_per_margin_day} |`),
    '',
    `Promotion test: ${report.conclusion.promote ? 'PASS' : 'FAIL'}. ${report.conclusion.reason}`,
    '',
    `Latest standardized profit effects: ${effectText(report.latest_model.standardized_profit_effects)}.`,
    `Latest standardized adverse-path effects: ${effectText(report.latest_model.standardized_adverse_effects)}.`,
    '',
    'The model is trained only from path outcomes available before each prediction timestamp. Formula and gate selection use train plus validation; the final chronological holdout is evaluated after selection.',
    '',
    'This is an offline, read-only challenger. It is not imported by the live trading path.',
    '',
  ].join('\n');
}

function main() {
  const args = parseArgv(process.argv.slice(2));
  const dataDir = process.env.DATA_DIR || path.join(__dirname, '..', 'data');
  const dbPath = args.db || process.env.DB_PATH || path.join(dataDir, 'noop.db');
  if (!fs.existsSync(dbPath)) throw new Error(`database not found: ${dbPath}`);
  const outputPath = args.out || path.join(dataDir, 'economic-call-value-study.json');
  const markdownPath = args.markdown || outputPath.replace(/\.json$/i, '.md');
  const modelsPath = args.models || path.join(dataDir, 'economic-call-value-models.json');
  const simulation = simulationConfig(args);
  const db = new Database(dbPath, { readonly: true, fileMustExist: true });
  db.pragma('query_only = ON');
  db.pragma('busy_timeout = 5000');
  try {
    console.log('Loading historical option-chain frames...');
    const data = loadHistoricalFrames(db, {
      days: args.days || 'all',
      from: args.from,
      to: args.to,
      cadenceHours: parseNumber(args['cadence-hours'], 1),
      maxFrames: parseNumber(args['max-frames'], null),
      callDteRange: [parseNumber(args['min-dte'], 5), parseNumber(args['max-dte'], 12)],
    });
    const outcomeOptions = {
      ...simulation,
      minBid: parseNumber(args['min-bid'], 4),
      maxCandidatesPerFrame: parseNumber(args['max-candidates-per-frame'], null),
      severeLossOnMargin: parseNumber(args['severe-loss-on-margin'], 0.02),
    };
    console.log('Replaying candidate paths under the actual exit policy...');
    const examples = buildEconomicExamples(data.frames, outcomeOptions);
    const predictionOptions = {
      minBid: outcomeOptions.minBid,
      minSamples: parseNumber(args['min-train-samples'], 500),
      minIndependentFrames: parseNumber(args['min-train-frames'], 120),
      maxTrainingSamples: parseNumber(args['max-train-samples'], 20000),
      retrainHours: parseNumber(args['retrain-hours'], 168),
      readinessCheckHours: parseNumber(args['readiness-check-hours'], 24),
      trainingWindowDays: parseNumber(args['training-window-days'], 180),
      embargoHours: parseNumber(args['embargo-hours'], 6),
      profitLambda: parseNumber(args['profit-lambda'], 8),
      adverseLambda: parseNumber(args['adverse-lambda'], 12),
      lossLambda: parseNumber(args['loss-lambda'], 2),
      downsideLambda: parseNumber(args['downside-lambda'], 2),
      lossIterations: parseNumber(args['loss-iterations'], 120),
    };
    console.log('Building leakage-safe walk-forward prediction tape...');
    const tape = buildEconomicPredictionTape(data.frames, examples, predictionOptions);
    if (!tape.first_prediction_at) throw new Error('history did not produce enough matured examples to train');
    const commonFrames = data.frames.filter((frame) => frame.timestamp_ms >= new Date(tape.first_prediction_at).getTime());
    const splits = splitChronologicalFrames(commonFrames, { trainFraction: 0.6, validationFraction: 0.2 });
    const configs = candidateConfigs(tape, splits.train, args);
    console.log(`Evaluating ${configs.length} value/risk gates without refitting the prediction model...`);
    const development = rankDevelopment(configs, tape, splits, simulation);
    const selected = development.find((row) => Number.isFinite(row.objective));
    if (!selected) throw new Error('no economic-value configuration traded in both train and validation');
    const selectedPolicy = (frames, suffix) => evaluate(
      frames,
      makeEconomicValuePolicy(tape, selected.config, `economic_value_selected_${suffix}`),
      simulation,
    );
    const baseline = (frames) => evaluate(frames, incumbentPolicy(), simulation);
    const raw = (frames) => evaluate(frames, makeRawScorePolicy({ minBid: 4, minRawScore: 65 }), simulation);
    const holdout = selectedPolicy(splits.test, 'holdout');
    const incumbent = {
      train: baseline(splits.train),
      validation: baseline(splits.validation),
      holdout: baseline(splits.test),
      full: baseline(commonFrames),
    };
    const rawScore = {
      train: raw(splits.train),
      validation: raw(splits.validation),
      holdout: raw(splits.test),
      full: raw(commonFrames),
    };
    const selectedChallenger = {
      config: selected.config,
      objective: selected.objective,
      train: selected.train,
      validation: selected.validation,
      holdout,
      full: selectedPolicy(commonFrames, 'full'),
    };
    const materialPnlHurdle = Math.max(1, incumbent.holdout.overlay_pnl * 0.02);
    const promote = holdout.overlay_pnl >= incumbent.holdout.overlay_pnl + materialPnlHurdle
      && holdout.pnl_per_margin_day >= incumbent.holdout.pnl_per_margin_day
      && holdout.loss_pnl >= incumbent.holdout.loss_pnl
      && holdout.tail_losses <= incumbent.holdout.tail_losses
      && holdout.trades >= 3;
    const latestArtifact = tape.artifacts.at(-1);
    const report = {
      schema_version: 1,
      engine: 'economic-call-value-study-v1',
      computed_at: new Date().toISOString(),
      isolation: 'offline/read-only; no live bot imports or database writes',
      historical_window: data.window,
      common_evaluation_window: period(commonFrames),
      split: {
        train: period(splits.train),
        validation: period(splits.validation),
        holdout: period(splits.test),
      },
      cadence_hours: data.cadence_hours,
      frames: data.frames.length,
      labels: economicExampleSummary(examples),
      outcome_policy: outcomeOptions,
      prediction_tape: {
        engine: tape.engine,
        options: tape.options,
        first_prediction_at: tape.first_prediction_at,
        prediction_count: tape.prediction_count,
        model_versions: tape.artifacts.length,
      },
      selection_rule: 'Prediction models are fixed walk-forward artifacts. Value/risk gates are generated from train predictions and selected by validation risk-adjusted P&L; holdout is then opened once.',
      searched_configurations: configs.length,
      development_leaderboard: development.slice(0, 20),
      incumbent,
      raw_score: rawScore,
      selected_challenger: selectedChallenger,
      latest_model: {
        version: latestArtifact.version,
        trained_at: latestArtifact.trained_at,
        label_through: latestArtifact.label_through,
        samples: latestArtifact.samples,
        independent_frames: latestArtifact.independent_frames,
        training_metrics: latestArtifact.training_metrics,
        standardized_profit_effects: standardizedFeatureEffects(latestArtifact, 'profit_model'),
        standardized_adverse_effects: standardizedFeatureEffects(latestArtifact, 'adverse_model'),
        standardized_adverse_breach_effects: standardizedFeatureEffects(latestArtifact, 'downside_model'),
      },
      conclusion: {
        promote,
        holdout_pnl_difference: round(holdout.overlay_pnl - incumbent.holdout.overlay_pnl, 6),
        required_holdout_pnl_improvement: round(materialPnlHurdle, 6),
        holdout_efficiency_difference: round(
          Number(holdout.pnl_per_margin_day || 0) - Number(incumbent.holdout.pnl_per_margin_day || 0),
          8,
        ),
        reason: promote
          ? 'The challenger materially beat production on untouched holdout P&L and P&L per margin-day without worse realized or tail losses; shadow execution is the next gate.'
          : 'The challenger did not materially beat production on every holdout profit, capital-efficiency, and downside requirement, so it remains research-only.',
      },
    };
    fs.mkdirSync(path.dirname(outputPath), { recursive: true });
    fs.mkdirSync(path.dirname(markdownPath), { recursive: true });
    fs.mkdirSync(path.dirname(modelsPath), { recursive: true });
    fs.writeFileSync(outputPath, `${JSON.stringify(report, null, 2)}\n`);
    fs.writeFileSync(markdownPath, `${renderMarkdown(report)}\n`);
    fs.writeFileSync(modelsPath, `${JSON.stringify({
      schema_version: 1,
      generated_at: report.computed_at,
      prediction_options: tape.options,
      artifacts: tape.artifacts,
    }, null, 2)}\n`);
    console.log(`Promotion test: ${promote ? 'PASS' : 'FAIL'}`);
    console.log(`JSON: ${outputPath}`);
    console.log(`Markdown: ${markdownPath}`);
    console.log(`Models: ${modelsPath}`);
  } finally {
    db.close();
  }
}

try {
  main();
} catch (error) {
  console.error(error instanceof Error ? error.stack || error.message : error);
  process.exitCode = 1;
}
