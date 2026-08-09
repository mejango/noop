'use strict';

const { round } = require('./utils');

function summarizeModelArtifacts(artifacts = []) {
  return artifacts.map((artifact) => ({
    version: artifact.version,
    trained_at: artifact.trained_at,
    train_start: artifact.train_start,
    train_end: artifact.train_end,
    label_through: artifact.label_through,
    training_cutoff: artifact.training_cutoff,
    embargo_hours: artifact.embargo_hours,
    training_window_days: artifact.training_window_days,
    samples: artifact.samples,
    available_matured_samples: artifact.available_matured_samples,
    max_training_samples: artifact.max_training_samples,
    independent_frames: artifact.independent_frames,
    training_metrics: artifact.training_metrics,
    feature_names: artifact.feature_names,
  }));
}

function compactResult(result) {
  const { equity_curve: equityCurve, trade_log: tradeLog, model_artifacts: modelArtifacts, ...summary } = result;
  return {
    ...summary,
    model_versions: summarizeModelArtifacts(modelArtifacts),
    equity_points: equityCurve.length,
    trade_log: tradeLog,
    equity_curve: equityCurve,
  };
}

function buildReport({ data, examples, results, options = {} }) {
  const compactResults = results.map(compactResult);
  const baseline = compactResults.find((result) => result.policy === 'no_call');
  return {
    schema_version: 1,
    engine: 'sell-call-walk-forward-backtest-v1',
    computed_at: new Date().toISOString(),
    isolation: 'offline/read-only; no live bot imports or database writes',
    historical_window: data.window,
    cadence_hours: data.cadence_hours,
    frames: data.frames.length,
    labeled_examples: examples.length,
    independent_example_frames: new Set(examples.map((example) => example.observed_at_ms)).size,
    coverage: data.coverage,
    options,
    results: compactResults.map((result) => ({
      ...result,
      incremental_return_vs_no_call: baseline
        ? round(result.total_return - baseline.total_return, 8)
        : null,
    })),
    limitations: [
      'Historical top-of-book quotes cannot prove that a maker order would have filled.',
      'Bid/ask mode assumes an immediately executable sale at bid and repurchase at ask.',
      'Hourly sampling can miss intrahour fills, adverse excursions, and exact expiry prints.',
      'The current-edge baseline uses the production multipliers with market trend and OI reconstructed from sampled historical frames.',
      'Margin is a configurable approximation, not a reconstruction of venue liquidation state.',
      'Results are research estimates and must remain out of live execution until shadow validation succeeds.',
    ],
  };
}

function percentage(value) {
  return value == null ? 'n/a' : `${(Number(value) * 100).toFixed(2)}%`;
}

function money(value) {
  return value == null ? 'n/a' : `$${Number(value).toFixed(2)}`;
}

function renderMarkdown(report) {
  const lines = [
    '# Sell-Call Walk-Forward Backtest',
    '',
    `Computed: ${report.computed_at}`,
    `Historical window: ${report.historical_window.from} to ${report.historical_window.to}`,
    `Cadence: ${report.cadence_hours}h; frames: ${report.frames}; labeled examples: ${report.labeled_examples} across ${report.independent_example_frames} independent frames`,
    '',
    '## Results',
    '',
    '| Policy | Ending NAV | Portfolio return | vs no-call | Overlay P&L | Call P&L | Trades | Win rate | Tail losses | Max drawdown |',
    '|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|',
  ];
  for (const result of report.results) {
    lines.push(`| ${result.policy} | ${money(result.ending_nav)} | ${percentage(result.total_return)} | ${percentage(result.incremental_return_vs_no_call)} | ${money(result.overlay_pnl)} | ${money(result.realized_call_pnl)} | ${result.trades} | ${percentage(result.win_rate)} | ${result.tail_losses} | ${percentage(result.max_drawdown)} |`);
  }
  lines.push('');
  for (const result of report.results) {
    lines.push(`## ${result.policy}`);
    lines.push('');
    lines.push(result.description || 'No description.');
    lines.push('');
    lines.push(`Premium received: ${money(result.total_premium_received)}; fees: ${money(result.total_fees)}; max margin: ${money(result.max_margin_used)}; return on max margin: ${percentage(result.return_on_max_margin)}.`);
    if (result.model_versions?.length) {
      lines.push('');
      lines.push(`Walk-forward model versions: ${result.model_versions.length}`);
      for (const model of result.model_versions) {
        lines.push(`- ${model.version}: trained ${model.trained_at}; labels through ${model.label_through}; n=${model.samples} across ${model.independent_frames} frames; capture RMSE=${model.training_metrics?.capture_rmse ?? 'n/a'}; tail log loss=${model.training_metrics?.tail_log_loss ?? 'n/a'}`);
      }
    }
    lines.push('');
  }
  lines.push('## Limitations');
  lines.push('');
  for (const limitation of report.limitations) lines.push(`- ${limitation}`);
  lines.push('');
  return `${lines.join('\n')}\n`;
}

module.exports = {
  buildReport,
  compactResult,
  renderMarkdown,
  summarizeModelArtifacts,
};
