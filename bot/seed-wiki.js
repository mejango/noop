#!/usr/bin/env node

/**
 * seed-wiki.js — One-time bootstrap script for the knowledge wiki.
 *
 * Reads journal entries, reviewed hypotheses, and active lessons from SQLite,
 * then synthesizes initial wiki content via Claude.
 *
 * Usage: node bot/seed-wiki.js
 */

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const axios = require('axios');
const db = require('./db');

const WIKI_DIR = process.env.WIKI_DIR || path.join(__dirname, '..', 'knowledge');
const SCHEMA_PATH = path.join(WIKI_DIR, 'schema.md');
const META_PATH = path.join(WIKI_DIR, '.meta.json');

const WIKI_ALL_PAGES = [
  'regimes/current.md',
  'regimes/history.md',
  'protection/pricing.md',
  'protection/windows.md',
  'protection/convexity.md',
  'indicators/leading.md',
  'indicators/correlations.md',
  'indicators/divergences.md',
  'strategy/lessons.md',
  'strategy/mistakes.md',
  'strategy/playbook.md',
];

async function seedWiki() {
  if (!process.env.ANTHROPIC_API_KEY) {
    console.error('ANTHROPIC_API_KEY not set');
    process.exit(1);
  }

  console.log('📚 Seeding wiki from journal history...');

  // Gather data from SQLite
  const journalEntries = db.getRecentJournalEntries(200); // last 200 entries
  const reviewedHypotheses = db.getReviewedHypotheses(50);
  const activeLessons = db.getActiveLessons();

  console.log(`  Journal entries: ${journalEntries.length}`);
  console.log(`  Reviewed hypotheses: ${reviewedHypotheses.length}`);
  console.log(`  Active lessons: ${activeLessons.length}`);

  if (journalEntries.length === 0) {
    console.log('⚠️  No journal entries found. Run the bot first to generate entries.');
    process.exit(0);
  }

  // Read schema
  const schema = fs.readFileSync(SCHEMA_PATH, 'utf-8');

  // Prepare journal summary (sample to fit token budget)
  const sampleEntries = journalEntries.slice(0, 100);
  const journalText = sampleEntries
    .map(e => `[${e.entry_type}] (${e.timestamp}) ${e.content.slice(0, 400)}`)
    .join('\n\n---\n\n');

  const hypothesesText = reviewedHypotheses
    .map(h => `#${h.id} [${h.outcome_status}] conf:${h.outcome_confidence} — ${h.content.slice(0, 200)}... VERDICT: ${h.outcome_verdict || 'none'}`)
    .join('\n\n');

  const lessonsText = activeLessons.length > 0
    ? activeLessons.map(l => `- ${l.lesson} (evidence: ${l.evidence_count})`).join('\n')
    : 'None';

  const prompt = `You are bootstrapping a knowledge wiki for a Spitznagel-style tail-risk hedging bot (ETH options on Lyra/Derive).

Synthesize the historical data below into 11 well-structured wiki pages. Each page must follow the schema exactly.

## Wiki Schema
${schema}

## Historical Journal Entries (${journalEntries.length} total, showing ${sampleEntries.length})
${journalText}

## Reviewed Hypotheses (${reviewedHypotheses.length} with verdicts)
${hypothesesText}

## Active Lessons
${lessonsText}

## Instructions
1. Synthesize patterns across ALL entries — don't just summarize recent ones
2. Use specific data values and dates from the entries as evidence
3. Identify contradictions or evolving patterns (regime transitions, etc.)
4. For lessons/mistakes: derive from hypothesis outcomes (disproven_costly = mistake, confirmed_convex = good pattern)
5. Each page MUST start with a bold TLDR line
6. Follow the required sections from the schema exactly
7. Today's date: ${new Date().toISOString().split('T')[0]}

Output each page as:
<wiki_page path="regimes/current.md">
[full page content]
</wiki_page>

Generate ALL 11 pages: ${WIKI_ALL_PAGES.join(', ')}`;

  console.log('📚 Sending to Claude for synthesis...');

  try {
    const response = await axios.post('https://api.anthropic.com/v1/messages', {
      model: 'claude-sonnet-4-6',
      max_tokens: 16384,
      messages: [{ role: 'user', content: prompt }],
    }, {
      headers: {
        'x-api-key': process.env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json',
      },
      timeout: 180000,
    });

    const text = response.data?.content?.[0]?.text || '';

    // Parse wiki_page blocks
    const pageRegex = /<wiki_page\s+path="([^"]+)">([\s\S]*?)<\/wiki_page>/g;
    let match;
    let writeCount = 0;

    while ((match = pageRegex.exec(text)) !== null) {
      const pagePath = match[1];
      const content = match[2].trim();

      if (!WIKI_ALL_PAGES.includes(pagePath)) {
        console.log(`  ⚠️ Unknown page path: ${pagePath}, skipping`);
        continue;
      }

      if (content.length < 50) {
        console.log(`  ⚠️ Content too short for ${pagePath} (${content.length} chars), skipping`);
        continue;
      }

      const fullPath = path.join(WIKI_DIR, pagePath);
      fs.mkdirSync(path.dirname(fullPath), { recursive: true });
      fs.writeFileSync(fullPath, content);
      writeCount++;
      console.log(`  ✅ ${pagePath} (${content.length} chars)`);
    }

    // Write meta
    const meta = {
      seeded_at: new Date().toISOString(),
      journal_entries_used: sampleEntries.length,
      hypotheses_used: reviewedHypotheses.length,
      lessons_used: activeLessons.length,
      pages_written: writeCount,
    };
    fs.writeFileSync(META_PATH, JSON.stringify(meta, null, 2));

    console.log(`\n📚 Wiki seeded: ${writeCount}/${WIKI_ALL_PAGES.length} pages written`);

    if (writeCount < WIKI_ALL_PAGES.length) {
      const written = [];
      const regex2 = /<wiki_page\s+path="([^"]+)">/g;
      let m;
      while ((m = regex2.exec(text)) !== null) written.push(m[1]);
      const missing = WIKI_ALL_PAGES.filter(p => !written.includes(p));
      console.log(`  Missing pages: ${missing.join(', ')}`);
      console.log('  Re-run or manually create these pages.');
    }
  } catch (e) {
    console.error('❌ Seed failed:', e.message);
    if (e.response?.data) {
      console.error('API response:', JSON.stringify(e.response.data, null, 2));
    }
    process.exit(1);
  }
}

seedWiki();
