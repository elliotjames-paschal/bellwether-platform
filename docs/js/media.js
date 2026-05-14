(function () {
  'use strict';

  // ─── State ──────────────────────────────────────────────────────────────────
  let summaryData = null;
  let outletsData = null;
  let citationsData = null;
  let detailDisplayCount = 20;
  let outletDisplayCount = 15;
  let outletShowAll = false;

  // Sorting state
  let sortColumn = 'citations';
  let sortDirection = 'desc';

  // ─── Init ───────────────────────────────────────────────────────────────────
  async function init() {
    try {
      const [summary, outlets, citations] = await Promise.all([
        fetch('data/media_summary.json').then(r => r.ok ? r.json() : null),
        fetch('data/media_outlets.json').then(r => r.ok ? r.json() : null),
        fetch('data/media_citations.json').then(r => r.ok ? r.json() : null),
      ]);

      summaryData = summary;
      outletsData = outlets;
      citationsData = citations;

      if (!summaryData || !outletsData || !citationsData) {
        showEmptyState();
        return;
      }

      renderDashboard();
      setupHandlers();
      setupModal();
    } catch (err) {
      console.error('Failed to load media data:', err);
      showEmptyState();
    }
  }

  function showEmptyState() {
    const el = document.getElementById('dashboard-view');
    if (el) {
      el.innerHTML += '<div class="empty-state"><h3>No data yet</h3><p>Media citation data will appear here once the pipeline has run.</p></div>';
    }
  }

  // ─── Dashboard ──────────────────────────────────────────────────────────────
  function renderDashboard() {
    renderMetaDate();
    renderHeroCollage();
    renderHeroStats();
    renderTrendChart();
    renderOutletTable();
  }

  function renderMetaDate() {
    const el = document.getElementById('meta-date');
    if (el && summaryData.generated_at) {
      const d = new Date(summaryData.generated_at);
      el.textContent = 'Updated ' + d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
    }
  }

  function renderHeroCollage() {
    const container = document.getElementById('hero-collage');
    if (!container || !citationsData) return;

    const citations = citationsData.citations || [];
    const withTitle = citations.filter(c => c.title && c.domain);
    if (!withTitle.length) return;

    // Topics regular people understand (not crypto/platform inside baseball)
    const goodTopics = new Set(['US Politics', 'Iran Conflict', 'Military & Defense', 'Fed & Rates', 'Nobel Prize', 'SpaceX IPO']);

    // Outlets people recognize
    const majorOutlets = new Set([
      'nytimes.com','washingtonpost.com','wsj.com','reuters.com','apnews.com',
      'bbc.com','bbc.co.uk','cnn.com','cnbc.com','bloomberg.com','foxnews.com',
      'forbes.com','newsweek.com','politico.com','thehill.com','abcnews.com',
      'nbcnews.com','cbsnews.com','theguardian.com','ft.com','economist.com',
      'nypost.com','finance.yahoo.com',
    ]);

    // Filter out crypto/platform navel-gazing titles
    const boringKeywords = /tokeniz|onchain|chainalysis|crypto|blockchain|solana|ethereum|bitcoin|robinhood|coinbase|NBA|NFL|NHL|MLB|PGA|NCAA|Super Bowl|March Madness|playoff|World Series|touchdown|quarterback|batting|pitcher|draft pick|fantasy football|fantasy basketball|DraftKings|FanDuel|sportsbook|sports bet/i;

    // Title must clearly be about prediction markets / betting odds
    const pmKeywords = /prediction market|betting (odds|market)|polymarket|kalshi|predictit|event contract|wagering|bettors|traders (bet|give|put|price)|odds (of|on|for|say|suggest|show|give|put|at)|percent chance|probability of/i;

    const scored = withTitle
      .filter(c => majorOutlets.has(c.domain) && !boringKeywords.test(c.title) && pmKeywords.test(c.title + ' ' + (c.sentence || '')))
      .map(c => {
        let score = 0;
        if (c.topic && goodTopics.has(c.topic)) score += 150;
        if (c.title.length < 70) score += 30;
        if (c.date) {
          const age = (Date.now() - new Date(c.date).getTime()) / 86400000;
          if (age < 7) score += 40 - age * 5;
        }
        return { c, score };
      });

    scored.sort((a, b) => b.score - a.score);

    // Deduplicate by title (radio syndication creates many copies)
    const seen = new Set();
    const deduped = [];
    for (const s of scored) {
      const key = s.c.title.slice(0, 40).toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);
      deduped.push(s);
    }

    const top = deduped.slice(0, 30).map(s => s.c);

    // Build two columns of article cards
    container.innerHTML = '';
    var col1 = document.createElement('div');
    col1.className = 'hero-feed-col';
    var col2 = document.createElement('div');
    col2.className = 'hero-feed-col';

    // Topic label mapping (shorten for wire-style display)
    var topicLabels = {
      'US Politics': 'POLITICS', 'Iran Conflict': 'WORLD', 'Regulation': 'REGULATION',
      'Crypto': 'MARKETS', 'Military & Defense': 'DEFENSE', 'Fed & Rates': 'ECONOMY',
    };

    function makeCard(c, isLead) {
      var div = document.createElement('div');
      div.className = 'hero-collage-item' + (isLead ? ' lead-item' : '');

      var maxLen = isLead ? 90 : 72;
      var title = (c.title || '').length > maxLen ? c.title.slice(0, maxLen - 3) + '\u2026' : c.title;
      var domain = c.domain || '';
      var outletName = c.domain_name || c.station || domain.replace(/\.com$|\.co\.uk$|\.org$/,'');
      var dateStr = '';
      if (c.date) {
        var d = new Date(c.date);
        dateStr = d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
      }
      var section = topicLabels[c.topic] || '';
      var excerpt = (c.sentence || '').replace(/<[^>]*>/g, '').trim();
      var excerptMax = isLead ? 130 : 90;
      if (excerpt.length > excerptMax) excerpt = excerpt.slice(0, excerptMax - 3) + '\u2026';

      var logoUrl = domain ? 'https://www.google.com/s2/favicons?domain=' + encodeURIComponent(domain) + '&sz=32' : '';

      div.innerHTML =
        (section ? '<div class="hero-collage-section">' + esc(section) + '</div>' : '') +
        '<div class="hero-collage-title">' + esc(title) + '</div>' +
        (excerpt ? '<div class="hero-collage-excerpt">' + esc(excerpt) + '</div>' : '') +
        '<div class="hero-collage-byline">' +
          (logoUrl ? '<img class="hero-collage-logo" src="' + esc(logoUrl) + '" alt="">' : '') +
          esc(outletName) +
          (dateStr ? ' <span class="byline-date">\u00b7 ' + esc(dateStr) + '</span>' : '') +
        '</div>';
      return div;
    }

    // Split items between columns
    var items1 = top.filter(function(_, i) { return i % 2 === 0; });
    var items2 = top.filter(function(_, i) { return i % 2 === 1; });

    // Duplicate items so the scroll loops seamlessly
    var all1 = items1.concat(items1);
    var all2 = items2.concat(items2);

    all1.forEach(function(c, i) { col1.appendChild(makeCard(c, i % items1.length % 4 === 0)); });
    all2.forEach(function(c, i) { col2.appendChild(makeCard(c, i % items2.length % 4 === 2)); });

    container.appendChild(col1);
    container.appendChild(col2);

    // JS-driven seamless scroll loop
    var paused = false;
    container.addEventListener('mouseenter', function() { paused = true; });
    container.addEventListener('mouseleave', function() { paused = false; });

    // Wait a frame for layout, then measure and start scrolling
    requestAnimationFrame(function() {
      // Height of the first set of items (half the column since we duplicated)
      var h1 = 0, h2 = 0;
      for (var i = 0; i < items1.length; i++) h1 += col1.children[i].offsetHeight;
      for (var i = 0; i < items2.length; i++) h2 += col2.children[i].offsetHeight;

      var y1 = 0, y2 = -h2; // col1 scrolls up, col2 starts scrolled up and scrolls down
      var speed1 = 0.3; // pixels per frame
      var speed2 = 0.25;

      function tick() {
        if (!paused) {
          // Column 1: scroll up
          y1 -= speed1;
          if (y1 <= -h1) y1 += h1;
          col1.style.transform = 'translateY(' + y1 + 'px)';

          // Column 2: scroll down
          y2 += speed2;
          if (y2 >= 0) y2 -= h2;
          col2.style.transform = 'translateY(' + y2 + 'px)';
        }
        requestAnimationFrame(tick);
      }
      requestAnimationFrame(tick);
    });
  }

  function typewriterHTML(el, html, charDelay, onDone) {
    // Parse HTML string, type out text characters while preserving tags
    el.innerHTML = '';
    var i = 0;
    function step() {
      if (i >= html.length) { if (onDone) onDone(); return; }
      // If we hit a tag, insert the whole tag at once
      if (html[i] === '<') {
        var close = html.indexOf('>', i);
        if (close !== -1) {
          i = close + 1;
          el.innerHTML = html.slice(0, i);
          requestAnimationFrame(step);
          return;
        }
      }
      i++;
      el.innerHTML = html.slice(0, i);
      setTimeout(step, charDelay);
    }
    step();
  }

  function renderHeroStats() {
    const hero = summaryData.hero;

    // Typewriter the statement, then animate the number in
    const stmtEl = document.getElementById('hero-statement');
    const pctEl = document.getElementById('hero-pct');
    if (pctEl) pctEl.style.opacity = '0';

    const stmtHTML = 'Of prediction market odds reported in media, the share <em>that could be misinformation</em> is';
    const proseEl2 = document.getElementById('hero-prose');
    const ruleEl = document.querySelector('.hero-rule');
    if (proseEl2) { proseEl2.style.opacity = '0'; proseEl2.style.transition = 'opacity 0.6s'; }
    if (ruleEl) { ruleEl.style.opacity = '0'; ruleEl.style.transition = 'opacity 0.6s'; }
    if (stmtEl) {
      typewriterHTML(stmtEl, stmtHTML, 25, function() {
        // After typing finishes, animate the number in
        if (pctEl) {
          pctEl.style.transition = 'opacity 0.4s';
          pctEl.style.opacity = '1';
        }
        animateValue('hero-pct', 0, hero.pct_not_reportable_all || hero.pct_not_reportable || 0, 1200, '<span class="pct-sign">%</span>', true);
        // After number finishes, fade in the rule and prose
        setTimeout(function() {
          if (ruleEl) ruleEl.style.opacity = '1';
          if (proseEl2) proseEl2.style.opacity = '1';
        }, 1300);
      });
    } else {
      // Fallback if element not found
      animateValue('hero-pct', 0, hero.pct_not_reportable_all || hero.pct_not_reportable || 0, 1200, '<span class="pct-sign">%</span>', true);
    }

    // Prose paragraph — raw mentions (unfiltered) → filtered citations → fragility finding
    const proseEl = document.getElementById('hero-prose');
    if (proseEl) {
      const rawMentions = (hero.total_raw_mentions_30d || ((hero.citations_mention_only || 0) + (hero.total_citations_30d || 0)) || 0).toLocaleString();
      const outlets = (hero.total_raw_outlets_30d || hero.total_outlets || 0).toLocaleString();
      const filtered = (hero.total_citations_30d || 0).toLocaleString();
      const pct = hero.pct_not_reportable || 0;

      proseEl.innerHTML = 'Every day, Bellwether scans U.S. news for prediction market citations. In the last 30 days, we found <strong>' + rawMentions + '</strong> mentions across <strong>' +
        outlets + '</strong> outlets. Of those, <strong>' + filtered +
        '</strong> cite a specific market\u2009\u2014\u2009and <strong>' + pct +
        '%</strong> of those markets lack the liquidity for reliable reporting.';
    }
  }

  function animateValue(id, start, end, duration, suffix, isHtml) {
    const el = document.getElementById(id);
    if (!el) return;
    if (end === 0) { el[isHtml ? 'innerHTML' : 'textContent'] = '0' + (suffix || ''); return; }

    const range = end - start;
    const startTime = performance.now();
    const sfx = suffix || '';

    function tick(now) {
      const elapsed = now - startTime;
      const progress = Math.min(elapsed / duration, 1);
      const eased = 1 - Math.pow(1 - progress, 3);
      const val = Math.round(start + range * eased).toLocaleString() + sfx;
      el[isHtml ? 'innerHTML' : 'textContent'] = val;
      if (progress < 1) requestAnimationFrame(tick);
    }
    requestAnimationFrame(tick);
  }

  // ─── Trend Chart ────────────────────────────────────────────────────────────
  let trendChart = null;

  const TIER_COLORS = {
    reportable: '#3A8A5C',
    caution: '#D4950A',
    fragile: '#D94A4A',
  };

  const CATEGORY_COLORS = {
    'US Politics': '#4A90D9',
    'Iran Conflict': '#D94A4A',
    'Regulation': '#7B61C2',
    'Crypto': '#D4950A',
    'Military & Defense': '#2D6A4F',
    'Trade & Tariffs': '#E07A3A',
    'Fed & Rates': '#3AA5A5',
  };

  function getTimelineEntries() {
    var all = summaryData.timeline || [];
    var cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - 30);
    return all.filter(function(e) {
      var key = e.date || e.week || '';
      return new Date(key + 'T00:00:00') >= cutoff;
    });
  }

  function buildChartData(view, entries) {
    var labels = entries.map(function(e) {
      var key = e.date || e.week || '';
      var d = new Date(key + 'T00:00:00');
      return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
    });

    var datasets = [];

    if (view === 'total') {
      datasets.push({
        label: 'Total Citations',
        data: entries.map(function(e) { return e.count; }),
        borderColor: '#4A90D9',
        backgroundColor: 'rgba(74, 144, 217, 0.08)',
        fill: true,
        tension: 0.3,
        borderWidth: 2,
        pointRadius: 3,
        pointHoverRadius: 5,
      });
    } else if (view === 'tiers') {
      ['reportable', 'caution', 'fragile'].forEach(function(tier) {
        datasets.push({
          label: tier.charAt(0).toUpperCase() + tier.slice(1),
          data: entries.map(function(e) { return (e.tiers || {})[tier] || 0; }),
          borderColor: TIER_COLORS[tier],
          backgroundColor: 'transparent',
          tension: 0.3,
          borderWidth: 2,
          pointRadius: 3,
          pointHoverRadius: 5,
        });
      });
    } else if (view === 'categories') {
      // Collect all category names across entries
      var catSet = {};
      entries.forEach(function(e) {
        var cats = e.categories || {};
        Object.keys(cats).forEach(function(c) { catSet[c] = (catSet[c] || 0) + cats[c]; });
      });
      // Sort by total count descending, take top 6
      var sortedCats = Object.keys(catSet).sort(function(a, b) { return catSet[b] - catSet[a]; }).slice(0, 6);
      var fallbackColors = ['#4A90D9', '#D94A4A', '#7B61C2', '#D4950A', '#2D6A4F', '#E07A3A', '#3AA5A5'];

      sortedCats.forEach(function(cat, i) {
        datasets.push({
          label: cat,
          data: entries.map(function(e) { return (e.categories || {})[cat] || 0; }),
          borderColor: CATEGORY_COLORS[cat] || fallbackColors[i % fallbackColors.length],
          backgroundColor: 'transparent',
          tension: 0.3,
          borderWidth: 2,
          pointRadius: 2,
          pointHoverRadius: 5,
        });
      });
    }

    return { labels: labels, datasets: datasets };
  }

  function renderTrendChart() {
    var canvas = document.getElementById('trend-chart');
    var select = document.getElementById('trend-view');
    if (!canvas || !summaryData) return;

    var entries = getTimelineEntries();
    if (!entries.length) {
      canvas.parentElement.innerHTML = '<div class="empty-state" style="padding:24px"><p>No timeline data available.</p></div>';
      return;
    }

    // If no category data exists yet (old weekly format), hide the category option
    var hasCategories = entries.some(function(e) { return e.categories && Object.keys(e.categories).length > 0; });
    if (select) {
      var catOption = select.querySelector('option[value="categories"]');
      if (catOption && !hasCategories) catOption.style.display = 'none';
    }

    function draw(view) {
      var chartData = buildChartData(view, entries);

      if (trendChart) {
        trendChart.data = chartData;
        trendChart.update();
        return;
      }

      trendChart = new Chart(canvas, {
        type: 'line',
        data: chartData,
        options: {
          responsive: true,
          maintainAspectRatio: false,
          interaction: {
            mode: 'index',
            intersect: false,
          },
          plugins: {
            legend: {
              display: true,
              position: 'top',
              align: 'start',
              labels: {
                font: { family: "'DM Sans', sans-serif", size: 12 },
                color: '#6B6B6B',
                boxWidth: 12,
                boxHeight: 2,
                padding: 16,
                usePointStyle: false,
              },
            },
            tooltip: {
              backgroundColor: '#1A1A1A',
              titleFont: { family: "'DM Sans', sans-serif", size: 12 },
              bodyFont: { family: "'JetBrains Mono', monospace", size: 12 },
              padding: 12,
              cornerRadius: 6,
            },
          },
          scales: {
            x: {
              grid: { display: false },
              ticks: {
                font: { family: "'DM Sans', sans-serif", size: 11 },
                color: '#6B6B6B',
                maxRotation: 0,
                maxTicksLimit: 10,
              },
              border: { color: '#E8E8E8' },
            },
            y: {
              beginAtZero: true,
              grid: { color: 'rgba(0,0,0,0.04)' },
              ticks: {
                font: { family: "'JetBrains Mono', monospace", size: 11 },
                color: '#6B6B6B',
                precision: 0,
              },
              border: { display: false },
            },
          },
        },
      });
    }

    draw('total');

    if (select) {
      select.addEventListener('change', function() { draw(this.value); });
    }
  }

  // ─── Outlet Table ───────────────────────────────────────────────────────────

  function getSortValue(outlet) {
    switch (sortColumn) {
      case 'name': return (outlet.domain_name || outlet.domain || '').toLowerCase();
      case 'citations': return outlet.citations_30d != null ? outlet.citations_30d : (outlet.total_citations || 0);
      case 'reportable': return outlet.pct_reportable != null ? outlet.pct_reportable : -1;
      case 'fragility': return outlet.avg_cost_to_move_5c != null ? outlet.avg_cost_to_move_5c : -1;
      case 'brier': return outlet.avg_brier != null ? outlet.avg_brier : 999;
      default: return 0;
    }
  }

  function sortOutlets(outlets) {
    const sorted = [...outlets];
    sorted.sort((a, b) => {
      let va = getSortValue(a);
      let vb = getSortValue(b);
      if (typeof va === 'string') {
        const cmp = va.localeCompare(vb);
        return sortDirection === 'asc' ? cmp : -cmp;
      }
      return sortDirection === 'asc' ? va - vb : vb - va;
    });
    return sorted;
  }

  function updateSortArrows() {
    document.querySelectorAll('.sort-arrow').forEach(el => {
      el.className = 'sort-arrow';
    });
    const active = document.getElementById('sort-' + sortColumn);
    if (active) {
      active.className = 'sort-arrow ' + sortDirection;
    }
  }

  function renderOutletTable() {
    const tbody = document.getElementById('outlet-tbody');
    const countEl = document.getElementById('outlet-count');
    if (!tbody || !outletsData) return;

    const outlets = outletsData.outlets || [];
    if (countEl) countEl.textContent = outlets.length + ' outlets';

    const sorted = sortOutlets(outlets);

    tbody.innerHTML = '';
    updateSortArrows();

    const visible = outletShowAll ? sorted : sorted.slice(0, outletDisplayCount);
    const showMoreEl = document.getElementById('outlet-show-more');
    const showMoreBtn = document.getElementById('outlet-show-more-btn');
    if (showMoreEl) {
      showMoreEl.style.display = sorted.length > outletDisplayCount && !outletShowAll ? '' : 'none';
    }
    if (showMoreBtn) {
      showMoreBtn.textContent = 'Show all ' + sorted.length + ' outlets';
    }

    visible.forEach(o => {
      const tr = document.createElement('tr');

      const plats = [];
      if (o.platforms.polymarket > 0) plats.push('<span class="plat-tag" style="color:#2563eb">PM ' + o.platforms.polymarket + '</span>');
      if (o.platforms.kalshi > 0) plats.push('<span class="plat-tag" style="color:#059669">K ' + o.platforms.kalshi + '</span>');

      const displayName = o.domain_name || o.domain;
      const pctR = o.pct_reportable != null ? o.pct_reportable + '%' : '\u2014';
      const avgF = o.avg_cost_to_move_5c != null ? formatVolume(o.avg_cost_to_move_5c) : '\u2014';
      const c24 = o.citations_24h != null ? o.citations_24h : (o.total_citations || 0);
      const c30 = o.citations_30d != null ? o.citations_30d : (o.total_citations || 0);

      const tiers = o.tier_breakdown || {};
      const tierTotal = (tiers.reportable || 0) + (tiers.caution || 0) + (tiers.fragile || 0);
      let tierBarHtml = '\u2014';
      if (tierTotal > 0) {
        const rPct = ((tiers.reportable || 0) / tierTotal * 100).toFixed(1);
        const cPct = ((tiers.caution || 0) / tierTotal * 100).toFixed(1);
        const fPct = ((tiers.fragile || 0) / tierTotal * 100).toFixed(1);
        tierBarHtml = `<div class="tier-bar" title="${tiers.reportable || 0} reportable, ${tiers.caution || 0} caution, ${tiers.fragile || 0} fragile">
          <div class="seg-reportable" style="width:${rPct}%"></div>
          <div class="seg-caution" style="width:${cPct}%"></div>
          <div class="seg-fragile" style="width:${fPct}%"></div>
        </div>`;
      }

      tr.innerHTML = `
        <td class="col-domain">${esc(displayName)}</td>
        <td class="col-num">${c30}<span style="font-size:11px;color:var(--bw-text-secondary);margin-left:4px">(${c24} today)</span></td>
        <td class="col-num">${pctR}</td>
        <td class="col-num">${avgF}</td>
        <td class="col-center">${tierBarHtml}</td>
        <td><div class="outlet-platforms">${plats.join('')}</div></td>
        <td class="col-arrow">&rsaquo;</td>
      `;

      tr.addEventListener('click', () => showOutletDetail(o.domain));
      tbody.appendChild(tr);
    });
  }

  // ─── Outlet Detail ──────────────────────────────────────────────────────────
  function showOutletDetail(domain) {
    detailDisplayCount = 20;

    const dashboardView = document.getElementById('dashboard-view');
    const detailView = document.getElementById('outlet-detail');
    dashboardView.style.display = 'none';
    detailView.classList.add('active');
    window.scrollTo({ top: 0, behavior: 'smooth' });

    renderOutletDetail(domain);
  }

  function showDashboard() {
    const dashboardView = document.getElementById('dashboard-view');
    const detailView = document.getElementById('outlet-detail');
    detailView.classList.remove('active');
    dashboardView.style.display = '';
  }

  function setupHandlers() {
    const backBtn = document.getElementById('back-btn');
    if (backBtn) backBtn.addEventListener('click', showDashboard);

    const loadMore = document.getElementById('detail-load-more');
    if (loadMore) {
      loadMore.addEventListener('click', () => {
        detailDisplayCount += 20;
        const domain = loadMore.dataset.domain;
        if (domain) renderDetailCitations(domain);
      });
    }

    const outletShowMoreBtn = document.getElementById('outlet-show-more-btn');
    if (outletShowMoreBtn) {
      outletShowMoreBtn.addEventListener('click', () => {
        outletShowAll = true;
        renderOutletTable();
      });
    }

    document.querySelectorAll('.outlet-table th.sortable').forEach(th => {
      th.addEventListener('click', () => {
        const col = th.dataset.sort;
        if (sortColumn === col) {
          sortDirection = sortDirection === 'desc' ? 'asc' : 'desc';
        } else {
          sortColumn = col;
          sortDirection = col === 'name' ? 'asc' : 'desc';
        }
        renderOutletTable();
      });
    });

    const methToggle = document.getElementById('methodology-toggle');
    const methSection = document.getElementById('methodology');
    if (methToggle && methSection) {
      methToggle.addEventListener('click', () => {
        methSection.classList.toggle('open');
      });
    }
  }

  function renderOutletDetail(domain) {
    const outlet = (outletsData.outlets || []).find(o => o.domain === domain);
    const headerEl = document.getElementById('outlet-detail-content');

    if (!outlet || !headerEl) return;

    const displayName = outlet.domain_name || outlet.domain;

    const platParts = [];
    if (outlet.platforms.polymarket > 0) platParts.push('Polymarket (' + outlet.platforms.polymarket + ')');
    if (outlet.platforms.kalshi > 0) platParts.push('Kalshi (' + outlet.platforms.kalshi + ')');

    const pctR = outlet.pct_reportable != null ? outlet.pct_reportable + '%' : '\u2014';
    const avgF = outlet.avg_cost_to_move_5c != null ? formatVolume(outlet.avg_cost_to_move_5c) : '\u2014';
    const avgB = outlet.avg_brier != null ? outlet.avg_brier.toFixed(3) : '\u2014';
    const c24 = outlet.citations_24h != null ? outlet.citations_24h : outlet.total_citations;
    const c30 = outlet.citations_30d != null ? outlet.citations_30d : outlet.total_citations;

    const tiers = outlet.tier_breakdown || {};
    const tierTotal = (tiers.reportable || 0) + (tiers.caution || 0) + (tiers.fragile || 0);
    let tierHtml = '';
    if (tierTotal > 0) {
      const rPct = ((tiers.reportable || 0) / tierTotal * 100).toFixed(1);
      const cPct = ((tiers.caution || 0) / tierTotal * 100).toFixed(1);
      const fPct = ((tiers.fragile || 0) / tierTotal * 100).toFixed(1);
      tierHtml = `
        <span class="outlet-detail-stat">
          <div class="tier-bar" style="min-width:120px;height:10px" title="${tiers.reportable || 0} reportable, ${tiers.caution || 0} caution, ${tiers.fragile || 0} fragile">
            <div class="seg-reportable" style="width:${rPct}%"></div>
            <div class="seg-caution" style="width:${cPct}%"></div>
            <div class="seg-fragile" style="width:${fPct}%"></div>
          </div>
        </span>`;
    }

    headerEl.innerHTML = `
      <div class="outlet-detail-header">
        <div>
          <div class="outlet-detail-name">${esc(displayName)}</div>
        </div>
        <div class="outlet-detail-stats">
          <span class="outlet-detail-stat"><strong>${c24}</strong> | <strong>${c30}</strong> citations (24h | 30d)</span>
          <span class="outlet-detail-stat"><strong>${pctR}</strong> reportable</span>
          <span class="outlet-detail-stat"><strong>${avgF}</strong> avg $ to move 5&cent;</span>
          <span class="outlet-detail-stat"><strong>${avgB}</strong> avg Brier</span>
          ${tierHtml}
          ${platParts.length ? '<span class="outlet-detail-stat">' + platParts.join(' \u00b7 ') + '</span>' : ''}
        </div>
      </div>
      <div class="section-header" style="padding-top:0">
        <h2>Citations</h2>
      </div>
    `;

    renderDetailCitations(domain);
  }

  function renderDetailCitations(domain) {
    const container = document.getElementById('detail-citations');
    const loadMore = document.getElementById('detail-load-more');
    if (!container || !citationsData) return;

    loadMore.dataset.domain = domain;

    const all = (citationsData.citations || []).filter(c => {
      return (c.domain === domain) || (c.station === domain);
    });

    const toShow = all.slice(0, detailDisplayCount);
    container.innerHTML = '';

    if (!toShow.length) {
      container.innerHTML = '<div class="empty-state"><p>No citations found for this outlet.</p></div>';
      if (loadMore) loadMore.style.display = 'none';
      return;
    }

    toShow.forEach(c => container.appendChild(buildCitationCard(c)));

    if (loadMore) {
      loadMore.style.display = all.length > detailDisplayCount ? '' : 'none';
      loadMore.textContent = 'Load More (' + Math.min(detailDisplayCount, all.length) + ' of ' + all.length + ')';
    }
  }

  // ─── Citation Card Builder ──────────────────────────────────────────────────
  function buildCitationCard(c) {
    const card = document.createElement('div');
    card.className = 'citation-card';

    const dateStr = c.date
      ? new Date(c.date).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })
      : '';
    const displayName = c.domain_name || c.station || c.domain || '';

    // Platform pill — link to platform contract if available
    let platHtml = '';
    const platUrl = buildPlatformUrl(c);
    if (c.platform === 'polymarket') {
      platHtml = platUrl
        ? '<a href="' + esc(platUrl) + '" target="_blank" rel="noopener" class="cc-platform pm">Polymarket</a>'
        : '<span class="cc-platform pm">Polymarket</span>';
    } else if (c.platform === 'kalshi') {
      platHtml = platUrl
        ? '<a href="' + esc(platUrl) + '" target="_blank" rel="noopener" class="cc-platform k">Kalshi</a>'
        : '<span class="cc-platform k">Kalshi</span>';
    }

    let sentenceHtml = '';
    if (c.sentence) {
      sentenceHtml = '<div class="cc-sentence">' + esc(c.sentence) + '</div>';
    }

    // Market match + fragility
    let matchHtml = '';
    if (c.market_question) {
      const tierNum = c.price_tier || 0;
      const tierClass = tierNum ? 'tier-' + tierNum : '';
      const tierLabel = c.tier_label || (tierNum === 1 ? 'Reportable' : tierNum === 2 ? 'Caution' : tierNum === 3 ? 'Fragile' : '');
      const fragText = c.fragility_score != null ? c.fragility_score : '';
      const probText = c.probability_cited != null ? (c.probability_cited * 100).toFixed(0) + '% cited' : '';
      const priceText = c.price_at_citation != null ? (c.price_at_citation * 100).toFixed(0) + '% actual' : '';

      let gapHtml = '';
      if (c.probability_cited != null && c.price_at_citation != null) {
        const gap = Math.abs(c.probability_cited - c.price_at_citation) * 100;
        const gapColor = gap <= 3 ? 'var(--bw-green)' : gap <= 10 ? 'var(--bw-amber)' : 'var(--bw-red)';
        gapHtml = '<span style="color:' + gapColor + ';font-weight:500">\u0394' + gap.toFixed(0) + 'pp</span>';
      }

      const questionHtml = '<strong>' + esc(c.market_question) + '</strong>';

      // Frag badge is clickable to open modal
      const badgeAttr = ' data-modal-id="' + esc(c.id) + '"';
      matchHtml = `
        <div class="cc-match">
          <div class="cc-match-info">
            ${questionHtml}
            ${probText || priceText ? '<div class="cc-prob">' + [probText, priceText, gapHtml].filter(Boolean).join(' \u00b7 ') + '</div>' : ''}
          </div>
          ${tierLabel ? '<span class="frag-badge ' + tierClass + '"' + badgeAttr + '>' + tierLabel + (fragText ? ' \u00b7 ' + fragText : '') + '</span>' : ''}
        </div>
      `;
    } else if (c.probability_cited != null) {
      matchHtml = `
        <div class="cc-match">
          <div class="cc-match-info">
            <div class="cc-prob">${(c.probability_cited * 100).toFixed(0)}% probability cited \u00b7 No matching Bellwether market found</div>
          </div>
        </div>
      `;
    }

    card.innerHTML = `
      <div class="cc-meta">
        <span class="cc-domain">${esc(displayName)}</span>
        <span class="cc-date">${dateStr}</span>
        ${platHtml}
        ${c.source_type === 'tv' ? '<span>TV</span>' : ''}
      </div>
      <div class="cc-title"><a href="${esc(c.url)}" target="_blank" rel="noopener">${esc(c.title || 'Untitled')}</a></div>
      ${sentenceHtml}
      ${matchHtml}
    `;

    return card;
  }

  // ─── Helpers ────────────────────────────────────────────────────────────────
  function esc(str) {
    if (!str) return '';
    const div = document.createElement('div');
    div.appendChild(document.createTextNode(str));
    return div.innerHTML;
  }

  function buildPlatformUrl(c) {
    if (!c) return '';
    // Prefer pre-computed market_url from pipeline
    if (c.market_url) return c.market_url;
    // Fallback: build URL from identifiers
    if (c.pm_slug) return 'https://polymarket.com/event/' + encodeURIComponent(c.pm_slug);
    if (c.pm_market_id) return 'https://polymarket.com/market/' + encodeURIComponent(c.pm_market_id);
    if (c.k_ticker) return 'https://kalshi.com/markets/' + encodeURIComponent(c.k_ticker);
    return '';
  }

  function formatVolume(v) {
    if (v == null) return '\u2014';
    if (v >= 1e6) return '$' + (v / 1e6).toFixed(1) + 'M';
    if (v >= 1e3) return '$' + (v / 1e3).toFixed(0) + 'K';
    return '$' + Math.round(v).toLocaleString();
  }

  // ──�� Contract Modal (same as monitor.html) ───────��─────────────────────────
  const LIVE_DATA_SERVER = 'https://api.bellwethermetrics.com';

  function setupModal() {
    const overlay = document.getElementById('media-modal');
    if (!overlay) return;

    overlay.addEventListener('click', (e) => {
      if (e.target === overlay) closeMediaModal();
    });
    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape') closeMediaModal();
    });

    // Delegate click on frag-badge (or any data-modal-id element)
    document.addEventListener('click', (e) => {
      const target = e.target.closest('[data-modal-id]');
      if (!target) return;
      e.preventDefault();
      openMediaModal(target.dataset.modalId);
    });
  }

  function closeMediaModal() {
    const modal = document.getElementById('media-modal');
    if (modal) {
      modal.classList.remove('visible');
      document.body.style.overflow = '';
    }
  }

  // ── Live data fetch (same API as monitor.html) ──
  async function fetchLiveData(tokenOrTicker, platform) {
    if (!tokenOrTicker) return null;
    try {
      const r = await fetch(LIVE_DATA_SERVER + '/api/metrics/' + platform + '/' + tokenOrTicker);
      if (!r.ok) return null;
      return await r.json();
    } catch (e) {
      console.warn('Live data fetch failed:', e);
      return null;
    }
  }

  function formatPrice(v) {
    if (v == null) return '\u2014';
    return Math.round(v * 100) + '%';
  }

  function renderLiveDataSection(data) {
    if (!data) {
      return '<div class="modal-live-data"><div class="modal-live-data-header">Live Market Depth</div><div class="modal-live-data-note">Live data not available for this market</div></div>';
    }

    const robustness = data.robustness || {};
    const vwap = data.vwap_details || data.vwap_6h || {};

    const costToMove = robustness.cost_to_move_5c != null ? formatVolume(robustness.cost_to_move_5c) : 'N/A';
    const vwapValue = data.bellwether_price != null ? Math.round(data.bellwether_price * 100) + '%' : 'No trades';
    const vwapLabel = data.price_label || '6h VWAP';

    const reportability = robustness.reportability || 'fragile';
    const badgeLabel = reportability.charAt(0).toUpperCase() + reportability.slice(1);

    return '<div class="modal-live-data">' +
      '<div class="modal-live-data-header">Live Market Depth</div>' +
      '<div class="modal-live-data-grid">' +
        '<div class="modal-live-data-item">' +
          '<div class="modal-live-data-label">Cost to Move 5\u00a2</div>' +
          '<div class="modal-live-data-value">' + costToMove + '</div>' +
          '<div class="modal-live-data-badge ' + reportability + '">' + badgeLabel + '</div>' +
        '</div>' +
        '<div class="modal-live-data-item">' +
          '<div class="modal-live-data-label">' + esc(vwapLabel) + '</div>' +
          '<div class="modal-live-data-value">' + vwapValue + '</div>' +
          '<div class="modal-live-data-sub">' + (vwap.trade_count != null ? vwap.trade_count : '\u2014') + ' trades</div>' +
        '</div>' +
      '</div>' +
      '<div class="modal-live-data-timestamp">Updated ' + (data.fetched_at ? new Date(data.fetched_at).toLocaleTimeString() : 'Unknown') + '</div>' +
    '</div>';
  }

  function openMediaModal(citationId) {
    if (!citationsData) return;
    const c = (citationsData.citations || []).find(x => x.id === citationId);
    if (!c || !c.market_question) return;

    const modal = document.getElementById('media-modal');
    const content = document.getElementById('media-modal-content');
    if (!modal || !content) return;

    const platformClass = c.platform === 'polymarket' ? 'pm' : 'kalshi';
    const platformLabel = c.platform === 'polymarket' ? 'Polymarket' : 'Kalshi';

    // ── Price box (same as renderMarketModal) ──
    let priceVal = '\u2014';
    let priceSub = '';
    if (c.price_at_citation != null) {
      priceVal = (c.price_at_citation * 100).toFixed(0) + '\u00a2';
      priceSub = 'at citation';
    } else if (c.probability_cited != null) {
      priceVal = (c.probability_cited * 100).toFixed(0) + '\u00a2';
      priceSub = 'cited';
    }

    const pricesHtml = `
      <div class="modal-price-box ${platformClass}">
        <div class="modal-price-label">${platformLabel}</div>
        <div class="modal-price-value">${priceVal}</div>
        <div class="modal-price-sub">${priceSub}${c.volume_usd != null ? ' \u00b7 ' + formatVolume(c.volume_usd) + ' vol' : ''}</div>
      </div>`;

    // ── Platform link (same as renderMarketModal) ��─
    let linkHtml = '';
    const url = buildPlatformUrl(c);
    if (url) {
      linkHtml = `<div class="modal-links single">
        <a href="${esc(url)}" target="_blank" rel="noopener" class="modal-link-box ${platformClass}">
          <div class="modal-link-info"><span class="modal-link-platform">${platformLabel}</span><span class="modal-link-text">View market details &amp; trade</span></div>
          <span class="modal-link-arrow">\u2197</span>
        </a>
      </div>`;
    }

    // ── BWR ticker ─���
    const tickerHtml = c.market_ticker
      ? '<div style="font-family:var(--font-mono);font-size:0.6875rem;color:var(--gray-500,#888);margin-top:2px">' + esc(c.market_ticker) + '</div>'
      : '';

    // ── Tier badge for header ──
    const tierNum = c.price_tier || 0;
    const tierBadgeHtml = c.tier_label
      ? '<span class="frag-badge tier-' + tierNum + '" style="font-size:11px;padding:2px 8px">' + esc(c.tier_label) + '</span>'
      : '';

    content.innerHTML = `
      <div class="modal-header">
        <div class="modal-header-info">
          <div class="modal-meta">
            <span class="platform-badge ${platformClass}">${platformClass === 'pm' ? 'PM' : 'K'}</span>
            ${tierBadgeHtml}
          </div>
          <h2 class="modal-title">${esc(c.market_question)}</h2>
          ${tickerHtml}
        </div>
        <button class="modal-close" aria-label="Close">&times;</button>
      </div>
      <div class="modal-body">
        <div class="modal-prices single-col">${pricesHtml}</div>
        <div class="modal-live-data-container"><div class="modal-live-data"><div class="modal-live-data-header">Live Market Depth</div><div class="modal-live-data-loading">Loading...</div></div></div>
        ${linkHtml}
      </div>
    `;

    const closeBtn = content.querySelector('.modal-close');
    if (closeBtn) closeBtn.addEventListener('click', closeMediaModal);

    modal.classList.add('visible');
    document.body.style.overflow = 'hidden';

    // ── Fetch live data (same as monitor.html openModal) ──
    const liveContainer = content.querySelector('.modal-live-data-container');
    const pmTokenId = c.pm_token_id || '';
    const kTicker = c.k_ticker || '';

    if (pmTokenId) {
      fetchLiveData(pmTokenId, 'polymarket').then(data => {
        if (liveContainer) liveContainer.innerHTML = renderLiveDataSection(data);
      }).catch(() => {
        if (liveContainer) liveContainer.innerHTML = renderLiveDataSection(null);
      });
    } else if (kTicker) {
      fetchLiveData(kTicker, 'kalshi').then(data => {
        if (liveContainer) liveContainer.innerHTML = renderLiveDataSection(data);
      }).catch(() => {
        if (liveContainer) liveContainer.innerHTML = renderLiveDataSection(null);
      });
    } else {
      if (liveContainer) liveContainer.innerHTML = renderLiveDataSection(null);
    }
  }

  // ─── Boot ───────────────────────────────────────────────────────────────────
  document.addEventListener('DOMContentLoaded', init);
})();
