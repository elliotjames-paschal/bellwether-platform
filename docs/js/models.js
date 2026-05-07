(function () {
  'use strict';

  // ─── State ──────────────────────────────────────────────────────────────────
  let data = null;
  let modelDisplayCount = 25;
  let modelShowAll = false;
  let sortColumn = 'rank';
  let sortDirection = 'asc';

  // ─── Init ───────────────────────────────────────────────────────────────────
  async function init() {
    try {
      const resp = await fetch('data/models_leaderboard.json');
      if (!resp.ok) { showEmptyState(); return; }
      data = await resp.json();
      if (!data || !data.leaderboard || !data.leaderboard.length) {
        showEmptyState();
        return;
      }
      renderDashboard();
      setupHandlers();
    } catch (err) {
      console.error('Failed to load models data:', err);
      showEmptyState();
    }
  }

  function showEmptyState() {
    var el = document.getElementById('dashboard-view');
    if (el) {
      el.innerHTML += '<div class="empty-state"><h3>No data yet</h3><p>Model leaderboard data will appear here once loaded.</p></div>';
    }
  }

  // ─── Dashboard ──────────────────────────────────────────────────────────────
  function renderDashboard() {
    renderHero();
    renderHeroCollage();
    renderChart();
    renderTable();
  }

  // ─── Hero ───────────────────────────────────────────────────────────────────
  function renderHero() {
    var totalModels = data.total_models || 0;
    var supersBrier = data.superforecaster_brier;
    var bestAI = data.best_ai_brier;
    var beatCount = data.models_that_beat_supers || 0;

    var stmtEl = document.getElementById('hero-statement');
    var numEl = document.getElementById('hero-number');
    var proseEl = document.getElementById('hero-prose');
    var ruleEl = document.querySelector('.hero-rule');

    if (numEl) numEl.style.opacity = '0';
    if (proseEl) { proseEl.style.opacity = '0'; proseEl.style.transition = 'opacity 0.6s'; }
    if (ruleEl) { ruleEl.style.opacity = '0'; ruleEl.style.transition = 'opacity 0.6s'; }

    var stmtHTML = 'Of <em>' + totalModels + ' AI models</em> benchmarked on real-world event prediction, the number that outperform human forecasters is';

    if (stmtEl) {
      typewriterHTML(stmtEl, stmtHTML, 25, function() {
        if (numEl) {
          numEl.textContent = '0';
          numEl.style.transition = 'opacity 0.4s';
          numEl.style.opacity = '1';
        }
        setTimeout(function() {
          if (ruleEl) ruleEl.style.opacity = '1';
          if (proseEl) proseEl.style.opacity = '1';
        }, 600);
      });
    }

    if (proseEl) {
      proseEl.innerHTML = 'AI models beat superforecasters by <strong>46%</strong> on structured data problems. ' +
        'But on elections, conflicts, and policy \u2014 the events that actually matter \u2014 every model loses. ' +
        'The best AI achieves a Brier score of <strong>' + (bestAI || '0.104') + '</strong> against superforecasters\u2019 <strong>' + (supersBrier || '0.086') + '</strong>. ' +
        'Models pattern-match. They don\u2019t understand the world.';
    }
  }

  function typewriterHTML(el, html, charDelay, onDone) {
    el.innerHTML = '';
    var i = 0;
    function step() {
      if (i >= html.length) { if (onDone) onDone(); return; }
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

  // ─── Hero Collage (scrolling model feed) ──────────────────────────────────
  function renderHeroCollage() {
    var container = document.getElementById('hero-collage');
    if (!container || !data) return;

    var models = data.leaderboard || [];
    // Filter out the superforecaster baseline for the feed
    var aiModels = models.filter(function(m) {
      return m.model.toLowerCase().indexOf('superforecaster') === -1;
    });
    // Take a subset for display
    var feedModels = aiModels.slice(0, 40);
    if (!feedModels.length) return;

    container.innerHTML = '';
    var col1 = document.createElement('div');
    col1.className = 'hero-feed-col';
    var col2 = document.createElement('div');
    col2.className = 'hero-feed-col';

    function makeCard(m, isTop) {
      var div = document.createElement('div');
      div.className = 'hero-collage-item' + (isTop ? ' top-item' : '');

      var modelName = m.model || '';
      // Strip common suffixes to keep card names short
      modelName = modelName.replace(/\s*\(zero shot with crowd forecast\)/i, '');
      modelName = modelName.replace(/\s*\(scratchpad with crowd forecast\)/i, '');
      modelName = modelName.replace(/\s*\(zero shot\)/i, '');
      modelName = modelName.replace(/\s*\(scratchpad\)/i, '');
      if (modelName.length > 32) modelName = modelName.slice(0, 29) + '\u2026';
      var org = m.organization || m.team || '';
      var rank = m.rank || '';
      var brierStr = m.brier_overall != null ? m.brier_overall.toFixed(3) : '\u2014';
      var datasetStr = m.brier_dataset != null ? m.brier_dataset.toFixed(3) : '\u2014';
      var marketStr = m.brier_market != null ? m.brier_market.toFixed(3) : '\u2014';

      var rankClass = rank <= 10 ? 'top-10' : rank <= 50 ? 'top-50' : 'rest';

      div.innerHTML =
        '<div class="hero-collage-section">' + esc(org) + '</div>' +
        '<div class="hero-collage-title">' + esc(modelName) + '</div>' +
        '<div class="hero-collage-scores">' +
          '<span class="score-item">Brier <span class="score-val">' + brierStr + '</span></span>' +
          '<span class="score-item">D <span class="score-val">' + datasetStr + '</span></span>' +
          '<span class="score-item">M <span class="score-val">' + marketStr + '</span></span>' +
        '</div>' +
        '<div class="hero-collage-byline">' +
          '<span class="rank-badge ' + rankClass + '">' + rank + '</span>' +
          ' Rank #' + rank +
        '</div>';
      return div;
    }

    var items1 = feedModels.filter(function(_, i) { return i % 2 === 0; });
    var items2 = feedModels.filter(function(_, i) { return i % 2 === 1; });

    // Duplicate for seamless loop
    var all1 = items1.concat(items1);
    var all2 = items2.concat(items2);

    all1.forEach(function(m, i) { col1.appendChild(makeCard(m, i % items1.length % 5 === 0)); });
    all2.forEach(function(m, i) { col2.appendChild(makeCard(m, i % items2.length % 5 === 2)); });

    container.appendChild(col1);
    container.appendChild(col2);

    // Scroll animation
    var paused = false;
    container.addEventListener('mouseenter', function() { paused = true; });
    container.addEventListener('mouseleave', function() { paused = false; });

    requestAnimationFrame(function() {
      var h1 = 0, h2 = 0;
      for (var i = 0; i < items1.length; i++) h1 += col1.children[i].offsetHeight;
      for (var i = 0; i < items2.length; i++) h2 += col2.children[i].offsetHeight;

      var y1 = 0, y2 = -h2;
      var speed1 = 0.3;
      var speed2 = 0.25;

      function tick() {
        if (!paused) {
          y1 -= speed1;
          if (y1 <= -h1) y1 += h1;
          col1.style.transform = 'translateY(' + y1 + 'px)';

          y2 += speed2;
          if (y2 >= 0) y2 -= h2;
          col2.style.transform = 'translateY(' + y2 + 'px)';
        }
        requestAnimationFrame(tick);
      }
      requestAnimationFrame(tick);
    });
  }

  // ─── Chart ──────────────────────────────────────────────────────────────────
  var modelsChart = null;

  function renderChart() {
    var canvas = document.getElementById('models-chart');
    var select = document.getElementById('chart-view');
    if (!canvas || !data) return;

    function draw(view) {
      var chartData = buildChartData(view);

      if (modelsChart) {
        modelsChart.data = chartData.data;
        modelsChart.options.indexAxis = chartData.indexAxis || 'x';
        modelsChart.options.scales.x = chartData.scalesX;
        modelsChart.options.scales.y = chartData.scalesY;
        modelsChart.config.type = chartData.type;
        modelsChart.update();
        return;
      }

      modelsChart = new Chart(canvas, {
        type: chartData.type,
        data: chartData.data,
        options: {
          responsive: true,
          maintainAspectRatio: false,
          indexAxis: chartData.indexAxis || 'x',
          interaction: { mode: 'index', intersect: false },
          plugins: {
            legend: {
              display: true, position: 'top', align: 'start',
              labels: {
                font: { family: "'DM Sans', sans-serif", size: 12 },
                color: '#6B6B6B', boxWidth: 12, boxHeight: 2, padding: 16,
              },
            },
            tooltip: {
              backgroundColor: '#1A1A1A',
              titleFont: { family: "'DM Sans', sans-serif", size: 12 },
              bodyFont: { family: "'JetBrains Mono', monospace", size: 12 },
              padding: 12, cornerRadius: 6,
            },
          },
          scales: {
            x: chartData.scalesX,
            y: chartData.scalesY,
          },
        },
      });
    }

    draw('topModels');
    if (select) {
      select.addEventListener('change', function() {
        // Destroy and recreate chart when type changes
        if (modelsChart) { modelsChart.destroy(); modelsChart = null; }
        draw(this.value);
      });
    }
  }

  function buildChartData(view) {
    var models = data.leaderboard || [];
    var superforecaster = models.find(function(m) { return m.model.toLowerCase().indexOf('superforecaster') !== -1; });
    var aiModels = models.filter(function(m) { return m.model.toLowerCase().indexOf('superforecaster') === -1; });

    var defaultScalesX = {
      grid: { display: false },
      ticks: { font: { family: "'DM Sans', sans-serif", size: 11 }, color: '#6B6B6B', maxRotation: 45 },
      border: { color: '#E8E8E8' },
    };
    var defaultScalesY = {
      beginAtZero: true,
      grid: { color: 'rgba(0,0,0,0.04)' },
      ticks: { font: { family: "'JetBrains Mono', monospace", size: 11 }, color: '#6B6B6B' },
      border: { display: false },
    };

    if (view === 'topModels') {
      var top15 = aiModels.slice(0, 15);
      var labels = top15.map(function(m) {
        var name = m.model.length > 30 ? m.model.slice(0, 27) + '\u2026' : m.model;
        return name;
      });
      var datasets = [
        {
          label: 'AI Model (Overall Brier)',
          data: top15.map(function(m) { return m.brier_overall; }),
          backgroundColor: 'rgba(74, 144, 217, 0.7)',
          borderColor: '#4A90D9',
          borderWidth: 1,
        },
      ];
      if (superforecaster) {
        datasets.push({
          label: 'Superforecaster Baseline',
          data: top15.map(function() { return superforecaster.brier_overall; }),
          type: 'line',
          borderColor: '#D94A4A',
          borderWidth: 2,
          borderDash: [6, 4],
          pointRadius: 0,
          fill: false,
        });
      }
      return {
        type: 'bar',
        data: { labels: labels, datasets: datasets },
        scalesX: Object.assign({}, defaultScalesX, { ticks: Object.assign({}, defaultScalesX.ticks, { maxRotation: 45 }) }),
        scalesY: Object.assign({}, defaultScalesY, {
          title: { display: true, text: 'Brier Score (lower = better)', font: { family: "'DM Sans', sans-serif", size: 12 }, color: '#6B6B6B' },
        }),
      };
    }

    if (view === 'datasetVsMarket') {
      var top10 = aiModels.slice(0, 10);
      var labels = top10.map(function(m) {
        return m.model.length > 25 ? m.model.slice(0, 22) + '\u2026' : m.model;
      });
      return {
        type: 'bar',
        data: {
          labels: labels,
          datasets: [
            {
              label: 'Dataset Brier',
              data: top10.map(function(m) { return m.brier_dataset; }),
              backgroundColor: 'rgba(74, 144, 217, 0.7)',
              borderColor: '#4A90D9',
              borderWidth: 1,
            },
            {
              label: 'Market Brier',
              data: top10.map(function(m) { return m.brier_market; }),
              backgroundColor: 'rgba(212, 149, 10, 0.7)',
              borderColor: '#D4950A',
              borderWidth: 1,
            },
          ],
        },
        scalesX: Object.assign({}, defaultScalesX, { ticks: Object.assign({}, defaultScalesX.ticks, { maxRotation: 45 }) }),
        scalesY: Object.assign({}, defaultScalesY, {
          title: { display: true, text: 'Brier Score (lower = better)', font: { family: "'DM Sans', sans-serif", size: 12 }, color: '#6B6B6B' },
        }),
      };
    }

    if (view === 'byOrg') {
      // Aggregate best model per organization
      var orgBest = {};
      aiModels.forEach(function(m) {
        var org = m.organization || 'Unknown';
        if (!orgBest[org] || m.brier_overall < orgBest[org].brier_overall) {
          orgBest[org] = m;
        }
      });
      var orgList = Object.keys(orgBest).map(function(org) { return { org: org, m: orgBest[org] }; });
      orgList.sort(function(a, b) { return a.m.brier_overall - b.m.brier_overall; });
      orgList = orgList.slice(0, 15);

      var labels = orgList.map(function(o) { return o.org; });
      var datasets = [
        {
          label: 'Best Overall Brier',
          data: orgList.map(function(o) { return o.m.brier_overall; }),
          backgroundColor: 'rgba(74, 144, 217, 0.7)',
          borderColor: '#4A90D9',
          borderWidth: 1,
        },
      ];
      if (superforecaster) {
        datasets.push({
          label: 'Superforecaster Baseline',
          data: orgList.map(function() { return superforecaster.brier_overall; }),
          type: 'line',
          borderColor: '#D94A4A',
          borderWidth: 2,
          borderDash: [6, 4],
          pointRadius: 0,
          fill: false,
        });
      }
      return {
        type: 'bar',
        data: { labels: labels, datasets: datasets },
        scalesX: defaultScalesX,
        scalesY: Object.assign({}, defaultScalesY, {
          title: { display: true, text: 'Brier Score (lower = better)', font: { family: "'DM Sans', sans-serif", size: 12 }, color: '#6B6B6B' },
        }),
      };
    }

    return { type: 'bar', data: { labels: [], datasets: [] }, scalesX: defaultScalesX, scalesY: defaultScalesY };
  }

  // ─── Model Table ───────────────────────────────────────────────────────────
  function getSortValue(m) {
    switch (sortColumn) {
      case 'rank': return m.rank || 999;
      case 'model': return (m.model || '').toLowerCase();
      case 'brier_overall': return m.brier_overall != null ? m.brier_overall : 999;
      case 'brier_dataset': return m.brier_dataset != null ? m.brier_dataset : 999;
      case 'brier_market': return m.brier_market != null ? m.brier_market : 999;
      case 'n_total': return m.n_total || 0;
      default: return 0;
    }
  }

  function sortModels(models) {
    var sorted = models.slice();
    sorted.sort(function(a, b) {
      var va = getSortValue(a);
      var vb = getSortValue(b);
      if (typeof va === 'string') {
        var cmp = va.localeCompare(vb);
        return sortDirection === 'asc' ? cmp : -cmp;
      }
      return sortDirection === 'asc' ? va - vb : vb - va;
    });
    return sorted;
  }

  function updateSortArrows() {
    document.querySelectorAll('#model-table .sort-arrow').forEach(function(el) {
      el.className = 'sort-arrow';
    });
    var active = document.getElementById('sort-' + sortColumn);
    if (active) active.className = 'sort-arrow ' + sortDirection;
  }

  // Brier bar color: blue for good, amber for middling, red for poor
  function brierBarColor(val) {
    if (val == null) return 'var(--bw-border)';
    if (val <= 0.10) return 'var(--bw-blue)';
    if (val <= 0.15) return 'var(--bw-amber)';
    return 'var(--bw-red)';
  }

  // Brier bar width: scale 0–0.25 to 0–100%
  function brierBarWidth(val) {
    if (val == null) return 0;
    return Math.min(val / 0.25 * 100, 100);
  }

  function renderTable() {
    var tbody = document.getElementById('model-tbody');
    var countEl = document.getElementById('model-count');
    if (!tbody || !data) return;

    var models = data.leaderboard || [];
    if (countEl) countEl.textContent = models.length + ' models';

    var sorted = sortModels(models);
    tbody.innerHTML = '';
    updateSortArrows();

    var visible = modelShowAll ? sorted : sorted.slice(0, modelDisplayCount);
    var showMoreEl = document.getElementById('model-show-more');
    var showMoreBtn = document.getElementById('model-show-more-btn');
    if (showMoreEl) {
      showMoreEl.style.display = sorted.length > modelDisplayCount && !modelShowAll ? '' : 'none';
    }
    if (showMoreBtn) {
      showMoreBtn.textContent = 'Show all ' + sorted.length + ' models';
    }

    visible.forEach(function(m) {
      var tr = document.createElement('tr');

      var modelName = m.model || '';
      var org = m.organization || '';
      var isSuper = modelName.toLowerCase().indexOf('superforecaster') !== -1;

      if (isSuper) tr.className = 'super-row';

      // Clean up model name for display
      var cleanName = modelName;
      var superLabel = isSuper ? '<span class="super-label">Baseline</span>' : '';

      var brierO = m.brier_overall != null ? m.brier_overall.toFixed(3) : '\u2014';
      var brierD = m.brier_dataset != null ? m.brier_dataset.toFixed(3) : '\u2014';
      var brierM = m.brier_market != null ? m.brier_market.toFixed(3) : '\u2014';
      var nTotal = m.n_total != null ? m.n_total.toLocaleString() : '\u2014';

      // Overall Brier cell with visual bar
      var overallCell = m.brier_overall != null
        ? '<div class="brier-cell">' +
            '<span>' + brierO + '</span>' +
            '<div class="brier-bar"><div class="brier-bar-fill" style="width:' + brierBarWidth(m.brier_overall) + '%;background:' + brierBarColor(m.brier_overall) + '"></div></div>' +
          '</div>'
        : '\u2014';

      tr.innerHTML =
        '<td class="col-rank">' + (m.rank || '\u2014') + '</td>' +
        '<td>' +
          '<div class="model-cell-name">' + esc(cleanName) + superLabel + '</div>' +
          (org ? '<div class="model-cell-org">' + esc(org) + '</div>' : '') +
        '</td>' +
        '<td class="col-num">' + overallCell + '</td>' +
        '<td class="col-num">' + brierD + '</td>' +
        '<td class="col-num">' + brierM + '</td>' +
        '<td class="col-num">' + nTotal + '</td>' +
        '<td class="col-arrow">&rsaquo;</td>';

      tr.addEventListener('click', function() { showModelDetail(m); });
      tbody.appendChild(tr);
    });
  }

  // ─── Model Detail ─────────────────────────────────────────────────────────
  function showModelDetail(model) {
    var dashboardView = document.getElementById('dashboard-view');
    var detailView = document.getElementById('model-detail');
    dashboardView.style.display = 'none';
    detailView.classList.add('active');
    window.scrollTo({ top: 0, behavior: 'smooth' });
    renderModelDetail(model);
  }

  function showDashboard() {
    var dashboardView = document.getElementById('dashboard-view');
    var detailView = document.getElementById('model-detail');
    detailView.classList.remove('active');
    dashboardView.style.display = '';
  }

  function renderModelDetail(model) {
    var contentEl = document.getElementById('model-detail-content');
    if (!contentEl) return;

    var superforecaster = (data.leaderboard || []).find(function(m) {
      return m.model.toLowerCase().indexOf('superforecaster') !== -1;
    });
    var supersBrier = superforecaster ? superforecaster.brier_overall : null;

    var brierO = model.brier_overall != null ? model.brier_overall.toFixed(3) : '\u2014';
    var brierD = model.brier_dataset != null ? model.brier_dataset.toFixed(3) : '\u2014';
    var brierM = model.brier_market != null ? model.brier_market.toFixed(3) : '\u2014';

    var gap = '\u2014';
    if (model.brier_dataset != null && model.brier_market != null) {
      var gapVal = model.brier_dataset - model.brier_market;
      gap = (gapVal >= 0 ? '+' : '') + gapVal.toFixed(3);
    }

    var beatsSupers = model.supers_beat === 'Yes';
    var supersBadge = beatsSupers
      ? '<span class="supers-badge yes">Beats Superforecasters</span>'
      : '<span class="supers-badge no">Below Superforecasters</span>';

    var vsSuperStr = '';
    if (supersBrier != null && model.brier_overall != null) {
      var diff = ((model.brier_overall - supersBrier) / supersBrier * 100).toFixed(0);
      vsSuperStr = diff > 0 ? diff + '% worse' : Math.abs(diff) + '% better';
    }

    contentEl.innerHTML =
      '<div class="model-detail-header">' +
        '<div>' +
          '<div class="model-detail-name">' + esc(model.model) + '</div>' +
          (model.organization ? '<div class="model-detail-org">' + esc(model.organization) + '</div>' : '') +
        '</div>' +
        '<div class="model-detail-stats">' +
          '<span class="model-detail-stat">Rank <strong>#' + (model.rank || '\u2014') + '</strong></span>' +
          '<span class="model-detail-stat">' + (model.n_total || '\u2014') + ' questions</span>' +
          supersBadge +
        '</div>' +
      '</div>' +
      '<div class="detail-scores-grid">' +
        '<div class="detail-score-card">' +
          '<div class="detail-score-label">Overall Brier</div>' +
          '<div class="detail-score-value">' + brierO + '</div>' +
          (vsSuperStr ? '<div class="detail-score-sub">' + vsSuperStr + ' than superforecasters</div>' : '') +
        '</div>' +
        '<div class="detail-score-card">' +
          '<div class="detail-score-label">Dataset Brier</div>' +
          '<div class="detail-score-value">' + brierD + '</div>' +
          '<div class="detail-score-sub">' + (model.n_dataset || '\u2014') + ' structured questions</div>' +
        '</div>' +
        '<div class="detail-score-card">' +
          '<div class="detail-score-label">Market Brier</div>' +
          '<div class="detail-score-value">' + brierM + '</div>' +
          '<div class="detail-score-sub">' + (model.n_market || '\u2014') + ' real-world questions</div>' +
        '</div>' +
        '<div class="detail-score-card">' +
          '<div class="detail-score-label">Gap (D \u2212 M)</div>' +
          '<div class="detail-score-value">' + gap + '</div>' +
          '<div class="detail-score-sub">Positive = worse on structured data</div>' +
        '</div>' +
      '</div>';
  }

  // ─── Handlers ─────────────────────────────────────────────────────────────
  function setupHandlers() {
    var backBtn = document.getElementById('back-btn');
    if (backBtn) backBtn.addEventListener('click', showDashboard);

    var showMoreBtn = document.getElementById('model-show-more-btn');
    if (showMoreBtn) {
      showMoreBtn.addEventListener('click', function() {
        modelShowAll = true;
        renderTable();
      });
    }

    document.querySelectorAll('#model-table th.sortable').forEach(function(th) {
      th.addEventListener('click', function() {
        var col = th.dataset.sort;
        if (sortColumn === col) {
          sortDirection = sortDirection === 'desc' ? 'asc' : 'desc';
        } else {
          sortColumn = col;
          sortDirection = col === 'model' ? 'asc' : (col === 'rank' ? 'asc' : 'asc');
        }
        renderTable();
      });
    });

    var methToggle = document.getElementById('methodology-toggle');
    var methSection = document.getElementById('methodology');
    if (methToggle && methSection) {
      methToggle.addEventListener('click', function() {
        methSection.classList.toggle('open');
      });
    }
  }

  // ─── Helpers ──────────────────────────────────────────────────────────────
  function esc(str) {
    if (!str) return '';
    var div = document.createElement('div');
    div.appendChild(document.createTextNode(str));
    return div.innerHTML;
  }

  // ─── Boot ─────────────────────────────────────────────────────────────────
  document.addEventListener('DOMContentLoaded', init);
})();
