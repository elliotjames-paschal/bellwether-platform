(function () {
  'use strict';

  // ─── State ──────────────────────────────────────────────────────────────────
  let summaryData = null;
  let outletsData = null;
  let citationsData = null;
  let detailDisplayCount = 20;
  let topicDisplayCount = 10;
  let activeTopic = null;

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
    renderHeroStats();
    renderOutletTooltip();
    renderTimeline();
    renderTopics();
    renderOutletTable();
  }

  function renderMetaDate() {
    const el = document.getElementById('meta-date');
    if (el && summaryData.generated_at) {
      const d = new Date(summaryData.generated_at);
      el.textContent = 'Updated ' + d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
    }
  }

  function renderHeroStats() {
    const hero = summaryData.hero;

    // Citations card — dual 24h | 30d
    animateValue('stat-citations-24h', 0, hero.total_citations_24h || 0, 800);
    animateValue('stat-citations-30d', 0, hero.total_citations_30d || 0, 800);

    // Outlets card
    animateValue('stat-outlets', 0, hero.total_outlets || 0, 800);
  }

  function renderOutletTooltip() {
    const list = document.getElementById('tt-outlet-list');
    if (!list || !outletsData) return;

    const outlets = outletsData.outlets || [];
    list.innerHTML = '';
    outlets.forEach(o => {
      const li = document.createElement('li');
      li.textContent = (o.domain_name || o.domain) + ' (' + o.total_citations + ')';
      list.appendChild(li);
    });
  }

  function animateValue(id, start, end, duration) {
    const el = document.getElementById(id);
    if (!el) return;
    if (end === 0) { el.textContent = '0'; return; }

    const range = end - start;
    const startTime = performance.now();

    function tick(now) {
      const elapsed = now - startTime;
      const progress = Math.min(elapsed / duration, 1);
      const eased = 1 - Math.pow(1 - progress, 3);
      el.textContent = Math.round(start + range * eased).toLocaleString();
      if (progress < 1) requestAnimationFrame(tick);
    }
    requestAnimationFrame(tick);
  }

  // ─── Timeline ───────────────────────────────────────────────────────────────
  function renderTimeline() {
    const container = document.getElementById('timeline-chart');
    const rangeEl = document.getElementById('timeline-range');
    if (!container || !summaryData) return;

    const timeline = summaryData.timeline || [];
    if (!timeline.length) {
      container.innerHTML = '<div class="empty-state" style="padding:16px;font-size:13px"><p>No timeline data yet.</p></div>';
      return;
    }

    // Find max count for scaling
    const maxCount = Math.max(...timeline.map(w => w.count), 1);

    // Show date range
    if (rangeEl && timeline.length > 0) {
      const first = new Date(timeline[0].week);
      const last = new Date(timeline[timeline.length - 1].week);
      rangeEl.textContent = first.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
        + ' \u2013 '
        + last.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
    }

    container.innerHTML = '';

    timeline.forEach(w => {
      const wrap = document.createElement('div');
      wrap.className = 'timeline-bar-wrap';

      const bar = document.createElement('div');
      bar.className = 'timeline-bar';
      const heightPct = Math.max(4, (w.count / maxCount) * 100);
      bar.style.height = heightPct + '%';

      // Color based on tier mix
      const tiers = w.tiers || {};
      const total = (tiers.reportable || 0) + (tiers.caution || 0) + (tiers.fragile || 0);
      if (total > 0) {
        const reportablePct = (tiers.reportable || 0) / total;
        if (reportablePct >= 0.6) bar.style.background = 'var(--bw-green)';
        else if (reportablePct >= 0.3) bar.style.background = 'var(--bw-amber)';
        else bar.style.background = 'var(--bw-red)';
        bar.style.opacity = '0.8';
      }

      // Tooltip
      const tooltip = document.createElement('div');
      tooltip.className = 'bar-tooltip';
      const weekDate = new Date(w.week);
      let tipText = weekDate.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
        + ': ' + w.count + ' citation' + (w.count !== 1 ? 's' : '');
      if (total > 0) {
        tipText += '\n' + (tiers.reportable || 0) + ' reportable, '
          + (tiers.caution || 0) + ' caution, '
          + (tiers.fragile || 0) + ' fragile';
      }
      tooltip.textContent = tipText;
      bar.appendChild(tooltip);

      // Week label
      const label = document.createElement('div');
      label.className = 'timeline-bar-label';
      label.textContent = weekDate.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });

      wrap.appendChild(bar);
      wrap.appendChild(label);
      container.appendChild(wrap);
    });
  }

  // ─── Topics ─────────────────────────────────────────────────────────────────
  function renderTopics() {
    const container = document.getElementById('topic-row');
    if (!container) return;

    const topics = summaryData.topics || [];
    if (!topics.length) {
      container.innerHTML = '<div class="empty-state" style="padding:24px"><p>No topic data available.</p></div>';
      return;
    }

    container.innerHTML = '';

    topics.forEach(t => {
      const card = document.createElement('div');
      card.className = 'topic-card';
      card.dataset.topic = t.name;

      let platformsHtml = '';
      (t.platforms || []).forEach(p => {
        if (p === 'polymarket') platformsHtml += '<span class="plat-pill pm">PM</span>';
        else if (p === 'kalshi') platformsHtml += '<span class="plat-pill k">K</span>';
      });

      card.innerHTML = `
        <div class="topic-name">${esc(t.name)}</div>
        <div class="topic-count">${t.count} <span>citations</span></div>
        <div class="topic-platforms">
          ${platformsHtml}
          <span style="font-size:11px;color:var(--bw-text-secondary)">${t.outlet_count} outlet${t.outlet_count !== 1 ? 's' : ''}</span>
        </div>
      `;

      card.addEventListener('click', () => toggleTopic(t.name));
      container.appendChild(card);
    });
  }

  function toggleTopic(topicName) {
    if (activeTopic === topicName) {
      closeTopic();
      return;
    }
    activeTopic = topicName;
    topicDisplayCount = 10;

    // Update active card styling
    document.querySelectorAll('.topic-card').forEach(c => {
      c.classList.toggle('active', c.dataset.topic === topicName);
    });

    renderTopicDetail(topicName);
  }

  function closeTopic() {
    activeTopic = null;
    document.querySelectorAll('.topic-card').forEach(c => c.classList.remove('active'));
    const detail = document.getElementById('topic-detail');
    if (detail) detail.style.display = 'none';
  }

  function renderTopicDetail(topicName) {
    const detail = document.getElementById('topic-detail');
    const titleEl = document.getElementById('topic-detail-title');
    const countEl = document.getElementById('topic-detail-count');
    const container = document.getElementById('topic-citations');
    const loadMore = document.getElementById('topic-load-more');

    if (!detail || !container || !citationsData) return;

    // Filter citations for this topic
    const all = (citationsData.citations || []).filter(c => c.topic === topicName);
    const toShow = all.slice(0, topicDisplayCount);

    if (titleEl) titleEl.textContent = topicName;
    if (countEl) countEl.textContent = all.length + ' citation' + (all.length !== 1 ? 's' : '');

    container.innerHTML = '';
    toShow.forEach(c => container.appendChild(buildCitationCard(c)));

    detail.style.display = 'block';

    if (loadMore) {
      loadMore.style.display = all.length > topicDisplayCount ? '' : 'none';
      loadMore.textContent = 'Load More (' + Math.min(topicDisplayCount, all.length) + ' of ' + all.length + ')';
      loadMore.dataset.topic = topicName;
    }

    // Smooth scroll to detail
    detail.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
  }

  // ─── Outlet Table ───────────────────────────────────────────────────────────

  function getSortValue(outlet) {
    switch (sortColumn) {
      case 'name': return (outlet.domain_name || outlet.domain || '').toLowerCase();
      case 'citations': return outlet.citations_30d != null ? outlet.citations_30d : (outlet.total_citations || 0);
      case 'reportable': return outlet.pct_reportable != null ? outlet.pct_reportable : -1;
      case 'fragility': return outlet.avg_fragility != null ? outlet.avg_fragility : -1;
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

    sorted.forEach(o => {
      const tr = document.createElement('tr');

      // Platforms
      const plats = [];
      if (o.platforms.polymarket > 0) plats.push('<span class="plat-tag" style="color:#2563eb">PM ' + o.platforms.polymarket + '</span>');
      if (o.platforms.kalshi > 0) plats.push('<span class="plat-tag" style="color:#059669">K ' + o.platforms.kalshi + '</span>');

      // Display name
      const displayName = o.domain_name || o.domain;

      // Stats (30d window)
      const pctR = o.pct_reportable != null ? o.pct_reportable + '%' : '\u2014';
      const avgF = o.avg_fragility != null ? '$' + o.avg_fragility.toLocaleString() : '\u2014';
      const avgB = o.avg_brier != null ? o.avg_brier.toFixed(3) : '\u2014';
      const c24 = o.citations_24h != null ? o.citations_24h : (o.total_citations || 0);
      const c30 = o.citations_30d != null ? o.citations_30d : (o.total_citations || 0);

      // Tier bar
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
        <td class="col-num"><span>${c24}</span> <span style="color:var(--bw-text-secondary)">|</span> <span>${c30}</span></td>
        <td class="col-num">${pctR}</td>
        <td class="col-num">${avgF}</td>
        <td class="col-num">${avgB}</td>
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
    // Back button from outlet detail
    const backBtn = document.getElementById('back-btn');
    if (backBtn) backBtn.addEventListener('click', showDashboard);

    // Load more in outlet detail
    const loadMore = document.getElementById('detail-load-more');
    if (loadMore) {
      loadMore.addEventListener('click', () => {
        detailDisplayCount += 20;
        const domain = loadMore.dataset.domain;
        if (domain) renderDetailCitations(domain);
      });
    }

    // Close topic detail
    const topicClose = document.getElementById('topic-close-btn');
    if (topicClose) topicClose.addEventListener('click', closeTopic);

    // Load more in topic detail
    const topicLoadMore = document.getElementById('topic-load-more');
    if (topicLoadMore) {
      topicLoadMore.addEventListener('click', () => {
        topicDisplayCount += 10;
        const topic = topicLoadMore.dataset.topic;
        if (topic) renderTopicDetail(topic);
      });
    }

    // Sortable table headers
    document.querySelectorAll('.outlet-table th.sortable').forEach(th => {
      th.addEventListener('click', () => {
        const col = th.dataset.sort;
        if (sortColumn === col) {
          sortDirection = sortDirection === 'desc' ? 'asc' : 'desc';
        } else {
          sortColumn = col;
          // Default direction: desc for numbers, asc for name
          sortDirection = col === 'name' ? 'asc' : 'desc';
        }
        renderOutletTable();
      });
    });

    // Methodology toggle
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
    const avgF = outlet.avg_fragility != null ? '$' + outlet.avg_fragility.toLocaleString() : '\u2014';
    const avgB = outlet.avg_brier != null ? outlet.avg_brier.toFixed(3) : '\u2014';
    const c24 = outlet.citations_24h != null ? outlet.citations_24h : outlet.total_citations;
    const c30 = outlet.citations_30d != null ? outlet.citations_30d : outlet.total_citations;

    // Tier breakdown for detail view
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

    let platHtml = '';
    if (c.platform === 'polymarket') platHtml = '<span class="cc-platform pm">Polymarket</span>';
    else if (c.platform === 'kalshi') platHtml = '<span class="cc-platform k">Kalshi</span>';

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

      // Accuracy gap
      let gapHtml = '';
      if (c.probability_cited != null && c.price_at_citation != null) {
        const gap = Math.abs(c.probability_cited - c.price_at_citation) * 100;
        const gapColor = gap <= 3 ? 'var(--bw-green)' : gap <= 10 ? 'var(--bw-amber)' : 'var(--bw-red)';
        gapHtml = '<span style="color:' + gapColor + ';font-weight:500">\u0394' + gap.toFixed(0) + 'pp</span>';
      }

      matchHtml = `
        <div class="cc-match">
          <div class="cc-match-info">
            <strong>${esc(c.market_question)}</strong>
            ${probText || priceText ? '<div class="cc-prob">' + [probText, priceText, gapHtml].filter(Boolean).join(' \u00b7 ') + '</div>' : ''}
          </div>
          ${tierLabel ? '<span class="frag-badge ' + tierClass + '">' + tierLabel + (fragText ? ' \u00b7 ' + fragText : '') + '</span>' : ''}
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

  // ─── Boot ───────────────────────────────────────────────────────────────────
  document.addEventListener('DOMContentLoaded', init);
})();
