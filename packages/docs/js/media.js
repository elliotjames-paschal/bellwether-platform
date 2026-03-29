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
    renderHeroStats();
    renderOutletTooltip();
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
    animateValue('stat-citations-24h', 0, hero.total_citations_24h || 0, 800);
    animateValue('stat-citations-30d', 0, hero.total_citations_30d || 0, 800);
    animateValue('stat-cite-prob', 0, hero.citations_with_probability || 0, 800);
    animateValue('stat-matched', 0, hero.citations_matched || 0, 800);
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

    detail.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
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

    sorted.forEach(o => {
      const tr = document.createElement('tr');

      const plats = [];
      if (o.platforms.polymarket > 0) plats.push('<span class="plat-tag" style="color:#2563eb">PM ' + o.platforms.polymarket + '</span>');
      if (o.platforms.kalshi > 0) plats.push('<span class="plat-tag" style="color:#059669">K ' + o.platforms.kalshi + '</span>');

      const displayName = o.domain_name || o.domain;
      const pctR = o.pct_reportable != null ? o.pct_reportable + '%' : '\u2014';
      const avgF = o.avg_cost_to_move_5c != null ? formatVolume(o.avg_cost_to_move_5c) : '\u2014';
      const avgB = o.avg_brier != null ? o.avg_brier.toFixed(3) : '\u2014';
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

    const topicClose = document.getElementById('topic-close-btn');
    if (topicClose) topicClose.addEventListener('click', closeTopic);

    const topicLoadMore = document.getElementById('topic-load-more');
    if (topicLoadMore) {
      topicLoadMore.addEventListener('click', () => {
        topicDisplayCount += 10;
        const topic = topicLoadMore.dataset.topic;
        if (topic) renderTopicDetail(topic);
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
    // Build URL for whichever platform we have identifiers for
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
