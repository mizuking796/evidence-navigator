import { GUIDELINES } from './guidelines-data.js';
import { CQ_DATA } from './cq-data.js';

const PUBMED_BASE = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils';
const JSTAGE_BASE = 'https://api.jstage.jst.go.jp/searchapi/do';
const S2_BASE = 'https://api.semanticscholar.org/graph/v1/paper/search';
const OPENALEX_BASE = 'https://api.openalex.org/works';
const CINII_BASE = 'https://cir.nii.ac.jp/opensearch/all';
const EPMC_BASE = 'https://www.ebi.ac.uk/europepmc/webservices/rest/search';
const MESH_LOOKUP = 'https://id.nlm.nih.gov/mesh/lookup/descriptor';
const GTRANSLATE = 'https://translate.googleapis.com/translate_a/single';

const GEMINI_BASE = 'https://generativelanguage.googleapis.com/v1beta/models';

const ALLOWED_ORIGINS = [
  'https://mizuking796.github.io',
  'http://localhost',
  'http://127.0.0.1',
];

function corsHeaders(request) {
  const origin = request?.headers?.get('Origin') || '';
  // file:// sends "null" origin; allow it for local dev alongside listed origins
  const isAllowed = origin === 'null' || ALLOWED_ORIGINS.some(o => origin.startsWith(o));
  return {
    'Access-Control-Allow-Origin': isAllowed ? (origin === 'null' ? '*' : origin) : ALLOWED_ORIGINS[0],
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
  };
}

// Simple rate limiting: max 60 requests per minute per IP
const rateLimitMap = new Map();
const RATE_LIMIT = 60;
const RATE_WINDOW = 60000;

let lastCleanup = 0;

function checkRateLimit(ip) {
  const now = Date.now();
  // Cleanup stale entries every RATE_WINDOW
  if (now - lastCleanup > RATE_WINDOW) {
    for (const [key, entry] of rateLimitMap) {
      if (now - entry.start > RATE_WINDOW) rateLimitMap.delete(key);
    }
    lastCleanup = now;
  }
  const entry = rateLimitMap.get(ip);
  if (!entry || now - entry.start > RATE_WINDOW) {
    rateLimitMap.set(ip, { start: now, count: 1 });
    return true;
  }
  entry.count++;
  if (entry.count > RATE_LIMIT) return false;
  return true;
}

export default {
  async fetch(request) {
    const url = new URL(request.url);
    const cors = corsHeaders(request);

    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: cors });
    }

    // Rate limiting
    const ip = request.headers.get('CF-Connecting-IP') || request.headers.get('X-Forwarded-For') || 'unknown';
    if (!checkRateLimit(ip)) {
      return new Response(JSON.stringify({ error: 'Too many requests' }), {
        status: 429,
        headers: { 'Content-Type': 'application/json', 'Retry-After': '60', ...cors },
      });
    }

    try {
      switch (url.pathname) {
        case '/api/search':
          return await handleSearch(url, cors);
        case '/api/mesh':
          return await handleMeshSuggest(url, cors);
        case '/api/suggest':
          return handleSuggest(url, cors);
        case '/api/cq/list':
          return handleCQList(url, cors);
        case '/api/cq/evidence':
          return await handleCQEvidence(url, cors);
        case '/api/translate':
          return await handleTranslate(url, cors);
        case '/api/ai/parse':
          if (request.method !== 'POST') return json({ error: 'POST required' }, 405, cors);
          try { return await handleAIParse(await request.json(), cors); }
          catch (e) { return json({ error: 'Invalid JSON body' }, 400, cors); }
        case '/api/ai/summary':
          if (request.method !== 'POST') return json({ error: 'POST required' }, 405, cors);
          try { return await handleAISummary(await request.json(), cors); }
          catch (e) { return json({ error: 'Invalid JSON body' }, 400, cors); }
        default:
          return json({ error: 'Not found' }, 404, cors);
      }
    } catch (e) {
      console.error('Worker error:', e);
      return json({ error: 'Internal server error' }, 500, cors);
    }
  },
};

function json(data, status = 200, cors = {}) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      'Content-Type': 'application/json',
      'X-Content-Type-Options': 'nosniff',
      'X-Frame-Options': 'DENY',
      'Referrer-Policy': 'strict-origin-when-cross-origin',
      ...cors,
    },
  });
}

// ── MeSH Suggest ──────────────────────────────────────────────

async function handleMeshSuggest(url, cors) {
  const q = url.searchParams.get('q') || '';
  if (q.length < 2) return json([], 200, cors);

  const res = await fetch(
    `${MESH_LOOKUP}?label=${encodeURIComponent(q)}&match=contains&limit=10`,
    { headers: { Accept: 'application/json' }, signal: AbortSignal.timeout(5000) }
  );
  if (!res.ok) return json([], 200, cors);
  const data = await res.json();

  const suggestions = Array.isArray(data)
    ? data.map(item => (typeof item === 'object' && item.label) ? item.label : String(item))
    : [];

  return json(suggestions, 200, cors);
}

// ── Suggest (CQ keywords + GL diseases) ─────────────────────

function handleSuggest(url, cors) {
  const q = (url.searchParams.get('q') || '').toLowerCase();
  if (q.length < 1) return json([], 200, cors);

  const seen = new Set();
  const results = [];

  // Search CQ keywords
  for (const cq of CQ_DATA) {
    for (const kw of cq.kw) {
      const kwL = kw.toLowerCase();
      if (kwL.includes(q) && !seen.has(kwL)) {
        seen.add(kwL);
        results.push(kw);
      }
    }
  }

  // Search GL disease names
  for (const gl of GUIDELINES) {
    for (const d of gl.diseases) {
      const dL = d.toLowerCase();
      if (dL.includes(q) && !seen.has(dL)) {
        seen.add(dL);
        results.push(d);
      }
    }
  }

  // Sort: exact prefix match first, then by length
  results.sort((a, b) => {
    const aStart = a.toLowerCase().startsWith(q) ? 0 : 1;
    const bStart = b.toLowerCase().startsWith(q) ? 0 : 1;
    if (aStart !== bStart) return aStart - bStart;
    return a.length - b.length;
  });

  return json(results.slice(0, 15), 200, cors);
}

// ── CQ List (browse all) ────────────────────────────────────

function handleCQList(url, cors) {
  const cat = url.searchParams.get('cat') || '';

  const glMap = new Map();
  for (const gl of GUIDELINES) glMap.set(gl.id, gl);

  // Group CQs by guideline
  const groups = {};
  for (const cq of CQ_DATA) {
    if (!groups[cq.gid]) {
      const gl = glMap.get(cq.gid);
      groups[cq.gid] = {
        gid: cq.gid,
        title: gl ? gl.title : cq.gid,
        titleEn: gl ? (gl.titleEn || '') : '',
        org: gl ? gl.org : '',
        url: gl ? gl.url : '',
        cat: gl ? gl.cat : '',
        country: gl ? (gl.country || 'JP') : 'JP',
        cqs: [],
      };
    }
    groups[cq.gid].cqs.push({
      cq: cq.cq,
      question: cq.q,
      type: cq.type,
      recommendation: cq.rec,
      evidenceLevel: cq.ev,
      page: cq.page || null,
      kw: cq.kw || [],
    });
  }

  let result = Object.values(groups);

  // Filter by category if specified
  if (cat) {
    result = result.filter(g => g.cat === cat);
  }

  return json({
    totalGuidelines: result.length,
    totalCQs: result.reduce((s, g) => s + g.cqs.length, 0),
    groups: result,
  }, 200, cors);
}

// ── CQ Evidence (on-demand PubMed SR/RCT search) ─────────────

async function handleCQEvidence(url, cors) {
  const q = url.searchParams.get('q') || '';
  const kw = url.searchParams.get('kw') || ''; // pre-attached English keywords from CQ data
  if (!q) return json({ error: 'q (CQ question text) required' }, 400, cors);

  // Extract meaningful keywords from CQ text
  let keywords = extractCQKeywords(q);

  // If English keywords are provided (from CQ kw field), prefer those for PubMed
  if (kw) {
    const kwTerms = kw.split(',').map(k => k.trim()).filter(Boolean);
    if (kwTerms.length) keywords = kwTerms.slice(0, 4);
  } else {
    // For Japanese keywords, try synonym expansion to get English equivalents
    const isJa = keywords.some(k => /[\u3000-\u9FFF]/.test(k));
    if (isJa) {
      const engTerms = [];
      for (const k of keywords) {
        const syns = SYN_MAP.get(k.toLowerCase());
        if (syns) {
          for (const s of syns) {
            if (/^[A-Za-z]/.test(s) && s.length > 1 && !engTerms.includes(s)) engTerms.push(s);
          }
        }
      }
      // Add English keywords for medical terms: 治療→treatment, 運動療法→exercise therapy
      const jaToEn = {
        '運動療法': 'exercise therapy', '理学療法': 'physical therapy', '作業療法': 'occupational therapy',
        '言語療法': 'speech therapy', '薬物療法': 'pharmacotherapy', '手術': 'surgery',
        '放射線': 'radiation', '化学療法': 'chemotherapy', '治療': 'treatment',
        '装具': 'orthosis', '義肢': 'prosthesis', '電気刺激': 'electrical stimulation',
      };
      for (const k of keywords) {
        if (jaToEn[k] && !engTerms.includes(jaToEn[k])) engTerms.push(jaToEn[k]);
      }
      if (engTerms.length >= 2) keywords = engTerms.slice(0, 4);
    }
  }

  if (!keywords.length) return json({ results: [], keywords: [] }, 200, cors);

  // Build PubMed query: keywords AND (SR OR MA OR RCT filter)
  const diseaseQuery = keywords.join(' AND ');
  const typeFilter = '(systematic review[pt] OR meta-analysis[pt] OR randomized controlled trial[pt])';
  const fullQuery = `(${diseaseQuery}) AND ${typeFilter}`;

  const searchUrl =
    `${PUBMED_BASE}/esearch.fcgi?db=pubmed` +
    `&term=${encodeURIComponent(fullQuery)}` +
    `&retmax=5&retmode=json&sort=relevance` +
    `&tool=evidence-navigator&email=evidence-navigator@example.com`;

  try {
    const searchRes = await fetch(searchUrl, { signal: AbortSignal.timeout(8000) });
    const searchData = await searchRes.json();
    const ids = searchData?.esearchresult?.idlist || [];
    if (!ids.length) return json({ results: [], keywords, query: fullQuery });

    const sumUrl =
      `${PUBMED_BASE}/esummary.fcgi?db=pubmed` +
      `&id=${ids.join(',')}` +
      `&retmode=json` +
      `&tool=evidence-navigator&email=evidence-navigator@example.com`;

    const sumRes = await fetch(sumUrl, { signal: AbortSignal.timeout(8000) });
    const sumData = await sumRes.json();

    const articles = [];
    for (const id of ids) {
      const a = sumData?.result?.[id];
      if (!a || !a.title) continue;
      const pubTypes = a.pubtype || [];
      articles.push({
        pmid: id,
        title: strip(a.title),
        authors: (a.authors || []).map(x => x.name).slice(0, 3),
        journal: a.source || '',
        year: yearOf(a.pubdate || ''),
        type: classifyPubType(pubTypes),
        url: `https://pubmed.ncbi.nlm.nih.gov/${id}/`,
      });
    }

    return json({ results: articles, keywords, query: fullQuery }, 200, cors);
  } catch (e) {
    return json({ results: [], keywords }, 200, cors);
  }
}

function extractCQKeywords(q) {
  // Remove CQ numbering
  let text = q.replace(/^(CQ\s*\d+[\s\-:：]*|Q\d+[\s\-:：]*)/i, '');

  // For Japanese text: extract medical/technical terms using regex patterns
  const isJa = /[\u3000-\u9FFF]/.test(text);
  if (isJa) {
    // Extract katakana words (medical terms like リハビリテーション, パーキンソン)
    const katakana = text.match(/[\u30A0-\u30FF\u31F0-\u31FF]{2,}/g) || [];
    // Extract kanji compounds (medical terms like 脳卒中, 運動療法)
    const kanji = text.match(/[\u4E00-\u9FFF]{2,}/g) || [];
    // Extract English/acronym terms embedded in Japanese text
    const english = text.match(/[A-Za-z][A-Za-z0-9\-]{1,}/g) || [];

    // Common non-medical kanji to filter out
    const jaStop = new Set(['患者', '対象', '場合', '方法', '結果', '効果', '可能', '必要', '有効', '推奨', '診療', '使用', '実施', '介入', '評価', '改善', '予防', '有用', '適応', '検討', '報告', '研究', '比較', '期間', '目的', '対応', '施行', '観点', '状態', '状況', '影響', '関連', '安全性', '有効性']);
    // Strip common suffixes: 患者, 症例, 療法 (keep root term)
    const cleanKanji = kanji.map(w => w.replace(/患者$|症例$/, '')).filter(w => w.length >= 2);
    const filtered = [...cleanKanji.filter(w => !jaStop.has(w)), ...katakana, ...english];

    // Deduplicate and take top 3
    const unique = [...new Set(filtered)];
    return unique.slice(0, 3);
  }

  // For English text: split by spaces, remove stopwords
  const enStop = new Set(['the', 'a', 'an', 'is', 'are', 'was', 'were', 'be', 'been', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'could', 'should', 'may', 'might', 'can', 'shall', 'of', 'in', 'to', 'for', 'with', 'on', 'at', 'by', 'from', 'as', 'or', 'and', 'but', 'if', 'not', 'no', 'what', 'which', 'who', 'how', 'when', 'where', 'that', 'this', 'it', 'its', 'they', 'them', 'their', 'we', 'our', 'you', 'your', 'after', 'before', 'during', 'between', 'through']);
  const words = text.replace(/[()[\]{}.,;:!?'"]/g, ' ').split(/\s+/).filter(w => {
    if (w.length < 2) return false;
    return !enStop.has(w.toLowerCase());
  });

  return words.slice(0, 4);
}

// ── Synonym Dictionary ───────────────────────────────────────
// Each group = set of equivalent terms. Input matching any → expand to all.
const SYNONYMS = [
  // リハビリ系
  ['リハ', 'リハビリ', 'リハビリテーション', 'rehabilitation'],
  ['PT', '理学療法', 'physical therapy', 'physiotherapy'],
  ['OT', '作業療法', 'occupational therapy'],
  ['ST', '言語聴覚療法', '言語療法', 'speech therapy'],
  // 疾患略語
  ['OA', '変形性関節症', 'osteoarthritis'],
  ['膝OA', '変形性膝関節症', 'knee osteoarthritis'],
  ['股OA', '変形性股関節症', 'hip osteoarthritis'],
  ['RA', '関節リウマチ', 'rheumatoid arthritis'],
  ['脳卒中', 'stroke', '脳梗塞', '脳出血', 'CVA'],
  ['心筋梗塞', 'MI', 'AMI', '急性心筋梗塞', 'myocardial infarction'],
  ['心不全', 'HF', 'heart failure', 'HFrEF', 'HFpEF'],
  ['COPD', '慢性閉塞性肺疾患'],
  ['DM', '糖尿病', 'diabetes'],
  ['ALS', '筋萎縮性側索硬化症'],
  ['PD', 'パーキンソン病', "Parkinson's disease"],
  ['ACL', '前十字靱帯', '前十字靭帯', 'anterior cruciate ligament'],
  ['TKA', '人工膝関節', '人工膝関節置換術'],
  ['THA', '人工股関節', '人工股関節置換術'],
  ['ICU', '集中治療', 'intensive care'],
  ['BPPV', '良性発作性頭位めまい症'],
  ['MCI', '軽度認知障害', 'mild cognitive impairment'],
  // 治療法
  ['運動療法', 'exercise therapy', '運動'],
  ['物理療法', 'physical modalities', '物療'],
  ['徒手療法', 'manual therapy', '徒手'],
  ['電気刺激', 'NMES', '神経筋電気刺激', 'electrical stimulation'],
  ['TENS', '経皮的電気神経刺激'],
  ['嚥下', '嚥下障害', 'dysphagia', '嚥下訓練'],
  ['ADL', '日常生活動作', 'activities of daily living'],
  ['QOL', '生活の質', 'quality of life'],
  ['ROM', '関節可動域', 'range of motion'],
  // 略語→正式名
  ['NSAIDs', '非ステロイド性抗炎症薬'],
  ['CRPS', '複合性局所疼痛症候群'],
  ['DVT', '深部静脈血栓症'],
  ['VTE', '静脈血栓塞栓症'],
  ['HAL', 'ロボットスーツ'],
  ['FIM', '機能的自立度評価'],
  ['BI', 'Barthel Index', 'バーセルインデックス'],
];

// Build lookup: term(lowercase) → Set of synonyms
const SYN_MAP = new Map();
for (const group of SYNONYMS) {
  const lowerGroup = group.map(t => t.toLowerCase());
  for (const term of lowerGroup) {
    if (!SYN_MAP.has(term)) SYN_MAP.set(term, new Set());
    for (const syn of group) SYN_MAP.get(term).add(syn);
  }
}

function expandSynonyms(terms) {
  const expanded = new Set(terms);
  for (const term of terms) {
    const syns = SYN_MAP.get(term.toLowerCase());
    if (syns) {
      for (const s of syns) expanded.add(s);
    }
  }
  return [...expanded];
}

// ── Search ────────────────────────────────────────────────────

async function handleSearch(url, cors) {
  // Support single 'q' param (split by spaces) OR separate fields
  const qParam = url.searchParams.get('q') || '';
  const disease = url.searchParams.get('disease') || '';
  const treatment = url.searchParams.get('treatment') || '';
  const topic = url.searchParams.get('topic') || '';
  const multilingual = url.searchParams.get('multilingual') === 'true';
  const patientVoice = url.searchParams.get('patientVoice') === 'true';

  // Build query parts from either q or individual fields
  const queryParts = [];
  if (qParam) {
    queryParts.push(...qParam.split(/\s+/).filter(Boolean));
  } else {
    if (disease) queryParts.push(disease);
    if (treatment) queryParts.push(treatment);
    if (topic) queryParts.push(topic);
  }

  if (queryParts.length === 0) {
    return json({ error: 'q, disease, treatment, or topic required' }, 400, cors);
  }

  // Synonym expansion (always, before multilingual)
  const expandedParts = expandSynonyms(queryParts);
  // Keep original queryParts for external DB, use expandedParts for CQ/GL search

  // Detect language
  const isJaQuery = isJapanese(queryParts.join(' '));

  // Multilingual OR auto-translate: get English equivalents for Japanese queries
  let translatedParts = null;
  let translatedDisease = '';
  let translatedTreatment = '';
  let translatedTopic = '';
  const needsTranslation = multilingual || isJaQuery;
  if (needsTranslation) {
    const srcLang = isJaQuery ? 'ja' : 'en';
    const tgtLang = isJaQuery ? 'en' : 'ja';

    const translations = await Promise.allSettled(
      queryParts.map(q => translate(q, srcLang, tgtLang))
    );
    translatedParts = translations.map((t) =>
      t.status === 'fulfilled' && t.value ? t.value : null
    );
    // Track translated terms for display (only show if user opted in)
    if (multilingual) {
      let idx = 0;
      if (disease)   { translatedDisease   = translatedParts[idx] || ''; idx++; }
      if (treatment) { translatedTreatment = translatedParts[idx] || ''; idx++; }
      if (topic)     { translatedTopic     = translatedParts[idx] || ''; }
    }
    // Filter out failed translations
    translatedParts = translatedParts.filter(Boolean);
  }

  // Build parallel search tasks
  // Strategy: each DB gets the language it handles best
  const searches = [];
  const searchLabels = [];
  const jaText = queryParts.join(' ');
  const enParts = isJaQuery && translatedParts?.length ? translatedParts : queryParts;
  const enText = enParts.join(' ');

  if (isJaQuery && !multilingual) {
    // Japanese query without multilingual:
    // - PubMed/S2: auto-translated English (these DBs can't search Japanese)
    // - J-STAGE/CiNii: original Japanese (native Japanese DBs)
    // - OpenAlex/EPMC: both (partial Japanese support)
    if (translatedParts?.length) {
      searches.push(searchPubMed(translatedParts)); searchLabels.push('pubmed');
      searches.push(searchS2(enText));              searchLabels.push('s2');
    }
    searches.push(searchJStage(jaText));    searchLabels.push('jstage');
    searches.push(searchOpenAlex(jaText));  searchLabels.push('openalex');
    searches.push(searchCiNii(jaText));     searchLabels.push('cinii');
    searches.push(searchEPMC(jaText));      searchLabels.push('epmc');
    // Also search OpenAlex/EPMC with English for broader coverage
    if (translatedParts?.length) {
      searches.push(searchOpenAlex(enText)); searchLabels.push('openalex');
      searches.push(searchEPMC(enText));     searchLabels.push('epmc');
    }
  } else if (multilingual && translatedParts?.length) {
    // Multilingual: both languages to all DBs
    for (const q of [{ parts: queryParts, text: jaText }, { parts: translatedParts, text: enText }]) {
      searches.push(searchPubMed(q.parts));   searchLabels.push('pubmed');
      searches.push(searchJStage(q.text));    searchLabels.push('jstage');
      searches.push(searchS2(q.text));        searchLabels.push('s2');
      searches.push(searchOpenAlex(q.text));  searchLabels.push('openalex');
      searches.push(searchCiNii(q.text));     searchLabels.push('cinii');
      searches.push(searchEPMC(q.text));      searchLabels.push('epmc');
    }
  } else {
    // English query or translation failed: original behavior
    searches.push(searchPubMed(queryParts)); searchLabels.push('pubmed');
    searches.push(searchJStage(jaText));     searchLabels.push('jstage');
    searches.push(searchS2(jaText));         searchLabels.push('s2');
    searches.push(searchOpenAlex(jaText));   searchLabels.push('openalex');
    searches.push(searchCiNii(jaText));      searchLabels.push('cinii');
    searches.push(searchEPMC(jaText));       searchLabels.push('epmc');
  }

  const settled = await Promise.allSettled(searches);

  // Collect all results and errors
  const allResults = [];
  const sourceErrors = {};
  for (let i = 0; i < settled.length; i++) {
    const s = settled[i];
    const label = searchLabels[i];
    if (s.status === 'rejected') {
      if (!sourceErrors[label]) sourceErrors[label] = s.reason?.message;
    } else {
      allResults.push(...s.value);
    }
  }

  // Smart dedup & merge
  const { results, sourceCounts } = deduplicateAndMerge(allResults);

  // National guidelines local search (with synonym expansion)
  const nationalGL = searchNationalGuidelines(expandedParts, translatedParts);
  sourceCounts.nationalGL = nationalGL.length;

  // Clinical Questions local search (with synonym expansion)
  const clinicalQuestions = searchClinicalQuestions(expandedParts, translatedParts);
  sourceCounts.clinicalQuestions = clinicalQuestions.length;

  // Patient Voice: additional qualitative research search
  let patientVoiceResults = [];
  if (patientVoice) {
    patientVoiceResults = await searchPatientVoice(queryParts, translatedParts);
  }

  return json({
    query: { disease, treatment, topic },
    multilingual: multilingual ? {
      translated: { disease: translatedDisease, treatment: translatedTreatment, topic: translatedTopic },
    } : null,
    totalCount: results.length,
    results: groupByEvidence(results),
    nationalGuidelines: nationalGL,
    clinicalQuestions,
    sources: { ...sourceCounts, errors: sourceErrors },
    patientVoice: patientVoice ? patientVoiceResults : undefined,
  }, 200, cors);
}

// ── Translation ──────────────────────────────────────────────

function isJapanese(text) {
  return /[\u3000-\u9FFF\uF900-\uFAFF]/.test(text);
}

async function translate(text, srcLang, tgtLang) {
  try {
    const url = `${GTRANSLATE}?client=gtx&sl=${srcLang}&tl=${tgtLang}&dt=t&q=${encodeURIComponent(text)}`;
    const res = await fetch(url, { signal: AbortSignal.timeout(5000) });
    const data = await res.json();
    // Response: [[["translated","original",...],...],...]
    const translated = data?.[0]?.map(seg => seg[0]).join('') || '';
    if (!translated || translated.toLowerCase() === text.toLowerCase()) return null;
    return translated;
  } catch {
    return null;
  }
}

async function handleTranslate(url, cors) {
  const text = url.searchParams.get('text') || '';
  if (!text) return json({ error: 'text required' }, 400, cors);
  const tgtLang = isJapanese(text) ? 'en' : 'ja';
  const srcLang = isJapanese(text) ? 'ja' : 'en';
  const result = await translate(text, srcLang, tgtLang);
  return json({ text: result || '', src: srcLang, tgt: tgtLang }, 200, cors);
}

// ── PubMed ────────────────────────────────────────────────────

async function searchPubMed(queryParts) {
  const query = queryParts.join(' AND ');

  // Step 1: esearch → PMIDs
  const searchUrl =
    `${PUBMED_BASE}/esearch.fcgi?db=pubmed` +
    `&term=${encodeURIComponent(query)}` +
    `&retmax=50&retmode=json&sort=relevance` +
    `&tool=evidence-navigator&email=evidence-navigator@example.com`;

  const searchRes = await fetch(searchUrl, { signal: AbortSignal.timeout(8000) });
  if (!searchRes.ok) throw new Error(`PubMed search HTTP ${searchRes.status}`);
  const searchData = await searchRes.json();
  const ids = searchData?.esearchresult?.idlist || [];
  if (!ids.length) return [];

  // Step 2: esummary → article details
  const sumUrl =
    `${PUBMED_BASE}/esummary.fcgi?db=pubmed` +
    `&id=${ids.join(',')}` +
    `&retmode=json` +
    `&tool=evidence-navigator&email=evidence-navigator@example.com`;

  const sumRes = await fetch(sumUrl, { signal: AbortSignal.timeout(8000) });
  if (!sumRes.ok) throw new Error(`PubMed summary HTTP ${sumRes.status}`);
  const sumData = await sumRes.json();

  const articles = [];
  for (const id of ids) {
    const a = sumData?.result?.[id];
    if (!a || !a.title) continue;

    const pubTypes = a.pubtype || [];
    const doi = (a.articleids || []).find(x => x.idtype === 'doi')?.value || '';

    articles.push({
      id: `pm-${id}`,
      title: strip(a.title),
      authors: (a.authors || []).map(x => x.name).slice(0, 5),
      journal: a.source || '',
      year: yearOf(a.pubdate || ''),
      pubTypes,
      evidenceLevel: classifyPubType(pubTypes),
      doi,
      url: `https://pubmed.ncbi.nlm.nih.gov/${id}/`,
      source: 'PubMed',
    });
  }
  return articles;
}

function strip(s) {
  return s.replace(/<[^>]*>/g, '');
}

function yearOf(s) {
  const m = s.match(/(\d{4})/);
  return m ? parseInt(m[1]) : null;
}

function classifyPubType(pubTypes) {
  const t = pubTypes.map(s => s.toLowerCase());
  if (t.some(s => s.includes('practice guideline') || s === 'guideline')) return 'guideline';
  if (t.some(s => s.includes('systematic review'))) return 'sr_ma';
  if (t.some(s => s.includes('meta-analysis'))) return 'sr_ma';
  if (t.some(s => s.includes('randomized controlled trial'))) return 'rct';
  if (t.some(s => s.includes('clinical trial'))) return 'clinical_trial';
  if (t.some(s => s.includes('observational') || s.includes('cohort') || s.includes('case-control'))) return 'observational';
  if (t.some(s => s.includes('case report'))) return 'case_report';
  if (t.some(s => s === 'review')) return 'review';
  return 'other';
}

// ── J-STAGE ───────────────────────────────────────────────────

async function searchJStage(query) {
  const url =
    `${JSTAGE_BASE}?service=3` +
    `&keyword=${encodeURIComponent(query)}` +
    `&count=20`;

  const res = await fetch(url, { signal: AbortSignal.timeout(8000) });
  if (!res.ok) throw new Error(`J-STAGE HTTP ${res.status}`);
  const xml = await res.text();
  return parseJStageXml(xml);
}

function parseJStageXml(xml) {
  const results = [];
  const entries = xml.split('<entry>').slice(1);

  for (const entry of entries) {
    // J-STAGE uses <article_title><ja>...</ja></article_title> and <title> at bottom
    const title = xmlNestedTag(entry, 'article_title', 'ja') || xmlTag(entry, 'title');
    const link = xmlNestedTag(entry, 'article_link', 'ja')
      || xmlNestedTag(entry, 'article_link', 'en')
      || xmlAttr(entry, 'link', 'href');
    // Prefer Japanese author names: <author><ja><name>...</name></ja></author>
    const authorBlock = xmlTag(entry, 'author');
    const jaAuthorBlock = authorBlock ? xmlTag(authorBlock, 'ja') : '';
    const enAuthorBlock = authorBlock ? xmlTag(authorBlock, 'en') : '';
    const authors = xmlAllCdata(jaAuthorBlock || enAuthorBlock || authorBlock || '', 'name');
    // Journal: <material_title><ja>...</ja></material_title>
    const journal = xmlNestedCdata(entry, 'material_title', 'ja')
      || xmlTag(entry, 'prism:publicationName');
    // Year: <pubyear>YYYY</pubyear>
    const year = xmlTag(entry, 'pubyear');
    const doi = xmlTag(entry, 'prism:doi');

    if (!title) continue;

    results.push({
      id: `js-${doi || Math.random().toString(36).slice(2, 8)}`,
      title: strip(title),
      authors: dedupAuthors(authors).slice(0, 5),
      journal: strip(journal || ''),
      year: year ? parseInt(year) : null,
      pubTypes: [],
      evidenceLevel: classifyByTitle(title),
      doi: doi || '',
      url: link || (doi ? `https://doi.org/${doi}` : ''),
      source: 'J-STAGE',
    });
  }
  return results;
}

function dedupAuthors(arr) {
  return [...new Set(arr.map(a => a.trim()).filter(Boolean))];
}

function xmlTag(xml, name) {
  const m = xml.match(new RegExp(`<${name}[^>]*>([\\s\\S]*?)</${name}>`));
  return m ? m[1].trim().replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, '$1') : '';
}

function xmlNestedTag(xml, parent, child) {
  const parentContent = xmlTag(xml, parent);
  if (!parentContent) return '';
  return xmlTag(parentContent, child);
}

function xmlNestedCdata(xml, parent, child) {
  const parentContent = xmlTag(xml, parent);
  if (!parentContent) return '';
  const inner = xmlTag(parentContent, child);
  return inner.replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, '$1');
}

function xmlAllCdata(xml, name) {
  const re = new RegExp(`<${name}[^>]*>([\\s\\S]*?)</${name}>`, 'g');
  const out = [];
  let m;
  while ((m = re.exec(xml))) {
    const val = m[1].trim().replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, '$1');
    if (val) out.push(val);
  }
  return out;
}

function xmlAttr(xml, tagName, attrName) {
  const m = xml.match(new RegExp(`<${tagName}[^>]*${attrName}="([^"]*)"[^>]*/?>`) );
  return m ? m[1] : '';
}

// ── Semantic Scholar ──────────────────────────────────────────

async function searchS2(query) {
  const url =
    `${S2_BASE}?query=${encodeURIComponent(query)}` +
    `&limit=20&fields=paperId,title,authors,year,venue,publicationTypes,externalIds,citationCount`;

  const res = await fetch(url, { signal: AbortSignal.timeout(8000) });
  if (!res.ok) {
    if (res.status === 429) return []; // Rate limited, graceful skip
    throw new Error(`S2 HTTP ${res.status}`);
  }
  const data = await res.json();

  return (data?.data || []).map(p => {
    const doi = p.externalIds?.DOI || '';
    const pmid = p.externalIds?.PubMed || '';
    return {
      id: `s2-${p.paperId}`,
      title: p.title || '',
      authors: (p.authors || []).map(a => a.name).slice(0, 5),
      journal: p.venue || '',
      year: p.year || null,
      pubTypes: p.publicationTypes || [],
      evidenceLevel: classifyS2Type(p.publicationTypes, p.title),
      doi,
      url: pmid
        ? `https://pubmed.ncbi.nlm.nih.gov/${pmid}/`
        : doi
          ? `https://doi.org/${doi}`
          : `https://www.semanticscholar.org/paper/${p.paperId}`,
      source: 'Semantic Scholar',
      citations: p.citationCount || 0,
    };
  });
}

function classifyS2Type(types, title) {
  const t = (types || []).map(s => s.toLowerCase());
  if (t.includes('metaanalysis') || t.includes('meta-analysis')) return 'sr_ma';
  if (t.includes('review') && title && /systematic/i.test(title)) return 'sr_ma';
  if (t.includes('clinicaltrial') || t.includes('clinical trial')) return 'clinical_trial';
  if (t.includes('casereport') || t.includes('case report')) return 'case_report';
  if (t.includes('review')) return 'review';
  // Fallback to title-based
  return classifyByTitle(title || '');
}

// ── OpenAlex ─────────────────────────────────────────────────

async function searchOpenAlex(query) {
  const url =
    `${OPENALEX_BASE}?search=${encodeURIComponent(query)}` +
    `&per_page=20` +
    `&select=id,title,authorships,publication_year,type,doi,primary_location,cited_by_count,language` +
    `&mailto=evidence-navigator@example.com`;

  const res = await fetch(url, { signal: AbortSignal.timeout(8000) });
  if (!res.ok) throw new Error(`OpenAlex HTTP ${res.status}`);
  const data = await res.json();

  return (data?.results || []).map(w => {
    const doi = (w.doi || '').replace('https://doi.org/', '');
    const journal = w.primary_location?.source?.display_name || '';
    return {
      id: `oa-${w.id?.split('/')?.pop() || ''}`,
      title: w.title || '',
      authors: (w.authorships || []).map(a => a.author?.display_name).filter(Boolean).slice(0, 5),
      journal,
      year: w.publication_year || null,
      pubTypes: w.type ? [w.type] : [],
      evidenceLevel: classifyOAType(w.type, w.title),
      doi,
      url: doi ? `https://doi.org/${doi}` : '',
      source: 'OpenAlex',
      citations: w.cited_by_count || 0,
      language: w.language || '',
    };
  });
}

function classifyOAType(type, title) {
  if (type === 'review') {
    // Distinguish SR/MA from narrative review
    const t = (title || '').toLowerCase();
    if (/systematic|meta[\s-]?analysis|メタ|システマティック/.test(t)) return 'sr_ma';
    return 'review';
  }
  return classifyByTitle(title || '');
}

// ── CiNii Research ───────────────────────────────────────────

async function searchCiNii(query) {
  const url =
    `${CINII_BASE}?q=${encodeURIComponent(query)}&format=json&count=20`;

  const res = await fetch(url, { signal: AbortSignal.timeout(8000) });
  if (!res.ok) throw new Error(`CiNii HTTP ${res.status}`);
  const data = await res.json();

  return (data?.items || []).map(item => {
    const title = item.title || '';
    const link = item.link?.['@id'] || '';
    const journal = item['prism:publicationName'] || '';
    const year = item['prism:publicationDate'] || '';
    const doi = item['dc:identifier']?.find?.(id => id['@type'] === 'cir:DOI')?.['@value'] || '';

    return {
      id: `cn-${link.split('/').pop() || Math.random().toString(36).slice(2, 8)}`,
      title: strip(title),
      authors: [], // CiNii opensearch doesn't include authors in list view
      journal: strip(journal),
      year: year ? parseInt(year) : null,
      pubTypes: item['dc:type'] ? [item['dc:type']] : [],
      evidenceLevel: classifyByTitle(title),
      doi,
      url: link,
      source: 'CiNii',
    };
  });
}

// ── Europe PMC ───────────────────────────────────────────────

async function searchEPMC(query) {
  const url =
    `${EPMC_BASE}?query=${encodeURIComponent(query)}` +
    `&format=json&pageSize=20&resultType=core`;

  const res = await fetch(url, { signal: AbortSignal.timeout(8000) });
  if (!res.ok) throw new Error(`EPMC HTTP ${res.status}`);
  const data = await res.json();

  return (data?.resultList?.result || []).map(p => {
    const pubTypes = p.pubTypeList?.pubType || [];
    const doi = p.doi || '';
    const pmid = p.pmid || '';
    return {
      id: `ep-${pmid || p.id || ''}`,
      title: strip(p.title || ''),
      authors: p.authorString
        ? p.authorString.split(', ').slice(0, 5)
        : [],
      journal: p.journalTitle || '',
      year: p.pubYear ? parseInt(p.pubYear) : null,
      pubTypes,
      evidenceLevel: classifyPubType(pubTypes),
      doi,
      url: pmid
        ? `https://pubmed.ncbi.nlm.nih.gov/${pmid}/`
        : doi
          ? `https://doi.org/${doi}`
          : `https://europepmc.org/article/${p.source}/${p.id}`,
      source: 'Europe PMC',
      citations: p.citedByCount || 0,
    };
  });
}

// ── Evidence Classification ──────────────────────────────────

function classifyByTitle(title) {
  const t = (title || '').toLowerCase();
  const tj = title || '';
  // Guideline
  if (/ガイドライン|guideline|推奨グレード|clinical recommendation|practice parameter|consensus statement/.test(t)) return 'guideline';
  // SR / Meta-analysis
  if (/システマティック|systematic|メタアナリシス|meta[\s-]?analysis|メタ分析|umbrella review|scoping review/.test(t)) return 'sr_ma';
  // RCT
  if (/ランダム化|randomiz|無作為化?比較|rct\b|controlled trial/.test(t)) return 'rct';
  // Clinical trial
  if (/臨床試験|clinical trial|介入研究|intervention study|pilot study|パイロット|feasibility/.test(t)) return 'clinical_trial';
  // Observational
  if (/コホート|cohort|観察研究|横断研究|cross[\s-]?sectional|前向き|後ろ向き|retrospectiv|prospectiv|追跡調査|縦断|longitudinal|case[\s-]?control|症例対照|レジストリ|registry|epidemiolog|prevalence|有病率|incidence|発生率|アンケート|survey|質問紙/.test(t)) return 'observational';
  // Case report / series
  if (/症例報告|case report|症例検討|case series|一例|1例|一症例|経験例/.test(t)) return 'case_report';
  // Review (broad)
  if (/レビュー|review|総説|文献的考察|文献検討|overview|narrative/.test(t)) return 'review';
  // Japanese-specific: study/investigation patterns → observational
  if (/についての検討|に関する検討|の検討|因子の検討|要因.{0,4}検討|発生要因/.test(tj)) return 'observational';
  if (/に関する研究|に関する調査|についての研究|の実態調査|の実態/.test(tj)) return 'observational';
  if (/解析|分析した|を分析|多変量|回帰|統計/.test(tj)) return 'observational';
  // Japanese: review/overview patterns
  if (/の現状と課題|現状と展望|の動向|の概要|の概説|の紹介|の基礎と応用|最新の|特集/.test(tj)) return 'review';
  if (/考え方と実際|の実際/.test(tj)) return 'review';
  // Japanese: report patterns → case report
  if (/の報告|について報告|を報告|を経験/.test(tj)) return 'case_report';
  // Effect/efficacy studies likely clinical
  if (/効果|有効性|efficacy|effectiveness|比較検討|comparison|治療成績|outcome/.test(t)) return 'clinical_trial';
  // Japanese: study with 影響/予後 → observational
  if (/影響|予後|関連|関与|相関|関係/.test(tj)) return 'observational';
  return 'other';
}

// ── Deduplication & Merge ─────────────────────────────────────

function normalizeTitle(title) {
  return (title || '')
    .toLowerCase()
    .replace(/[^\w\s\u3000-\u9FFF\uFF00-\uFFEF]/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function dedupKey(r) {
  if (r.doi) return `doi:${r.doi.toLowerCase().replace(/^https?:\/\/doi\.org\//, '')}`;
  const norm = normalizeTitle(r.title);
  if (norm.length > 10) return `t:${norm}:${r.year || '?'}`;
  return `id:${r.id}`;
}

const EV_RANK = {
  guideline: 0, sr_ma: 1, rct: 2, clinical_trial: 3,
  observational: 4, case_report: 5, review: 6, other: 7,
};

function bestEvidence(a, b) {
  return (EV_RANK[a] ?? 7) <= (EV_RANK[b] ?? 7) ? a : b;
}

function mergeInto(existing, newer) {
  // Evidence: keep the more specific (higher rank = lower number)
  existing.evidenceLevel = bestEvidence(existing.evidenceLevel, newer.evidenceLevel);
  // Citations: take highest
  existing.citations = Math.max(existing.citations || 0, newer.citations || 0);
  // DOI: fill in if missing
  if (!existing.doi && newer.doi) existing.doi = newer.doi;
  // Authors: prefer longer list
  if (newer.authors.length > existing.authors.length) existing.authors = newer.authors;
  // Journal: fill in if missing
  if (!existing.journal && newer.journal) existing.journal = newer.journal;
  // Year: fill in if missing
  if (!existing.year && newer.year) existing.year = newer.year;
  // URL: prefer PubMed > DOI > others
  if (newer.url && newer.url.includes('pubmed.ncbi') && !existing.url.includes('pubmed.ncbi')) {
    existing.url = newer.url;
  }
  // PubTypes: merge unique
  const ptSet = new Set([...existing.pubTypes, ...newer.pubTypes]);
  existing.pubTypes = [...ptSet];
  // Sources: track all databases that found this article
  if (!existing.foundIn) existing.foundIn = [existing.source];
  if (!existing.foundIn.includes(newer.source)) existing.foundIn.push(newer.source);
  // Language
  if (!existing.language && newer.language) existing.language = newer.language;
}

function deduplicateAndMerge(allResults) {
  const map = new Map(); // dedupKey → result
  const sourceCounts = { pubmed: 0, jstage: 0, s2: 0, openalex: 0, cinii: 0, epmc: 0 };

  for (const r of allResults) {
    const key = dedupKey(r);
    if (map.has(key)) {
      mergeInto(map.get(key), r);
    } else {
      r.foundIn = [r.source];
      map.set(key, r);
      // Count by primary source (first seen)
      const srcKey = r.source === 'PubMed' ? 'pubmed'
        : r.source === 'J-STAGE' ? 'jstage'
        : r.source === 'Semantic Scholar' ? 's2'
        : r.source === 'OpenAlex' ? 'openalex'
        : r.source === 'Europe PMC' ? 'epmc' : 'cinii';
      sourceCounts[srcKey]++;
    }
  }

  return { results: [...map.values()], sourceCounts };
}

// ── Grouping ──────────────────────────────────────────────────

const EVIDENCE_ORDER = [
  'guideline', 'sr_ma', 'rct', 'clinical_trial',
  'observational', 'case_report', 'review', 'other',
];

function groupByEvidence(results) {
  const grouped = {};
  for (const level of EVIDENCE_ORDER) grouped[level] = [];
  for (const r of results) {
    const level = grouped[r.evidenceLevel] ? r.evidenceLevel : 'other';
    grouped[level].push(r);
  }
  for (const level of EVIDENCE_ORDER) {
    grouped[level].sort((a, b) => (b.year || 0) - (a.year || 0));
  }
  return grouped;
}

// ── AI Parse (Natural Language → Structured Query) ───────────

async function handleAIParse(body, cors) {
  const { query, apiKey } = body || {};
  if (!query) return json({ error: 'query required' }, 400, cors);
  if (!apiKey) return json({ error: 'apiKey required' }, 400, cors);

  const prompt = `あなたは医療文献検索の専門家です。以下の臨床的な質問・シナリオを分析し、エビデンス検索に最適な構造化検索語に変換してください。

入力: "${query}"

以下のJSON形式で回答してください（JSONのみ、説明不要）:
{
  "disease": "疾患名（MeSH用語推奨）",
  "treatment": "治療法・介入（該当する場合）",
  "topic": "関心事項（予後、合併症、診断など）",
  "patientVoice": true/false（患者体験・質的研究も検索すべきか）,
  "interpretation": "入力をどう解釈したか（日本語で1文）"
}

注意:
- 疾患名・治療法はできるだけMeSH用語（英語）を使用
- 該当しないフィールドは空文字""に
- 患者の体験や生活の質に関する質問ならpatientVoiceをtrue`;

  try {
    const res = await fetch(
      `${GEMINI_BASE}/gemini-2.0-flash:generateContent?key=${apiKey}`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          contents: [{ parts: [{ text: prompt }] }],
          generationConfig: { temperature: 0.1, maxOutputTokens: 500 },
        }),
        signal: AbortSignal.timeout(15000),
      }
    );

    if (!res.ok) {
      console.error('Gemini API error:', res.status, await res.text());
      return json({ error: 'AI service error' }, 502, cors);
    }

    const data = await res.json();
    const text = data?.candidates?.[0]?.content?.parts?.[0]?.text || '';

    // Extract JSON from response (may be wrapped in ```json blocks)
    const jsonMatch = text.match(/\{[\s\S]*\}/);
    if (!jsonMatch) return json({ error: 'Failed to parse AI response' }, 500, cors);

    const parsed = JSON.parse(jsonMatch[0]);
    return json(parsed, 200, cors);
  } catch (e) {
    return json({ error: 'AI parse failed' }, 500, cors);
  }
}

// ── AI Summary ───────────────────────────────────────────────

async function handleAISummary(body, cors) {
  const { results, query, apiKey } = body || {};
  if (!results || !apiKey) return json({ error: 'results and apiKey required' }, 400, cors);

  // Build a concise summary of search results for the AI
  const articleList = [];
  const evidenceLevels = ['guideline', 'sr_ma', 'rct', 'clinical_trial', 'observational', 'case_report', 'review', 'other'];
  for (const level of evidenceLevels) {
    for (const item of (results[level] || []).slice(0, 5)) {
      articleList.push(`[${level.toUpperCase()}] ${item.title} (${item.year || '?'}) - ${item.journal || ''}`);
    }
  }

  if (!articleList.length) return json({ summary: '検索結果がありません。' }, 200, cors);

  const prompt = `あなたは医療文献のエビデンスサマリーを作成する専門家です。以下の検索結果を分析し、臨床的に重要なポイントをナラティブにまとめてください。

検索クエリ: ${JSON.stringify(query)}

検索で見つかった文献（最大40件の要約）:
${articleList.join('\n')}

以下の形式で日本語のサマリーを作成してください:
1. **エビデンスの概要**（2-3文）: 全体的な傾向
2. **主要な知見**（箇条書き3-5点）: ガイドライン・SR・RCTからの重要な知見
3. **エビデンスの質**（1-2文）: エビデンスレベルの分布と信頼性
4. **臨床的示唆**（1-2文）: 実践への示唆

注意: あくまで検索結果のタイトルからの推察であり、全文を読んだものではないことを明記してください。`;

  try {
    const res = await fetch(
      `${GEMINI_BASE}/gemini-2.0-flash:generateContent?key=${apiKey}`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          contents: [{ parts: [{ text: prompt }] }],
          generationConfig: { temperature: 0.3, maxOutputTokens: 1000 },
        }),
        signal: AbortSignal.timeout(30000),
      }
    );

    if (!res.ok) {
      console.error('Gemini API error:', res.status);
      return json({ error: 'AI service error' }, 502, cors);
    }

    const data = await res.json();
    const summary = data?.candidates?.[0]?.content?.parts?.[0]?.text || '';
    return json({ summary }, 200, cors);
  } catch (e) {
    return json({ error: 'AI summary failed' }, 500, cors);
  }
}

// ── Patient Voice Search ─────────────────────────────────────

const QUAL_TERMS = [
  'qualitative research', 'patient experience', 'patient perspective',
  'lived experience', 'patient-reported outcome', 'quality of life',
  'illness narrative', 'patient satisfaction', 'shared decision making',
];
const QUAL_TERMS_JA = [
  '患者体験', '患者の声', 'QOL', '生活の質', '語り',
  '質的研究', 'ナラティブ', '患者報告アウトカム', '当事者',
];

async function searchPatientVoice(queryParts, translatedParts) {
  const baseQuery = queryParts.join(' ');
  const qualEn = QUAL_TERMS.slice(0, 4).map(t => `"${t}"`).join(' OR ');
  const qualJa = QUAL_TERMS_JA.slice(0, 4).join(' OR ');
  const isJa = isJapanese(baseQuery);

  // For English DBs (PubMed/EPMC): use English query parts
  // If query is Japanese, auto-translate for PubMed/EPMC
  let enParts = queryParts;
  let enBase = baseQuery;
  if (isJa) {
    if (translatedParts && translatedParts.length > 0) {
      enParts = translatedParts;
      enBase = translatedParts.join(' ');
    } else {
      // Auto-translate Japanese → English for PubMed/EPMC
      const translations = await Promise.allSettled(
        queryParts.map(q => translate(q, 'ja', 'en'))
      );
      const autoTranslated = translations
        .map(t => t.status === 'fulfilled' && t.value ? t.value : null)
        .filter(Boolean);
      if (autoTranslated.length > 0) {
        enParts = autoTranslated;
        enBase = autoTranslated.join(' ');
      }
    }
  }

  const searches = [
    searchPatientVoicePubMed(enParts),
    searchEPMC(`${enBase} AND (${qualEn})`),
  ];

  // Japanese qualitative search (J-STAGE / CiNii)
  if (isJa) {
    searches.push(searchJStage(`${baseQuery} ${QUAL_TERMS_JA[0]}`));
    searches.push(searchCiNii(`${baseQuery} ${qualJa.split(' OR ')[0]}`));
  }

  // Translated query for broader coverage (non-Japanese → add J-STAGE/CiNii)
  if (translatedParts && translatedParts.length > 0 && !isJa) {
    const transQuery = translatedParts.join(' ');
    searches.push(searchJStage(`${transQuery} ${QUAL_TERMS_JA[0]}`));
  }

  const settled = await Promise.allSettled(searches);
  const allResults = [];
  for (const s of settled) {
    if (s.status === 'fulfilled') allResults.push(...s.value);
  }

  // Dedup and tag as patient voice
  const { results } = deduplicateAndMerge(allResults);
  return results.map(r => ({ ...r, isPatientVoice: true })).slice(0, 30);
}

async function searchPatientVoicePubMed(queryParts) {
  const diseaseQuery = queryParts.join(' AND ');
  const qualFilter = '("qualitative research"[Publication Type] OR "patient reported outcome"[tw] OR "lived experience"[tw] OR "quality of life"[tw] OR "patient experience"[tw] OR "patient perspective"[tw])';
  const query = `(${diseaseQuery}) AND ${qualFilter}`;

  const searchUrl =
    `${PUBMED_BASE}/esearch.fcgi?db=pubmed` +
    `&term=${encodeURIComponent(query)}` +
    `&retmax=20&retmode=json&sort=relevance` +
    `&tool=evidence-navigator&email=evidence-navigator@example.com`;

  const searchRes = await fetch(searchUrl, { signal: AbortSignal.timeout(8000) });
  if (!searchRes.ok) throw new Error(`PubMed PV search HTTP ${searchRes.status}`);
  const searchData = await searchRes.json();
  const ids = searchData?.esearchresult?.idlist || [];
  if (!ids.length) return [];

  const sumUrl =
    `${PUBMED_BASE}/esummary.fcgi?db=pubmed` +
    `&id=${ids.join(',')}` +
    `&retmode=json` +
    `&tool=evidence-navigator&email=evidence-navigator@example.com`;

  const sumRes = await fetch(sumUrl, { signal: AbortSignal.timeout(8000) });
  if (!sumRes.ok) throw new Error(`PubMed PV summary HTTP ${sumRes.status}`);
  const sumData = await sumRes.json();

  const articles = [];
  for (const id of ids) {
    const a = sumData?.result?.[id];
    if (!a || !a.title) continue;
    const pubTypes = a.pubtype || [];
    const doi = (a.articleids || []).find(x => x.idtype === 'doi')?.value || '';
    articles.push({
      id: `pm-${id}`,
      title: strip(a.title),
      authors: (a.authors || []).map(x => x.name).slice(0, 5),
      journal: a.source || '',
      year: yearOf(a.pubdate || ''),
      pubTypes,
      evidenceLevel: classifyPubType(pubTypes),
      doi,
      url: `https://pubmed.ncbi.nlm.nih.gov/${id}/`,
      source: 'PubMed',
    });
  }
  return articles;
}

// ── National Guidelines Local Search ─────────────────────────

function searchNationalGuidelines(queryParts, translatedParts) {
  const allTerms = [...queryParts];
  if (translatedParts) allTerms.push(...translatedParts);
  const terms = allTerms.map(t => t.toLowerCase());

  const scored = [];
  for (const gl of GUIDELINES) {
    let score = 0;
    const keywords = gl.diseases.map(d => d.toLowerCase());
    const titleLower = gl.title.toLowerCase();

    for (const term of terms) {
      if (keywords.some(k => k === term)) score += 10;
      else if (keywords.some(k => k.includes(term) || term.includes(k))) score += 5;
      if (titleLower.includes(term)) score += 3;
    }

    if (score > 0) {
      scored.push({
        id: `gl-${gl.id}`,
        title: gl.title,
        organization: gl.org,
        category: gl.cat,
        year: gl.year,
        url: gl.url,
        diseases: gl.diseases,
        score,
        source: '国内GL',
        evidenceLevel: 'guideline',
      });
    }
  }

  scored.sort((a, b) => b.score - a.score || (b.year || 0) - (a.year || 0));
  return scored;
}

// ── Clinical Questions Local Search ───────────────────────────

function searchClinicalQuestions(queryParts, translatedParts) {
  const allTerms = [...queryParts];
  if (translatedParts) allTerms.push(...translatedParts);
  const terms = allTerms.map(t => t.toLowerCase());

  // Build guideline lookup for title/org
  const glMap = new Map();
  for (const gl of GUIDELINES) glMap.set(gl.id, gl);

  const scored = [];
  for (const cq of CQ_DATA) {
    let score = 0;
    const kwLower = cq.kw.map(k => k.toLowerCase());
    const qLower = cq.q.toLowerCase();

    for (const term of terms) {
      if (kwLower.some(k => k === term)) score += 10;
      else if (kwLower.some(k => k.includes(term) || term.includes(k))) score += 5;
      if (qLower.includes(term)) score += 3;
    }

    if (score > 0) {
      const gl = glMap.get(cq.gid);
      scored.push({
        gid: cq.gid,
        cq: cq.cq,
        question: cq.q,
        type: cq.type,
        recommendation: cq.rec,
        evidenceLevel: cq.ev,
        keywords: cq.kw,
        guidelineTitle: gl ? gl.title : '',
        guidelineOrg: gl ? gl.org : '',
        guidelineUrl: gl ? gl.url : '',
        page: cq.page || null,
        score,
      });
    }
  }

  scored.sort((a, b) => b.score - a.score);
  return scored;
}
