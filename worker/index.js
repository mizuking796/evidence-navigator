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

const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
};

export default {
  async fetch(request) {
    const url = new URL(request.url);

    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: CORS_HEADERS });
    }

    try {
      switch (url.pathname) {
        case '/api/search':
          return await handleSearch(url);
        case '/api/mesh':
          return await handleMeshSuggest(url);
        case '/api/suggest':
          return handleSuggest(url);
        case '/api/cq/list':
          return handleCQList(url);
        case '/api/translate':
          return await handleTranslate(url);
        case '/api/ai/parse':
          if (request.method !== 'POST') return json({ error: 'POST required' }, 405);
          return await handleAIParse(await request.json());
        case '/api/ai/summary':
          if (request.method !== 'POST') return json({ error: 'POST required' }, 405);
          return await handleAISummary(await request.json());
        default:
          return json({ error: 'Not found' }, 404);
      }
    } catch (e) {
      return json({ error: e.message }, 500);
    }
  },
};

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'Content-Type': 'application/json', ...CORS_HEADERS },
  });
}

// ── MeSH Suggest ──────────────────────────────────────────────

async function handleMeshSuggest(url) {
  const q = url.searchParams.get('q') || '';
  if (q.length < 2) return json([]);

  const res = await fetch(
    `${MESH_LOOKUP}?label=${encodeURIComponent(q)}&match=contains&limit=10`,
    { headers: { Accept: 'application/json' } }
  );
  const data = await res.json();

  const suggestions = Array.isArray(data)
    ? data.map(item => (typeof item === 'object' && item.label) ? item.label : String(item))
    : [];

  return json(suggestions);
}

// ── Suggest (CQ keywords + GL diseases) ─────────────────────

function handleSuggest(url) {
  const q = (url.searchParams.get('q') || '').toLowerCase();
  if (q.length < 1) return json([]);

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

  return json(results.slice(0, 15));
}

// ── CQ List (browse all) ────────────────────────────────────

function handleCQList(url) {
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
        org: gl ? gl.org : '',
        url: gl ? gl.url : '',
        cat: gl ? gl.cat : '',
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
  });
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

async function handleSearch(url) {
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
    return json({ error: 'q, disease, treatment, or topic required' }, 400);
  }

  // Synonym expansion (always, before multilingual)
  const expandedParts = expandSynonyms(queryParts);
  // Keep original queryParts for external DB, use expandedParts for CQ/GL search

  // Multilingual: translate and search in both languages
  let translatedParts = null;
  let translatedDisease = '';
  let translatedTreatment = '';
  let translatedTopic = '';
  if (multilingual) {
    const isJa = isJapanese(disease || treatment || topic);
    const srcLang = isJa ? 'ja' : 'en';
    const tgtLang = isJa ? 'en' : 'ja';

    const translations = await Promise.allSettled(
      queryParts.map(q => translate(q, srcLang, tgtLang))
    );
    translatedParts = translations.map((t) =>
      t.status === 'fulfilled' && t.value ? t.value : null
    );
    // Track translated terms for display
    let idx = 0;
    if (disease)   { translatedDisease   = translatedParts[idx] || ''; idx++; }
    if (treatment) { translatedTreatment = translatedParts[idx] || ''; idx++; }
    if (topic)     { translatedTopic     = translatedParts[idx] || ''; }
    // Filter out failed translations
    translatedParts = translatedParts.filter(Boolean);
  }

  // Build parallel search tasks: 5 sources × (1 or 2 languages)
  const allQueries = [{ parts: queryParts, text: queryParts.join(' ') }];
  if (translatedParts && translatedParts.length > 0) {
    allQueries.push({ parts: translatedParts, text: translatedParts.join(' ') });
  }

  const searches = [];
  const searchLabels = [];
  for (const q of allQueries) {
    searches.push(searchPubMed(q.parts));   searchLabels.push('pubmed');
    searches.push(searchJStage(q.text));    searchLabels.push('jstage');
    searches.push(searchS2(q.text));        searchLabels.push('s2');
    searches.push(searchOpenAlex(q.text));  searchLabels.push('openalex');
    searches.push(searchCiNii(q.text));     searchLabels.push('cinii');
    searches.push(searchEPMC(q.text));      searchLabels.push('epmc');
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
  });
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

async function handleTranslate(url) {
  const text = url.searchParams.get('text') || '';
  if (!text) return json({ error: 'text required' }, 400);
  const tgtLang = isJapanese(text) ? 'en' : 'ja';
  const srcLang = isJapanese(text) ? 'ja' : 'en';
  const result = await translate(text, srcLang, tgtLang);
  return json({ text: result || '', src: srcLang, tgt: tgtLang });
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

  const searchRes = await fetch(searchUrl);
  const searchData = await searchRes.json();
  const ids = searchData?.esearchresult?.idlist || [];
  if (!ids.length) return [];

  // Step 2: esummary → article details
  const sumUrl =
    `${PUBMED_BASE}/esummary.fcgi?db=pubmed` +
    `&id=${ids.join(',')}` +
    `&retmode=json` +
    `&tool=evidence-navigator&email=evidence-navigator@example.com`;

  const sumRes = await fetch(sumUrl);
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

  const res = await fetch(url);
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

async function handleAIParse(body) {
  const { query, apiKey } = body || {};
  if (!query) return json({ error: 'query required' }, 400);
  if (!apiKey) return json({ error: 'apiKey required' }, 400);

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
      const err = await res.text();
      return json({ error: `Gemini API error: ${res.status} ${err}` }, 502);
    }

    const data = await res.json();
    const text = data?.candidates?.[0]?.content?.parts?.[0]?.text || '';

    // Extract JSON from response (may be wrapped in ```json blocks)
    const jsonMatch = text.match(/\{[\s\S]*\}/);
    if (!jsonMatch) return json({ error: 'Failed to parse AI response', raw: text }, 500);

    const parsed = JSON.parse(jsonMatch[0]);
    return json(parsed);
  } catch (e) {
    return json({ error: `AI parse failed: ${e.message}` }, 500);
  }
}

// ── AI Summary ───────────────────────────────────────────────

async function handleAISummary(body) {
  const { results, query, apiKey } = body || {};
  if (!results || !apiKey) return json({ error: 'results and apiKey required' }, 400);

  // Build a concise summary of search results for the AI
  const articleList = [];
  const evidenceLevels = ['guideline', 'sr_ma', 'rct', 'clinical_trial', 'observational', 'case_report', 'review', 'other'];
  for (const level of evidenceLevels) {
    for (const item of (results[level] || []).slice(0, 5)) {
      articleList.push(`[${level.toUpperCase()}] ${item.title} (${item.year || '?'}) - ${item.journal || ''}`);
    }
  }

  if (!articleList.length) return json({ summary: '検索結果がありません。' });

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
      const err = await res.text();
      return json({ error: `Gemini API error: ${res.status} ${err}` }, 502);
    }

    const data = await res.json();
    const summary = data?.candidates?.[0]?.content?.parts?.[0]?.text || '';
    return json({ summary });
  } catch (e) {
    return json({ error: `AI summary failed: ${e.message}` }, 500);
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

  const searches = [
    searchPatientVoicePubMed(queryParts),
    searchEPMC(`${baseQuery} AND (${qualEn})`),
  ];

  // Add Japanese qualitative search
  const isJa = isJapanese(baseQuery);
  if (isJa) {
    searches.push(searchJStage(`${baseQuery} ${QUAL_TERMS_JA[0]}`));
    searches.push(searchCiNii(`${baseQuery} ${qualJa.split(' OR ')[0]}`));
  }

  // Translated query for broader coverage
  if (translatedParts && translatedParts.length > 0) {
    const transQuery = translatedParts.join(' ');
    if (!isJa) {
      searches.push(searchJStage(`${transQuery} ${QUAL_TERMS_JA[0]}`));
    } else {
      searches.push(searchEPMC(`${transQuery} AND (${qualEn})`));
    }
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

  const searchRes = await fetch(searchUrl);
  const searchData = await searchRes.json();
  const ids = searchData?.esearchresult?.idlist || [];
  if (!ids.length) return [];

  const sumUrl =
    `${PUBMED_BASE}/esummary.fcgi?db=pubmed` +
    `&id=${ids.join(',')}` +
    `&retmode=json` +
    `&tool=evidence-navigator&email=evidence-navigator@example.com`;

  const sumRes = await fetch(sumUrl);
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
