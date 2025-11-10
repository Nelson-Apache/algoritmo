"""
CooccurrenceNetworkAnalyzer
===========================

Grafo no dirigido de coocurrencia de términos en documentos académicos.

- Cada nodo representa un término (keyword o token frecuente del abstract)
- Cada arista (u, v) indica que ambos términos aparecen juntos en al menos un documento
- El peso de la arista es el número de documentos donde coocurren

Funcionalidad principal:
- build_graph(articles, min_doc_freq=1, min_term_len=3, top_tokens_per_doc=None)
- degree(term): grado simple del nodo
- components(): componentes conexas (lista de listas de términos)
- top_degrees(k): lista de términos con mayor grado

Entrada esperada (lista de artículos):
article = {
  'id': str|int,
  'abstract': str (opcional),
  'keywords': list[str] | str (opcional)
}

Notas de implementación:
- Normaliza términos a minúsculas.
- Filtro por longitud mínima y frecuencia documental mínima.
- Tokenización simple con regex para abstracts: palabras alfanuméricas y guiones, tamaño >= min_term_len.
- Puede limitar tokens por documento (top_tokens_per_doc) ordenando por frecuencia local si se desea evitar demasiados nodos.
"""
from __future__ import annotations
from typing import Dict, List, Any, Iterable, Tuple, Set, Optional, Callable
import re
import math
from typing import cast

# Integraciones opcionales
try:
    from .ConceptsCategoryAnalyzer import ConceptsCategoryAnalyzer  # type: ignore
except Exception:
    ConceptsCategoryAnalyzer = None  # type: ignore

try:
    from .HierarchicalClusteringAnalyzer import HierarchicalClusteringAnalyzer  # type: ignore
except Exception:
    HierarchicalClusteringAnalyzer = None  # type: ignore

try:
    from .SimilitudTextualClasico import SimilitudTextualClasico  # type: ignore
except Exception:
    SimilitudTextualClasico = None  # type: ignore

try:
    from .SimilitudTextualIA import SimilitudTextualIA  # type: ignore
except Exception:
    SimilitudTextualIA = None  # type: ignore

class CooccurrenceNetworkAnalyzer:
    def __init__(self,
                 min_doc_freq: int = 1,
                 min_term_len: int = 3,
                 top_tokens_per_doc: Optional[int] = None,
                 # Enriquecimiento por conceptos (GAIE)
                 use_concepts: bool = True,
                 concepts_top_k: int = 15,
                 concepts_text_field: str = 'abstract',
                 # Ponderación por similitud de documentos (TF-IDF coseno via HCA)
                 use_doc_similarity_weighting: bool = True,
                 doc_sim_alpha: float = 0.5,
                 # Ponderación/filtrado por similitud entre términos (texto a texto)
                 similarity_backend: Optional[str] = 'classic',  # 'classic'|'ia'|None
                 similarity_methods: Optional[List[str]] = None,  # classic: ['coseno','tfidf','levenshtein','jarowinkler']; ia: ['sbert','hf']
                 term_sim_alpha: float = 0.4,
                 min_term_similarity: float = 0.0) -> None:
        self.min_doc_freq = max(1, int(min_doc_freq))
        self.min_term_len = max(1, int(min_term_len))
        self.top_tokens_per_doc = top_tokens_per_doc if (top_tokens_per_doc is None or top_tokens_per_doc > 0) else None
        # Opciones de integración
        self.use_concepts = use_concepts
        self.concepts_top_k = concepts_top_k
        self.concepts_text_field = concepts_text_field
        self.use_doc_similarity_weighting = use_doc_similarity_weighting
        self.doc_sim_alpha = float(max(0.0, min(2.0, doc_sim_alpha)))
        self.similarity_backend = (similarity_backend or '').lower() if similarity_backend else None
        self.similarity_methods = [m.lower() for m in (similarity_methods or (['coseno'] if self.similarity_backend=='classic' else ['sbert']))]
        self.term_sim_alpha = float(max(0.0, min(2.0, term_sim_alpha)))
        self.min_term_similarity = float(max(0.0, min(1.0, min_term_similarity)))
        # Estructuras
        self.term_docs: Dict[str, Set[int]] = {}
        self.nodes: List[str] = []
        self.edges: List[Tuple[str, str, float]] = []  # (u,v,w)
        self._built = False
        # Backends lazy
        self._cca = ConceptsCategoryAnalyzer() if (self.use_concepts and ConceptsCategoryAnalyzer) else None
        self._hca = HierarchicalClusteringAnalyzer() if (self.use_doc_similarity_weighting and HierarchicalClusteringAnalyzer) else None
        self._simc = SimilitudTextualClasico() if (self.similarity_backend=='classic' and SimilitudTextualClasico) else None
        self._simi = SimilitudTextualIA() if (self.similarity_backend=='ia' and SimilitudTextualIA) else None

    # ----------------------------- Construcción -----------------------------
    def build_graph(self, articles: List[Dict[str, Any]]) -> None:
        self.term_docs.clear()
        self.nodes.clear()
        self.edges.clear()
        self._built = False
        # Normalizar textos para integraciones
        abstracts: List[str] = [str(a.get('abstract','') or '') for a in articles]
        titles: List[str] = [str(a.get('title','') or '') for a in articles]
        if self.concepts_text_field.lower() == 'title':
            concept_texts = titles
        else:
            concept_texts = abstracts

        # Recolectar términos por documento
        for doc_idx, art in enumerate(articles):
            kws = self._normalize_keywords(art.get('keywords'))
            abstract = str(art.get('abstract', '') or '')
            tokens = self._tokenize(abstract)
            # Posible recorte de tokens del abstract
            if self.top_tokens_per_doc is not None and len(tokens) > self.top_tokens_per_doc:
                # ordenar por frecuencia local desc
                freq: Dict[str, int] = {}
                for t in tokens:
                    freq[t] = freq.get(t, 0) + 1
                tokens = sorted(freq.keys(), key=lambda x: freq[x], reverse=True)[: self.top_tokens_per_doc]
            terms = set(kws) | set(tokens)
            # Enriquecer con términos de GAIE si está habilitado
            if self._cca is not None:
                try:
                    # Ejecutar una sola vez fuera del loop sería ideal, pero depende de top_k.
                    # Aquí lo haremos perezoso: computar all_gen_terms la primera vez
                    if not hasattr(self, '_cca_gen_terms_cache'):
                        rep = self._cca.analyze(concept_texts, top_k=self.concepts_top_k)
                        all_gen_terms = [x['term'] for x in rep.get('generated_terms', []) if isinstance(x, dict) and x.get('term')]
                        # compilar regex por término
                        compile_rx = getattr(self._cca, '_compile_phrase_regex', None)
                        compiled = [(t, compile_rx(t)) for t in all_gen_terms] if callable(compile_rx) else []
                        setattr(self, '_cca_gen_terms_cache', compiled)
                    compiled = getattr(self, '_cca_gen_terms_cache', [])
                    txt = (concept_texts[doc_idx] if doc_idx < len(concept_texts) else abstract).lower()
                    for term, rx in compiled:
                        try:
                            if rx.search(txt):
                                terms.add(str(term).lower())
                        except Exception:
                            continue
                except Exception:
                    pass
            for t in terms:
                if len(t) < self.min_term_len:
                    continue
                self.term_docs.setdefault(t, set()).add(doc_idx)
        # Filtrar por frecuencia documental mínima
        filtered_terms = [t for t, docs in self.term_docs.items() if len(docs) >= self.min_doc_freq]
        filtered_terms.sort()
        self.nodes = filtered_terms
        # Opcional: matriz de similitud de documentos (TF-IDF coseno) vía HCA
        doc_sim: Optional[List[List[float]]] = None
        if self._hca is not None:
            try:
                # reutilizar vectorización de HCA para obtener similitud coseno
                X = self._hca._vectorize(abstracts)
                # cosine_similarity ya está en HCA; replicamos aquí para evitar imports extra
                from sklearn.metrics.pairwise import cosine_similarity as _cos
                S = _cos(X)
                # normalizar [0,1], asegurar diagonal 1
                import numpy as _np  # type: ignore
                if hasattr(_np, 'clip'):
                    S = _np.clip(S, 0.0, 1.0)
                doc_sim = S.tolist()
            except Exception:
                doc_sim = None

        # Preparar función de similitud entre términos
        def term_similarity(a: str, b: str) -> float:
            a = (a or '').strip()
            b = (b or '').strip()
            if not a or not b:
                return 0.0
            try:
                if self.similarity_backend == 'classic' and self._simc is not None:
                    scores: List[float] = []
                    for m in self.similarity_methods:
                        if m == 'coseno' and hasattr(self._simc, 'similitud_coseno'):
                            scores.append(float(self._simc.similitud_coseno(a, b)))
                        elif m == 'tfidf' and hasattr(self._simc, 'similitud_tfidf'):
                            scores.append(float(self._simc.similitud_tfidf(a, b, corpus=[a, b])))
                        elif m == 'levenshtein' and hasattr(self._simc, 'distancia_levenshtein'):
                            scores.append(float(self._simc.distancia_levenshtein(a, b)))
                        elif m == 'jarowinkler' and hasattr(self._simc, 'distancia_jaro_winkler'):
                            scores.append(float(self._simc.distancia_jaro_winkler(a, b)))
                    if scores:
                        # Asegurar [0,1]
                        vals = [max(0.0, min(1.0, s)) for s in scores]
                        return float(sum(vals)/len(vals))
                elif self.similarity_backend == 'ia' and self._simi is not None:
                    scores: List[float] = []
                    for m in self.similarity_methods:
                        if m == 'sbert' and hasattr(self._simi, 'similitud_sbert'):
                            s = float(self._simi.similitud_sbert(a, b))
                            # similitud coseno en [-1,1]; normalizar
                            scores.append((s+1.0)/2.0 if s < 0 else s)
                        elif m == 'hf' and hasattr(self._simi, 'similitud_transformer'):
                            s = float(self._simi.similitud_transformer(a, b))
                            scores.append((s+1.0)/2.0 if s < 0 else s)
                    if scores:
                        vals = [max(0.0, min(1.0, s)) for s in scores]
                        return float(sum(vals)/len(vals))
            except Exception:
                return 0.0
            return 0.0

        # Construir aristas (no dirigido) por intersección > 0, con ponderaciones opcionales
        for i in range(len(filtered_terms)):
            ti = filtered_terms[i]
            docs_i = self.term_docs[ti]
            for j in range(i+1, len(filtered_terms)):
                tj = filtered_terms[j]
                inter = docs_i & self.term_docs[tj]
                if not inter:
                    continue
                base_w = float(len(inter))
                w = base_w
                # ponderación por similitud promedio entre documentos donde coocurren
                if doc_sim is not None and self.use_doc_similarity_weighting:
                    idxs = list(inter)
                    if len(idxs) >= 2:
                        ssum = 0.0; cnt = 0
                        for aidx in range(len(idxs)):
                            for bidx in range(aidx+1, len(idxs)):
                                ssum += float(doc_sim[idxs[aidx]][idxs[bidx]])
                                cnt += 1
                        avg_sim = (ssum / cnt) if cnt > 0 else 1.0
                    else:
                        avg_sim = 1.0
                    w *= (1.0 + self.doc_sim_alpha * max(0.0, min(1.0, avg_sim)))
                # ponderación/filtrado por similitud entre términos
                if self.similarity_backend is not None:
                    st = term_similarity(ti, tj)
                    if st < self.min_term_similarity:
                        continue
                    w *= (1.0 + self.term_sim_alpha * max(0.0, min(1.0, st)))
                self.edges.append((ti, tj, float(w)))
        self._built = True

    # ----------------------------- Consultas -------------------------------
    def degree(self, term: str) -> int:
        if not self._built:
            raise RuntimeError('El grafo no ha sido construido aún.')
        d = 0
        for u, v, _ in self.edges:
            if term == u or term == v:
                d += 1
        return d

    def weighted_degree(self, term: str) -> float:
        if not self._built:
            raise RuntimeError('El grafo no ha sido construido aún.')
        d = 0.0
        for u, v, w in self.edges:
            if term == u or term == v:
                d += w
        return d

    def top_degrees(self, k: int = 10, weighted: bool = False) -> List[Tuple[str, float]]:
        if not self._built:
            raise RuntimeError('El grafo no ha sido construido aún.')
        scores: List[Tuple[str, float]] = []
        for t in self.nodes:
            val = self.weighted_degree(t) if weighted else self.degree(t)
            scores.append((t, val))
        scores.sort(key=lambda x: x[1], reverse=True)
        return scores[: max(0, k)]

    def components(self) -> List[List[str]]:
        if not self._built:
            raise RuntimeError('El grafo no ha sido construido aún.')
        parent = {t: t for t in self.nodes}
        def find(x: str) -> str:
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x
        def union(a: str, b: str) -> None:
            ra, rb = find(a), find(b)
            if ra != rb:
                parent[rb] = ra
        for u, v, _ in self.edges:
            union(u, v)
        sets: Dict[str, List[str]] = {}
        for t in self.nodes:
            r = find(t)
            sets.setdefault(r, []).append(t)
        return list(sets.values())

    # ----------------------------- Helpers ---------------------------------
    @staticmethod
    def _tokenize(text: str) -> List[str]:
        if not text:
            return []
        tokens = re.findall(r'[A-Za-z][A-Za-z0-9_-]{2,}', text.lower())
        return tokens

    @staticmethod
    def _normalize_keywords(kws: Any) -> List[str]:
        if kws is None:
            return []
        if isinstance(kws, str):
            parts = [p.strip() for p in kws.replace(';', ',').split(',') if p.strip()]
        elif isinstance(kws, (list, tuple, set)):
            parts = [str(p).strip() for p in kws if str(p).strip()]
        else:
            return []
        out = []
        for p in parts:
            p = p.lower()
            if p:
                out.append(p)
        return sorted(list(set(out)))

    # ----------------------------- Export ----------------------------------
    def export(self) -> Dict[str, Any]:
        if not self._built:
            raise RuntimeError('El grafo no ha sido construido aún.')
        return {
            'nodes': [{'id': t, 'label': t} for t in self.nodes],
            'edges': [{'source': u, 'target': v, 'weight': float(w)} for (u, v, w) in self.edges],
            'node_count': len(self.nodes),
            'edge_count': len(self.edges),
            'components': self.components(),
            'component_count': len(self.components())
        }

__all__ = ['CooccurrenceNetworkAnalyzer']
