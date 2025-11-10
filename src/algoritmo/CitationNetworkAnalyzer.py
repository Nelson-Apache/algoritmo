"""
Analizador de Red de Citaciones (grafo dirigido con pesos)
=========================================================

Esta clase construye y analiza una red de citaciones académicas donde:
- Cada nodo es un artículo (id único)
- Cada arista dirigida u -> v indica que el artículo u cita al artículo v
- El peso de la arista representa la fortaleza de la relación (∈ [0,1])

Fuentes de relaciones:
1) Citaciones explícitas (si el dato viene en el artículo)
2) Inferencia por similitud de título, autores o palabras clave
   - Por defecto usa algoritmos clásicos de `SimilitudTextualClasico`
   - Opcionalmente puede usar `SimilitudTextualIA` si está disponible

Funcionalidad principal:
- build_graph: construcción automática del grafo y pesos
- shortest_path_dijkstra: camino mínimo entre dos artículos
- all_pairs_floyd_warshall: caminos mínimos entre todos los pares
- strongly_connected_components: componentes fuertemente conexas (SCC)

Entrada esperada de artículos (lista de dicts):
article = {
  'id': str|int,                    # ID único del artículo
  'title': str,                     # Título
  'authors': list[str] | str,       # Autores (lista o string separado por comas)
  'keywords': list[str] | str,      # Palabras clave (lista o string)
  'citations': list[str|int]        # IDs citados explícitamente (opcional)
}

Notas sobre pesos y costos:
- Si la arista es explícita, su peso por defecto es 1.0
- Si se infiere por similitud, el peso es una combinación de similitudes
  de título, autores y palabras clave (ponderable), recortada a [0,1]
- Para caminos mínimos, el costo por defecto es 1/(w+eps), así aristas
  con mayor peso tienen menor costo (más "cercanas").

Ejemplo mínimo:
>>> from src.algoritmo.CitationNetworkAnalyzer import CitationNetworkAnalyzer
>>> analyzer = CitationNetworkAnalyzer()
>>> articles = [
...   {'id': 'A', 'title': 'Deep Learning for NLP', 'authors': ['Smith','Lee'], 'keywords': ['NLP','DL'], 'citations': ['B']},
...   {'id': 'B', 'title': 'Neural Networks in Text', 'authors': ['Kim'], 'keywords': ['NLP','NN'], 'citations': []},
...   {'id': 'C', 'title': 'Graph Methods for Documents', 'authors': ['Lopez'], 'keywords': ['Graph','Text']}
... ]
>>> analyzer.build_graph(articles, infer_if_missing=True)
>>> dist, path = analyzer.shortest_path_dijkstra('C', 'B')
>>> sccs = analyzer.strongly_connected_components()
"""
from __future__ import annotations

from typing import Dict, List, Tuple, Any, Callable, Optional, Iterable
import re
import math
import heapq
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from typing import cast
try:
    import numpy as _np  # numpy es usado cuando IA soporta embeddings por lotes
except Exception:  # pragma: no cover
    _np = None  # type: ignore

try:
    # Usaremos la clase clásica si está disponible (está en el proyecto)
    from .SimilitudTextualClasico import SimilitudTextualClasico  # type: ignore
except Exception:
    SimilitudTextualClasico = None  # type: ignore

try:
    # Opcional: IA (puede no estar instalada la dependencia)
    from .SimilitudTextualIA import SimilitudTextualIA  # type: ignore
except Exception:
    SimilitudTextualIA = None  # type: ignore

# Análisis de conceptos y clustering jerárquico del proyecto
try:
    from .ConceptsCategoryAnalyzer import ConceptsCategoryAnalyzer  # type: ignore
except Exception:
    ConceptsCategoryAnalyzer = None  # type: ignore

try:
    from .HierarchicalClusteringAnalyzer import HierarchicalClusteringAnalyzer  # type: ignore
except Exception:
    HierarchicalClusteringAnalyzer = None  # type: ignore


class CitationNetworkAnalyzer:
    """Construcción y análisis de grafos de citaciones dirigidos y ponderados."""

    def __init__(
        self,
        similarity_backend: str = 'classic',  # 'classic' | 'ia'
        title_weight: float = 0.6,
        authors_weight: float = 0.25,
        keywords_weight: float = 0.15,
        infer_threshold: float = 0.6,   # umbral para crear aristas inferidas
        infer_top_k: Optional[int] = 3, # limita salida por nodo para evitar gráficos densos
        cost_mode: str = 'inverse',     # 'inverse' (1/(w+eps)) | 'linear' (1-w) | 'unit'
        classic_methods: Optional[List[str]] = None,  # e.g., ['coseno','jaccard','dice','overlap']
        ai_methods: Optional[List[str]] = None,       # e.g., ['sbert','hf']
        ia_timeout_sec: Optional[float] = 15.0        # timeout por llamada IA (None para desactivar)
    ) -> None:
        # Configuración de similitud / inferencia
        self.similarity_backend = similarity_backend
        self.title_weight = title_weight
        self.authors_weight = authors_weight
        self.keywords_weight = keywords_weight
        self.infer_threshold = infer_threshold
        self.infer_top_k = infer_top_k
        self.cost_mode = cost_mode
        self.ia_timeout_sec = ia_timeout_sec

        # Preferencias de similitud
        self.classic_methods = [m.lower() for m in (classic_methods or ['coseno'])]
        self.ai_methods = [m.lower() for m in (ai_methods or ['hf'])]

        # Estructuras del grafo
        self.nodes: Dict[Any, Dict[str, Any]] = {}           # id -> datos del artículo
        self.adj: Dict[Any, Dict[Any, float]] = {}            # u -> {v: weight}

        # Inicializar backends de similitud si están disponibles
        self._sim_classic = SimilitudTextualClasico() if SimilitudTextualClasico else None
        self._sim_ia = SimilitudTextualIA() if (SimilitudTextualIA and similarity_backend == 'ia') else None
        self._concepts = ConceptsCategoryAnalyzer() if ConceptsCategoryAnalyzer else None
        self._hcluster = HierarchicalClusteringAnalyzer() if HierarchicalClusteringAnalyzer else None
        # Ejecutores (lazy) para timeouts IA
        self._ia_executor: Optional[ThreadPoolExecutor] = None

    # ---------------------------------------------------------------------
    # Construcción del grafo
    # ---------------------------------------------------------------------
    def build_graph(self, articles: List[Dict[str, Any]], infer_if_missing: bool = True,
                    enrich_with_concepts: bool = False, text_field: str = 'abstract',
                    concepts_top_k: int = 15,
                    progress_callback: Optional[Callable[[float, str], None]] = None) -> None:
        """
        Construye el grafo de citaciones.

        - Agrega aristas explícitas con peso 1.0
        - Si infer_if_missing=True, infiere aristas por similitud (título/autores/keywords)
          y crea aristas dirigidas (u->v) para cada u hacia sus v más similares
          (umbral y top_k configurables).
        """
        # Reset
        self.nodes.clear()
        self.adj.clear()

        # Normalizar y registrar nodos
        for a in articles:
            aid = a.get('id')
            if aid is None:
                raise ValueError('Cada artículo debe tener un id único (clave "id").')
            self.nodes[aid] = {
                'title': a.get('title', '') or '',
                'authors': self._normalize_authors(a.get('authors')),
                'keywords': self._normalize_keywords(a.get('keywords')),
                'citations': list(a.get('citations', []) or []),
            }
            self.adj.setdefault(aid, {})

        # Enriquecimiento opcional de keywords con conceptos extraídos del campo de texto
        if enrich_with_concepts:
            try:
                self._enrich_keywords_with_concepts(text_field=text_field, top_k=concepts_top_k)
            except Exception:
                # En caso de fallo, continuar sin enriquecimiento
                pass

        # 1) Aristas explícitas
        for u, data in self.nodes.items():
            for v in data['citations']:
                if v in self.nodes and u != v:
                    self._add_edge(u, v, 1.0)

        # 2) Inferencia por similitud (si procede)
        if infer_if_missing:
            self._infer_edges_by_similarity(progress_callback=progress_callback)

    # ---------------------------------------------------------------------
    # Similitud e inferencia de aristas
    # ---------------------------------------------------------------------
    def _infer_edges_by_similarity(self, progress_callback: Optional[Callable[[float, str], None]] = None) -> None:
        ids = list(self.nodes.keys())
        n = len(ids)
        if n < 2:
            return

        # Prepara corpus de títulos para backends clásicos (si los usamos)
        titles = [self.nodes[i]['title'] for i in ids]

        # Similitud simétrica precomputada para todos los pares (i<j)
        total_pairs = (n * (n - 1)) // 2
        done_pairs = 0
        sim_matrix: List[List[float]] = [[0.0] * n for _ in range(n)]

        # Ruta 1 (rápida): IA con embeddings por lotes para TÍTULOS, si está disponible
        used_batched = False
        if self.similarity_backend == 'ia' and self._sim_ia is not None:
            # Intentar primero usar comparar_multiples para obtener la matriz de similitud por lotes
            comp_multi = getattr(self._sim_ia, 'comparar_multiples', None)
            try:
                if callable(comp_multi):
                    if progress_callback:
                        progress_callback(2.0, 'Calculando matriz IA por lotes (títulos)…')
                    use_sbert = 'sbert' in self.ai_methods
                    use_hf = 'hf' in self.ai_methods
                    # Resultado esperado: Dict[str, Any]; el analizador marcaba errores por tratarlo como object
                    raw_res: Any = comp_multi(
                        titles,
                        usar_sbert=use_sbert,
                        usar_transformer=use_hf,
                        top_k=10  # no crítico; la matriz es lo que usamos
                    )
                    res: Dict[str, Any] = raw_res if isinstance(raw_res, dict) else {}
                    S = None  # matriz combinada de similitud de títulos
                    # ----- SBERT -----
                    if use_sbert:
                        sbert_data = res.get('SBERT')
                        if isinstance(sbert_data, dict):
                            sb_matrix = sbert_data.get('matrix')
                            if isinstance(sb_matrix, list):
                                S = _np.array(sb_matrix, dtype=float) if _np is not None else sb_matrix  # type: ignore[assignment]
                    # ----- HF MeanPooling -----
                    if use_hf:
                        hf_data = res.get('HF-MeanPooling')
                        if isinstance(hf_data, dict):
                            hf_matrix = hf_data.get('matrix')
                            if isinstance(hf_matrix, list):
                                hf_arr = _np.array(hf_matrix, dtype=float) if _np is not None else hf_matrix
                                if S is None:
                                    S = hf_arr
                                else:
                                    # promedio simple entre matrices disponibles
                                    if _np is not None and isinstance(S, _np.ndarray):
                                        S = (S + hf_arr) / 2.0  # type: ignore[operator]
                                    else:
                                        # fallback sin numpy: promedio elemento a elemento
                                        nS = [[0.0]*n for _ in range(n)]
                                        for i in range(n):
                                            for j in range(n):
                                                nS[i][j] = float((S[i][j] + hf_arr[i][j]) / 2.0)  # type: ignore[index]
                                        S = nS
                    if S is not None:
                        # Normalizar a [0,1] con (x+1)/2 y clip
                        if _np is not None and isinstance(S, _np.ndarray):
                            S = _np.clip((S + 1.0) / 2.0, 0.0, 1.0)
                        else:
                            for i in range(n):
                                for j in range(n):
                                    val = float(S[i][j])  # type: ignore[index]
                                    val = (val + 1.0) / 2.0
                                    if val < 0.0: val = 0.0
                                    if val > 1.0: val = 1.0
                                    S[i][j] = val  # type: ignore[index]

                        # Combinar con autores/keywords (Jaccard) usando pesos
                        for i in range(n):
                            ai = self.nodes[ids[i]]
                            for j in range(i + 1, n):
                                aj = self.nodes[ids[j]]
                                s_title = float(S[i][j]) if _np is None or not isinstance(S, _np.ndarray) else float(S[i, j])
                                s_auth = self._jaccard(ai['authors'], aj['authors']) if ai['authors'] or aj['authors'] else 0.0
                                s_kw = self._jaccard(ai['keywords'], aj['keywords']) if ai['keywords'] or aj['keywords'] else 0.0
                                parts: List[Tuple[float, float]] = []
                                if s_title > 0: parts.append((s_title, self.title_weight))
                                if s_auth > 0: parts.append((s_auth, self.authors_weight))
                                if s_kw > 0: parts.append((s_kw, self.keywords_weight))
                                score = 0.0
                                if parts:
                                    num = sum(s*w for s, w in parts)
                                    den = sum(w for _, w in parts)
                                    score = float(num/den) if den > 0 else 0.0
                                sim_matrix[i][j] = score
                                sim_matrix[j][i] = score
                            if progress_callback and (i % max(1, n // 40) == 0 or i == n - 1):
                                pct = 45.0 * ((i + 1) / n)
                                progress_callback(pct, f"Combinando similitudes (título+autores+keywords): {i+1}/{n} nodos")
                        used_batched = True
            except Exception:
                used_batched = False

        # Si no se pudo con comparar_multiples, intentar métodos genéricos de embedding por lotes (si existen)
        if self.similarity_backend == 'ia' and self._sim_ia is not None and _np is not None and not used_batched:
            embed_fn = None
            for name in ('embed_texts', 'encode_texts', 'encode', 'vectorize_texts', 'get_embeddings', 'sentence_embeddings', 'batch_embeddings', 'embed_many'):
                if hasattr(self._sim_ia, name):
                    embed_fn = getattr(self._sim_ia, name)
                    break
            if callable(embed_fn):
                try:
                    if progress_callback:
                        progress_callback(2.0, 'Preparando embeddings IA para títulos…')
                    embs = cast(Any, embed_fn)(titles)
                    E = _np.array(embs, dtype=float)
                    norms = _np.linalg.norm(E, axis=1, keepdims=True) + 1e-12
                    E = E / norms
                    S = E @ E.T
                    S = _np.clip((S + 1.0) / 2.0, 0.0, 1.0)
                    for i in range(n):
                        ai = self.nodes[ids[i]]
                        for j in range(i + 1, n):
                            aj = self.nodes[ids[j]]
                            s_title = float(S[i, j])
                            s_auth = self._jaccard(ai['authors'], aj['authors']) if ai['authors'] or aj['authors'] else 0.0
                            s_kw = self._jaccard(ai['keywords'], aj['keywords']) if ai['keywords'] or aj['keywords'] else 0.0
                            parts: List[Tuple[float, float]] = []
                            if s_title > 0: parts.append((s_title, self.title_weight))
                            if s_auth > 0: parts.append((s_auth, self.authors_weight))
                            if s_kw > 0: parts.append((s_kw, self.keywords_weight))
                            score = 0.0
                            if parts:
                                num = sum(s*w for s, w in parts)
                                den = sum(w for _, w in parts)
                                score = float(num/den) if den > 0 else 0.0
                            sim_matrix[i][j] = score
                            sim_matrix[j][i] = score
                        if progress_callback and (i % max(1, n // 40) == 0 or i == n - 1):
                            pct = 45.0 * ((i + 1) / n)
                            progress_callback(pct, f"Combinando similitudes (título+autores+keywords): {i+1}/{n} nodos")
                    used_batched = True
                except Exception:
                    used_batched = False

        # Ruta 2 (fallback): cálculo par-a-par (IA con timeout o métodos clásicos)
        if not used_batched:
            for i in range(n):
                ui = ids[i]
                for j in range(i + 1, n):
                    vj = ids[j]
                    score = self._combined_similarity(
                        self.nodes[ui], self.nodes[vj],
                        titles_i=titles[i], titles_j=titles[j], all_titles=titles
                    )
                    sim_matrix[i][j] = score
                    sim_matrix[j][i] = score
                    done_pairs += 1
                    if progress_callback and (done_pairs % max(1, total_pairs // 50) == 0 or done_pairs == total_pairs):
                        pct = 50.0 * (done_pairs / max(1, total_pairs))  # 0..50% en la fase de cómputo de similitudes
                        modo = 'IA por pares' if self.similarity_backend == 'ia' else 'métodos clásicos'
                        progress_callback(pct, f"Calculando similitudes ({modo}): {done_pairs}/{total_pairs} pares")

        # Para cada nodo u, seleccionar vecinos v por umbral/top-k
        for i in range(n):
            ui = ids[i]
            sim_list = [(ids[j], sim_matrix[i][j]) for j in range(n) if j != i and sim_matrix[i][j] >= self.infer_threshold]
            sim_list.sort(key=lambda x: x[1], reverse=True)
            if self.infer_top_k is not None:
                sim_list = sim_list[: self.infer_top_k]
            # Añadir aristas dirigidas u->v con el peso de similitud
            for v, w in sim_list:
                if v not in self.adj[ui] or w > self.adj[ui][v]:
                    self._add_edge(ui, v, float(max(0.0, min(1.0, w))))
            if progress_callback and n > 0 and (i % max(1, n // 50) == 0 or i == n - 1):
                # 50..95% para la fase de selección de top-k por nodo
                pct = 50.0 + 45.0 * ((i + 1) / n)
                progress_callback(pct, f"Seleccionando vecinos por similitud: {i+1}/{n} nodos")

    def _combined_similarity(
        self,
        a: Dict[str, Any], b: Dict[str, Any],
        titles_i: str, titles_j: str, all_titles: List[str]
    ) -> float:
        # Título
        s_title = self._title_similarity(titles_i, titles_j, all_titles)
        # Autores (Jaccard sobre conjuntos normalizados)
        s_auth = self._jaccard(a['authors'], b['authors']) if a['authors'] or b['authors'] else 0.0
        # Keywords (Jaccard)
        s_kw = self._jaccard(a['keywords'], b['keywords']) if a['keywords'] or b['keywords'] else 0.0

        # Combinación ponderada (solo promedia sobre los que están presentes)
        parts: List[Tuple[float, float]] = []  # (score, weight)
        if s_title > 0:
            parts.append((s_title, self.title_weight))
        if s_auth > 0:
            parts.append((s_auth, self.authors_weight))
        if s_kw > 0:
            parts.append((s_kw, self.keywords_weight))
        if not parts:
            return 0.0
        num = sum(s * w for s, w in parts)
        den = sum(w for _, w in parts)
        return float(num / den) if den > 0 else 0.0

    # ------------------ backends de similitud ------------------
    def _title_similarity(self, t1: str, t2: str, all_titles: List[str]) -> float:
        t1 = (t1 or '').strip()
        t2 = (t2 or '').strip()
        if not t1 or not t2:
            return 0.0
        scores: List[float] = []

        if self.similarity_backend == 'ia':
            if self._sim_ia is not None:
                # IA: permitir combinar métodos seleccionados
                for m in self.ai_methods:
                    try:
                        fn = None
                        if m == 'sbert' and hasattr(self._sim_ia, 'similitud_sbert'):
                            fn = getattr(self._sim_ia, 'similitud_sbert')
                        elif m == 'hf' and hasattr(self._sim_ia, 'similitud_transformer'):
                            fn = getattr(self._sim_ia, 'similitud_transformer')
                        if fn is None:
                            continue
                        # Ejecutar con timeout opcional para evitar cuelgues
                        sc: Optional[float]
                        if self.ia_timeout_sec and self.ia_timeout_sec > 0:
                            if self._ia_executor is None:
                                self._ia_executor = ThreadPoolExecutor(max_workers=1)
                            fut = self._ia_executor.submit(fn, t1, t2)
                            try:
                                sc = float(fut.result(timeout=self.ia_timeout_sec))
                            except FuturesTimeoutError:
                                # Cancelar y continuar con otros métodos/backups
                                try:
                                    fut.cancel()
                                except Exception:
                                    pass
                                sc = None
                        else:
                            sc = float(fn(t1, t2))
                        if sc is not None and not math.isnan(sc):
                            # normalizar a [0,1] si hiciera falta
                            scores.append(float((sc + 1.0) / 2.0) if sc < 0 else float(sc))
                    except Exception:
                        continue
        else:
            # Clásico: mezclar múltiples métodos si se seleccionaron (del módulo SimilitudTextualClasico)
            for m in self.classic_methods:
                try:
                    if m == 'coseno' and self._sim_classic is not None and hasattr(self._sim_classic, 'similitud_coseno'):
                        sc = float(self._sim_classic.similitud_coseno(t1, t2))  # type: ignore[attr-defined]
                        scores.append(max(0.0, min(1.0, sc)))
                    elif m == 'tfidf' and self._sim_classic is not None and hasattr(self._sim_classic, 'similitud_tfidf'):
                        sc = float(self._sim_classic.similitud_tfidf(t1, t2, corpus=all_titles))  # type: ignore[attr-defined]
                        scores.append(max(0.0, min(1.0, sc)))
                    elif m == 'levenshtein' and self._sim_classic is not None and hasattr(self._sim_classic, 'distancia_levenshtein'):
                        sc = float(self._sim_classic.distancia_levenshtein(t1, t2))  # type: ignore[attr-defined]
                        scores.append(max(0.0, min(1.0, sc)))
                    elif m == 'jarowinkler' and self._sim_classic is not None and hasattr(self._sim_classic, 'distancia_jaro_winkler'):
                        sc = float(self._sim_classic.distancia_jaro_winkler(t1, t2))  # type: ignore[attr-defined]
                        scores.append(max(0.0, min(1.0, sc)))
                except Exception:
                    continue

        if scores:
            # promedio simple de métodos habilitados
            return float(max(0.0, min(1.0, sum(scores) / len(scores))))

        # Fallback: jaccard de tokens
        return self._jaccard(self._tokenize(t1), self._tokenize(t2))

    # ---------------------------------------------------------------------
    # Algoritmos de caminos mínimos
    # ---------------------------------------------------------------------
    def _edge_cost(self, w: float) -> float:
        w = max(0.0, min(1.0, float(w)))
        if self.cost_mode == 'linear':
            return 1.0 - w
        if self.cost_mode == 'unit':
            return 1.0
        # inverse por defecto
        return 1.0 / (w + 1e-6)

    def shortest_path_dijkstra(self, source: Any, target: Any) -> Tuple[float, List[Any]]:
        """Dijkstra desde source hasta target. Retorna (costo, camino)."""
        if source not in self.nodes or target not in self.nodes:
            raise KeyError('source o target no se encuentran en el grafo.')

        dist: Dict[Any, float] = {node: math.inf for node in self.nodes}
        prev: Dict[Any, Optional[Any]] = {node: None for node in self.nodes}
        dist[source] = 0.0
        pq: List[Tuple[float, Any]] = [(0.0, source)]

        while pq:
            d, u = heapq.heappop(pq)
            if d != dist[u]:
                continue
            if u == target:
                break
            for v, w in self.adj.get(u, {}).items():
                cost = self._edge_cost(w)
                nd = d + cost
                if nd < dist[v]:
                    dist[v] = nd
                    prev[v] = u
                    heapq.heappush(pq, (nd, v))

        # Reconstruir camino
        path: List[Any] = []
        if dist[target] != math.inf:
            cur: Optional[Any] = target
            while cur is not None:
                path.append(cur)
                cur = prev[cur]
            path.reverse()
        return dist[target], path

    def dijkstra_from(self, source: Any) -> Tuple[Dict[Any, float], Dict[Any, Optional[Any]]]:
        """Dijkstra single-source: devuelve distancias y predecesores desde `source` a todos los nodos."""
        if source not in self.nodes:
            raise KeyError('source no se encuentra en el grafo.')
        dist: Dict[Any, float] = {node: math.inf for node in self.nodes}
        prev: Dict[Any, Optional[Any]] = {node: None for node in self.nodes}
        dist[source] = 0.0
        pq: List[Tuple[float, Any]] = [(0.0, source)]
        while pq:
            d, u = heapq.heappop(pq)
            if d != dist[u]:
                continue
            for v, w in self.adj.get(u, {}).items():
                cost = self._edge_cost(w)
                nd = d + cost
                if nd < dist[v]:
                    dist[v] = nd
                    prev[v] = u
                    heapq.heappush(pq, (nd, v))
        return dist, prev

    @staticmethod
    def _reconstruct_path(prev: Dict[Any, Optional[Any]], src: Any, tgt: Any) -> List[Any]:
        path: List[Any] = []
        cur: Optional[Any] = tgt
        seen = set()
        while cur is not None and cur not in seen:
            seen.add(cur)
            path.append(cur)
            if cur == src:
                break
            cur = prev.get(cur)
        path.reverse()
        return path

    def top_k_shortest_paths_global(self, k: int = 10) -> List[Dict[str, Any]]:
        """
        Calcula los k caminos mínimos globales (de menor costo) considerando todos los pares (u,v), u!=v.
        Devuelve una lista ordenada ascendentemente por costo con elementos:
        { 'source': str, 'target': str, 'cost': float, 'path': [ids] }
        """
        # Max-heap basado en costo negativo
        heap: List[Tuple[float, Tuple[float, Any, Any, List[Any]]]] = []
        ids = list(self.nodes.keys())
        for src in ids:
            dist, prev = self.dijkstra_from(src)
            for tgt, cost in dist.items():
                if tgt == src or not (cost < math.inf):
                    continue
                path = self._reconstruct_path(prev, src, tgt)
                if len(path) < 2:
                    continue
                entry = (float(cost), src, tgt, path)
                if len(heap) < k:
                    heapq.heappush(heap, (-entry[0], entry))
                else:
                    worst_cost_neg, _ = heap[0]
                    if entry[0] < -worst_cost_neg:
                        heapq.heapreplace(heap, (-entry[0], entry))
        out: List[Tuple[float, Any, Any, List[Any]]] = []
        while heap:
            _, e = heapq.heappop(heap)
            out.append(e)
        out.sort(key=lambda x: x[0])
        return [
            {'source': str(src), 'target': str(tgt), 'cost': float(cost), 'path': [str(x) for x in path]}
            for (cost, src, tgt, path) in out
        ]

    def top_sccs(self, k: int = 10) -> List[Dict[str, Any]]:
        """Top de componentes fuertemente conexas por tamaño."""
        sccs = self.strongly_connected_components()
        sccs_sorted = sorted(sccs, key=lambda comp: len(comp), reverse=True)
        top = sccs_sorted[:max(0, k)]
        return [
            {'size': len(comp), 'nodes': [str(n) for n in comp]}
            for comp in top
        ]

    def all_pairs_floyd_warshall(self) -> Dict[Any, Dict[Any, float]]:
        """Floyd–Warshall para todos los pares. Devuelve dict[source][target] = costo."""
        ids = list(self.nodes.keys())
        idx: Dict[Any, int] = {nid: i for i, nid in enumerate(ids)}
        n = len(ids)
        # Inicialización
        dist = [[math.inf] * n for _ in range(n)]
        for i in range(n):
            dist[i][i] = 0.0
        for u, nbrs in self.adj.items():
            i = idx[u]
            for v, w in nbrs.items():
                j = idx[v]
                dist[i][j] = min(dist[i][j], self._edge_cost(w))
        # Relajación
        for k in range(n):
            dk = dist[k]
            for i in range(n):
                dik = dist[i][k]
                if dik == math.inf:
                    continue
                di = dist[i]
                for j in range(n):
                    alt = dik + dk[j]
                    if alt < di[j]:
                        di[j] = alt
        # Convertir a dict de dicts
        out: Dict[Any, Dict[Any, float]] = {u: {} for u in ids}
        for i, u in enumerate(ids):
            for j, v in enumerate(ids):
                out[u][v] = dist[i][j]
        return out

    # ---------------------------------------------------------------------
    # Componentes fuertemente conexas (SCC) - Kosaraju
    # ---------------------------------------------------------------------
    def strongly_connected_components(self) -> List[List[Any]]:
        """Devuelve las SCCs como listas de nodos usando el algoritmo de Kosaraju."""
        visited: Dict[Any, bool] = {u: False for u in self.nodes}
        order: List[Any] = []

        def dfs1(u: Any) -> None:
            visited[u] = True
            for v in self.adj.get(u, {}):
                if not visited[v]:
                    dfs1(v)
            order.append(u)

        for u in self.nodes:
            if not visited[u]:
                dfs1(u)

        # Grafo transpuesto
        rev: Dict[Any, List[Any]] = {u: [] for u in self.nodes}
        for u, nbrs in self.adj.items():
            for v in nbrs.keys():
                rev[v].append(u)

        visited = {u: False for u in self.nodes}
        sccs: List[List[Any]] = []

        def dfs2(u: Any, acc: List[Any]) -> None:
            visited[u] = True
            acc.append(u)
            for v in rev.get(u, []):
                if not visited[v]:
                    dfs2(v, acc)

        for u in reversed(order):
            if not visited[u]:
                comp: List[Any] = []
                dfs2(u, comp)
                sccs.append(comp)

        return sccs

    # ---------------------------------------------------------------------
    # Utilidades y helpers
    # ---------------------------------------------------------------------
    def _enrich_keywords_with_concepts(self, text_field: str = 'abstract', top_k: int = 15) -> None:
        """
        Usa ConceptsCategoryAnalyzer para extraer términos relevantes a partir de un
        campo de texto (p. ej., 'abstract' o 'title') y los agrega a las keywords
        de cada artículo donde aparezcan.

        Nota: Requiere que ConceptsCategoryAnalyzer esté disponible. Si no lo está,
        la función no hace nada.
        """
        if not self._concepts:
            return
        # Recolectar textos
        texts: List[str] = []
        for nid, data in self.nodes.items():
            txt = str(data.get(text_field, '') or '')
            if not txt:
                # fallback: título
                txt = str(data.get('title', '') or '')
            texts.append(txt)

        if len(texts) < 2:
            return

        # Ejecutar análisis para obtener términos generados
        try:
            report = self._concepts.analyze(texts, top_k=top_k)
            gen_terms = [g['term'] for g in report.get('generated_terms', [])]
        except Exception:
            gen_terms = []

        if not gen_terms:
            return

        # Compilar regex por término usando lógica del analizador
        compile_rx = getattr(self._concepts, '_compile_phrase_regex', None)
        if not callable(compile_rx):
            return
        compiled: List[Tuple[str, re.Pattern]] = [(t, compile_rx(t)) for t in gen_terms]  # type: ignore[assignment]

        # Para cada documento, si contiene el término, añadir a keywords
        for idx, (nid, data) in enumerate(self.nodes.items()):
            txt = (str(data.get(text_field, '') or '')).lower()
            if not txt:
                txt = (str(data.get('title', '') or '')).lower()
            add_terms: List[str] = []
            for term, rx in compiled:
                if rx.search(txt):
                    add_terms.append(term)
            if add_terms:
                merged = set(data.get('keywords', []) or []) | {t.lower() for t in add_terms}
                data['keywords'] = sorted(list(merged))

    def _add_edge(self, u: Any, v: Any, w: float) -> None:
        self.adj.setdefault(u, {})
        self.adj[u][v] = float(w)

    @staticmethod
    def _tokenize(text: str) -> List[str]:
        # Separar por no alfanuméricos, minúsculas
        out: List[str] = []
        cur: List[str] = []
        for ch in (text or '').lower():
            if ch.isalnum():
                cur.append(ch)
            else:
                if cur:
                    out.append(''.join(cur))
                    cur = []
        if cur:
            out.append(''.join(cur))
        return [t for t in out if t]

    @staticmethod
    def _normalize_authors(auth: Any) -> List[str]:
        if auth is None:
            return []
        if isinstance(auth, str):
            items = [a.strip() for a in auth.split(',')]
        elif isinstance(auth, (list, tuple, set)):
            items = list(auth)
        else:
            return []
        norm = []
        for a in items:
            s = str(a).strip().lower()
            if s:
                norm.append(s)
        return sorted(list(set(norm)))

    @staticmethod
    def _normalize_keywords(kws: Any) -> List[str]:
        if kws is None:
            return []
        if isinstance(kws, str):
            items = [k.strip() for k in kws.replace(';', ',').split(',')]
        elif isinstance(kws, (list, tuple, set)):
            items = list(kws)
        else:
            return []
        norm = []
        for k in items:
            s = str(k).strip().lower()
            if s:
                norm.append(s)
        return sorted(list(set(norm)))

    @staticmethod
    def _jaccard(a: Iterable[str] | List[str], b: Iterable[str] | List[str]) -> float:
        sa = set(a)
        sb = set(b)
        if not sa and not sb:
            return 0.0
        inter = len(sa & sb)
        union = len(sa | sb)
        return float(inter / union) if union > 0 else 0.0

    @staticmethod
    def _dice(a: Iterable[str] | List[str], b: Iterable[str] | List[str]) -> float:
        sa = set(a)
        sb = set(b)
        if not sa and not sb:
            return 0.0
        inter = len(sa & sb)
        return float((2.0 * inter) / (len(sa) + len(sb))) if (len(sa) + len(sb)) > 0 else 0.0

    @staticmethod
    def _overlap(a: Iterable[str] | List[str], b: Iterable[str] | List[str]) -> float:
        sa = set(a)
        sb = set(b)
        if not sa and not sb:
            return 0.0
        inter = len(sa & sb)
        m = min(len(sa), len(sb))
        return float(inter / m) if m > 0 else 0.0

    # Export util
    def edges(self) -> List[Tuple[Any, Any, float]]:
        """Devuelve la lista de aristas (u, v, w)."""
        out: List[Tuple[Any, Any, float]] = []
        for u, nbrs in self.adj.items():
            for v, w in nbrs.items():
                out.append((u, v, w))
        return out

    def neighbors(self, u: Any) -> Dict[Any, float]:
        return dict(self.adj.get(u, {}))

    def has_edge(self, u: Any, v: Any) -> bool:
        return v in self.adj.get(u, {})

    # ---------------------------------------------------------------------
    # Integración con HierarchicalClusteringAnalyzer (opcional)
    # ---------------------------------------------------------------------
    def hierarchical_clusters(self, text_field: str = 'title', algorithms: List[str] | None = None,
                              labels: Optional[List[str]] = None, max_docs: int = 150,
                              output_dir: Optional[str] = None, base_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Ejecuta clustering jerárquico sobre un campo de texto (título/abstract) de los artículos
        usando HierarchicalClusteringAnalyzer si está disponible. Devuelve el dict de resultados
        del analizador (incluyendo CCC y dendrogramas en base64 si SciPy está presente).
        """
        if not self._hcluster:
            return {'error': 'HierarchicalClusteringAnalyzer no está disponible.'}
        texts: List[str] = []
        ids: List[str] = []
        for nid, data in self.nodes.items():
            ids.append(str(nid))
            t = str(data.get(text_field, '') or '')
            if not t:
                t = str(data.get('title', '') or '')
            texts.append(t)
        use_labels = labels if labels is not None else ids
        return self._hcluster.analyze(texts, algorithms=algorithms, labels=use_labels,
                                      max_docs=max_docs, output_dir=output_dir, base_name=base_name)
