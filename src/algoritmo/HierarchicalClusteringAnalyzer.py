"""
Hierarchical Clustering Analyzer
================================

Implementa 3 algoritmos de clustering jerárquico (single, complete, average)
para construir dendrogramas sobre abstracts científicos. Incluye:
- Preprocesamiento con TF-IDF (unigramas/bigramas)
- Similitud coseno y distancia (1 - coseno)
- Enlaces (linkage) con SciPy y dendrogramas en base64
- Métrica de coherencia: Cophenetic Correlation Coefficient (CCC)

Dependencias: numpy, scikit-learn, scipy, matplotlib
"""
from __future__ import annotations
from typing import List, Dict, Any, Optional
import io
import base64
import re

import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

try:
    from scipy.cluster.hierarchy import linkage as _hc_linkage, dendrogram as _hc_dendrogram, cophenet as _hc_cophenet
    from scipy.spatial.distance import squareform as _sp_squareform
    _SCIPY_OK = True
except Exception:
    _SCIPY_OK = False

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


class HierarchicalClusteringAnalyzer:
    """Clustering jerárquico con varios métodos de enlace y dendrogramas.

    Uso:
        analyzer = HierarchicalClusteringAnalyzer()
        res = analyzer.analyze(abstracts, algorithms=["single","complete","average"], max_docs=120)
        # res: {
        #   'vectors_shape': (n_docs, n_terms),
        #   'algorithms': [ ... ],
        #   'results': {
        #       'single': {'ccc': float, 'dendrogram_png_base64': str}, ...
        #   },
        #   'best': {'algorithm': str, 'ccc': float}
        # }
    """

    def __init__(self, language_stopwords: str | None = 'english'):
        self.language_stopwords = language_stopwords

    @staticmethod
    def _normalize_text(text: str) -> str:
        if not isinstance(text, str):
            text = '' if text is None else str(text)
        t = text.lower()
        t = re.sub(r"[\r\n\t]", " ", t)
        t = re.sub(r"\s+", " ", t).strip()
        return t

    def _vectorize(self, abstracts: List[str]):
        texts = [self._normalize_text(a) for a in abstracts]
        vec = TfidfVectorizer(
            lowercase=True,
            stop_words=self.language_stopwords,
            token_pattern=r'(?u)\b[\w-]{3,}\b',
            ngram_range=(1, 2),
            max_df=0.9,
            min_df=2
        )
        X = vec.fit_transform(texts)
        return X

    @staticmethod
    def _cosine_distance_matrix(X) -> np.ndarray:
        # cos_sim es (n x n) en [0,1]; distancia = 1 - sim
        cos_sim = cosine_similarity(X)
        D = 1.0 - cos_sim
        # Asegurar simetría y ceros en diagonal
        np.fill_diagonal(D, 0.0)
        return D

    @staticmethod
    def _plot_dendrogram(linkage_matrix, labels: Optional[List[str]] = None, title: str = '', save_path: Optional[str] = None) -> str:
        fig = plt.figure(figsize=(10, 5), dpi=130)
        try:
            _hc_dendrogram(linkage_matrix, labels=labels, leaf_rotation=90)  # type: ignore
            plt.title(title)
            plt.tight_layout()
            buf = io.BytesIO()
            fig.savefig(buf, format='png', bbox_inches='tight')
            # Guardar a disco si se indica ruta
            if save_path:
                try:
                    fig.savefig(save_path, format='png', bbox_inches='tight')
                except Exception:
                    pass
            plt.close(fig)
            buf.seek(0)
            return base64.b64encode(buf.getvalue()).decode('utf-8')
        except Exception:
            plt.close(fig)
            raise

    def analyze(self, abstracts: List[str], algorithms: List[str] | None = None,
                labels: Optional[List[str]] = None, max_docs: int = 150,
                output_dir: Optional[str] = None, base_name: Optional[str] = None) -> Dict[str, Any]:
        if algorithms is None:
            algorithms = ["single", "complete", "average"]

        # Truncar por rendimiento si es necesario
        docs = abstracts[:max_docs] if (max_docs and len(abstracts) > max_docs) else abstracts
        if labels is not None and len(labels) == len(abstracts):
            use_labels = labels[:len(docs)]
        else:
            use_labels = None

        X = self._vectorize(docs)
        if X.shape[0] < 2:
            return { 'error': 'Se requieren al menos 2 documentos para agrupar.' }

        D = self._cosine_distance_matrix(X)

        results: Dict[str, Any] = {}
        best_algo = None
        best_ccc = -1.0

        if not _SCIPY_OK:
            return { 'error': 'SciPy no está disponible para clustering jerárquico.' }

        # Convertir la matriz de distancias cuadrada a vector (condensed) para SciPy
        condensed = _sp_squareform(D, checks=False)  # type: ignore

        for algo in algorithms:
            try:
                Z = _hc_linkage(condensed, method=algo)  # type: ignore
                ccc, _ = _hc_cophenet(Z, condensed)  # type: ignore
                save_path = None
                if output_dir:
                    import os
                    os.makedirs(output_dir, exist_ok=True)
                    safe_base = (base_name or 'dendrogram').strip().replace(' ', '_')
                    save_path = os.path.join(output_dir, f"{safe_base}_{algo}.png")
                img_b64 = self._plot_dendrogram(Z, labels=use_labels, title=f"Dendrograma - {algo}", save_path=save_path)
                results[algo] = {
                    'ccc': float(ccc),
                    'dendrogram_base64': img_b64,
                    'file_path': save_path
                }
                if ccc > best_ccc:
                    best_ccc = ccc
                    best_algo = algo
            except Exception as e:
                results[algo] = { 'error': str(e) }

        return {
            'vectors_shape': (X.shape[0], X.shape[1]),
            'algorithms': algorithms,
            'results': results,
            'best': { 'algorithm': best_algo, 'ccc': float(best_ccc) if best_algo else None }
        }
