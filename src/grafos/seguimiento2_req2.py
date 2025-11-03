# src/grafos/seguimiento2_req2.py
import re
import itertools
from typing import List, Dict, Any, Tuple
import networkx as nx
import matplotlib.pyplot as plt


def _normalize_text(s: str) -> str:
    s = s.lower()
    # espacios compactados
    s = re.sub(r'\s+', ' ', s).strip()
    return s


class Seguimiento2Req2:
    """
    Requerimiento 2:
    1) Construcción automática del grafo de coocurrencia (no dirigido)
    2) Cálculo del grado de cada nodo (unweighted degree) y fuerza (suma de pesos)
    3) Detección de componentes conexas (posibles temas)
    """
    def __init__(self, min_cooc: int = 1, max_terms_per_doc: int = 60):
        """
        :param min_cooc: umbral mínimo de coocurrencia para crear/retener una arista
        :param max_terms_per_doc: para evitar explosión combinatoria en docs muy largos
        """
        self.min_cooc = int(min_cooc)
        self.max_terms_per_doc = int(max_terms_per_doc)
        self.G = nx.Graph()
        self._vocab_set = set()

    # ------------------------------------------------------------------
    # 1) Construcción automática del grafo de coocurrencia
    # ------------------------------------------------------------------
    def build_from_documents(self, abstracts: List[str], candidate_terms: List[str]) -> None:
        """
        Construye el grafo a partir de abstracts y un vocabulario de términos candidatos.
        - Detecta en cada documento qué términos aparecen (con límites y normalización).
        - Suma pesos de coocurrencia por documento (cada par que co-aparece aumenta +1).
        """
        # Normalizar términos y ordenarlos por longitud desc (mejor detección de frases)
        norm_terms = []
        seen = set()
        for t in candidate_terms:
            if not t:
                continue
            nt = _normalize_text(str(t))
            if nt and nt not in seen:
                seen.add(nt)
                norm_terms.append(nt)
        norm_terms.sort(key=len, reverse=True)
        self._vocab_set = set(norm_terms)

        # Inicializar nodos
        for t in norm_terms:
            self.G.add_node(t)

        # Precompilar regex para cada término (respeta límites de palabra si aplica)
        # Si es multi-palabra, usamos búsqueda por substring "suave"; si es una palabra, usamos \b...\b
        term_patterns: Dict[str, re.Pattern] = {}
        for term in norm_terms:
            if ' ' in term:
                pat = re.compile(re.escape(term))
            else:
                pat = re.compile(r'\b' + re.escape(term) + r'\b')
            term_patterns[term] = pat

        # Recorrer documentos
        for raw in abstracts:
            txt = _normalize_text(str(raw or ''))
            if not txt:
                continue

            # Términos presentes en este doc
            present: List[str] = []
            for term in norm_terms:
                if term_patterns[term].search(txt):
                    present.append(term)
                if len(present) >= self.max_terms_per_doc:
                    break

            if len(present) < 2:
                continue

            # Sumar coocurrencias para todas las combinaciones del documento
            for a, b in itertools.combinations(sorted(set(present)), 2):
                if self.G.has_edge(a, b):
                    self.G[a][b]['weight'] += 1
                else:
                    self.G.add_edge(a, b, weight=1)

        # Aplicar umbral mínimo
        to_remove = []
        for u, v, data in self.G.edges(data=True):
            if data.get('weight', 0) < self.min_cooc:
                to_remove.append((u, v))
        self.G.remove_edges_from(to_remove)

        # Eliminar nodos aislados
        isolated = list(nx.isolates(self.G))
        self.G.remove_nodes_from(isolated)

    # ------------------------------------------------------------------
    # 2) Grados / fuerza
    # ------------------------------------------------------------------
    def node_degrees(self) -> Dict[str, int]:
        """Grado (unweighted) por nodo."""
        return dict(self.G.degree())

    def node_strength(self) -> Dict[str, float]:
        """Fuerza (suma de pesos de incidentes) por nodo."""
        return dict(self.G.degree(weight='weight'))

    # ------------------------------------------------------------------
    # 3) Componentes conexas
    # ------------------------------------------------------------------
    def connected_components(self) -> List[List[str]]:
        comps = []
        for comp in nx.connected_components(self.G):
            comps.append(sorted(list(comp)))
        # ordenar por tamaño desc
        comps.sort(key=len, reverse=True)
        return comps

    # ------------------------------------------------------------------
    # Resumen y dibujo
    # ------------------------------------------------------------------
    def summary(self, top_k: int = 10) -> Dict[str, Any]:
        deg = self.node_degrees()
        strg = self.node_strength()
        top_deg = sorted(deg.items(), key=lambda x: x[1], reverse=True)[:top_k]
        top_str = sorted(strg.items(), key=lambda x: x[1], reverse=True)[:top_k]
        comps = self.connected_components()
        return {
            'n_nodes': self.G.number_of_nodes(),
            'n_edges': self.G.number_of_edges(),
            'top_degree': top_deg,
            'top_strength': top_str,
            'n_components': len(comps),
            'components': comps[:10],  # primeras 10 por tamaño
        }

    def draw(self, path_png: str, with_labels: bool = True) -> None:
        if self.G.number_of_nodes() == 0:
            # generar un lienzo vacío legible
            plt.figure(figsize=(8, 6))
            plt.text(0.5, 0.5, "Grafo vacío", ha='center', va='center')
            plt.axis('off')
            plt.tight_layout()
            plt.savefig(path_png, dpi=140, bbox_inches='tight')
            plt.close()
            return

        pos = nx.spring_layout(self.G, seed=42)
        plt.figure(figsize=(10, 8))
        weights = [self.G[u][v]['weight'] for u, v in self.G.edges()]
        nx.draw_networkx_nodes(self.G, pos, node_size=600)
        nx.draw_networkx_edges(self.G, pos, width=[0.6 + 0.6*w for w in weights])
        if with_labels:
            nx.draw_networkx_labels(self.G, pos, font_size=8)
        plt.title("Grafo de Coocurrencia de Términos")
        plt.tight_layout()
        plt.savefig(path_png, dpi=160, bbox_inches='tight')
        plt.close()
