import networkx as nx
from typing import Dict, List, Tuple, Any


class Seguimiento2Req1:
    """
    Clase para construir y analizar una red de citaciones entre artículos científicos.
    Cumple los requerimientos:
    1. Construcción automática del grafo de citaciones.
    2. Cálculo de caminos mínimos (Dijkstra o Floyd–Warshall).
    3. Identificación de componentes fuertemente conexas.
    """

    def __init__(self):
        # Grafo dirigido con pesos
        self.grafo = nx.DiGraph()

    # ==========================================================
    # 1️⃣ CONSTRUCCIÓN DEL GRAFO
    # ==========================================================
    def construir_red(self, articulos: List[Dict[str, Any]]):
        """
        Construye el grafo de citaciones.
        Cada artículo debe incluir: id, titulo, autores, palabras_clave, y opcionalmente citaciones (lista de IDs citados).
        """
        # Agregar nodos
        for art in articulos:
            self.grafo.add_node(
                art["id"],
                titulo=art.get("titulo", ""),
                autores=art.get("autores", []),
                palabras_clave=art.get("palabras_clave", [])
            )

        # Agregar aristas
        for art1 in articulos:
            id1 = art1["id"]

            # Si tiene citas explícitas, se priorizan
            if "citaciones" in art1 and art1["citaciones"]:
                for citado in art1["citaciones"]:
                    if citado != id1:
                        self.grafo.add_edge(id1, citado, weight=1.0)
                continue

            # Caso contrario, inferir por similitud
            for art2 in articulos:
                id2 = art2["id"]
                if id1 == id2:
                    continue
                peso = self._calcular_similitud(art1, art2)
                if peso >= 0.3:  # umbral mínimo para considerar relación
                    self.grafo.add_edge(id1, id2, weight=round(peso, 3))

    def _calcular_similitud(self, art1: Dict, art2: Dict) -> float:
        """
        Calcula similitud entre dos artículos basada en coincidencia de palabras clave, autores y título.
        """
        similitud = 0.0

        # Palabras clave
        kw1 = set(map(str.lower, art1.get("palabras_clave", [])))
        kw2 = set(map(str.lower, art2.get("palabras_clave", [])))
        if kw1 and kw2:
            similitud += len(kw1 & kw2) / max(len(kw1), len(kw2))

        # Autores
        aut1 = set(map(str.lower, art1.get("autores", [])))
        aut2 = set(map(str.lower, art2.get("autores", [])))
        if aut1 and aut2:
            similitud += len(aut1 & aut2) / max(len(aut1), len(aut2))

        # Coincidencia parcial de título
        titulo1 = art1.get("titulo", "").lower()
        titulo2 = art2.get("titulo", "").lower()
        if any(word in titulo2 for word in titulo1.split()[:2]):
            similitud += 0.3

        return min(similitud, 1.0)

    # ==========================================================
    # 2️⃣ CAMINOS MÍNIMOS
    # ==========================================================
    def camino_minimo(self, origen: str, destino: str) -> Tuple[List[str], float]:
        """
        Calcula el camino mínimo entre dos artículos con Dijkstra.
        """
        try:
            ruta = nx.dijkstra_path(self.grafo, origen, destino, weight="weight")
            distancia = nx.dijkstra_path_length(self.grafo, origen, destino, weight="weight")
            return ruta, distancia
        except nx.NetworkXNoPath:
            return [], float("inf")

    def matriz_caminos_minimos(self) -> Dict[str, Dict[str, float]]:
        """
        Calcula la matriz de distancias mínimas entre todos los nodos (Floyd–Warshall).
        """
        return dict(nx.floyd_warshall(self.grafo, weight="weight"))

    # ==========================================================
    # 3️⃣ COMPONENTES FUERTEMENTE CONEXAS
    # ==========================================================
    def componentes_fuertemente_conexas(self) -> List[List[str]]:
        """
        Retorna las componentes fuertemente conexas (listas de IDs).
        """
        return [list(c) for c in nx.strongly_connected_components(self.grafo)]

    def subgrafos_componentes(self) -> List[nx.DiGraph]:
        """
        Devuelve los subgrafos de cada componente conexa.
        """
        return [self.grafo.subgraph(c).copy() for c in nx.strongly_connected_components(self.grafo)]

    # ==========================================================
    # RESUMEN
    # ==========================================================
    def resumen(self) -> Dict[str, Any]:
        """
        Retorna un resumen general del grafo.
        """
        return {
            "n_nodos": self.grafo.number_of_nodes(),
            "n_aristas": self.grafo.number_of_edges(),
            "n_componentes": len(list(nx.strongly_connected_components(self.grafo)))
        }
