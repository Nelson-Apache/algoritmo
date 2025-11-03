import networkx as nx
from typing import Dict, List, Tuple


class Seguimiento2Req1:
    """
    Clase para construir y analizar una red de citaciones entre artículos científicos.
    """

    def __init__(self):
        # Grafo dirigido para representar las citaciones
        self.grafo = nx.DiGraph()

    def construir_red(self, articulos: List[Dict[str, any]]):
        """
        Construye la red de citaciones a partir de una lista de artículos.
        Cada artículo debe tener al menos: 'id', 'titulo', 'autores', 'palabras_clave', y opcionalmente 'citaciones'.

        Si no hay datos explícitos de citaciones, las relaciones se infieren por similitud de títulos, autores o palabras clave.
        """
        for art in articulos:
            self.grafo.add_node(art["id"], titulo=art["titulo"], autores=art["autores"], palabras_clave=art["palabras_clave"])

        for art1 in articulos:
            for art2 in articulos:
                if art1["id"] != art2["id"]:
                    peso = self._calcular_similitud(art1, art2)
                    if peso > 0:
                        self.grafo.add_edge(art1["id"], art2["id"], weight=peso)

    def _calcular_similitud(self, art1: Dict, art2: Dict) -> float:
        """
        Calcula una similitud simple entre dos artículos basada en coincidencia de palabras clave, autores y título.
        Retorna un valor entre 0 y 1.
        """
        similitud = 0

        # Coincidencia de palabras clave
        set_kw1 = set(art1.get("palabras_clave", []))
        set_kw2 = set(art2.get("palabras_clave", []))
        if set_kw1 and set_kw2:
            similitud += len(set_kw1.intersection(set_kw2)) / max(len(set_kw1), len(set_kw2))

        # Coincidencia de autores
        set_aut1 = set(art1.get("autores", []))
        set_aut2 = set(art2.get("autores", []))
        if set_aut1 and set_aut2:
            similitud += len(set_aut1.intersection(set_aut2)) / max(len(set_aut1), len(set_aut2))

        # Coincidencia parcial en título
        if art1["titulo"].split()[0] in art2["titulo"] or art2["titulo"].split()[0] in art1["titulo"]:
            similitud += 0.3

        # Normalización simple
        return min(similitud, 1.0)

    def caminos_minimos(self, origen: str, destino: str) -> Tuple[List[str], float]:
        """
        Calcula el camino mínimo entre dos artículos usando Dijkstra.
        Retorna la ruta y la distancia total.
        """
        try:
            ruta = nx.dijkstra_path(self.grafo, origen, destino, weight="weight")
            distancia = nx.dijkstra_path_length(self.grafo, origen, destino, weight="weight")
            return ruta, distancia
        except nx.NetworkXNoPath:
            return [], float("inf")

    def componentes_fuertemente_conexas(self) -> List[List[str]]:
        """
        Retorna las componentes fuertemente conexas del grafo.
        """
        return [list(c) for c in nx.strongly_connected_components(self.grafo)]

    def resumen_grafo(self):
        """
        Muestra información general del grafo.
        """
        print(f"Nodos: {self.grafo.number_of_nodes()}")
        print(f"Aristas: {self.grafo.number_of_edges()}")
        print(f"Componentes fuertemente conexas: {len(self.componentes_fuertemente_conexas())}")
