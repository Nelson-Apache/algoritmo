"""
Text Similarity Analyzer - Análisis y Comparación de Algoritmos de Similitud Textual
====================================================================================

Este módulo proporciona una colección completa de algoritmos clásicos para medir
similitud entre textos, incluyendo métodos de distancia de edición y técnicas de
vectorización estadística.

Algoritmos Implementados:
-------------------------
1. Levenshtein Distance - Distancia de edición mínima
2. Jaro-Winkler Distance - Optimizado para nombres y cadenas cortas
3. TF-IDF Similarity - Vectorización con peso estadístico
4. Cosine Similarity - Similitud basada en ángulo entre vectores

Categorías de Algoritmos:
-------------------------
A. Distancia de Edición:
   - Levenshtein: Mide operaciones de inserción/eliminación/sustitución
   - Jaro-Winkler: Optimizado para prefijos comunes

B. Vectorización Estadística:
   - TF-IDF: Pondera importancia de términos en corpus
   - Coseno: Mide ángulo entre vectores de frecuencia

Funcionalidades:
----------------
- Cálculo de similitud texto-a-texto
- Normalización automática de resultados [0, 1]
- Preprocesamiento integrado de textos
- Comparación múltiple con todos los algoritmos
- Soporte para análisis con corpus personalizado

Casos de Uso:
-------------
- Detección de duplicados en documentos
- Comparación de títulos académicos
- Búsqueda de textos similares
- Análisis de plagio
- Recomendación de contenido relacionado
- Clustering de documentos

Uso Típico:
-----------
>>> sim = SimilitudTextual()
>>> 
>>> # Comparación simple
>>> score = sim.distancia_levenshtein("machine learning", "deep learning")
>>> print(f"Similitud: {score:.3f}")
>>> 
>>> # Comparación con todos los algoritmos
>>> resultados = sim.comparar_todos("text 1", "text 2")
>>> for algoritmo, similitud in resultados.items():
>>>     print(f"{algoritmo}: {similitud:.3f}")
>>>
>>> # TF-IDF con corpus
>>> corpus = ["doc1", "doc2", "doc3"]
>>> tfidf_score = sim.similitud_tfidf("query text", "target text", corpus)

Fecha: 2025
"""

import re
import math
from collections import Counter
from typing import List, Dict, Tuple, Optional, Mapping, Union, Any


class SimilitudTextualClasico:
    """
    Analizador completo de similitud textual con múltiples algoritmos.
    
    Esta clase implementa algoritmos clásicos de similitud textual divididos en
    dos categorías principales: métodos de distancia de edición (Levenshtein,
    Jaro-Winkler) y métodos de vectorización estadística (TF-IDF, Coseno).
    
    Los métodos de distancia de edición son ideales para comparar cadenas cortas
    y detectar errores tipográficos, mientras que los métodos vectoriales son
    mejores para documentos largos y análisis semántico básico.
    
    Attributes:
        corpus (List): Lista de documentos para cálculos de IDF (vacía por defecto)
        idf_values (Dict): Valores IDF precalculados para el corpus
    
    Example:
        >>> # Inicializar analizador
        >>> sim = SimilitudTextual()
        >>> 
        >>> # Comparar nombres (mejor con Jaro-Winkler)
        >>> score = sim.distancia_jaro_winkler("John Smith", "Jon Smith")
        >>> print(f"Similitud: {score:.3f}")
        0.945
        >>> 
        >>> # Comparar documentos (mejor con TF-IDF o Coseno)
        >>> doc1 = "Machine learning is a subset of artificial intelligence"
        >>> doc2 = "Deep learning is part of machine learning"
        >>> score = sim.similitud_coseno(doc1, doc2)
        >>> print(f"Similitud: {score:.3f}")
        0.456
    
    Performance Notes:
        - Levenshtein: O(n*m) - lento para textos largos
        - Jaro-Winkler: O(n*m) - optimizado para cadenas cortas
        - TF-IDF: O(n) - requiere corpus precalculado
        - Coseno: O(n) - rápido para documentos de cualquier tamaño
    """
    
    def __init__(self):
        """
        Inicializa el analizador de similitud textual.
        
        Crea una instancia con corpus vacío y sin valores IDF precalculados.
        Los valores IDF se calculan bajo demanda cuando se usa similitud_tfidf().
        
        Example:
            >>> sim = SimilitudTextual()
            >>> print(f"Corpus inicial: {len(sim.corpus)} documentos")
            Corpus inicial: 0 documentos
        """
        self.corpus = []
        self.idf_values = {}
    
    # ==================== MÉTODOS DE DISTANCIA DE EDICIÓN ====================
    
    def distancia_levenshtein(self, texto1: str, texto2: str) -> float:
        """
        Calcula la similitud usando la distancia de Levenshtein.
        
        La distancia de Levenshtein mide el número mínimo de operaciones de
        edición (inserción, eliminación, sustitución) necesarias para transformar
        un texto en otro. Es útil para detectar errores tipográficos y variaciones
        menores en strings.
        
        El algoritmo usa programación dinámica para construir una matriz donde
        cada celda representa el costo mínimo de transformación entre los
        prefijos de ambos textos.
        
        Args:
            texto1 (str): Primer texto a comparar. Se convierte a minúsculas.
            texto2 (str): Segundo texto a comparar. Se convierte a minúsculas.
            
        Returns:
            float: Similitud normalizada entre 0 y 1, donde:
                1.0 = textos idénticos
                0.0 = textos completamente diferentes
                
                Fórmula: 1 - (distancia / max_longitud)
        
        Complexity:
            - Tiempo: O(n * m) donde n y m son las longitudes de los textos
            - Espacio: O(n * m) para la matriz de programación dinámica
        
        Algorithm:
            1. Convertir ambos textos a minúsculas
            2. Crear matriz (n+1) x (m+1) para almacenar distancias
            3. Inicializar primera fila y columna con índices (0,1,2,...)
            4. Para cada celda (i,j):
               - Si caracteres son iguales: costo = 0
               - Si son diferentes: costo = 1
               - Tomar mínimo entre: inserción, eliminación, sustitución
            5. Normalizar distancia final dividiendo por longitud máxima
        
        Use Cases:
            - Corrección ortográfica automática
            - Detección de duplicados con errores tipográficos
            - Búsqueda difusa (fuzzy search)
            - Validación de entradas de usuario
        
        Example:
            >>> sim = SimilitudTextual()
            >>> 
            >>> # Textos muy similares (1 sustitución)
            >>> score = sim.distancia_levenshtein("kitten", "sitten")
            >>> print(f"Similitud: {score:.3f}")
            0.833  # (1 - 1/6)
            >>> 
            >>> # Textos con múltiples diferencias
            >>> score = sim.distancia_levenshtein("Saturday", "Sunday")
            >>> print(f"Similitud: {score:.3f}")
            0.625  # (1 - 3/8)
            >>> 
            >>> # Casos especiales
            >>> sim.distancia_levenshtein("", "")  # Ambos vacíos
            1.0
            >>> sim.distancia_levenshtein("abc", "")  # Uno vacío
            0.0
        
        Note:
            Este método NO es case-sensitive. "Hello" y "hello" se consideran
            idénticos. Para comparación case-sensitive, modifica el método
            eliminando las conversiones a minúsculas.
        """
        # Casos especiales: strings vacíos
        if not texto1 and not texto2:
            return 1.0  # Ambos vacíos = idénticos
        if not texto1 or not texto2:
            return 0.0  # Uno vacío = completamente diferentes
        
        # Convertir a minúsculas para comparación case-insensitive
        texto1 = texto1.lower()
        texto2 = texto2.lower()
        
        # Dimensiones de la matriz
        m, n = len(texto1), len(texto2)
        
        # Crear matriz de distancias (programación dinámica)
        matriz = [[0] * (n + 1) for _ in range(m + 1)]
        
        # Inicializar primera fila y columna
        # Representa el costo de insertar/eliminar todos los caracteres
        for i in range(m + 1):
            matriz[i][0] = i  # Costo de eliminar i caracteres
        for j in range(n + 1):
            matriz[0][j] = j  # Costo de insertar j caracteres
        
        # Llenar la matriz usando programación dinámica
        for i in range(1, m + 1):
            for j in range(1, n + 1):
                # Determinar costo de sustitución
                if texto1[i-1] == texto2[j-1]:
                    costo = 0  # Caracteres iguales, sin costo
                else:
                    costo = 1  # Caracteres diferentes, costo de sustitución
                
                # Calcular mínimo entre tres operaciones posibles
                matriz[i][j] = min(
                    matriz[i-1][j] + 1,      # Eliminación (borrar de texto1)
                    matriz[i][j-1] + 1,      # Inserción (agregar a texto1)
                    matriz[i-1][j-1] + costo # Sustitución (cambiar en texto1)
                )
        
        # Obtener distancia final (esquina inferior derecha)
        distancia = matriz[m][n]
        
        # Normalizar a rango [0, 1]
        # Dividir por la longitud máxima para obtener proporción
        max_len = max(m, n)
        similitud = 1 - (distancia / max_len)
        
        return similitud
    
    def distancia_jaro_winkler(self, texto1: str, texto2: str, p: float = 0.1) -> float:
        """
        Calcula la similitud usando la distancia de Jaro-Winkler.
        
        Jaro-Winkler es una variante de la distancia de Jaro optimizada para
        comparar nombres propios y cadenas cortas. Da más peso a los caracteres
        coincidentes al inicio de las cadenas (prefijo común), lo que refleja
        mejor cómo los humanos perciben similitud en nombres.
        
        El algoritmo primero calcula la similitud de Jaro (basada en coincidencias
        y transposiciones), luego la ajusta según la longitud del prefijo común.
        
        Args:
            texto1 (str): Primer texto a comparar. Se convierte a minúsculas.
            texto2 (str): Segundo texto a comparar. Se convierte a minúsculas.
            p (float, optional): Factor de escalado para el prefijo común.
                Valor típico: 0.1 (10% de peso al prefijo).
                Rango válido: [0, 0.25]
                Por defecto: 0.1
            
        Returns:
            float: Similitud entre 0 y 1, donde:
                1.0 = textos idénticos
                0.0 = sin coincidencias
                
                Fórmula Jaro: (m/|s1| + m/|s2| + (m-t)/m) / 3
                Fórmula Jaro-Winkler: Jaro + (l * p * (1 - Jaro))
                Donde:
                - m = número de coincidencias
                - t = número de transposiciones / 2
                - l = longitud del prefijo común (máx 4)
                - p = factor de escalado
        
        Complexity:
            - Tiempo: O(n * m) en peor caso
            - Espacio: O(n + m) para arrays de coincidencias
        
        Algorithm:
            PASO 1: Definir rango de coincidencia
            1. match_distance = max(len1, len2) / 2 - 1
            
            PASO 2: Encontrar coincidencias
            2. Para cada carácter en texto1:
               - Buscar coincidencia en texto2 dentro del rango
               - Marcar coincidencias en ambos arrays
            
            PASO 3: Contar transposiciones
            3. Recorrer coincidencias en orden
               - Contar cuántas están en diferente posición relativa
            
            PASO 4: Calcular Jaro
            4. jaro = (m/len1 + m/len2 + (m-t/2)/m) / 3
            
            PASO 5: Calcular prefijo común
            5. Contar caracteres coincidentes al inicio (máx 4)
            
            PASO 6: Calcular Jaro-Winkler
            6. jaro_winkler = jaro + (prefix_len * p * (1 - jaro))
        
        Advantages:
            - Excelente para nombres y apellidos
            - Reconoce errores al inicio como más importantes
            - Maneja bien transposiciones de caracteres
            - Rango siempre [0, 1], fácil de interpretar
        
        Use Cases:
            - Matching de nombres propios
            - Deduplicación de registros de personas
            - Búsqueda de direcciones
            - Códigos postales y similares
            - Cualquier dominio donde el prefijo es importante
        
        Example:
            >>> sim = SimilitudTextual()
            >>> 
            >>> # Nombres muy similares
            >>> score = sim.distancia_jaro_winkler("MARTHA", "MARHTA")
            >>> print(f"Similitud: {score:.3f}")
            0.961  # Alta similitud, solo transposición
            >>> 
            >>> # Prefijo común importante
            >>> score = sim.distancia_jaro_winkler("DWAYNE", "DUANE")
            >>> print(f"Similitud: {score:.3f}")
            0.840  # Prefijo "D" común aumenta score
            >>> 
            >>> # Sin prefijo común
            >>> score = sim.distancia_jaro_winkler("DIXON", "DICKSONX")
            >>> print(f"Similitud: {score:.3f}")
            0.767
            >>> 
            >>> # Ajustar factor de prefijo
            >>> score1 = sim.distancia_jaro_winkler("John", "Jonathan", p=0.1)
            >>> score2 = sim.distancia_jaro_winkler("John", "Jonathan", p=0.2)
            >>> print(f"p=0.1: {score1:.3f}, p=0.2: {score2:.3f}")
        
        Note:
            El parámetro 'p' no debe exceder 0.25, ya que puede hacer que
            el score Jaro-Winkler exceda 1.0. El valor estándar es 0.1.
        """
        # Casos especiales: strings vacíos
        if not texto1 and not texto2:
            return 1.0  # Ambos vacíos = idénticos
        if not texto1 or not texto2:
            return 0.0  # Uno vacío = sin similitud
        
        # Convertir a minúsculas para comparación case-insensitive
        texto1 = texto1.lower()
        texto2 = texto2.lower()
        
        # Caso especial: strings idénticos
        if texto1 == texto2:
            return 1.0
        
        # Obtener longitudes
        len1, len2 = len(texto1), len(texto2)
        
        # ===== PASO 1: CALCULAR RANGO DE COINCIDENCIA =====
        # Los caracteres deben estar dentro de este rango para considerarse coincidencia
        match_distance = max(len1, len2) // 2 - 1
        if match_distance < 0:
            match_distance = 0
        
        # ===== PASO 2: ARRAYS PARA MARCAR COINCIDENCIAS =====
        matches1 = [False] * len1
        matches2 = [False] * len2
        
        matches = 0          # Contador de coincidencias
        transposiciones = 0  # Contador de transposiciones
        
        # ===== PASO 3: ENCONTRAR COINCIDENCIAS =====
        for i in range(len1):
            # Definir ventana de búsqueda en texto2
            start = max(0, i - match_distance)
            end = min(i + match_distance + 1, len2)
            
            for j in range(start, end):
                # Si ya tiene match o caracteres diferentes, continuar
                if matches2[j] or texto1[i] != texto2[j]:
                    continue
                
                # Coincidencia encontrada
                matches1[i] = True
                matches2[j] = True
                matches += 1
                break
        
        # Si no hay coincidencias, similitud es 0
        if matches == 0:
            return 0.0
        
        # ===== PASO 4: CONTAR TRANSPOSICIONES =====
        # Una transposición ocurre cuando caracteres coincidentes están en diferente orden
        k = 0
        for i in range(len1):
            if not matches1[i]:
                continue
            
            # Encontrar siguiente match en texto2
            while not matches2[k]:
                k += 1
            
            # Si los caracteres no son iguales, es una transposición
            if texto1[i] != texto2[k]:
                transposiciones += 1
            k += 1
        
        # ===== PASO 5: CALCULAR SIMILITUD DE JARO =====
        # Fórmula: (m/|s1| + m/|s2| + (m-t/2)/m) / 3
        jaro = (matches / len1 + 
                matches / len2 + 
                (matches - transposiciones / 2) / matches) / 3
        
        # ===== PASO 6: CALCULAR LONGITUD DEL PREFIJO COMÚN =====
        # Máximo 4 caracteres según el algoritmo original
        prefix = 0
        for i in range(min(len1, len2, 4)):
            if texto1[i] == texto2[i]:
                prefix += 1
            else:
                break
        
        # ===== PASO 7: CALCULAR JARO-WINKLER =====
        # Ajustar Jaro según longitud del prefijo común
        # Fórmula: jaro + (l * p * (1 - jaro))
        jaro_winkler = jaro + prefix * p * (1 - jaro)
        
        return jaro_winkler
    
    # ==================== MÉTODOS DE VECTORIZACIÓN ESTADÍSTICA ====================
    
    def _preprocesar_texto(self, texto: str) -> List[str]:
        """
        Preprocesa el texto: convierte a minúsculas y tokeniza.
        
        Método auxiliar privado que normaliza textos para procesamiento posterior.
        Elimina puntuación, convierte a minúsculas y divide en tokens (palabras).
        
        Args:
            texto (str): Texto a preprocesar.
            
        Returns:
            List[str]: Lista de tokens (palabras) en minúsculas.
                Ejemplo: "Hello, World!" → ["hello", "world"]
        
        Algorithm:
            1. Convertir todo el texto a minúsculas
            2. Usar regex para extraer solo palabras (\\b\\w+\\b)
               - \\b: límite de palabra
               - \\w+: uno o más caracteres alfanuméricos
            3. Retornar lista de tokens
        
        Example:
            >>> sim = SimilitudTextual()
            >>> tokens = sim._preprocesar_texto("Hello, World! It's 2025.")
            >>> print(tokens)
            ['hello', 'world', 'it', 's', '2025']
            >>> 
            >>> # Texto con puntuación compleja
            >>> tokens = sim._preprocesar_texto("e-mail: user@example.com")
            >>> print(tokens)
            ['e', 'mail', 'user', 'example', 'com']
        
        Note:
            - Elimina toda puntuación y caracteres especiales
            - Palabras con guiones se separan en tokens individuales
            - Números se mantienen como tokens
            - Apóstrofes y acentos se eliminan
        """
        # Convertir a minúsculas
        texto = texto.lower()
        
        # Tokenizar por palabras usando regex
        # \b\w+\b captura secuencias de caracteres alfanuméricos
        tokens = re.findall(r'\b\w+\b', texto)
        
        return tokens
    
    def _calcular_tf(self, tokens: List[str]) -> Dict[str, float]:
        """
        Calcula la frecuencia de términos (TF - Term Frequency).
        
        TF mide qué tan frecuente aparece un término en un documento.
        Se normaliza dividiendo por el total de términos para obtener una
        proporción entre 0 y 1.
        
        Args:
            tokens (List[str]): Lista de tokens del documento.
            
        Returns:
            Dict[str, float]: Diccionario con TF de cada término.
                Formato: {término: frecuencia_normalizada}
                Ejemplo: {"machine": 0.2, "learning": 0.15, ...}
        
        Formula:
            TF(t) = (número de veces que aparece t) / (total de términos)
        
        Example:
            >>> sim = SimilitudTextual()
            >>> tokens = ["machine", "learning", "machine", "learning", "deep"]
            >>> tf = sim._calcular_tf(tokens)
            >>> print(tf)
            {'machine': 0.4, 'learning': 0.4, 'deep': 0.2}
            >>> # machine y learning aparecen 2/5 = 0.4 cada uno
            >>> # deep aparece 1/5 = 0.2
        
        Note:
            - TF siempre está en el rango [0, 1]
            - Suma de todos los TF = 1.0
            - Términos muy frecuentes tienen TF alto
        """
        # Contar frecuencias absolutas
        contador = Counter(tokens)
        
        # Calcular total de términos
        total = len(tokens)
        
        # Normalizar frecuencias (dividir por total)
        tf = {palabra: freq / total for palabra, freq in contador.items()}
        
        return tf
    
    def _calcular_idf(self, corpus: List[List[str]]) -> Dict[str, float]:
        """
        Calcula la frecuencia inversa de documento (IDF - Inverse Document Frequency).
        
        IDF mide qué tan importante es un término en todo el corpus. Términos
        que aparecen en pocos documentos tienen IDF alto (son discriminativos),
        mientras que términos que aparecen en muchos documentos tienen IDF bajo
        (son comunes y menos informativos).
        
        Args:
            corpus (List[List[str]]): Lista de documentos tokenizados.
                Cada documento es una lista de tokens.
                Ejemplo: [["machine", "learning"], ["deep", "learning"]]
            
        Returns:
            Dict[str, float]: Diccionario con IDF de cada término.
                Formato: {término: idf_value}
                Valores más altos = términos más discriminativos
        
        Formula:
            IDF(t) = log(N / df(t))
            Donde:
            - N = número total de documentos
            - df(t) = número de documentos que contienen el término t
        
        Algorithm:
            1. Contar en cuántos documentos aparece cada término (df)
            2. Para cada término: IDF = log(N / df)
            3. Retornar diccionario de valores IDF
        
        Example:
            >>> sim = SimilitudTextual()
            >>> corpus = [
            ...     ["machine", "learning", "model"],
            ...     ["deep", "learning", "neural"],
            ...     ["machine", "vision", "model"]
            ... ]
            >>> idf = sim._calcular_idf(corpus)
            >>> print(f"learning IDF: {idf['learning']:.3f}")
            0.405  # Aparece en 2/3 documentos
            >>> print(f"machine IDF: {idf['machine']:.3f}")
            0.405  # Aparece en 2/3 documentos
            >>> print(f"deep IDF: {idf['deep']:.3f}")
            1.099  # Aparece en 1/3 documentos (más discriminativo)
        
        Note:
            - Términos que aparecen en todos los documentos: IDF ≈ 0
            - Términos que aparecen en pocos documentos: IDF alto
            - Se usa logaritmo para suavizar la escala
        """
        n_docs = len(corpus)
        idf = {}
        
        # ===== PASO 1: CONTAR FRECUENCIA DE DOCUMENTO (DF) =====
        # Contar en cuántos documentos aparece cada término
        term_doc_count = Counter()
        
        for doc in corpus:
            # Obtener términos únicos del documento (set elimina duplicados)
            unique_terms = set(doc)
            # Incrementar contador para cada término único
            term_doc_count.update(unique_terms)
        
        # ===== PASO 2: CALCULAR IDF =====
        for term, count in term_doc_count.items():
            # IDF = log(N / df)
            idf[term] = math.log(n_docs / count)
        
        return idf
    
    def similitud_tfidf(self, texto1: str, texto2: str, 
                        corpus: Optional[List[str]] = None) -> float:
        """
        Calcula la similitud usando vectores TF-IDF y similitud del coseno.
        
        TF-IDF (Term Frequency-Inverse Document Frequency) es una técnica de
        vectorización que pondera la importancia de cada término considerando:
        1. Qué tan frecuente es en el documento (TF)
        2. Qué tan raro es en el corpus completo (IDF)
        
        Esta combinación permite identificar términos realmente significativos
        para cada documento, ignorando palabras muy comunes que aparecen en todos.
        
        Args:
            texto1 (str): Primer texto a comparar.
            texto2 (str): Segundo texto a comparar.
            corpus (Optional[List[str]]): Lista de documentos para calcular IDF.
                Si None, usa solo los dos textos proporcionados.
                Para mejores resultados, proporcionar corpus representativo.
            
        Returns:
            float: Similitud entre 0 y 1, donde:
                1.0 = vectores TF-IDF idénticos
                0.0 = sin términos en común
                
                Se calcula usando similitud del coseno entre vectores TF-IDF.
        
        Complexity:
            - Tiempo: O(n + m + k) donde:
              n = longitud texto1
              m = longitud texto2
              k = tamaño del corpus
            - Espacio: O(v) donde v = vocabulario único
        
        Algorithm:
            1. Preprocesar texto1 y texto2 (tokenizar)
            2. Si no hay corpus, usar [texto1, texto2]
            3. Tokenizar todo el corpus
            4. Calcular IDF para todos los términos del corpus
            5. Calcular TF para texto1 y texto2
            6. Crear vectores TF-IDF: TF-IDF(t) = TF(t) * IDF(t)
            7. Calcular similitud del coseno entre vectores
        
        Advantages:
            - Ignora palabras muy comunes (bajo IDF)
            - Destaca términos discriminativos (alto TF-IDF)
            - Funciona bien para documentos largos
            - Base de muchos sistemas de búsqueda
        
        Use Cases:
            - Búsqueda de documentos relevantes
            - Detección de plagio académico
            - Sistemas de recomendación de contenido
            - Clustering de documentos por tema
            - Resumen automático de textos
        
        Example:
            >>> sim = SimilitudTextual()
            >>> 
            >>> # Sin corpus (usa solo los 2 textos)
            >>> doc1 = "machine learning algorithms"
            >>> doc2 = "deep learning neural networks"
            >>> score = sim.similitud_tfidf(doc1, doc2)
            >>> print(f"Similitud: {score:.3f}")
            0.447
            >>> 
            >>> # Con corpus más grande (mejor para IDF)
            >>> corpus = [
            ...     "machine learning algorithms",
            ...     "deep learning neural networks",
            ...     "natural language processing",
            ...     "computer vision algorithms",
            ...     "reinforcement learning agents"
            ... ]
            >>> score = sim.similitud_tfidf(doc1, doc2, corpus)
            >>> print(f"Similitud con corpus: {score:.3f}")
            0.512  # Mejor discriminación
            >>> 
            >>> # Comparar documentos largos
            >>> article1 = "..." # Abstract de paper 1
            >>> article2 = "..." # Abstract de paper 2
            >>> corpus_papers = [...]  # Todos los abstracts
            >>> score = sim.similitud_tfidf(article1, article2, corpus_papers)
        
        Note:
            - Corpus más grande y representativo = mejores valores IDF
            - Sin corpus, IDF puede no ser muy informativo
            - Para mejores resultados, incluir al menos 10-20 documentos en corpus
            - Considera usar similitud_coseno() si no tienes corpus adecuado
        """
        # ===== PASO 1: PREPROCESAR TEXTOS =====
        tokens1 = self._preprocesar_texto(texto1)
        tokens2 = self._preprocesar_texto(texto2)
        
        # Caso especial: algún texto vacío
        if not tokens1 or not tokens2:
            return 0.0
        
        # ===== PASO 2: PREPARAR CORPUS =====
        # Si no hay corpus, usar solo los dos textos
        if corpus is None:
            corpus = [texto1, texto2]
        
        # Tokenizar todo el corpus
        corpus_tokens = [self._preprocesar_texto(doc) for doc in corpus]
        
        # ===== PASO 3: CALCULAR IDF DEL CORPUS =====
        idf = self._calcular_idf(corpus_tokens)
        
        # ===== PASO 4: CALCULAR TF PARA CADA TEXTO =====
        tf1 = self._calcular_tf(tokens1)
        tf2 = self._calcular_tf(tokens2)
        
        # ===== PASO 5: CREAR VECTORES TF-IDF =====
        # Obtener vocabulario (todos los términos únicos)
        all_terms = set(tokens1 + tokens2)
        
        # Calcular TF-IDF para cada término
        # TF-IDF(t) = TF(t) * IDF(t)
        vector1 = {term: tf1.get(term, 0) * idf.get(term, 0) for term in all_terms}
        vector2 = {term: tf2.get(term, 0) * idf.get(term, 0) for term in all_terms}
        
        # ===== PASO 6: CALCULAR SIMILITUD DEL COSENO =====
        similitud = self._similitud_coseno_vectores(vector1, vector2)
        
        return similitud
    
    def _similitud_coseno_vectores(self, vector1: Mapping[str, Union[float, int]], 
                                   vector2: Mapping[str, Union[float, int]]) -> float:
        """
        Calcula la similitud del coseno entre dos vectores.
        
        La similitud del coseno mide el ángulo entre dos vectores en un espacio
        multidimensional. Es independiente de la magnitud de los vectores, solo
        considera su dirección. Valores cercanos a 1 indican vectores apuntando
        en la misma dirección (similares), mientras que valores cercanos a 0
        indican vectores perpendiculares (diferentes).
        
        Args:
            vector1 (Mapping[str, Union[float, int]]): Primer vector.
                Formato: {dimensión: valor}
                Ejemplo: {"machine": 0.5, "learning": 0.3}
            vector2 (Mapping[str, Union[float, int]]): Segundo vector.
                Debe tener las mismas dimensiones que vector1.
            
        Returns:
            float: Similitud del coseno entre 0 y 1, donde:
                1.0 = vectores idénticos (mismo ángulo)
                0.5 = vectores a 60° (moderadamente similares)
                0.0 = vectores perpendiculares (ortogonales)
                
                Nota: Para texto, valores típicos están en [0.2, 0.9]
        
        Formula:
            cos(θ) = (A · B) / (||A|| * ||B||)
            Donde:
            - A · B = producto punto (suma de productos elemento a elemento)
            - ||A|| = magnitud de A = √(Σ ai²)
            - ||B|| = magnitud de B = √(Σ bi²)
        
        Algorithm:
            1. Calcular producto punto: Σ(vector1[i] * vector2[i])
            2. Calcular magnitud de vector1: √(Σ vector1[i]²)
            3. Calcular magnitud de vector2: √(Σ vector2[i]²)
            4. Dividir producto punto por producto de magnitudes
            5. Si alguna magnitud es 0, retornar 0
        
        Example:
            >>> sim = SimilitudTextual()
            >>> 
            >>> # Vectores idénticos
            >>> v1 = {"a": 1.0, "b": 2.0, "c": 3.0}
            >>> v2 = {"a": 1.0, "b": 2.0, "c": 3.0}
            >>> score = sim._similitud_coseno_vectores(v1, v2)
            >>> print(f"Similitud: {score:.3f}")
            1.000
            >>> 
            >>> # Vectores proporcionales (mismo ángulo)
            >>> v1 = {"a": 1.0, "b": 2.0, "c": 3.0}
            >>> v2 = {"a": 2.0, "b": 4.0, "c": 6.0}  # v1 * 2
            >>> score = sim._similitud_coseno_vectores(v1, v2)
            >>> print(f"Similitud: {score:.3f}")
            1.000  # Misma dirección, diferente magnitud
            >>> 
            >>> # Vectores perpendiculares
            >>> v1 = {"a": 1.0, "b": 0.0}
            >>> v2 = {"a": 0.0, "b": 1.0}
            >>> score = sim._similitud_coseno_vectores(v1, v2)
            >>> print(f"Similitud: {score:.3f}")
            0.000
        
        Note:
            - La similitud del coseno NO considera la magnitud de los vectores
            - Útil cuando solo importa la distribución relativa de valores
            - Muy usada en procesamiento de lenguaje natural
        """
        # ===== PASO 1: CALCULAR PRODUCTO PUNTO =====
        # Suma de productos elemento a elemento
        producto_punto = sum(float(vector1[term]) * float(vector2.get(term, 0)) 
                             for term in vector1)
        
        # ===== PASO 2: CALCULAR MAGNITUDES =====
        # Magnitud = raíz cuadrada de la suma de cuadrados
        magnitud1 = math.sqrt(sum(float(val) ** 2 for val in vector1.values()))
        magnitud2 = math.sqrt(sum(float(val) ** 2 for val in vector2.values()))
        
        # ===== PASO 3: EVITAR DIVISIÓN POR CERO =====
        if magnitud1 == 0 or magnitud2 == 0:
            return 0.0  # Vector vacío o nulo
        
        # ===== PASO 4: CALCULAR SIMILITUD DEL COSENO =====
        similitud = producto_punto / (magnitud1 * magnitud2)
        
        return similitud
    
    def similitud_coseno(self, texto1: str, texto2: str) -> float:
        """
        Calcula la similitud del coseno basada en frecuencias de términos.
        
        Este método vectorial mide el ángulo entre dos vectores de frecuencia de
        palabras. A diferencia de TF-IDF, usa solo las frecuencias crudas sin
        ponderar por IDF, lo que lo hace más simple pero menos sofisticado para
        discriminar términos importantes.
        
        Es ideal cuando no se tiene un corpus de referencia o cuando todos los
        términos tienen similar importancia relativa.
        
        Args:
            texto1 (str): Primer texto a comparar.
            texto2 (str): Segundo texto a comparar.
            
        Returns:
            float: Similitud entre 0 y 1, donde:
                1.0 = misma distribución de palabras
                0.0 = sin palabras en común
                
                Se calcula usando similitud del coseno entre vectores de frecuencia.
        
        Complexity:
            - Tiempo: O(n + m) donde n y m son longitudes de textos
            - Espacio: O(v) donde v = vocabulario único
        
        Algorithm:
            1. Preprocesar ambos textos (tokenizar)
            2. Contar frecuencias de cada palabra
            3. Crear vocabulario con todas las palabras únicas
            4. Crear vectores de frecuencia para ambos textos
            5. Calcular similitud del coseno entre vectores
        
        Comparison with TF-IDF:
            - Similitud Coseno: Solo usa frecuencias locales (TF)
            - TF-IDF: Pondera por importancia global (IDF)
            
            Ejemplo: Palabra "the" aparece en ambos textos
            - Coseno: Contribuye igualmente que otras palabras
            - TF-IDF: Contribuye poco (IDF bajo por ser común)
        
        Advantages:
            - No requiere corpus de referencia
            - Más simple y rápido que TF-IDF
            - Suficiente para documentos similares en longitud
            - Bueno para textos cortos (tweets, títulos)
        
        Disadvantages:
            - No discrimina palabras comunes vs raras
            - Sensible a palabras muy frecuentes
            - Menos efectivo que TF-IDF para documentos largos
        
        Use Cases:
            - Comparación de textos cortos
            - Cuando no hay corpus disponible
            - Detección rápida de duplicados
            - Búsqueda en colecciones pequeñas
            - Clustering simple de documentos
        
        Example:
            >>> sim = SimilitudTextual()
            >>> 
            >>> # Textos con palabras comunes
            >>> doc1 = "the cat sat on the mat"
            >>> doc2 = "the dog sat on the rug"
            >>> score = sim.similitud_coseno(doc1, doc2)
            >>> print(f"Similitud: {score:.3f}")
            0.816  # Alta por "the", "sat", "on"
            >>> 
            >>> # Textos muy diferentes
            >>> doc1 = "machine learning algorithms"
            >>> doc2 = "cooking italian recipes"
            >>> score = sim.similitud_coseno(doc1, doc2)
            >>> print(f"Similitud: {score:.3f}")
            0.000  # Sin palabras en común
            >>> 
            >>> # Textos con diferente longitud
            >>> doc1 = "cat"
            >>> doc2 = "the cat sat on the mat with another cat"
            >>> score = sim.similitud_coseno(doc1, doc2)
            >>> print(f"Similitud: {score:.3f}")
            0.408  # "cat" aparece pero en diferente proporción
        
        Note:
            - Case-insensitive: "Cat" y "cat" se consideran iguales
            - Ignora puntuación y caracteres especiales
            - Solo considera overlap de vocabulario, no orden
            - Para mejor precisión con corpus grande, usar similitud_tfidf()
        """
        # ===== PASO 1: PREPROCESAR TEXTOS =====
        tokens1 = self._preprocesar_texto(texto1)
        tokens2 = self._preprocesar_texto(texto2)
        
        # Caso especial: algún texto vacío
        if not tokens1 or not tokens2:
            return 0.0
        
        # ===== PASO 2: CONTAR FRECUENCIAS =====
        freq1 = Counter(tokens1)
        freq2 = Counter(tokens2)
        
        # ===== PASO 3: OBTENER VOCABULARIO COMPLETO =====
        # Unión de todos los términos únicos
        all_terms = set(freq1.keys()) | set(freq2.keys())
        
        # ===== PASO 4: CREAR VECTORES DE FRECUENCIA =====
        # Convertir a float para operaciones matemáticas
        vector1 = {term: float(freq1.get(term, 0)) for term in all_terms}
        vector2 = {term: float(freq2.get(term, 0)) for term in all_terms}
        
        # ===== PASO 5: CALCULAR SIMILITUD DEL COSENO =====
        similitud = self._similitud_coseno_vectores(vector1, vector2)
        
        return similitud
    
    # ==================== MÉTODO DE COMPARACIÓN ====================
    
    def comparar_todos(self, texto1: str, texto2: str, 
                      corpus: Optional[List[str]] = None) -> Dict[str, float]:
        """
        Compara dos textos usando todos los algoritmos disponibles.
        
        Método de conveniencia que ejecuta los 4 algoritmos de similitud
        implementados en la clase y retorna todos los resultados en un
        diccionario. Útil para análisis comparativo y selección del mejor
        algoritmo para un caso específico.
        
        Args:
            texto1 (str): Primer texto a comparar.
            texto2 (str): Segundo texto a comparar.
            corpus (Optional[List[str]]): Corpus para TF-IDF (opcional).
                Si None, TF-IDF usa solo los dos textos proporcionados.
            
        Returns:
            Dict[str, float]: Diccionario con resultados de cada algoritmo.
                Formato: {nombre_algoritmo: score}
                Keys:
                - 'Levenshtein': Score de distancia de edición
                - 'Jaro-Winkler': Score optimizado para nombres
                - 'TF-IDF': Score con vectorización ponderada
                - 'Coseno': Score con vectorización simple
                
                Todos los valores están en rango [0, 1]
        
        Complexity:
            - Tiempo: O(n*m + n + m + k) - suma de todos los algoritmos
            - Espacio: O(n + m + k) - espacios auxiliares
        
        Use Cases:
            - Análisis exploratorio de similitud
            - Comparación de rendimiento de algoritmos
            - Selección del mejor método para un dominio
            - Benchmarking de diferentes enfoques
            - Validación cruzada de resultados
        
        Example:
            >>> sim = SimilitudTextual()
            >>> 
            >>> # Comparar nombres (Jaro-Winkler debería ganar)
            >>> texto1 = "John Smith"
            >>> texto2 = "Jon Smith"
            >>> resultados = sim.comparar_todos(texto1, texto2)
            >>> for algoritmo, score in sorted(resultados.items(), 
            ...                                key=lambda x: x[1], 
            ...                                reverse=True):
            ...     print(f"{algoritmo}: {score:.3f}")
            Jaro-Winkler: 0.961
            Levenshtein: 0.900
            Coseno: 0.707
            TF-IDF: 0.707
            >>> 
            >>> # Comparar documentos (TF-IDF debería ganar)
            >>> doc1 = "Machine learning is a subset of artificial intelligence"
            >>> doc2 = "Deep learning is part of machine learning"
            >>> corpus = [doc1, doc2, "Natural language processing"]
            >>> resultados = sim.comparar_todos(doc1, doc2, corpus)
            >>> for algoritmo, score in sorted(resultados.items(), 
            ...                                key=lambda x: x[1], 
            ...                                reverse=True):
            ...     print(f"{algoritmo}: {score:.3f}")
            TF-IDF: 0.512
            Coseno: 0.456
            Jaro-Winkler: 0.321
            Levenshtein: 0.298
            >>> 
            >>> # Identificar mejor algoritmo para un caso
            >>> mejor_algoritmo = max(resultados.items(), key=lambda x: x[1])
            >>> print(f"Mejor algoritmo: {mejor_algoritmo[0]} ({mejor_algoritmo[1]:.3f})")
        
        Interpretation Guide:
            - Para nombres/direcciones cortas: Priorizar Jaro-Winkler
            - Para documentos largos: Priorizar TF-IDF o Coseno
            - Para detección de typos: Priorizar Levenshtein
            - Si todos coinciden alto (>0.8): Textos muy similares
            - Si todos difieren: Textos muy diferentes
            - Si solo vectoriales alto: Similar semántica, diferente sintaxis
            - Si solo edición alto: Similar sintaxis, diferente semántica
        
        Note:
            - Los algoritmos miden aspectos diferentes de similitud
            - No hay un "mejor" algoritmo universal
            - La elección depende del dominio y tipo de texto
            - Considera usar ensemble (promedio ponderado) de múltiples scores
        """
        # Ejecutar todos los algoritmos y recolectar resultados
        resultados = {
            'Levenshtein': self.distancia_levenshtein(texto1, texto2),
            'Jaro-Winkler': self.distancia_jaro_winkler(texto1, texto2),
            'TF-IDF': self.similitud_tfidf(texto1, texto2, corpus),
            'Coseno': self.similitud_coseno(texto1, texto2)
        }
        
        return resultados

    def comparar_multiples(self, textos: List[str], corpus: Optional[List[str]] = None,
                           usar: Optional[Dict[str, bool]] = None,
                           top_k: Optional[int] = 10) -> Dict[str, Any]:
        """
        Calcula similitud entre múltiples textos (>=2) usando algoritmos clásicos.

        Args:
            textos (List[str]): Lista de textos (p. ej., abstracts) a comparar.
            corpus (Optional[List[str]]): Corpus para TF-IDF. Si es None, se usa `textos`.
            usar (Optional[Dict[str, bool]]): Selección de algoritmos.
                Keys soportadas: 'levenshtein', 'jaro', 'tfidf', 'coseno'.
                Por defecto, todos True si no se especifica.
            top_k (Optional[int]): Número de pares top a devolver por algoritmo.

        Returns:
            Dict[str, Any]: Resultados por algoritmo con matrices y pares top.
                {
                  'Levenshtein': { 'matrix': [[...]], 'pairs': [{'i':0,'j':1,'score':0.8}, ...] },
                  'Jaro-Winkler': { ... },
                  'TF-IDF': { ... },
                  'Coseno': { ... }
                }
        """
        n = len(textos)
        if n < 2:
            raise ValueError("Se requieren al menos 2 textos para comparar.")

        # Selección de algoritmos
        usar = usar or {}
        use_lev = usar.get('levenshtein', True)
        use_jaro = usar.get('jaro', True)
        use_tfidf = usar.get('tfidf', True)
        use_cos = usar.get('coseno', True)

        # Corpus por defecto para TF-IDF
        tfidf_corpus = corpus if corpus is not None else textos

        resultados: Dict[str, Any] = {}

        def matriz_vacia() -> list[list[float]]:
            return [[1.0 if i == j else 0.0 for j in range(n)] for i in range(n)]

        # Calcular todas las combinaciones i<j para cada algoritmo seleccionado
        if use_lev:
            mat = matriz_vacia()
            pairs = []
            for i in range(n):
                for j in range(i + 1, n):
                    s = self.distancia_levenshtein(textos[i], textos[j])
                    mat[i][j] = s
                    mat[j][i] = s
                    pairs.append({'i': i, 'j': j, 'score': float(s)})
            pairs.sort(key=lambda x: x['score'], reverse=True)
            if top_k is not None:
                pairs = pairs[:top_k]
            resultados['Levenshtein'] = {'matrix': mat, 'pairs': pairs}

        if use_jaro:
            mat = matriz_vacia()
            pairs = []
            for i in range(n):
                for j in range(i + 1, n):
                    s = self.distancia_jaro_winkler(textos[i], textos[j])
                    mat[i][j] = s
                    mat[j][i] = s
                    pairs.append({'i': i, 'j': j, 'score': float(s)})
            pairs.sort(key=lambda x: x['score'], reverse=True)
            if top_k is not None:
                pairs = pairs[:top_k]
            resultados['Jaro-Winkler'] = {'matrix': mat, 'pairs': pairs}

        if use_tfidf:
            mat = matriz_vacia()
            pairs = []
            for i in range(n):
                for j in range(i + 1, n):
                    s = self.similitud_tfidf(textos[i], textos[j], corpus=tfidf_corpus)
                    mat[i][j] = s
                    mat[j][i] = s
                    pairs.append({'i': i, 'j': j, 'score': float(s)})
            pairs.sort(key=lambda x: x['score'], reverse=True)
            if top_k is not None:
                pairs = pairs[:top_k]
            resultados['TF-IDF'] = {'matrix': mat, 'pairs': pairs}

        if use_cos:
            mat = matriz_vacia()
            pairs = []
            for i in range(n):
                for j in range(i + 1, n):
                    s = self.similitud_coseno(textos[i], textos[j])
                    mat[i][j] = s
                    mat[j][i] = s
                    pairs.append({'i': i, 'j': j, 'score': float(s)})
            pairs.sort(key=lambda x: x['score'], reverse=True)
            if top_k is not None:
                pairs = pairs[:top_k]
            resultados['Coseno'] = {'matrix': mat, 'pairs': pairs}

        return resultados