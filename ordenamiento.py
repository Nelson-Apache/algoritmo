"""
Academic Sorting Analyzer - Análisis y Comparación de Algoritmos de Ordenamiento
==================================================================================

Este módulo proporciona un framework completo para analizar y comparar el rendimiento
de 12 algoritmos de ordenamiento diferentes aplicados a datos académicos (artículos,
papers, publicaciones) extraídos de bases de datos como EBSCO.

Algoritmos Implementados:
-------------------------
1. TimSort - O(n log n) - Algoritmo híbrido usado por Python
2. CombSort - O(n log n) promedio - Mejora de BubbleSort
3. SelectionSort - O(n²) - Algoritmo básico de selección
4. TreeSort - O(n log n) promedio - Basado en árbol binario
5. PigeonholeSort - O(n + k) - Distribución por categorías
6. BucketSort - O(n + k) promedio - Distribución en buckets
7. QuickSort - O(n log n) promedio - Divide y conquista
8. HeapSort - O(n log n) - Basado en heap binario
9. BitonicSort - O(n log²n) - Para procesamiento paralelo
10. GnomeSort - O(n²) promedio - Similar a InsertionSort
11. BinaryInsertionSort - O(n²) - InsertionSort optimizado
12. RadixSort - O(d*(n+k)) - Ordenamiento por dígitos

Funcionalidades:
----------------
- Ejecución y medición de tiempo de todos los algoritmos
- Generación de gráficos comparativos
- Análisis de autores más frecuentes
- Exportación de resultados ordenados
- Reportes detallados de rendimiento

Criterio de Ordenamiento:
--------------------------
Los datos se ordenan por:
1. Año de publicación (ascendente)
2. Título del artículo (alfabético, case-insensitive)

Uso Típico:
-----------
>>> analyzer = AcademicSortingAnalyzer("articles.csv")
>>> results = analyzer.run_all_algorithms()
>>> analyzer.create_time_comparison_chart(results)
>>> top_authors = analyzer.get_top_authors(15)

O usar la función de conveniencia:
>>> analyze_academic_data("articles.csv", "output_analysis")

Autor: [Tu nombre]
Fecha: 2025
Licencia: [Tu licencia]
"""

import pandas as pd
import matplotlib.cm as cm
import matplotlib.pyplot as plt
import seaborn as sns
import time
import re
from typing import List, Tuple, Dict
from collections import Counter
import numpy as np


class TreeNode:
    """
    Nodo para implementación de Tree Sort (árbol binario de búsqueda).
    
    Esta clase representa un nodo individual en un árbol binario de búsqueda
    utilizado por el algoritmo TreeSort. Cada nodo almacena un valor y
    referencias a sus hijos izquierdo y derecho.
    
    Attributes:
        val: Valor almacenado en el nodo. Puede ser cualquier tipo comparable.
        left (TreeNode): Referencia al hijo izquierdo (valores menores).
        right (TreeNode): Referencia al hijo derecho (valores mayores o iguales).
    
    Example:
        >>> node = TreeNode((2023, "Machine Learning", 0))
        >>> node.val
        (2023, 'Machine Learning', 0)
        >>> node.left is None
        True
        >>> node.right is None
        True
    """
    def __init__(self, val):
        """
        Inicializa un nodo del árbol binario.
        
        Args:
            val: Valor a almacenar en el nodo. Típicamente una tupla
                (año, título, índice) para ordenamiento de datos académicos.
        """
        self.val = val
        self.left = None
        self.right = None


class AcademicSortingAnalyzer:
    """
    Analizador completo de algoritmos de ordenamiento para datos académicos.
    
    Esta clase implementa 12 algoritmos de ordenamiento diferentes y proporciona
    herramientas para comparar su rendimiento, visualizar resultados y analizar
    datos académicos (artículos, publicaciones, papers).
    
    La clase carga datos desde un CSV, los prepara para ordenamiento (extrayendo
    años de fechas, normalizando títulos), ejecuta múltiples algoritmos midiendo
    su tiempo de ejecución, y genera reportes y visualizaciones comparativas.
    
    Attributes:
        csv_file (str): Ruta al archivo CSV con datos académicos
        df (pd.DataFrame): DataFrame con los datos cargados y preparados
    
    Expected CSV Structure:
        El CSV debe contener al menos estas columnas:
        - title: Título del artículo
        - publication_date: Fecha de publicación (cualquier formato)
        - authors: Autores separados por punto y coma (opcional)
        
    Prepared Data Columns:
        Después de load_data(), se agregan:
        - title_clean: Título limpio (sin NaN)
        - year: Año extraído de publication_date
        - sort_key: Tupla (year, title_normalized) para ordenamiento
    
    Example:
        >>> # Uso básico
        >>> analyzer = AcademicSortingAnalyzer("ebsco_articles.csv")
        >>> results = analyzer.run_all_algorithms()
        >>> analyzer.create_time_comparison_chart(results)
        
        >>> # Análisis de autores
        >>> top_authors = analyzer.get_top_authors(15)
        >>> print(top_authors.head())
        
        >>> # Reporte completo
        >>> analyzer.generate_complete_report("analysis_2025")
    
    Performance Notes:
        - Para datasets pequeños (<1000 registros), todos los algoritmos son rápidos
        - Para datasets grandes (>10000 registros), usar TimSort, QuickSort, HeapSort
        - Algoritmos O(n²) como SelectionSort pueden ser muy lentos con >5000 registros
    """
    
    def __init__(self, csv_file: str):
        """
        Inicializa el analizador con un archivo CSV de datos académicos.
        
        Args:
            csv_file (str): Ruta al archivo CSV que contiene los datos académicos.
                El archivo debe existir y ser un CSV válido con encoding UTF-8.
                Debe contener al menos las columnas 'title' y 'publication_date'.
        
        Side Effects:
            - Llama automáticamente a load_data() para cargar el CSV
            - Imprime mensajes de carga y preparación en consola
        
        Raises:
            Exception: Si hay error al cargar el archivo (propagado desde load_data)
        
        Example:
            >>> analyzer = AcademicSortingAnalyzer("articles.csv")
            ✅ Datos cargados: 1,234 registros
            📋 Columnas disponibles: ['title', 'authors', 'publication_date', ...]
            📊 Datos preparados con 1,234 registros válidos
        """
        self.csv_file = csv_file
        self.df = None
        self.load_data()
        
    def load_data(self):
        """
        Carga los datos desde el archivo CSV y los prepara para ordenamiento.
        
        Lee el CSV especificado en __init__, muestra información sobre las
        columnas disponibles y llama a _prepare_data() para limpiar y
        normalizar los datos.
        
        Side Effects:
            - Popula self.df con el DataFrame cargado
            - Llama a _prepare_data() para agregar columnas calculadas
            - Imprime mensajes informativos en consola
        
        Raises:
            Exception: Si el archivo no existe, no es CSV válido, tiene
                encoding incorrecto, o cualquier otro error de lectura.
                En caso de error, inicializa self.df como DataFrame vacío
                para evitar AttributeError posteriores.
        
        Example:
            >>> analyzer = AcademicSortingAnalyzer("articles.csv")
            # load_data() se llama automáticamente
            ✅ Datos cargados: 5,432 registros
            📋 Columnas disponibles: ['id', 'title', 'authors', 'publication_date', ...]
            📊 Datos preparados con 5,432 registros válidos
        
        Note:
            Si el CSV no tiene encoding UTF-8, puede fallar. Considera
            modificar el parámetro encoding según tus necesidades.
        """
        try:
            # Cargar CSV con pandas
            self.df = pd.read_csv(self.csv_file, encoding='utf-8')
            print(f"✅ Datos cargados: {len(self.df)} registros")
            
            # Mostrar columnas disponibles para información del usuario
            print(f"📋 Columnas disponibles: {list(self.df.columns)}")
            
            # Limpiar y preparar datos para ordenamiento
            self._prepare_data()
            
        except Exception as e:
            print(f"❌ Error cargando datos: {e}")
            # Asegurar que self.df no quede en None para evitar AttributeError posteriores
            self.df = pd.DataFrame([])
            raise

    def _prepare_data(self):
        """
        Prepara los datos para ordenamiento añadiendo columnas calculadas.
        
        Método privado que realiza las siguientes transformaciones:
        1. Crea 'title_clean': Título sin valores NaN, convertido a string
        2. Crea 'year': Año extraído de 'publication_date' (0 si no hay fecha)
        3. Crea 'sort_key': Tupla (year, title_normalized) para ordenamiento
        
        La columna 'sort_key' es crítica ya que define el criterio de ordenamiento
        usado por todos los algoritmos: primero por año ascendente, luego por
        título alfabético (case-insensitive).
        
        Side Effects:
            - Añade columnas 'title_clean', 'year', 'sort_key' a self.df
            - Imprime mensaje de confirmación con número de registros
        
        Example:
            >>> # Llamado automáticamente por load_data()
            >>> # Después de preparar datos:
            >>> print(analyzer.df[['title', 'title_clean', 'year', 'sort_key']].head())
            
        Note:
            Si el DataFrame está vacío o es None, no hace nada y solo
            imprime una advertencia.
        """
        if self.df is None or self.df.empty:
            print("⚠️ DataFrame vacío. No se preparan datos de ordenamiento.")
            return
            
        # ===== PASO 1: LIMPIAR TÍTULO =====
        # Convertir NaN a string vacío y asegurar que todo sea string
        if 'title' in self.df.columns:
            self.df['title_clean'] = self.df['title'].fillna('').astype(str)
        
        # ===== PASO 2: EXTRAER AÑO DE PUBLICACIÓN =====
        if 'publication_date' in self.df.columns:
            # Aplicar función de extracción de año a cada fecha
            self.df['year'] = self.df['publication_date'].apply(self._extract_year)
        else:
            # Si no hay columna de fecha, usar año 0 para todos
            self.df['year'] = 0
            
        # ===== PASO 3: CREAR CLAVE DE ORDENAMIENTO =====
        # Tupla (año, título_normalizado) para ordenamiento compuesto
        self.df['sort_key'] = self.df.apply(
            lambda row: (row.get('year', 0), row.get('title_clean', '').lower().strip()), 
            axis=1
        )
        
        print(f"📊 Datos preparados con {len(self.df)} registros válidos")

    def _extract_year(self, date_str) -> int:
        """
        Extrae el año (4 dígitos) de una string de fecha.
        
        Busca un patrón de 4 dígitos que comience con 19 o 20 (años 1900-2099)
        en la string de fecha proporcionada. Esto permite manejar múltiples
        formatos de fecha sin necesidad de parsing complejo.
        
        Args:
            date_str: String que contiene una fecha en cualquier formato.
                Ejemplos: "2023-01-15", "January 2023", "2023", "01/15/2023"
        
        Returns:
            int: Año de 4 dígitos si se encuentra (1900-2099), 0 si no hay año válido.
        
        Algorithm:
            1. Verificar si es NaN o string vacío → retornar 0
            2. Buscar regex: \\b(19|20)\\d{2}\\b (año de 4 dígitos)
            3. Si se encuentra → retornar como int
            4. Si no se encuentra → retornar 0
        
        Example:
            >>> analyzer._extract_year("2023-01-15")
            2023
            >>> analyzer._extract_year("Published in 2023")
            2023
            >>> analyzer._extract_year("January 15, 2023")
            2023
            >>> analyzer._extract_year("No date here")
            0
            >>> analyzer._extract_year(None)
            0
            >>> analyzer._extract_year(pd.NaT)
            0
        
        Note:
            Solo encuentra años entre 1900-2099. Fechas antiguas (ej: 1850)
            o futuras lejanas (ej: 2150) retornarán 0.
        """
        # Verificar si es NaN o vacío
        if pd.isna(date_str) or date_str == '':
            return 0
            
        # Buscar patrón de año: 19xx o 20xx (4 dígitos)
        # \b = word boundary (límite de palabra)
        # (19|20) = empieza con 19 o 20
        # \d{2} = seguido de dos dígitos más
        year_match = re.search(r'\b(19|20)\d{2}\b', str(date_str))
        
        if year_match:
            return int(year_match.group())
        
        return 0

    def _create_sortable_data(self) -> List[Tuple]:
        """
        Crea una lista de tuplas preparada para ordenamiento.
        
        Convierte el DataFrame en una lista de tuplas con el formato:
        (año, título_normalizado, índice_original)
        
        Esta estructura es necesaria porque:
        1. Los algoritmos de ordenamiento operan sobre listas, no DataFrames
        2. Necesitamos mantener el índice original para reconstruir el DataFrame
        3. La tupla (año, título) se ordena naturalmente en Python
        
        Returns:
            List[Tuple]: Lista de tuplas con formato (year, title, original_index).
                year (int): Año de publicación
                title (str): Título normalizado en minúsculas sin espacios extra
                original_index: Índice de la fila en el DataFrame original
                
                Retorna lista vacía si df es None o está vacío.
        
        Example:
            >>> data = analyzer._create_sortable_data()
            >>> print(data[:3])
            [
                (2023, 'machine learning basics', 0),
                (2022, 'deep neural networks', 1),
                (2023, 'ai in healthcare', 2)
            ]
            
            >>> # Después de ordenar
            >>> sorted_data = sorted(data)
            >>> print(sorted_data[:3])
            [
                (2022, 'deep neural networks', 1),
                (2023, 'ai in healthcare', 2),
                (2023, 'machine learning basics', 0)
            ]
        
        Note:
            El índice original (tercer elemento) es crítico para poder
            reconstruir el DataFrame con _build_result_dataframe().
        """
        # Verificar que hay datos disponibles
        if self.df is None or self.df.empty:
            return []
            
        sortable_data = []
        
        # Iterar sobre cada fila del DataFrame
        for idx, row in self.df.iterrows():
            # Extraer año (0 si no existe)
            year = row.get('year', 0)
            
            # Extraer y normalizar título (minúsculas, sin espacios extra)
            title = row.get('title_clean', '').lower().strip()
            
            # Crear tupla: (año, título, índice_original)
            sortable_data.append((year, title, idx))
        
        return sortable_data

    def _build_result_dataframe(self, sorted_data: List[Tuple]) -> pd.DataFrame:
        """
        Reconstruye DataFrame a partir de datos ordenados.
        
        Toma la lista de tuplas ordenadas y reconstruye un DataFrame con las
        filas en el nuevo orden. Esto permite retornar el dataset completo
        ordenado, no solo una lista de tuplas.
        
        Args:
            sorted_data (List[Tuple]): Lista de tuplas ordenadas en formato
                (year, title, original_index). El tercer elemento (índice)
                se usa para recuperar las filas del DataFrame original.
        
        Returns:
            pd.DataFrame: DataFrame con todas las columnas originales, pero
                con las filas reordenadas según sorted_data. Los índices se
                resetean a 0, 1, 2, ... N-1.
                
                Retorna DataFrame vacío si self.df es None o está vacío.
        
        Algorithm:
            1. Extraer índices originales del tercer elemento de cada tupla
            2. Usar iloc para obtener filas en el orden especificado
            3. Resetear índices para que sean secuenciales
        
        Example:
            >>> sorted_data = [(2022, 'title a', 5), (2023, 'title b', 2)]
            >>> result_df = analyzer._build_result_dataframe(sorted_data)
            >>> print(result_df.index.tolist())
            [0, 1]  # Índices reseteados
            >>> # Las filas corresponden a los índices originales 5 y 2
        
        Note:
            El DataFrame resultante tiene los mismos datos que el original,
            solo cambia el orden de las filas.
        """
        # Verificar que hay DataFrame disponible
        if self.df is None or self.df.empty:
            return pd.DataFrame([])
            
        # Extraer índices originales (tercer elemento de cada tupla)
        sorted_indices = [item[2] for item in sorted_data if len(item) > 2]
        
        # Obtener filas en el orden especificado y resetear índices
        return self.df.iloc[sorted_indices].reset_index(drop=True)

    # ==================== FUNCIONES AUXILIARES PARA ALGORITMOS ====================

    def _heapify(self, arr, n, i):
        """
        Función heapify para HeapSort - mantiene propiedad de heap máximo.
        
        Reordena el subárbol con raíz en el índice i para mantener la propiedad
        de max-heap (padre >= hijos). Se usa tanto en la construcción inicial
        del heap como en la fase de extracción.
        
        Args:
            arr (list): Arreglo a heapificar (modificado in-place)
            n (int): Tamaño del heap a considerar
            i (int): Índice de la raíz del subárbol a heapificar
        
        Algorithm:
            1. Asumir que i es el más grande
            2. Calcular índices de hijos: left = 2*i+1, right = 2*i+2
            3. Si hijo izquierdo > raíz, actualizar índice del más grande
            4. Si hijo derecho > más grande actual, actualizar
            5. Si el más grande no es i, intercambiar y heapificar recursivamente
        
        Complexity:
            Tiempo: O(log n) - altura del árbol
            Espacio: O(log n) - stack de recursión
        
        Example:
            >>> arr = [3, 5, 1, 4, 2]
            >>> analyzer._heapify(arr, 5, 0)
            # arr puede quedar como [5, 4, 1, 3, 2] dependiendo de la estructura
        """
        largest = i         # Inicializar largest como raíz
        left = 2 * i + 1    # Hijo izquierdo = 2*i + 1
        right = 2 * i + 2   # Hijo derecho = 2*i + 2
        
        # Ver si hijo izquierdo existe y es mayor que raíz
        if left < n and arr[left] > arr[largest]:
            largest = left
        
        # Ver si hijo derecho existe y es mayor que el más grande actual
        if right < n and arr[right] > arr[largest]:
            largest = right
        
        # Si el más grande no es la raíz, intercambiar
        if largest != i:
            arr[i], arr[largest] = arr[largest], arr[i]
            
            # Heapificar recursivamente el subárbol afectado
            self._heapify(arr, n, largest)

    def _quick_sort_partition(self, arr, low, high):
        """
        Función de partición para QuickSort.
        
        Particiona el arreglo alrededor de un pivot (último elemento), colocando
        todos los elementos menores a la izquierda y mayores a la derecha.
        
        Args:
            arr (list): Arreglo a particionar (modificado in-place)
            low (int): Índice inicial del segmento a particionar
            high (int): Índice final del segmento (se usa como pivot)
        
        Returns:
            int: Índice final del pivot después de la partición
        
        Algorithm (Esquema de Lomuto):
            1. Elegir pivot = arr[high] (último elemento)
            2. i = low - 1 (índice del elemento más pequeño)
            3. Para cada elemento j de low a high-1:
                Si arr[j] <= pivot:
                    Incrementar i
                    Intercambiar arr[i] con arr[j]
            4. Colocar pivot en su posición final: intercambiar arr[i+1] con arr[high]
            5. Retornar i+1 (posición del pivot)
        
        Example:
            >>> arr = [3, 1, 4, 1, 5, 9, 2, 6]
            >>> pi = analyzer._quick_sort_partition(arr, 0, 7)
            # arr queda particionado alrededor del pivot (6)
            # pi es la posición final del pivot
        """
        pivot = arr[high]  # Elegir último elemento como pivot
        i = low - 1        # Índice del elemento más pequeño
        
        # Recorrer desde low hasta high-1
        for j in range(low, high):
            # Si elemento actual es menor o igual al pivot
            if arr[j] <= pivot:
                i += 1
                # Intercambiar arr[i] con arr[j]
                arr[i], arr[j] = arr[j], arr[i]
        
        # Colocar pivot en su posición correcta
        arr[i + 1], arr[high] = arr[high], arr[i + 1]
        return i + 1

    def _quick_sort_recursive(self, arr, low, high):
        """
        Función recursiva principal de QuickSort.
        
        Implementa el algoritmo divide-y-conquista de QuickSort:
        1. Particionar el arreglo
        2. Ordenar recursivamente la parte izquierda
        3. Ordenar recursivamente la parte derecha
        
        Args:
            arr (list): Arreglo a ordenar (modificado in-place)
            low (int): Índice inicial del segmento a ordenar
            high (int): Índice final del segmento a ordenar
        
        Base Case:
            Si low >= high, el segmento tiene 0 o 1 elementos (ya ordenado)
        
        Example:
            >>> arr = [3, 1, 4, 1, 5, 9, 2, 6, 5]
            >>> analyzer._quick_sort_recursive(arr, 0, len(arr)-1)
            >>> print(arr)
            [1, 1, 2, 3, 4, 5, 5, 6, 9]
        """
        if low < high:
            # Particionar y obtener índice del pivot
            pi = self._quick_sort_partition(arr, low, high)
            
            # Ordenar elementos antes del pivot
            self._quick_sort_recursive(arr, low, pi - 1)
            
            # Ordenar elementos después del pivot
            self._quick_sort_recursive(arr, pi + 1, high)

    def _insert_tree_node(self, root, val):
        """
        Inserta un nuevo nodo en el árbol binario de búsqueda (BST).
        
        Inserta recursivamente un valor en el BST manteniendo la propiedad:
        - Valores menores van a la izquierda
        - Valores mayores o iguales van a la derecha
        
        Args:
            root (TreeNode): Raíz del árbol o subárbol
            val: Valor a insertar (típicamente tupla (year, title, index))
        
        Returns:
            TreeNode: Nueva raíz del árbol después de la inserción
        
        Example:
            >>> root = None
            >>> root = analyzer._insert_tree_node(root, (2023, "title a", 0))
            >>> root = analyzer._insert_tree_node(root, (2022, "title b", 1))
            >>> root = analyzer._insert_tree_node(root, (2024, "title c", 2))
            # Estructura del árbol:
            #       (2023, "title a", 0)
            #      /                    \\
            # (2022, "title b", 1)   (2024, "title c", 2)
        """
        # Caso base: si no hay nodo, crear uno nuevo
        if root is None:
            return TreeNode(val)
            
        # Insertar en subárbol izquierdo si es menor
        if val < root.val:
            root.left = self._insert_tree_node(root.left, val)
        # Insertar en subárbol derecho si es mayor o igual
        else:
            root.right = self._insert_tree_node(root.right, val)
            
        return root

    def _inorder_traversal(self, root, result):
        """
        Recorrido inorden (in-order traversal) del árbol binario.
        
        Recorre el árbol en orden: izquierda → raíz → derecha.
        Este recorrido en un BST produce los elementos en orden ascendente.
        
        Args:
            root (TreeNode): Raíz del árbol o subárbol a recorrer
            result (list): Lista donde se acumulan los valores ordenados
                (modificada in-place)
        
        Algorithm:
            1. Recorrer subárbol izquierdo recursivamente
            2. Visitar raíz (agregar a result)
            3. Recorrer subárbol derecho recursivamente
        
        Example:
            >>> # Árbol:  5
            >>> #         / \\
            >>> #        3   7
            >>> result = []
            >>> analyzer._inorder_traversal(root, result)
            >>> print(result)
            [3, 5, 7]  # Orden ascendente
        """
        if root:
            # Primero recorrer subárbol izquierdo
            self._inorder_traversal(root.left, result)
            
            # Luego visitar raíz
            result.append(root.val)
            
            # Finalmente recorrer subárbol derecho
            self._inorder_traversal(root.right, result)

    def _bitonic_merge(self, arr, low, cnt, up):
        """
        Función merge para BitonicSort - fusiona secuencia bitónica.
        
        Una secuencia bitónica es aquella que primero crece y luego decrece
        (o viceversa). Este método fusiona recursivamente comparando elementos
        a distancia k y intercambiándolos según la dirección de ordenamiento.
        
        Args:
            arr (list): Arreglo a fusionar (modificado in-place)
            low (int): Índice inicial del segmento
            cnt (int): Número de elementos en el segmento (debe ser potencia de 2)
            up (bool): Dirección de ordenamiento (True=ascendente, False=descendente)
        
        Note:
            BitonicSort es útil para procesamiento paralelo ya que las
            comparaciones en cada nivel pueden hacerse independientemente.
        """
        if cnt > 1:
            k = cnt // 2
            
            # Comparar y posiblemente intercambiar elementos a distancia k
            for i in range(low, low + k):
                # Intercambiar si (arr[i] > arr[i+k]) == up
                # Esto significa: si vamos ascendente y arr[i] > arr[i+k], intercambiar
                if (arr[i] > arr[i + k]) == up:
                    arr[i], arr[i + k] = arr[i + k], arr[i]
            
            # Recursivamente fusionar mitades
            self._bitonic_merge(arr, low, k, up)
            self._bitonic_merge(arr, low + k, k, up)

    def _bitonic_sort_recursive(self, arr, low, cnt, up):
        """
        Función recursiva principal de BitonicSort.
        
        Crea recursivamente una secuencia bitónica y luego la fusiona.
        El algoritmo requiere que el tamaño del arreglo sea potencia de 2.
        
        Args:
            arr (list): Arreglo a ordenar (modificado in-place)
            low (int): Índice inicial del segmento
            cnt (int): Número de elementos (debe ser potencia de 2)
            up (bool): Dirección de ordenamiento
        """
        if cnt > 1:
            k = cnt // 2
            
            # Ordenar primera mitad en orden ascendente
            self._bitonic_sort_recursive(arr, low, k, True)
            
            # Ordenar segunda mitad en orden descendente
            self._bitonic_sort_recursive(arr, low + k, k, False)
            
            # Fusionar toda la secuencia en dirección 'up'
            self._bitonic_merge(arr, low, cnt, up)

    def _binary_search_insertion(self, arr, val, start, end):
        """
        Búsqueda binaria para encontrar posición de inserción.
        
        Encuentra la posición correcta donde insertar 'val' en arr[start:end+1]
        para mantener el orden. Usado por BinaryInsertionSort.
        
        Args:
            arr (list): Arreglo ordenado donde buscar posición
            val: Valor a insertar
            start (int): Índice inicial de búsqueda
            end (int): Índice final de búsqueda
        
        Returns:
            int: Índice donde debe insertarse val
        
        Example:
            >>> arr = [1, 3, 5, 7, 9]
            >>> pos = analyzer._binary_search_insertion(arr, 6, 0, 4)
            >>> print(pos)
            3  # Debe insertarse entre 5 y 7
        """
        # Caso base 1: solo un elemento
        if start == end:
            return start if arr[start] > val else start + 1
            
        # Caso base 2: rango inválido
        if start > end:
            return start
        
        # Buscar en mitad del rango
        mid = (start + end) // 2
        
        if arr[mid] < val:
            # val va en la mitad derecha
            return self._binary_search_insertion(arr, val, mid + 1, end)
        elif arr[mid] > val:
            # val va en la mitad izquierda
            return self._binary_search_insertion(arr, val, start, mid - 1)
        else:
            # Valor igual encontrado
            return mid

    def _counting_sort_for_radix(self, arr, exp):
        """
        Counting Sort estable para RadixSort según un dígito específico.
        
        Ordena el arreglo según el dígito en la posición 'exp' (unidades,
        decenas, centenas, etc.) manteniendo estabilidad (orden relativo de
        elementos iguales se preserva).
        
        Args:
            arr (list): Arreglo de tuplas (year, title, index) a ordenar
            exp (int): Posición del dígito (1=unidades, 10=decenas, 100=centenas)
        
        Algorithm:
            1. Contar ocurrencias de cada dígito (0-9)
            2. Calcular posiciones acumulativas
            3. Construir arreglo de salida en orden estable
            4. Copiar de vuelta al arreglo original
        """
        n = len(arr)
        output = [None] * n  # Arreglo de salida
        count = [0] * 10     # Contador para dígitos 0-9
        
        # ===== PASO 1: CONTAR OCURRENCIAS =====
        for i in range(n):
            # Extraer dígito en posición exp del año (primer elemento de tupla)
            index = (arr[i][0] // exp) % 10
            count[index] += 1
        
        # ===== PASO 2: CALCULAR POSICIONES ACUMULATIVAS =====
        for i in range(1, 10):
            count[i] += count[i - 1]
        
        # ===== PASO 3: CONSTRUIR ARREGLO DE SALIDA (desde el final para estabilidad) =====
        i = n - 1
        while i >= 0:
            index = (arr[i][0] // exp) % 10
            output[count[index] - 1] = arr[i]
            count[index] -= 1
            i -= 1
        
        # ===== PASO 4: COPIAR RESULTADO A ARREGLO ORIGINAL =====
        for i in range(n):
            arr[i] = output[i]

    # ==================== ALGORITMOS DE ORDENAMIENTO ====================

    def tim_sort(self) -> Tuple[pd.DataFrame, float]:
        """
        TimSort - Algoritmo híbrido de ordenamiento usado por Python.
        
        TimSort combina Merge Sort e Insertion Sort, aprovechando las ventajas
        de ambos. Es el algoritmo usado internamente por Python en sorted() y
        list.sort(). Es estable y tiene excelente rendimiento en datos reales.
        
        Returns:
            Tuple[pd.DataFrame, float]: Tupla con:
                - DataFrame ordenado por (año, título)
                - Tiempo de ejecución en segundos
        
        Complexity:
            - Tiempo mejor caso: O(n) - datos casi ordenados
            - Tiempo promedio: O(n log n)
            - Tiempo peor caso: O(n log n)
            - Espacio: O(n)
        
        Advantages:
            - Extremadamente rápido en datos reales
            - Estable (preserva orden de elementos iguales)
            - Adaptativo (aprovecha orden existente)
            - Implementación nativa optimizada en C
        
        Use Cases:
            - Datasets de cualquier tamaño
            - Cuando se necesita estabilidad
            - Datos parcialmente ordenados
            - Uso general (mejor opción por defecto)
        
        Example:
            >>> df_sorted, time_taken = analyzer.tim_sort()
            >>> print(f"Ordenado en {time_taken*1000:.2f}ms")
            >>> print(df_sorted[['year', 'title']].head())
        """
        # Crear lista de tuplas ordenables
        data = self._create_sortable_data()
        
        # Medir tiempo de ejecución
        start_time = time.perf_counter()
        
        # Python usa TimSort internamente en sorted()
        sorted_data = sorted(data, key=lambda x: (x[0], x[1]))
        
        end_time = time.perf_counter()
        
        # Reconstruir DataFrame con orden nuevo
        result_df = self._build_result_dataframe(sorted_data)
        return result_df, end_time - start_time

    def comb_sort(self) -> Tuple[pd.DataFrame, float]:
        """
        CombSort - Mejora de BubbleSort con gap decreciente.
        
        CombSort mejora BubbleSort usando un "gap" (distancia entre elementos
        comparados) que decrece gradualmente con factor de reducción 1.3.
        Elimina "tortugas" (valores pequeños al final) más rápido que BubbleSort.
        
        Returns:
            Tuple[pd.DataFrame, float]: Tupla con DataFrame ordenado y tiempo.
        
        Complexity:
            - Tiempo promedio: O(n²/2^p) donde p es número de incrementos
            - Tiempo peor caso: O(n²)
            - Espacio: O(1) - in-place
        
        Algorithm:
            1. Empezar con gap = n
            2. Reducir gap por factor 1.3 en cada iteración
            3. Comparar elementos a distancia gap
            4. Intercambiar si están en orden incorrecto
            5. Repetir hasta gap = 1 y no haya más intercambios
        
        Advantages:
            - Más rápido que BubbleSort
            - Simple de implementar
            - In-place (no usa memoria extra)
        
        Disadvantages:
            - Sigue siendo O(n²) en peor caso
            - No estable
            - Superado por algoritmos O(n log n)
        
        Example:
            >>> df_sorted, time_taken = analyzer.comb_sort()
        """
        data = self._create_sortable_data()
        
        start_time = time.perf_counter()
        
        n = len(data)
        gap = n              # Gap inicial = tamaño del arreglo
        shrink = 1.3         # Factor de reducción óptimo
        sorted_flag = False  # Indica si hay que seguir iterando
        
        while not sorted_flag:
            # Reducir gap
            gap = int(gap / shrink)
            if gap <= 1:
                gap = 1
                sorted_flag = True  # Última pasada con gap=1
            
            i = 0
            # Comparar elementos a distancia gap
            while i + gap < n:
                if data[i] > data[i + gap]:
                    # Intercambiar elementos desordenados
                    data[i], data[i + gap] = data[i + gap], data[i]
                    sorted_flag = False  # Hubo cambio, seguir iterando
                i += 1
        
        end_time = time.perf_counter()
        result_df = self._build_result_dataframe(data)
        return result_df, end_time - start_time

    def selection_sort(self) -> Tuple[pd.DataFrame, float]:
        """
        SelectionSort - Algoritmo simple de selección del mínimo.
        
        En cada iteración, encuentra el elemento mínimo del segmento no ordenado
        y lo intercambia con el primer elemento de ese segmento. Simple pero
        ineficiente para datasets grandes.
        
        Returns:
            Tuple[pd.DataFrame, float]: Tupla con DataFrame ordenado y tiempo.
        
        Complexity:
            - Tiempo todos los casos: O(n²)
            - Espacio: O(1) - in-place
            - Comparaciones: n(n-1)/2 siempre
            - Intercambios: O(n) - solo uno por iteración
        
        Algorithm:
            1. Para cada posición i de 0 a n-1:
               a. Encontrar índice del mínimo en arr[i:n]
               b. Intercambiar arr[i] con el mínimo
        
        Advantages:
            - Muy simple de entender e implementar
            - Mínimo número de intercambios: O(n)
            - In-place (no usa memoria extra)
        
        Disadvantages:
            - O(n²) siempre, incluso si ya está ordenado
            - No estable en implementación básica
            - Muy lento para datasets >5000 elementos
        
        Use Cases:
            - Datasets muy pequeños (<100 elementos)
            - Cuando el costo de intercambios es alto
            - Propósitos educativos
        
        Example:
            >>> df_sorted, time_taken = analyzer.selection_sort()
            # Para 1000 elementos: ~100-200ms
            # Para 10000 elementos: ~10-20 segundos
        """
        data = self._create_sortable_data()
        
        start_time = time.perf_counter()
        
        n = len(data)
        # Para cada posición en el arreglo
        for i in range(n):
            min_idx = i  # Asumir que el mínimo es el elemento actual
            
            # Buscar el mínimo en el resto del arreglo
            for j in range(i + 1, n):
                if data[j] < data[min_idx]:
                    min_idx = j
            
            # Intercambiar el mínimo encontrado con el elemento en posición i
            data[i], data[min_idx] = data[min_idx], data[i]
        
        end_time = time.perf_counter()
        result_df = self._build_result_dataframe(data)
        return result_df, end_time - start_time

    def tree_sort(self) -> Tuple[pd.DataFrame, float]:
        """
        TreeSort - Ordenamiento mediante árbol binario de búsqueda (BST).
        
        Construye un árbol binario de búsqueda insertando todos los elementos,
        luego realiza un recorrido inorden para obtener los elementos ordenados.
        
        Returns:
            Tuple[pd.DataFrame, float]: Tupla con DataFrame ordenado y tiempo.
        
        Complexity:
            - Tiempo promedio: O(n log n) - árbol balanceado
            - Tiempo peor caso: O(n²) - árbol desbalanceado (datos ordenados)
            - Espacio: O(n) - nodos del árbol
        
        Algorithm:
            1. Crear árbol vacío (root = None)
            2. Insertar cada elemento en el BST
            3. Recorrer árbol inorden (izquierda → raíz → derecha)
            4. El recorrido produce elementos en orden ascendente
        
        Advantages:
            - O(n log n) en promedio
            - Útil si necesitas mantener el árbol después
            - Fácil de entender conceptualmente
        
        Disadvantages:
            - O(n²) en peor caso (datos ya ordenados)
            - Usa O(n) espacio adicional
            - No estable
            - Puede causar stack overflow por recursión profunda
        
        Use Cases:
            - Cuando necesitas el BST para otras operaciones
            - Datasets con datos aleatorios
            - Propósitos educativos
        
        Example:
            >>> df_sorted, time_taken = analyzer.tree_sort()
        """
        data = self._create_sortable_data()
        
        start_time = time.perf_counter()
        
        # Construir árbol binario de búsqueda
        root = None
        for item in data:
            root = self._insert_tree_node(root, item)
        
        # Recorrer árbol inorden para obtener elementos ordenados
        sorted_data = []
        self._inorder_traversal(root, sorted_data)
        
        end_time = time.perf_counter()
        result_df = self._build_result_dataframe(sorted_data)
        return result_df, end_time - start_time

    def pigeonhole_sort(self) -> Tuple[pd.DataFrame, float]:
        """
        PigeonholeSort - Ordenamiento por distribución en casilleros.
        
        Distribuye elementos en "casilleros" (pigeonholes) según su valor (año),
        luego ordena cada casillero y concatena. Eficiente cuando el rango de
        valores es pequeño comparado con el número de elementos.
        
        Returns:
            Tuple[pd.DataFrame, float]: Tupla con DataFrame ordenado y tiempo.
        
        Complexity:
            - Tiempo: O(n + k) donde k = rango de años
            - Espacio: O(n + k)
        
        Algorithm:
            1. Encontrar rango de años: [min_year, max_year]
            2. Crear k casilleros (uno por cada año posible)
            3. Distribuir elementos por año en casilleros
            4. Ordenar cada casillero por título
            5. Concatenar casilleros en orden
        
        Advantages:
            - O(n + k) muy rápido si k es pequeño
            - Simple de implementar
            - Estable si se implementa correctamente
        
        Disadvantages:
            - Requiere conocer el rango de valores
            - Ineficiente si k >> n
            - Usa O(k) espacio adicional
        
        Use Cases:
            - Datos con rango pequeño y conocido (años, calificaciones)
            - Cuando k ≈ n
            - Distribuciones uniformes
        
        Example:
            >>> df_sorted, time_taken = analyzer.pigeonhole_sort()
            # Para datos de 2000-2025 (k=25): muy rápido
            # Para datos de 1800-2025 (k=225): menos eficiente
        """
        data = self._create_sortable_data()
        
        start_time = time.perf_counter()
        
        # Manejar caso de datos vacíos
        if not data:
            end_time = time.perf_counter()
            return (self.df.copy() if isinstance(self.df, pd.DataFrame) else pd.DataFrame([]), 
                    end_time - start_time)
        
        # Extraer años y calcular rango
        years = [item[0] for item in data]
        min_year = min(years)
        max_year = max(years)
        range_years = max_year - min_year + 1
        
        # Crear casilleros (pigeonholes) - uno por cada año posible
        pigeonholes = [[] for _ in range(range_years)]
        
        # Distribuir elementos en casilleros según año
        for item in data:
            year_idx = item[0] - min_year  # Índice del casillero
            pigeonholes[year_idx].append(item)
        
        # Ordenar cada casillero por título y concatenar
        sorted_data = []
        for hole in pigeonholes:
            hole.sort(key=lambda x: x[1])  # Ordenar por título
            sorted_data.extend(hole)
        
        end_time = time.perf_counter()
        result_df = self._build_result_dataframe(sorted_data)
        return result_df, end_time - start_time

    def bucket_sort(self) -> Tuple[pd.DataFrame, float]:
        """
        BucketSort - Ordenamiento por distribución en cubetas.
        
        Similar a PigeonholeSort, pero usa años únicos como buckets en lugar
        de crear un bucket por cada año posible. Más eficiente en memoria
        para rangos grandes con gaps.
        
        Returns:
            Tuple[pd.DataFrame, float]: Tupla con DataFrame ordenado y tiempo.
        
        Complexity:
            - Tiempo promedio: O(n + k) donde k = número de años únicos
            - Tiempo peor caso: O(n²) si todos en un bucket
            - Espacio: O(n + k)
        
        Algorithm:
            1. Identificar años únicos en los datos
            2. Crear un bucket por cada año único
            3. Distribuir elementos en buckets según año
            4. Ordenar cada bucket por título
            5. Concatenar buckets en orden de año
        
        Advantages:
            - O(n + k) si distribución uniforme
            - Usa menos memoria que PigeonholeSort para rangos con gaps
            - Flexible con distribución de valores
        
        Disadvantages:
            - O(n²) si distribución muy desigual
            - Requiere ordenar cada bucket
        
        Differences from PigeonholeSort:
            - PigeonholeSort: bucket por cada valor posible en rango
            - BucketSort: bucket solo por valores que existen
            
            Ejemplo: años [2000, 2005, 2025]
            - PigeonholeSort: 26 buckets (2000-2025)
            - BucketSort: 3 buckets (2000, 2005, 2025)
        
        Example:
            >>> df_sorted, time_taken = analyzer.bucket_sort()
        """
        data = self._create_sortable_data()
        
        start_time = time.perf_counter()
        
        # Manejar caso de datos vacíos
        if not data:
            end_time = time.perf_counter()
            return (self.df.copy() if isinstance(self.df, pd.DataFrame) else pd.DataFrame([]), 
                    end_time - start_time)
        
        # Obtener años únicos presentes en los datos
        years = set(item[0] for item in data)
        
        # Crear diccionario de buckets - uno por año único
        year_buckets = {year: [] for year in years}
        
        # Distribuir elementos en buckets según año
        for item in data:
            year_buckets[item[0]].append(item)
        
        # Ordenar cada bucket por título y concatenar en orden de año
        sorted_data = []
        for year in sorted(years):  # Procesar años en orden ascendente
            bucket = year_buckets[year]
            bucket.sort(key=lambda x: x[1])  # Ordenar por título
            sorted_data.extend(bucket)
        
        end_time = time.perf_counter()
        result_df = self._build_result_dataframe(sorted_data)
        return result_df, end_time - start_time

    def quick_sort(self) -> Tuple[pd.DataFrame, float]:
        """
        QuickSort - Algoritmo divide-y-conquista clásico.
        
        Uno de los algoritmos más rápidos en la práctica. Usa estrategia
        divide-y-conquista: particiona el arreglo alrededor de un pivot y
        ordena recursivamente las partes.
        
        Returns:
            Tuple[pd.DataFrame, float]: Tupla con DataFrame ordenado y tiempo.
        
        Complexity:
            - Tiempo promedio: O(n log n)
            - Tiempo peor caso: O(n²) - datos ordenados con pivot mal elegido
            - Espacio: O(log n) - stack de recursión
        
        Algorithm:
            1. Elegir pivot (último elemento en esta implementación)
            2. Particionar: menores a la izquierda, mayores a la derecha
            3. Ordenar recursivamente parte izquierda
            4. Ordenar recursivamente parte derecha
        
        Advantages:
            - Muy rápido en promedio: O(n log n)
            - In-place: usa O(log n) espacio
            - Cache-friendly: buena localidad de referencia
            - Usado ampliamente en sistemas de producción
        
        Disadvantages:
            - O(n²) en peor caso (raro con pivot aleatorio)
            - No estable en implementación estándar
            - Recursión profunda puede causar stack overflow
        
        Improvements:
            - Elegir pivot aleatorio o mediana de tres
            - Cambiar a InsertionSort para subarreglos pequeños
            - Implementación iterativa para evitar stack overflow
        
        Example:
            >>> df_sorted, time_taken = analyzer.quick_sort()
            # Típicamente uno de los más rápidos junto con TimSort
        """
        data = self._create_sortable_data()
        
        start_time = time.perf_counter()
        # Ordenar in-place mediante recursión
        self._quick_sort_recursive(data, 0, len(data) - 1)
        end_time = time.perf_counter()
        
        result_df = self._build_result_dataframe(data)
        return result_df, end_time - start_time

    def heap_sort(self) -> Tuple[pd.DataFrame, float]:
        """
        HeapSort - Ordenamiento mediante heap binario.
        
        Construye un max-heap del arreglo, luego extrae repetidamente el
        máximo (raíz) y lo coloca al final. Garantiza O(n log n) en todos
        los casos.
        
        Returns:
            Tuple[pd.DataFrame, float]: Tupla con DataFrame ordenado y tiempo.
        
        Complexity:
            - Tiempo todos los casos: O(n log n)
            - Espacio: O(1) - in-place
        
        Algorithm:
            FASE 1: Construir Max-Heap
            1. Para cada nodo desde n/2-1 hasta 0:
               Heapificar subárbol con raíz en ese nodo
            
            FASE 2: Extraer Elementos
            2. Para i desde n-1 hasta 1:
               a. Intercambiar arr[0] (máximo) con arr[i]
               b. Heapificar arr[0:i] para restaurar max-heap
        
        Advantages:
            - O(n log n) garantizado (sin peor caso O(n²))
            - In-place: O(1) espacio adicional
            - No requiere recursión (puede ser iterativo)
            - Útil cuando memoria es limitada
        
        Disadvantages:
            - No estable
            - Constante mayor que QuickSort en promedio
            - Pobre localidad de cache
        
        Use Cases:
            - Cuando se requiere O(n log n) garantizado
            - Memoria limitada (in-place)
            - Sistemas en tiempo real (tiempo predecible)
        
        Example:
            >>> df_sorted, time_taken = analyzer.heap_sort()
        """
        data = self._create_sortable_data()
        
        start_time = time.perf_counter()
        
        n = len(data)
        
        # ===== FASE 1: CONSTRUIR MAX-HEAP =====
        # Heapificar desde el último nodo interno hasta la raíz
        for i in range(n // 2 - 1, -1, -1):
            self._heapify(data, n, i)
        
        # ===== FASE 2: EXTRAER ELEMENTOS UNO POR UNO =====
        for i in range(n - 1, 0, -1):
            # Mover raíz actual (máximo) al final
            data[0], data[i] = data[i], data[0]
            
            # Heapificar el heap reducido
            self._heapify(data, i, 0)
        
        end_time = time.perf_counter()
        result_df = self._build_result_dataframe(data)
        return result_df, end_time - start_time

    def bitonic_sort(self) -> Tuple[pd.DataFrame, float]:
        """
        BitonicSort - Ordenamiento para procesamiento paralelo.
        
        Algoritmo diseñado para hardware paralelo. Construye una secuencia
        bitónica (primero crece, luego decrece) y la fusiona. Requiere que
        el tamaño del arreglo sea potencia de 2.
        
        Returns:
            Tuple[pd.DataFrame, float]: Tupla con DataFrame ordenado y tiempo.
        
        Complexity:
            - Tiempo: O(n log²n) - más lento que O(n log n)
            - Espacio: O(n) - puede necesitar padding
            - Pero: Altamente paralelizable
        
        Algorithm:
            1. Extender arreglo a siguiente potencia de 2 (rellenar con valores grandes)
            2. Construir secuencia bitónica recursivamente:
               - Primera mitad ordenar ascendente
               - Segunda mitad ordenar descendente
            3. Fusionar secuencia bitónica
            4. Remover padding agregado
        
        Advantages:
            - Excelente para GPUs y hardware paralelo
            - Todas las comparaciones en cada nivel son independientes
            - Determinístico y predecible
        
        Disadvantages:
            - O(n log²n) más lento que O(n log n) en secuencial
            - Requiere tamaño potencia de 2
            - Complejo de implementar correctamente
        
        Use Cases:
            - Procesamiento paralelo en GPU
            - Hardware especializado (FPGAs)
            - Cuando se requiere alta paralelización
        
        Example:
            >>> df_sorted, time_taken = analyzer.bitonic_sort()
            # Más lento en CPU secuencial, pero útil para demostración
        """
        data = self._create_sortable_data()
        
        start_time = time.perf_counter()
        
        # ===== PASO 1: EXTENDER A POTENCIA DE 2 =====
        n = len(data)
        # Calcular siguiente potencia de 2
        next_power_of_2 = 1 << (n - 1).bit_length()
        
        # Llenar con elementos muy grandes (irán al final)
        max_item = (9999, 'zzzzz', -1)
        while len(data) < next_power_of_2:
            data.append(max_item)
        
        # ===== PASO 2: ORDENAR BITÓNICAMENTE =====
        self._bitonic_sort_recursive(data, 0, len(data), True)
        
        # ===== PASO 3: REMOVER PADDING =====
        data = data[:n]
        
        end_time = time.perf_counter()
        result_df = self._build_result_dataframe(data)
        return result_df, end_time - start_time

    def gnome_sort(self) -> Tuple[pd.DataFrame, float]:
        """
        GnomeSort - Algoritmo simple similar a InsertionSort.
        
        También conocido como "Stupid Sort". Similar a InsertionSort pero más
        simple: compara elementos adyacentes, si están en orden correcto avanza,
        si no los intercambia y retrocede.
        
        Returns:
            Tuple[pd.DataFrame, float]: Tupla con DataFrame ordenado y tiempo.
        
        Complexity:
            - Tiempo mejor caso: O(n) - datos ya ordenados
            - Tiempo promedio: O(n²)
            - Tiempo peor caso: O(n²)
            - Espacio: O(1) - in-place
        
        Algorithm:
            1. Empezar en posición 0
            2. Si está en inicio o elemento >= anterior:
               Avanzar una posición
            3. Si elemento < anterior:
               Intercambiar con anterior
               Retroceder una posición
            4. Repetir hasta llegar al final
        
        Advantages:
            - Extremadamente simple de implementar
            - O(n) si ya está casi ordenado
            - Estable
            - In-place
        
        Disadvantages:
            - O(n²) en promedio y peor caso
            - Muy lento para datasets grandes
            - No tiene ventajas sobre InsertionSort
        
        Use Cases:
            - Propósitos educativos
            - Datasets muy pequeños
            - Cuando simplicidad es más importante que eficiencia
        
        Example:
            >>> df_sorted, time_taken = analyzer.gnome_sort()
        """
        data = self._create_sortable_data()
        
        start_time = time.perf_counter()
        
        index = 0
        n = len(data)
        
        while index < n:
            # Si estamos en el inicio o el elemento actual >= anterior
            if index == 0 or data[index] >= data[index - 1]:
                index += 1  # Avanzar
            else:
                # Elemento actual < anterior: intercambiar y retroceder
                data[index], data[index - 1] = data[index - 1], data[index]
                index -= 1
        
        end_time = time.perf_counter()
        result_df = self._build_result_dataframe(data)
        return result_df, end_time - start_time

    def binary_insertion_sort(self) -> Tuple[pd.DataFrame, float]:
        """
        BinaryInsertionSort - InsertionSort optimizado con búsqueda binaria.
        
        Mejora InsertionSort usando búsqueda binaria para encontrar la posición
        de inserción, reduciendo comparaciones de O(n²) a O(n log n). Sin embargo,
        los movimientos siguen siendo O(n²).
        
        Returns:
            Tuple[pd.DataFrame, float]: Tupla con DataFrame ordenado y tiempo.
        
        Complexity:
            - Comparaciones: O(n log n) - búsqueda binaria
            - Movimientos: O(n²) - shift de elementos
            - Tiempo total: O(n²) - dominado por movimientos
            - Espacio: O(1) - in-place
        
        Algorithm:
            1. Para cada elemento i desde 1 hasta n-1:
               a. Usar búsqueda binaria para encontrar posición correcta en arr[0:i]
               b. Desplazar elementos hacia la derecha
               c. Insertar elemento en posición encontrada
        
        Advantages:
            - Menos comparaciones que InsertionSort estándar
            - Estable
            - In-place
            - O(n) en mejor caso (datos ordenados)
        
        Disadvantages:
            - Sigue siendo O(n²) debido a movimientos
            - No mejora significativamente el rendimiento total
            - Más complejo que InsertionSort estándar
        
        Use Cases:
            - Cuando comparaciones son caras pero movimientos baratos
            - Propósitos educativos
            - Datasets pequeños a medianos
        
        Example:
            >>> df_sorted, time_taken = analyzer.binary_insertion_sort()
        """
        data = self._create_sortable_data()
        
        start_time = time.perf_counter()
        
        n = len(data)
        
        # Para cada elemento desde el segundo hasta el último
        for i in range(1, n):
            key = data[i]
            
            # Encontrar posición de inserción usando búsqueda binaria
            pos = self._binary_search_insertion(data, key, 0, i - 1)
            
            # Desplazar elementos hacia la derecha para hacer espacio
            for j in range(i - 1, pos - 1, -1):
                data[j + 1] = data[j]
            
            # Insertar elemento en su posición correcta
            data[pos] = key
        
        end_time = time.perf_counter()
        result_df = self._build_result_dataframe(data)
        return result_df, end_time - start_time

    def radix_sort(self) -> Tuple[pd.DataFrame, float]:
        """
        RadixSort - Ordenamiento por dígitos/caracteres.
        
        Ordena números procesando dígito por dígito desde el menos significativo
        (unidades) hasta el más significativo (miles). Usa CountingSort estable
        como subrutina para cada dígito.
        
        Returns:
            Tuple[pd.DataFrame, float]: Tupla con DataFrame ordenado y tiempo.
        
        Complexity:
            - Tiempo: O(d * (n + k)) donde:
              d = número de dígitos
              n = número de elementos
              k = rango de valores (0-9 para dígitos)
            - Espacio: O(n + k)
        
        Algorithm:
            1. Encontrar número máximo de dígitos (año más grande)
            2. Para cada posición de dígito (1, 10, 100, 1000):
               a. Ordenar por ese dígito usando CountingSort estable
            3. Ordenar por título dentro de cada grupo de año
        
        Advantages:
            - O(d*n) para enteros de tamaño fijo
            - Estable
            - Puede ser muy rápido para enteros pequeños
        
        Disadvantages:
            - Solo funciona con enteros o strings
            - Ineficiente si d es grande
            - Usa memoria adicional O(n)
            - Implementación más compleja
        
        Use Cases:
            - Ordenar enteros de tamaño fijo (códigos postales, fechas)
            - Cuando d es pequeño comparado con log n
            - Necesidad de estabilidad
        
        Example:
            >>> df_sorted, time_taken = analyzer.radix_sort()
            # Para años (4 dígitos): d=4, muy eficiente
        """
        data = self._create_sortable_data()
        
        start_time = time.perf_counter()
        
        # Manejar caso de datos vacíos
        if not data:
            end_time = time.perf_counter()
            return (self.df.copy() if isinstance(self.df, pd.DataFrame) else pd.DataFrame([]), 
                    end_time - start_time)
        
        # ===== PASO 1: OBTENER AÑO MÁXIMO =====
        max_year = max(item[0] for item in data) if data else 0
        
        # ===== PASO 2: ORDENAR POR CADA DÍGITO =====
        # Empezar con exp=1 (unidades), luego 10 (decenas), 100 (centenas), etc.
        exp = 1
        while max_year // exp > 0:
            self._counting_sort_for_radix(data, exp)
            exp *= 10
        
        # ===== PASO 3: ORDENAR POR TÍTULO DENTRO DE CADA AÑO =====
        current_year = None
        year_group_start = 0
        
        for i in range(len(data)):
            if data[i][0] != current_year:
                # Nuevo año encontrado: ordenar grupo anterior por título
                if current_year is not None:
                    year_group = data[year_group_start:i]
                    year_group.sort(key=lambda x: x[1])
                    data[year_group_start:i] = year_group
                
                current_year = data[i][0]
                year_group_start = i
        
        # Ordenar último grupo
        if len(data) > year_group_start:
            year_group = data[year_group_start:]
            year_group.sort(key=lambda x: x[1])
            data[year_group_start:] = year_group
        
        end_time = time.perf_counter()
        result_df = self._build_result_dataframe(data)
        return result_df, end_time - start_time

    # ==================== ANÁLISIS Y VISUALIZACIÓN ====================

    def run_all_algorithms(self) -> Dict[str, Tuple[pd.DataFrame, float]]:
        """
        Ejecuta todos los 12 algoritmos de ordenamiento y mide su rendimiento.
        
        Este es el método principal para benchmarking. Ejecuta cada algoritmo
        secuencialmente, mide su tiempo de ejecución con precisión, y captura
        cualquier error que ocurra.
        
        Returns:
            Dict[str, Tuple[pd.DataFrame, float]]: Diccionario donde:
                - Key: Nombre del algoritmo
                - Value: Tupla (DataFrame ordenado, tiempo en segundos)
                         o (None, float('inf')) si hubo error
        
        Algorithms Executed:
            1. TimSort
            2. CombSort
            3. SelectionSort
            4. TreeSort
            5. PigeonholeSort
            6. BucketSort
            7. QuickSort
            8. HeapSort
            9. BitonicSort
            10. GnomeSort
            11. BinaryInsertionSort
            12. RadixSort
        
        Output:
            Imprime progreso en consola:
            - ⏳ Mensaje de inicio para cada algoritmo
            - ✅ Tiempo de ejecución en ms si exitoso
            - ❌ Mensaje de error si falla
        
        Example:
            >>> results = analyzer.run_all_algorithms()
            🚀 Ejecutando todos los algoritmos de ordenamiento...
            ⏳ Ejecutando TimSort...
            ✅ TimSort completado en 12.345ms
            ⏳ Ejecutando CombSort...
            ✅ CombSort completado en 45.678ms
            ...
            
            >>> # Analizar resultados
            >>> for name, (df, time_taken) in results.items():
            ...     if time_taken != float('inf'):
            ...         print(f"{name}: {time_taken*1000:.2f}ms")
        
        Note:
            Los algoritmos se ejecutan en el orden listado. Si alguno falla,
            los demás continúan ejecutándose.
        """
        print("🚀 Ejecutando todos los algoritmos de ordenamiento...")
        
        # Lista de tuplas (nombre, función)
        algorithms = [
            ("TimSort", self.tim_sort),
            ("CombSort", self.comb_sort),
            ("SelectionSort", self.selection_sort),
            ("TreeSort", self.tree_sort),
            ("PigeonholeSort", self.pigeonhole_sort),
            ("BucketSort", self.bucket_sort),
            ("QuickSort", self.quick_sort),
            ("HeapSort", self.heap_sort),
            ("BitonicSort", self.bitonic_sort),
            ("GnomeSort", self.gnome_sort),
            ("BinaryInsertionSort", self.binary_insertion_sort),
            ("RadixSort", self.radix_sort),
        ]
        
        results = {}
        
        # Ejecutar cada algoritmo
        for name, algorithm in algorithms:
            try:
                print(f"⏳ Ejecutando {name}...")
                result_df, exec_time = algorithm()
                results[name] = (result_df, exec_time)
                print(f"✅ {name} completado en {exec_time*1000:.3f}ms")
            except Exception as e:
                # Capturar errores y continuar con el resto
                print(f"❌ Error en {name}: {e}")
                results[name] = (None, float('inf'))
        
        return results

    def create_time_comparison_chart(self, results: Dict[str, Tuple[pd.DataFrame, float]], 
                                   save_path: str = "sorting_times_comparison.png"):
        """
        Crea gráfico de barras comparando tiempos de ejecución de algoritmos.
        
        Genera una visualización profesional con matplotlib mostrando los
        tiempos de ejecución de cada algoritmo ordenados de más rápido a
        más lento. Incluye colores degradados y valores anotados.
        
        Args:
            results (Dict[str, Tuple[pd.DataFrame, float]]): Resultados de
                run_all_algorithms(). Debe contener tiempos de ejecución.
            save_path (str, optional): Ruta donde guardar la imagen PNG.
                Por defecto "sorting_times_comparison.png".
        
        Chart Features:
            - Barras ordenadas ascendente por tiempo (más rápido primero)
            - Colores degradados usando colormap 'viridis'
            - Valores de tiempo anotados sobre cada barra
            - Grid horizontal para facilitar lectura
            - Tamaño: 14x8 pulgadas
            - Resolución: 300 DPI
        
        Example:
            >>> results = analyzer.run_all_algorithms()
            >>> analyzer.create_time_comparison_chart(results)
            📊 Gráfico guardado en: sorting_times_comparison.png
            
            >>> # Personalizar nombre de archivo
            >>> analyzer.create_time_comparison_chart(
            ...     results,
            ...     save_path="benchmark_results_2025.png"
            ... )
        
        Note:
            El gráfico se muestra automáticamente con plt.show() y también
            se guarda en disco. Los algoritmos que fallaron (tiempo=inf)
            se excluyen del gráfico.
        """
        # ===== PASO 1: EXTRAER NOMBRES Y TIEMPOS VÁLIDOS =====
        names = []
        times = []
        
        for name, (df, time_taken) in results.items():
            # Solo incluir algoritmos que ejecutaron exitosamente
            if time_taken != float('inf'):
                names.append(name)
                times.append(time_taken * 1000)  # Convertir a milisegundos
        
        # ===== PASO 2: ORDENAR POR TIEMPO ASCENDENTE =====
        sorted_data = sorted(zip(names, times), key=lambda x: x[1])
        names, times = zip(*sorted_data)
        
        # ===== PASO 3: CREAR GRÁFICO =====
        plt.figure(figsize=(14, 8))
        
        # Obtener colormap viridis (degradado azul-verde-amarillo)
        cmap = cm.get_cmap('viridis')
        colors = cmap(np.linspace(0, 1, len(names))) if len(names) else []
        
        # Crear barras con colores degradados
        bars = plt.bar(range(len(names)), times, color=colors)
        
        # ===== PASO 4: CONFIGURAR EJES Y TÍTULOS =====
        plt.xlabel('Algoritmos de Ordenamiento', fontsize=12)
        plt.ylabel('Tiempo de Ejecución (ms)', fontsize=12)
        plt.title('Comparación de Tiempos de Ejecución - Algoritmos de Ordenamiento\n(Productos Académicos)', 
                  fontsize=14)
        plt.xticks(range(len(names)), names, rotation=45, ha='right')
        
        # ===== PASO 5: ANOTAR VALORES SOBRE BARRAS =====
        for i, (bar, time_val) in enumerate(zip(bars, times)):
            # Calcular posición del texto (encima de la barra)
            plt.text(bar.get_x() + bar.get_width()/2., 
                    bar.get_height() + max(times)*0.01,
                    f'{time_val:.2f}ms', 
                    ha='center', va='bottom', fontsize=9)
        
        # ===== PASO 6: AGREGAR GRID Y AJUSTAR LAYOUT =====
        plt.grid(axis='y', alpha=0.3)
        plt.tight_layout()
        
        # ===== PASO 7: GUARDAR Y MOSTRAR =====
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.show()
        
        print(f"📊 Gráfico guardado en: {save_path}")

    def get_top_authors(self, top_n: int = 15) -> pd.DataFrame:
        """
        Obtiene los autores más frecuentes en el dataset.
        
        Analiza la columna 'authors' del DataFrame, separa autores individuales,
        cuenta sus apariciones y retorna los top N más frecuentes. Útil para
        identificar autores prolíficos o colaboradores frecuentes.
        
        Args:
            top_n (int, optional): Número de autores a retornar en el top.
                Por defecto 15.
        
        Returns:
            pd.DataFrame: DataFrame con dos columnas:
                - 'Autor': Nombre del autor
                - 'Apariciones': Número de veces que aparece
                
                Ordenado descendente por apariciones.
                Retorna DataFrame vacío si no hay datos o columna 'authors'.
        
        Algorithm:
            1. Iterar sobre todos los registros
            2. Para cada registro, separar autores por ';'
            3. Limpiar espacios y agregar a lista
            4. Contar apariciones con Counter
            5. Obtener top N más comunes
            6. Crear DataFrame con resultados
        
        Example:
            >>> top_authors = analyzer.get_top_authors(10)
            📝 Analizando top 10 autores...
            ✅ Top 10 autores identificados
            >>> print(top_authors)
                Autor              Apariciones
            0   John Smith         45
            1   Jane Doe           38
            2   Michael Johnson    32
            ...
            
            >>> # Exportar resultados
            >>> top_authors.to_csv("top_authors.csv", index=False)
        
        Use Cases:
            - Identificar investigadores prolíficos
            - Análisis de redes de colaboración
            - Bibliometría y análisis de citas
            - Informes de productividad académica
        
        Note:
            Asume que los autores están separados por ';' en la columna 'authors'.
            Modifica el separador si tu dataset usa otro formato.
        """
        print(f"📝 Analizando top {top_n} autores...")
        
        all_authors = []
        
        # Verificar que hay datos y columna 'authors'
        if not isinstance(self.df, pd.DataFrame) or self.df.empty or 'authors' not in self.df.columns:
            return pd.DataFrame(columns=['Autor','Apariciones'])

        # ===== PASO 1: EXTRAER TODOS LOS AUTORES =====
        for authors_str in self.df['authors']:
            # Verificar que no es NaN ni vacío
            if pd.notna(authors_str) and authors_str != '':
                # Dividir por punto y coma (separador de autores)
                authors_list = str(authors_str).split(';')
                
                # Limpiar y agregar cada autor
                for author in authors_list:
                    cleaned_author = author.strip()
                    if cleaned_author:
                        all_authors.append(cleaned_author)
        
        # ===== PASO 2: CONTAR APARICIONES =====
        author_counts = Counter(all_authors)
        
        # ===== PASO 3: OBTENER TOP N =====
        top_authors = author_counts.most_common(top_n)
        
        # ===== PASO 4: CREAR DATAFRAME =====
        top_authors_df = pd.DataFrame(top_authors, columns=['Autor', 'Apariciones'])
        
        print(f"✅ Top {len(top_authors_df)} autores identificados")
        
        return top_authors_df

    def save_sorted_results(self, results: Dict[str, Tuple[pd.DataFrame, float]], 
                           output_dir: str = "sorted_results"):
        """
        Guarda los resultados ordenados de cada algoritmo en archivos CSV.
        
        Crea un directorio y guarda un archivo CSV por cada algoritmo que
        ejecutó exitosamente. Útil para inspeccionar resultados individuales
        o verificar que todos los algoritmos ordenan correctamente.
        
        Args:
            results (Dict[str, Tuple[pd.DataFrame, float]]): Resultados de
                run_all_algorithms().
            output_dir (str, optional): Directorio donde crear los archivos.
                Se crea si no existe. Por defecto "sorted_results".
        
        Files Created:
            - {output_dir}/TimSort_sorted.csv
            - {output_dir}/QuickSort_sorted.csv
            - {output_dir}/HeapSort_sorted.csv
            - ... (uno por cada algoritmo)
        
        Example:
            >>> results = analyzer.run_all_algorithms()
            >>> analyzer.save_sorted_results(results)
            💾 Guardando resultados en directorio: sorted_results
            ✅ TimSort: guardado en sorted_results/TimSort_sorted.csv (tiempo: 12.345ms)
            ✅ QuickSort: guardado en sorted_results/QuickSort_sorted.csv (tiempo: 15.678ms)
            ...
            
            >>> # Personalizar directorio
            >>> analyzer.save_sorted_results(results, "analysis_2025/sorted_data")
        
        Note:
            Los archivos se guardan con encoding UTF-8 sin índice.
            Solo se guardan algoritmos que ejecutaron exitosamente
            (df is not None).
        """
        import os
        
        # Crear directorio si no existe
        os.makedirs(output_dir, exist_ok=True)
        
        print(f"💾 Guardando resultados en directorio: {output_dir}")
        
        # Guardar cada resultado
        for name, (df, time_taken) in results.items():
            if df is not None:
                filename = f"{output_dir}/{name}_sorted.csv"
                df.to_csv(filename, index=False, encoding='utf-8')
                print(f"✅ {name}: guardado en {filename} (tiempo: {time_taken*1000:.3f}ms)")

    def generate_complete_report(self, output_base: str = "academic_sorting_analysis"):
        """
        Genera reporte completo con análisis de ordenamiento y autores.
        
        Este es el método "todo en uno" que ejecuta el análisis completo:
        1. Ejecuta todos los algoritmos
        2. Genera gráfico de comparación de tiempos
        3. Analiza top 15 autores
        4. Guarda resultados ordenados
        5. Crea reporte de texto con estadísticas
        
        Args:
            output_base (str, optional): Nombre base para archivos de salida.
                Por defecto "academic_sorting_analysis".
        
        Files Generated:
            1. {output_base}_time_comparison.png - Gráfico de barras
            2. {output_base}_top_authors.csv - Top autores
            3. {output_base}_sorted_data/ - Directorio con CSVs ordenados
            4. {output_base}_execution_times.txt - Reporte de texto
        
        Example:
            >>> analyzer.generate_complete_report("ml_articles_analysis")
            📋 Generando reporte completo...
            🚀 Ejecutando todos los algoritmos de ordenamiento...
            ...
            📊 Gráfico guardado en: ml_articles_analysis_time_comparison.png
            📝 Top autores guardado en: ml_articles_analysis_top_authors.csv
            💾 Guardando resultados en directorio: ml_articles_analysis_sorted_data
            📊 Reporte de tiempos guardado en: ml_articles_analysis_execution_times.txt
            🎉 Análisis completo finalizado!
        
        Report Contents:
            El archivo .txt incluye:
            - Nombre del archivo analizado
            - Total de registros
            - Lista de algoritmos ordenada por tiempo
            - Tiempo de ejecución de cada uno en ms
        
        Use Cases:
            - Análisis completo de un dataset
            - Benchmarking de rendimiento
            - Generación de informes académicos
            - Comparación de datasets
        """
        print("📋 Generando reporte completo...")
        
        # ===== PASO 1: EJECUTAR TODOS LOS ALGORITMOS =====
        results = self.run_all_algorithms()
        
        # ===== PASO 2: CREAR GRÁFICO DE TIEMPOS =====
        chart_path = f"{output_base}_time_comparison.png"
        self.create_time_comparison_chart(results, chart_path)
        
        # ===== PASO 3: ANALIZAR TOP AUTORES =====
        top_authors = self.get_top_authors(15)
        authors_path = f"{output_base}_top_authors.csv"
        top_authors.to_csv(authors_path, index=False, encoding='utf-8')
        print(f"📝 Top autores guardado en: {authors_path}")
        
        # ===== PASO 4: GUARDAR RESULTADOS ORDENADOS =====
        self.save_sorted_results(results, f"{output_base}_sorted_data")
        
        # ===== PASO 5: CREAR REPORTE DE TEXTO =====
        times_report = f"{output_base}_execution_times.txt"
        with open(times_report, 'w', encoding='utf-8') as f:
            f.write("=== REPORTE DE TIEMPOS DE EJECUCIÓN ===\n\n")
            f.write(f"Archivo analizado: {self.csv_file}\n")
            total_registros = len(self.df) if isinstance(self.df, pd.DataFrame) else 0
            f.write(f"Total de registros: {total_registros}\n\n")
            
            # Ordenar resultados por tiempo
            sorted_times = sorted(
                [(name, time_taken) for name, (df, time_taken) in results.items()], 
                key=lambda x: x[1]
            )
            
            f.write("TIEMPOS DE EJECUCIÓN (ordenado ascendente):\n")
            f.write("-" * 50 + "\n")
            
            for i, (name, time_taken) in enumerate(sorted_times, 1):
                if time_taken != float('inf'):
                    f.write(f"{i:2d}. {name:<20}: {time_taken*1000:8.3f} ms\n")
                else:
                    f.write(f"{i:2d}. {name:<20}: ERROR\n")
        
        print(f"📊 Reporte de tiempos guardado en: {times_report}")
        print("🎉 Análisis completo finalizado!")


# ============================================================================
# FUNCIÓN DE CONVENIENCIA
# ============================================================================

def analyze_academic_data(csv_file: str, output_base: str = "academic_analysis"):
    """
    Función de conveniencia para analizar datos académicos con un solo comando.
    
    Wrapper que simplifica el uso de AcademicSortingAnalyzer ejecutando
    automáticamente el análisis completo y retornando la instancia del
    analizador para análisis adicional si es necesario.
    
    Args:
        csv_file (str): Ruta al archivo CSV con datos académicos
        output_base (str, optional): Nombre base para archivos de salida.
            Por defecto "academic_analysis".
    
    Returns:
        AcademicSortingAnalyzer: Instancia del analizador con datos cargados
            y análisis completado.
    
    Process:
        1. Crea instancia de AcademicSortingAnalyzer
        2. Llama a generate_complete_report()
        3. Retorna analizador para uso adicional
    
    Example:
        >>> # Uso simple - análisis completo
        >>> analyzer = analyze_academic_data("articles.csv")
        
        >>> # Uso con nombre personalizado
        >>> analyzer = analyze_academic_data(
        ...     "ebsco_data.csv",
        ...     output_base="ebsco_analysis_2025"
        ... )
        
        >>> # Continuar con análisis adicional
        >>> top_20_authors = analyzer.get_top_authors(20)
        >>> print(top_20_authors.head())
    
    Note:
        Esta es la forma más rápida de obtener un análisis completo.
        Para más control, usar AcademicSortingAnalyzer directamente.
    """
    # Crear instancia del analizador
    analyzer = AcademicSortingAnalyzer(csv_file)
    
    # Ejecutar análisis completo
    analyzer.generate_complete_report(output_base)
    
    return analyzer


# ============================================================================
# EJEMPLO DE USO / SCRIPT PRINCIPAL
# ============================================================================

if __name__ == "__main__":
    """
    Script principal para ejecutar el analizador de forma interactiva.
    
    Solicita al usuario la ruta del archivo CSV y ejecuta el análisis completo,
    mostrando resultados en consola y generando todos los archivos de salida.
    """
    print("=== ANALIZADOR DE ORDENAMIENTO ACADÉMICO ===")
    print()
    
    # Solicitar archivo CSV al usuario
    csv_file = input("Ingresa la ruta del archivo CSV: ").strip()
    
    # Validar que se proporcionó un archivo
    if not csv_file:
        print("❌ Debes proporcionar un archivo CSV")
        exit(1)
    
    try:
        # Ejecutar análisis completo
        analyzer = analyze_academic_data(csv_file)
        
        # Mostrar resumen final
        print("\n" + "="*60)
        print("✅ ANÁLISIS COMPLETADO")
        print("📁 Revisa los archivos generados:")
        print("   📊 Gráfico de tiempos de ejecución")
        print("   📝 Top 15 autores más frecuentes")
        print("   📋 Datos ordenados por cada algoritmo")
        print("   📄 Reporte detallado de tiempos")
        print("="*60)
        
    except Exception as e:
        print(f"❌ Error durante el análisis: {e}")
        import traceback
        traceback.print_exc()