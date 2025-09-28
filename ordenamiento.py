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
    """Nodo para Tree Sort"""
    def __init__(self, val):
        self.val = val
        self.left = None
        self.right = None


class AcademicSortingAnalyzer:
    """
    Analizador de ordenamiento para productos académicos con visualización de tiempos
    """
    
    def __init__(self, csv_file: str):
        """
        Inicializa el analizador con el archivo CSV
        
        Args:
            csv_file (str): Ruta al archivo CSV con datos académicos
        """
        self.csv_file = csv_file
        self.df = None
        self.load_data()
        
    def load_data(self):
        """Carga los datos del CSV"""
        try:
            self.df = pd.read_csv(self.csv_file, encoding='utf-8')
            print(f"✅ Datos cargados: {len(self.df)} registros")
            
            # Mostrar columnas disponibles
            print(f"📋 Columnas disponibles: {list(self.df.columns)}")
            
            # Limpiar y preparar datos
            self._prepare_data()
            
        except Exception as e:
            print(f"❌ Error cargando datos: {e}")
            # Asegurar que self.df no quede en None para evitar AttributeError posteriores
            self.df = pd.DataFrame([])
            raise

    def _prepare_data(self):
        """Prepara los datos para ordenamiento"""
        if self.df is None or self.df.empty:
            print("⚠️ DataFrame vacío. No se preparan datos de ordenamiento.")
            return
        # Limpiar título
        if 'title' in self.df.columns:
            self.df['title_clean'] = self.df['title'].fillna('').astype(str)
        
        # Extraer año de publicación
        if 'publication_date' in self.df.columns:
            self.df['year'] = self.df['publication_date'].apply(self._extract_year)
        else:
            self.df['year'] = 0
            
        # Crear clave de ordenamiento: año + título
        self.df['sort_key'] = self.df.apply(
            lambda row: (row.get('year', 0), row.get('title_clean', '').lower().strip()), 
            axis=1
        )
        
        print(f"📊 Datos preparados con {len(self.df)} registros válidos")

    def _extract_year(self, date_str) -> int:
        """Extrae el año de una fecha"""
        if pd.isna(date_str) or date_str == '':
            return 0
            
        # Buscar año de 4 dígitos
        year_match = re.search(r'\b(19|20)\d{2}\b', str(date_str))
        if year_match:
            return int(year_match.group())
        
        return 0

    def _create_sortable_data(self) -> List[Tuple]:
        """
        Crea lista de tuplas para ordenamiento
        Returns:
            List[Tuple]: Lista de (año, título, índice_original)
        """
        if self.df is None or self.df.empty:
            return []
        sortable_data = []
        for idx, row in self.df.iterrows():
            year = row.get('year', 0)
            title = row.get('title_clean', '').lower().strip()
            sortable_data.append((year, title, idx))
        
        return sortable_data

    def _build_result_dataframe(self, sorted_data: List[Tuple]) -> pd.DataFrame:
        """Construye DataFrame resultado a partir de datos ordenados"""
        if self.df is None or self.df.empty:
            return pd.DataFrame([])
        sorted_indices = [item[2] for item in sorted_data if len(item) > 2]
        return self.df.iloc[sorted_indices].reset_index(drop=True)

    # ==================== FUNCIONES AUXILIARES PARA ALGORITMOS ====================

    def _heapify(self, arr, n, i):
        """Función heapify para heap sort"""
        largest = i
        left = 2 * i + 1
        right = 2 * i + 2
        
        if left < n and arr[left] > arr[largest]:
            largest = left
        
        if right < n and arr[right] > arr[largest]:
            largest = right
        
        if largest != i:
            arr[i], arr[largest] = arr[largest], arr[i]
            self._heapify(arr, n, largest)

    def _quick_sort_partition(self, arr, low, high):
        """Función partition para quick sort"""
        pivot = arr[high]
        i = low - 1
        
        for j in range(low, high):
            if arr[j] <= pivot:
                i += 1
                arr[i], arr[j] = arr[j], arr[i]
        
        arr[i + 1], arr[high] = arr[high], arr[i + 1]
        return i + 1

    def _quick_sort_recursive(self, arr, low, high):
        """Función recursiva para quick sort"""
        if low < high:
            pi = self._quick_sort_partition(arr, low, high)
            self._quick_sort_recursive(arr, low, pi - 1)
            self._quick_sort_recursive(arr, pi + 1, high)

    def _insert_tree_node(self, root, val):
        """Inserta nodo en árbol binario"""
        if root is None:
            return TreeNode(val)
        if val < root.val:
            root.left = self._insert_tree_node(root.left, val)
        else:
            root.right = self._insert_tree_node(root.right, val)
        return root

    def _inorder_traversal(self, root, result):
        """Recorrido inorden del árbol"""
        if root:
            self._inorder_traversal(root.left, result)
            result.append(root.val)
            self._inorder_traversal(root.right, result)

    def _bitonic_merge(self, arr, low, cnt, up):
        """Función merge para bitonic sort"""
        if cnt > 1:
            k = cnt // 2
            for i in range(low, low + k):
                if (arr[i] > arr[i + k]) == up:
                    arr[i], arr[i + k] = arr[i + k], arr[i]
            self._bitonic_merge(arr, low, k, up)
            self._bitonic_merge(arr, low + k, k, up)

    def _bitonic_sort_recursive(self, arr, low, cnt, up):
        """Función recursiva para bitonic sort"""
        if cnt > 1:
            k = cnt // 2
            self._bitonic_sort_recursive(arr, low, k, True)
            self._bitonic_sort_recursive(arr, low + k, k, False)
            self._bitonic_merge(arr, low, cnt, up)

    def _binary_search_insertion(self, arr, val, start, end):
        """Búsqueda binaria para insertion sort"""
        if start == end:
            return start if arr[start] > val else start + 1
        if start > end:
            return start
        
        mid = (start + end) // 2
        if arr[mid] < val:
            return self._binary_search_insertion(arr, val, mid + 1, end)
        elif arr[mid] > val:
            return self._binary_search_insertion(arr, val, start, mid - 1)
        else:
            return mid

    def _counting_sort_for_radix(self, arr, exp):
        """Counting sort para radix sort"""
        n = len(arr)
        output = [None] * n
        count = [0] * 10
        
        # Contar ocurrencias
        for i in range(n):
            index = (arr[i][0] // exp) % 10
            count[index] += 1
        
        # Cambiar count[i]
        for i in range(1, 10):
            count[i] += count[i - 1]
        
        # Construir output
        i = n - 1
        while i >= 0:
            index = (arr[i][0] // exp) % 10
            output[count[index] - 1] = arr[i]
            count[index] -= 1
            i -= 1
        
        # Copiar output a arr
        for i in range(n):
            arr[i] = output[i]

    # ==================== ALGORITMOS DE ORDENAMIENTO ====================

    def tim_sort(self) -> Tuple[pd.DataFrame, float]:
        """TimSort - O(n log n) promedio, O(n log n) peor caso, O(n) mejor caso"""
        data = self._create_sortable_data()
        
        start_time = time.perf_counter()
        # Python usa TimSort internamente
        sorted_data = sorted(data, key=lambda x: (x[0], x[1]))
        end_time = time.perf_counter()
        
        result_df = self._build_result_dataframe(sorted_data)
        return result_df, end_time - start_time

    def comb_sort(self) -> Tuple[pd.DataFrame, float]:
        """CombSort - O(n log n) promedio, O(n²) peor caso"""
        data = self._create_sortable_data()
        
        start_time = time.perf_counter()
        
        n = len(data)
        gap = n
        shrink = 1.3
        sorted_flag = False
        
        while not sorted_flag:
            gap = int(gap / shrink)
            if gap <= 1:
                gap = 1
                sorted_flag = True
            
            i = 0
            while i + gap < n:
                if data[i] > data[i + gap]:
                    data[i], data[i + gap] = data[i + gap], data[i]
                    sorted_flag = False
                i += 1
        
        end_time = time.perf_counter()
        result_df = self._build_result_dataframe(data)
        return result_df, end_time - start_time

    def selection_sort(self) -> Tuple[pd.DataFrame, float]:
        """SelectionSort - O(n²) todos los casos"""
        data = self._create_sortable_data()
        
        start_time = time.perf_counter()
        
        n = len(data)
        for i in range(n):
            min_idx = i
            for j in range(i + 1, n):
                if data[j] < data[min_idx]:
                    min_idx = j
            data[i], data[min_idx] = data[min_idx], data[i]
        
        end_time = time.perf_counter()
        result_df = self._build_result_dataframe(data)
        return result_df, end_time - start_time

    def tree_sort(self) -> Tuple[pd.DataFrame, float]:
        """TreeSort - O(n log n) promedio, O(n²) peor caso"""
        data = self._create_sortable_data()
        
        start_time = time.perf_counter()
        
        root = None
        for item in data:
            root = self._insert_tree_node(root, item)
        
        sorted_data = []
        self._inorder_traversal(root, sorted_data)
        
        end_time = time.perf_counter()
        result_df = self._build_result_dataframe(sorted_data)
        return result_df, end_time - start_time

    def pigeonhole_sort(self) -> Tuple[pd.DataFrame, float]:
        """PigeonholeSort - O(n + k) donde k es el rango de años"""
        data = self._create_sortable_data()
        
        start_time = time.perf_counter()
        
        if not data:
            end_time = time.perf_counter()
            return (self.df.copy() if isinstance(self.df, pd.DataFrame) else pd.DataFrame([]), end_time - start_time)
        
        # Usar solo años para pigeonhole sort
        years = [item[0] for item in data]
        min_year = min(years)
        max_year = max(years)
        range_years = max_year - min_year + 1
        
        # Crear pigeonholes por año
        pigeonholes = [[] for _ in range(range_years)]
        
        # Distribuir por año
        for item in data:
            year_idx = item[0] - min_year
            pigeonholes[year_idx].append(item)
        
        # Ordenar dentro de cada año por título
        sorted_data = []
        for hole in pigeonholes:
            hole.sort(key=lambda x: x[1])  # Ordenar por título
            sorted_data.extend(hole)
        
        end_time = time.perf_counter()
        result_df = self._build_result_dataframe(sorted_data)
        return result_df, end_time - start_time

    def bucket_sort(self) -> Tuple[pd.DataFrame, float]:
        """BucketSort - O(n + k) promedio, O(n²) peor caso"""
        data = self._create_sortable_data()
        
        start_time = time.perf_counter()
        
        if not data:
            end_time = time.perf_counter()
            return (self.df.copy() if isinstance(self.df, pd.DataFrame) else pd.DataFrame([]), end_time - start_time)
        
        # Usar años únicos como buckets
        years = set(item[0] for item in data)
        year_buckets = {year: [] for year in years}
        
        # Distribuir en buckets por año
        for item in data:
            year_buckets[item[0]].append(item)
        
        # Ordenar cada bucket por título y concatenar
        sorted_data = []
        for year in sorted(years):
            bucket = year_buckets[year]
            bucket.sort(key=lambda x: x[1])
            sorted_data.extend(bucket)
        
        end_time = time.perf_counter()
        result_df = self._build_result_dataframe(sorted_data)
        return result_df, end_time - start_time

    def quick_sort(self) -> Tuple[pd.DataFrame, float]:
        """QuickSort - O(n log n) promedio, O(n²) peor caso"""
        data = self._create_sortable_data()
        
        start_time = time.perf_counter()
        self._quick_sort_recursive(data, 0, len(data) - 1)
        end_time = time.perf_counter()
        
        result_df = self._build_result_dataframe(data)
        return result_df, end_time - start_time

    def heap_sort(self) -> Tuple[pd.DataFrame, float]:
        """HeapSort - O(n log n) todos los casos"""
        data = self._create_sortable_data()
        
        start_time = time.perf_counter()
        
        n = len(data)
        
        # Construir heap
        for i in range(n // 2 - 1, -1, -1):
            self._heapify(data, n, i)
        
        # Extraer elementos
        for i in range(n - 1, 0, -1):
            data[0], data[i] = data[i], data[0]
            self._heapify(data, i, 0)
        
        end_time = time.perf_counter()
        result_df = self._build_result_dataframe(data)
        return result_df, end_time - start_time

    def bitonic_sort(self) -> Tuple[pd.DataFrame, float]:
        """BitonicSort - O(n log²n) todos los casos"""
        data = self._create_sortable_data()
        
        start_time = time.perf_counter()
        
        # Extender a potencia de 2
        n = len(data)
        next_power_of_2 = 1 << (n - 1).bit_length()
        
        # Llenar con elementos muy grandes
        max_item = (9999, 'zzzzz', -1)
        while len(data) < next_power_of_2:
            data.append(max_item)
        
        self._bitonic_sort_recursive(data, 0, len(data), True)
        
        # Remover elementos agregados
        data = data[:n]
        
        end_time = time.perf_counter()
        result_df = self._build_result_dataframe(data)
        return result_df, end_time - start_time

    def gnome_sort(self) -> Tuple[pd.DataFrame, float]:
        """GnomeSort - O(n²) promedio y peor caso, O(n) mejor caso"""
        data = self._create_sortable_data()
        
        start_time = time.perf_counter()
        
        index = 0
        n = len(data)
        
        while index < n:
            if index == 0 or data[index] >= data[index - 1]:
                index += 1
            else:
                data[index], data[index - 1] = data[index - 1], data[index]
                index -= 1
        
        end_time = time.perf_counter()
        result_df = self._build_result_dataframe(data)
        return result_df, end_time - start_time

    def binary_insertion_sort(self) -> Tuple[pd.DataFrame, float]:
        """BinaryInsertionSort - O(n²) todos los casos"""
        data = self._create_sortable_data()
        
        start_time = time.perf_counter()
        
        n = len(data)
        
        for i in range(1, n):
            key = data[i]
            pos = self._binary_search_insertion(data, key, 0, i - 1)
            
            # Mover elementos
            for j in range(i - 1, pos - 1, -1):
                data[j + 1] = data[j]
            
            data[pos] = key
        
        end_time = time.perf_counter()
        result_df = self._build_result_dataframe(data)
        return result_df, end_time - start_time

    def radix_sort(self) -> Tuple[pd.DataFrame, float]:
        """RadixSort - O(d * (n + k)) donde d es número de dígitos"""
        data = self._create_sortable_data()
        
        start_time = time.perf_counter()
        
        if not data:
            end_time = time.perf_counter()
            return (self.df.copy() if isinstance(self.df, pd.DataFrame) else pd.DataFrame([]), end_time - start_time)
        
        # Obtener año máximo para determinar dígitos
        max_year = max(item[0] for item in data) if data else 0
        
        # Aplicar radix sort por año
        exp = 1
        while max_year // exp > 0:
            self._counting_sort_for_radix(data, exp)
            exp *= 10
        
        # Ordenar por título dentro de cada grupo de año
        current_year = None
        year_group_start = 0
        
        for i in range(len(data)):
            if data[i][0] != current_year:
                if current_year is not None:
                    # Ordenar grupo anterior por título
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
        Ejecuta todos los algoritmos de ordenamiento y retorna resultados
        
        Returns:
            Dict: Diccionario con nombre del algoritmo y tupla (DataFrame, tiempo)
        """
        print("🚀 Ejecutando todos los algoritmos de ordenamiento...")
        
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
        
        for name, algorithm in algorithms:
            try:
                print(f"⏳ Ejecutando {name}...")
                result_df, exec_time = algorithm()
                results[name] = (result_df, exec_time)
                print(f"✅ {name} completado en {exec_time*1000:.3f}ms")
            except Exception as e:
                print(f"❌ Error en {name}: {e}")
                results[name] = (None, float('inf'))
        
        return results

    def create_time_comparison_chart(self, results: Dict[str, Tuple[pd.DataFrame, float]], 
                                   save_path: str = "sorting_times_comparison.png"):
        """
        Crea gráfico de barras comparando tiempos de ejecución
        
        Args:
            results: Resultados de run_all_algorithms()
            save_path: Ruta para guardar el gráfico
        """
        # Extraer nombres y tiempos
        names = []
        times = []
        
        for name, (df, time_taken) in results.items():
            if time_taken != float('inf'):
                names.append(name)
                times.append(time_taken * 1000)  # Convertir a milisegundos
        
        # Ordenar por tiempo ascendente
        sorted_data = sorted(zip(names, times), key=lambda x: x[1])
        names, times = zip(*sorted_data)
        
        # Crear gráfico
        plt.figure(figsize=(14, 8))
        # Obtener mapa de colores de matplotlib colormaps
        cmap = cm.get_cmap('viridis')
        colors = cmap(np.linspace(0, 1, len(names))) if len(names) else []
        bars = plt.bar(range(len(names)), times, color=colors)
        
        plt.xlabel('Algoritmos de Ordenamiento', fontsize=12)
        plt.ylabel('Tiempo de Ejecución (ms)', fontsize=12)
        plt.title('Comparación de Tiempos de Ejecución - Algoritmos de Ordenamiento\n(Productos Académicos)', fontsize=14)
        plt.xticks(range(len(names)), names, rotation=45, ha='right')
        
        # Agregar valores en las barras
        for i, (bar, time_val) in enumerate(zip(bars, times)):
            plt.text(bar.get_x() + bar.get_width()/2., bar.get_height() + max(times)*0.01,
                    f'{time_val:.2f}ms', ha='center', va='bottom', fontsize=9)
        
        plt.grid(axis='y', alpha=0.3)
        plt.tight_layout()
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.show()
        
        print(f"📊 Gráfico guardado en: {save_path}")

    def get_top_authors(self, top_n: int = 15) -> pd.DataFrame:
        """
        Obtiene los autores con más apariciones
        
        Args:
            top_n: Número de autores a retornar
            
        Returns:
            pd.DataFrame: DataFrame con autores ordenados por apariciones
        """
        print(f"📝 Analizando top {top_n} autores...")
        
        all_authors = []
        
        # Extraer todos los autores
        if not isinstance(self.df, pd.DataFrame) or self.df.empty or 'authors' not in self.df.columns:
            return pd.DataFrame(columns=['Autor','Apariciones'])

        for authors_str in self.df['authors']:
            if pd.notna(authors_str) and authors_str != '':
                # Dividir autores (asumiendo separación por ;)
                authors_list = str(authors_str).split(';')
                for author in authors_list:
                    cleaned_author = author.strip()
                    if cleaned_author:
                        all_authors.append(cleaned_author)
        
        # Contar apariciones
        author_counts = Counter(all_authors)
        
        # Crear DataFrame con top autores
        top_authors = author_counts.most_common(top_n)
        
        top_authors_df = pd.DataFrame(top_authors, columns=['Autor', 'Apariciones'])
        
        print(f"✅ Top {len(top_authors_df)} autores identificados")
        
        return top_authors_df

    def save_sorted_results(self, results: Dict[str, Tuple[pd.DataFrame, float]], 
                           output_dir: str = "sorted_results"):
        """
        Guarda los resultados ordenados de cada algoritmo
        
        Args:
            results: Resultados de run_all_algorithms()
            output_dir: Directorio donde guardar los archivos
        """
        import os
        
        # Crear directorio si no existe
        os.makedirs(output_dir, exist_ok=True)
        
        print(f"💾 Guardando resultados en directorio: {output_dir}")
        
        for name, (df, time_taken) in results.items():
            if df is not None:
                filename = f"{output_dir}/{name}_sorted.csv"
                df.to_csv(filename, index=False, encoding='utf-8')
                print(f"✅ {name}: guardado en {filename} (tiempo: {time_taken*1000:.3f}ms)")

    def generate_complete_report(self, output_base: str = "academic_sorting_analysis"):
        """
        Genera reporte completo con análisis de ordenamiento y autores
        
        Args:
            output_base: Nombre base para los archivos de salida
        """
        print("📋 Generando reporte completo...")
        
        # 1. Ejecutar todos los algoritmos
        results = self.run_all_algorithms()
        
        # 2. Crear gráfico de comparación de tiempos
        chart_path = f"{output_base}_time_comparison.png"
        self.create_time_comparison_chart(results, chart_path)
        
        # 3. Obtener top 15 autores
        top_authors = self.get_top_authors(15)
        authors_path = f"{output_base}_top_authors.csv"
        top_authors.to_csv(authors_path, index=False, encoding='utf-8')
        print(f"📝 Top autores guardado en: {authors_path}")
        
        # 4. Guardar resultados ordenados
        self.save_sorted_results(results, f"{output_base}_sorted_data")
        
        # 5. Crear reporte de tiempos
        times_report = f"{output_base}_execution_times.txt"
        with open(times_report, 'w', encoding='utf-8') as f:
            f.write("=== REPORTE DE TIEMPOS DE EJECUCIÓN ===\n\n")
            f.write(f"Archivo analizado: {self.csv_file}\n")
            total_registros = len(self.df) if isinstance(self.df, pd.DataFrame) else 0
            f.write(f"Total de registros: {total_registros}\n\n")
            
            # Ordenar por tiempo
            sorted_times = sorted([(name, time_taken) for name, (df, time_taken) in results.items()], 
                                key=lambda x: x[1])
            
            f.write("TIEMPOS DE EJECUCIÓN (ordenado ascendente):\n")
            f.write("-" * 50 + "\n")
            
            for i, (name, time_taken) in enumerate(sorted_times, 1):
                if time_taken != float('inf'):
                    f.write(f"{i:2d}. {name:<20}: {time_taken*1000:8.3f} ms\n")
                else:
                    f.write(f"{i:2d}. {name:<20}: ERROR\n")
        
        print(f"📊 Reporte de tiempos guardado en: {times_report}")
        print("🎉 Análisis completo finalizado!")


# Función de conveniencia
def analyze_academic_data(csv_file: str, output_base: str = "academic_analysis"):
    """
    Función de conveniencia para analizar datos académicos
    
    Args:
        csv_file: Archivo CSV con datos académicos
        output_base: Nombre base para archivos de salida
    """
    analyzer = AcademicSortingAnalyzer(csv_file)
    analyzer.generate_complete_report(output_base)
    return analyzer


# Ejemplo de uso
if __name__ == "__main__":
    print("=== ANALIZADOR DE ORDENAMIENTO ACADÉMICO ===")
    
    csv_file = input("Ingresa la ruta del archivo CSV: ").strip()
    
    if not csv_file:
        print("❌ Debes proporcionar un archivo CSV")
        exit(1)
    
    try:
        # Ejecutar análisis completo
        analyzer = analyze_academic_data(csv_file)
        
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