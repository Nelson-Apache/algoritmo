"""
EBSCO Data Cleaner - Limpieza y Deduplicación de Datos Académicos
===================================================================

Este módulo proporciona herramientas para limpiar, normalizar y eliminar
duplicados de datasets extraídos de la base de datos académica EBSCO.

El proceso de limpieza incluye:
- Eliminación de títulos vacíos o inválidos
- Detección y eliminación inteligente de duplicados
- Normalización de texto (espacios, caracteres especiales)
- Generación de reportes detallados de limpieza
- Exportación a múltiples formatos con información de trazabilidad

Criterios de Duplicación:
--------------------------
Se considera que dos registros son duplicados si comparten:
1. Título normalizado (sin puntuación, minúsculas, sin espacios extra)
2. DOI (Digital Object Identifier) - si está disponible
3. Autores normalizados

Archivos Generados:
-------------------
1. *_LIMPIO.csv: Dataset final sin duplicados ni registros inválidos
2. *_COMPLETO.csv: Dataset original con columna indicando registros eliminados
3. *_REPORTE.txt: Reporte detallado con estadísticas de limpieza

Workflow Típico:
----------------
1. Cargar datos desde CSV
2. Limpiar texto y normalizar campos
3. Identificar duplicados usando hash MD5
4. Eliminar duplicados manteniendo el primer registro de cada grupo
5. Generar reportes y exportar resultados

Autor: [Tu nombre]
Fecha: 2025
Licencia: [Tu licencia]
"""

import pandas as pd
import re
from typing import List, Dict, Tuple, Optional, Any
import hashlib
from datetime import datetime
import os


class DataCleaner:
    """
    Clase principal para limpieza y deduplicación de datos de EBSCO.
    
    Esta clase implementa un pipeline completo de limpieza de datos que
    procesa archivos CSV de EBSCO, elimina duplicados usando algoritmos
    de hash, normaliza texto y genera reportes detallados.
    
    La clase mantiene dos versiones del dataset:
    1. df_original: Dataset original sin modificar
    2. df_clean: Dataset limpio después del procesamiento
    
    Además, almacena metadata completa sobre qué registros fueron eliminados
    y por qué razón, permitiendo trazabilidad total del proceso.
    
    Attributes:
        input_file (str): Ruta al archivo CSV de entrada
        df_original (Optional[pd.DataFrame]): DataFrame con datos originales
        df_clean (Optional[pd.DataFrame]): DataFrame con datos limpios
        duplicate_info (Dict[int, Dict[str, Any]]): Información sobre registros
            duplicados eliminados. Key: índice del registro eliminado,
            Value: dict con razón, índice conservado, etc.
        cleaning_stats (Dict[str, int]): Estadísticas del proceso de limpieza
            incluyendo conteos de registros originales, finales, eliminados.
    
    Example:
        >>> cleaner = DataCleaner("ebsco_articles.csv")
        >>> cleaner.load_data()
        >>> df_clean = cleaner.clean_data()
        >>> cleaner.save_files("articles_cleaned")
        
        >>> # Análisis de duplicados
        >>> dup_analysis = cleaner.get_duplicate_analysis()
        >>> print(dup_analysis.head())
    
    Note:
        Los DataFrames se inicializan como None y se cargan/generan
        mediante los métodos load_data() y clean_data() respectivamente.
    """
    
    def __init__(self, input_file: str):
        """
        Inicializa el limpiador de datos con un archivo CSV de entrada.
        
        Configura las estructuras de datos necesarias para el proceso de
        limpieza, incluyendo DataFrames, diccionarios de información de
        duplicados y contadores de estadísticas.
        
        Args:
            input_file (str): Ruta al archivo CSV que contiene los datos
                de EBSCO a limpiar. Debe ser un CSV válido con encoding UTF-8
                y debe incluir al menos la columna 'title'.
        
        Initializes:
            - df_original: None (se carga con load_data())
            - df_clean: None (se genera con clean_data())
            - duplicate_info: Diccionario vacío para metadata de duplicados
            - cleaning_stats: Diccionario con contadores en 0
        
        Note:
            Este método solo inicializa estructuras, no carga los datos.
            Debe llamarse load_data() explícitamente después de la inicialización.
        
        Example:
            >>> cleaner = DataCleaner("articles.csv")
            >>> # Ahora se debe llamar cleaner.load_data()
        """
        self.input_file = input_file
        
        # DataFrames principales (se inicializan como None hasta load_data / clean_data)
        self.df_original: Optional[pd.DataFrame] = None
        self.df_clean: Optional[pd.DataFrame] = None
        
        # Diccionario de información de duplicados
        # Key: índice del registro eliminado
        # Value: diccionario con metadata (razón, índice conservado, título, etc.)
        self.duplicate_info: Dict[int, Dict[str, Any]] = {}
        
        # Estadísticas de limpieza
        self.cleaning_stats: Dict[str, int] = {
            'original_count': 0,              # Número de registros originales
            'clean_count': 0,                 # Número de registros después de limpiar
            'duplicates_removed': 0,          # Cantidad de duplicados eliminados
            'empty_titles_removed': 0,        # Registros sin título eliminados
            'invalid_records_removed': 0      # Otros registros inválidos
        }
        
    def load_data(self) -> bool:
        """
        Carga los datos desde el archivo CSV especificado en __init__.
        
        Lee el archivo CSV usando pandas con encoding UTF-8 y almacena el
        resultado en df_original. También inicializa el contador de registros
        originales en las estadísticas de limpieza.
        
        Returns:
            bool: True si la carga fue exitosa, False si hubo algún error
                (archivo no encontrado, formato inválido, encoding incorrecto, etc.)
        
        Side Effects:
            - Popula self.df_original con el DataFrame cargado
            - Actualiza self.cleaning_stats['original_count']
            - Imprime mensaje de éxito o error en consola
        
        Raises:
            No lanza excepciones - captura todos los errores y retorna False.
            Los errores se imprimen en consola para debugging.
        
        Example:
            >>> cleaner = DataCleaner("articles.csv")
            >>> if cleaner.load_data():
            ...     print("Datos cargados exitosamente")
            ...     print(f"Columnas: {cleaner.df_original.columns.tolist()}")
            ... else:
            ...     print("Error al cargar datos")
            ✅ Datos cargados exitosamente: 1,234 registros
        
        Note:
            Asume que el CSV usa encoding UTF-8. Si tu archivo usa otro
            encoding (ej: latin-1, iso-8859-1), modifica el parámetro encoding.
        """
        try:
            # Cargar CSV con pandas
            self.df_original = pd.read_csv(self.input_file, encoding='utf-8')
            
            # Actualizar contador de registros originales
            self.cleaning_stats['original_count'] = len(self.df_original)
            
            # Mensaje de éxito con formato de miles
            print(f"Datos cargados exitosamente: {len(self.df_original):,} registros")
            return True
            
        except Exception as e:
            # Capturar cualquier error y mostrar mensaje
            print(f"Error cargando el archivo: {e}")
            return False
    
    def clean_text(self, text: str) -> str:
        """
        Limpia y normaliza texto eliminando caracteres especiales.
        
        Realiza múltiples transformaciones al texto para normalizarlo:
        - Convierte a string si es otro tipo
        - Elimina saltos de línea, retornos de carro y tabulaciones
        - Normaliza múltiples espacios a un solo espacio
        - Elimina espacios al inicio y final
        
        Este método es útil para preparar campos de texto antes de
        comparaciones o almacenamiento, asegurando consistencia.
        
        Args:
            text (str): Texto a limpiar. Puede ser cualquier tipo que
                sea convertible a string.
        
        Returns:
            str: Texto limpio y normalizado. Retorna string vacío "" si
                el input es None, NaN o string vacío.
        
        Process:
            1. Verificar si es NaN o vacío → retornar ""
            2. Convertir a string si no lo es
            3. Reemplazar \\r, \\n, \\t con espacios
            4. Colapsar múltiples espacios a uno solo
            5. Eliminar espacios iniciales y finales
        
        Example:
            >>> cleaner = DataCleaner("data.csv")
            >>> text = "Hello\\n\\nWorld\\t\\tTest   Multiple  Spaces  "
            >>> cleaned = cleaner.clean_text(text)
            >>> print(f"'{cleaned}'")
            'Hello World Test Multiple Spaces'
            
            >>> # Maneja NaN y valores vacíos
            >>> cleaner.clean_text(None)
            ''
            >>> cleaner.clean_text("")
            ''
            >>> cleaner.clean_text(float('nan'))
            ''
        
        Note:
            Este método NO elimina puntuación ni convierte a minúsculas.
            Para eso, ver normalize_title().
        """
        # Verificar si el texto es NaN o vacío
        if pd.isna(text) or text == "":
            return ""
        
        # Convertir a string si no lo es (ej: números, fechas, etc.)
        text = str(text)
        
        # Eliminar caracteres de control (\\r, \\n, \\t) y reemplazar con espacio
        text = re.sub(r'[\r\n\t]+', ' ', text)
        
        # Normalizar múltiples espacios a un solo espacio
        text = re.sub(r'\s+', ' ', text)
        
        # Eliminar espacios al inicio y final
        text = text.strip()
        
        return text
    
    def normalize_title(self, title: str) -> str:
        """
        Normaliza títulos para comparación de duplicados.
        
        Aplica transformaciones agresivas al título para permitir detección
        de duplicados incluso cuando hay pequeñas diferencias de formato,
        puntuación o capitalización.
        
        El título normalizado se usa SOLO para comparación, no se guarda
        en el dataset final. El título original se mantiene intacto.
        
        Args:
            title (str): Título original del artículo
        
        Returns:
            str: Título normalizado en minúsculas, sin puntuación ni
                caracteres especiales, con espacios normalizados. Retorna
                string vacío si el input es None, NaN o vacío.
        
        Transformations:
            1. Convertir a minúsculas
            2. Eliminar TODA la puntuación y caracteres especiales
            3. Mantener solo caracteres alfanuméricos y espacios
            4. Normalizar espacios múltiples a uno solo
            5. Eliminar espacios iniciales y finales
        
        Example:
            >>> cleaner = DataCleaner("data.csv")
            
            >>> # Títulos similares se normalizan igual
            >>> t1 = "Machine Learning: A Comprehensive Guide"
            >>> t2 = "machine learning a comprehensive guide"
            >>> t3 = "Machine Learning - A Comprehensive Guide!"
            >>> 
            >>> cleaner.normalize_title(t1)
            'machine learning a comprehensive guide'
            >>> cleaner.normalize_title(t2)
            'machine learning a comprehensive guide'
            >>> cleaner.normalize_title(t3)
            'machine learning a comprehensive guide'
            >>> # Los tres se consideran duplicados
        
        Use Cases:
            - Detección de duplicados con pequeñas variaciones
            - Comparación de títulos case-insensitive
            - Matching fuzzy de títulos similares
        
        Note:
            Esta normalización es MUY agresiva. Títulos genuinamente
            diferentes pero con palabras similares podrían colisionar.
            Por eso se combina con DOI y autores para mejor precisión.
        """
        # Verificar si el título es NaN o vacío
        if pd.isna(title) or title == "":
            return ""
        
        # Convertir a string y luego a minúsculas
        normalized = str(title).lower()
        
        # Eliminar TODA la puntuación y caracteres especiales
        # Mantener solo: letras, números, espacios
        # [^\w\s] significa: todo lo que NO sea word character (letras, números, _) ni espacios
        normalized = re.sub(r'[^\w\s]', '', normalized)
        
        # Normalizar espacios múltiples a un solo espacio
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        
        return normalized
    
    def create_duplicate_key(self, row: pd.Series) -> str:
        """
        Crea una clave única (hash) para identificar registros duplicados.
        
        Genera un hash MD5 basado en la combinación de título normalizado,
        DOI y autores. Dos registros con el mismo hash se consideran duplicados.
        
        El hash permite comparaciones rápidas (O(1)) en lugar de comparaciones
        string por string (O(n)) para cada par de registros.
        
        Args:
            row (pd.Series): Fila del DataFrame que representa un artículo.
                Debe contener (idealmente) las columnas: 'title', 'doi', 'authors'
        
        Returns:
            str: Hash MD5 de 32 caracteres hexadecimales que identifica
                únicamente la combinación de título, DOI y autores.
        
        Algorithm:
            1. Normalizar título (minúsculas, sin puntuación)
            2. Normalizar DOI (minúsculas, sin espacios)
            3. Normalizar autores (minúsculas, sin espacios)
            4. Concatenar con pipe "|" como separador
            5. Generar hash MD5 de la string concatenada
        
        Hash Collisions:
            MD5 tiene probabilidad casi nula de colisión para este caso de uso.
            Si dos registros diferentes generan el mismo hash (extremadamente
            raro), se considerarían erróneamente duplicados. En la práctica,
            esto no ocurre con datasets académicos.
        
        Example:
            >>> cleaner = DataCleaner("data.csv")
            >>> row1 = pd.Series({
            ...     'title': 'Machine Learning: An Introduction',
            ...     'doi': '10.1234/ml.2023.001',
            ...     'authors': 'John Smith; Jane Doe'
            ... })
            >>> 
            >>> key1 = cleaner.create_duplicate_key(row1)
            >>> print(len(key1))  # Hash MD5 es siempre 32 caracteres
            32
            >>> print(key1[:8])  # Primeros 8 caracteres
            'a3f7b2c1'
            >>> 
            >>> # Mismo contenido → mismo hash
            >>> row2 = pd.Series({
            ...     'title': 'MACHINE LEARNING - AN INTRODUCTION!!!',
            ...     'doi': '10.1234/ml.2023.001',
            ...     'authors': 'john smith; jane doe'
            ... })
            >>> key2 = cleaner.create_duplicate_key(row2)
            >>> key1 == key2  # Son duplicados
            True
        
        Performance:
            Generación de hash es O(1) y muy rápida. Permite detectar
            duplicados en datasets de millones de registros eficientemente.
        
        Note:
            Si los campos title, doi o authors no existen en row, se usan
            strings vacíos. Registros sin información podrían generar
            hashes similares erróneamente.
        """
        # Obtener y normalizar título
        title = self.normalize_title(row.get('title', ''))
        
        # Obtener y normalizar DOI (convertir a minúsculas y eliminar espacios)
        doi = str(row.get('doi', '')).lower().strip()
        
        # Obtener y normalizar autores (convertir a minúsculas y eliminar espacios)
        authors = str(row.get('authors', '')).lower().strip()
        
        # Crear string combinada usando pipe como separador
        # Formato: "titulo|doi|autores"
        combined = f"{title}|{doi}|{authors}"
        
        # Generar hash MD5 (32 caracteres hexadecimales)
        hash_object = hashlib.md5(combined.encode('utf-8'))
        return hash_object.hexdigest()
    
    def identify_duplicates(self, df: Optional[pd.DataFrame] = None) -> Dict[str, List[int]]:
        """
        Identifica grupos de registros duplicados en el DataFrame.
        
        Itera sobre todos los registros del DataFrame, genera una clave hash
        para cada uno y agrupa registros con la misma clave. Retorna solo
        los grupos que tienen más de un registro (duplicados reales).
        
        Args:
            df (Optional[pd.DataFrame], optional): DataFrame a analizar.
                Si es None, usa self.df_original. Por defecto None.
        
        Returns:
            Dict[str, List[int]]: Diccionario donde:
                - Key: Hash MD5 que identifica el grupo de duplicados
                - Value: Lista de índices de registros que comparten ese hash
                Solo incluye grupos con 2+ registros (duplicados reales).
        
        Raises:
            ValueError: Si no hay DataFrame cargado (df is None y 
                self.df_original is None)
        
        Algorithm:
            1. Para cada fila en el DataFrame:
                a. Generar clave hash usando create_duplicate_key()
                b. Agregar índice a la lista del hash correspondiente
            2. Filtrar solo grupos con len(lista) > 1
            3. Calcular estadísticas (total de duplicados a eliminar)
        
        Performance:
            - Tiempo: O(n) donde n = número de registros
            - Espacio: O(d) donde d = número de duplicados
            - Muy eficiente incluso con millones de registros
        
        Example:
            >>> cleaner = DataCleaner("articles.csv")
            >>> cleaner.load_data()
            >>> duplicates = cleaner.identify_duplicates()
            🔍 Identificando duplicados...
            📊 Encontrados 45 grupos de duplicados
            📊 Total de registros duplicados a eliminar: 123
            
            >>> # Analizar un grupo específico
            >>> first_group_key = list(duplicates.keys())[0]
            >>> indices = duplicates[first_group_key]
            >>> print(f"Grupo con {len(indices)} duplicados en índices: {indices}")
            Grupo con 3 duplicados en índices: [42, 156, 789]
            
            >>> # Ver títulos de ese grupo
            >>> for idx in indices:
            ...     title = cleaner.df_original.iloc[idx]['title']
            ...     print(f"  [{idx}] {title[:50]}...")
        
        Duplicate Elimination Strategy:
            De cada grupo de duplicados, se MANTIENE el primero (índice más bajo)
            y se ELIMINAN todos los demás. Esto se hace en clean_data().
            
            Ejemplo: Si el grupo es [42, 156, 789]:
            - Se mantiene el registro 42
            - Se eliminan los registros 156 y 789
        
        Note:
            Este método solo IDENTIFICA duplicados, no los elimina.
            La eliminación se hace en clean_data().
        """
        print("🔍 Identificando duplicados...")
        
        # Determinar qué DataFrame usar
        if df is None:
            df = self.df_original
        if df is None:
            raise ValueError("No hay DataFrame cargado para detección de duplicados.")

        # Diccionario para agrupar índices por hash
        duplicate_groups: Dict[str, List[int]] = {}

        # Iterar sobre cada fila del DataFrame
        for idx, row in df.iterrows():  # type: ignore[union-attr]
            # Generar clave hash para esta fila
            key: str = self.create_duplicate_key(row)
            
            # Inicializar lista para este hash si no existe
            if key not in duplicate_groups:
                duplicate_groups[key] = []
            
            # Agregar índice a la lista de este hash
            duplicate_groups[key].append(idx)  # type: ignore[arg-type]

        # Filtrar solo grupos con duplicados (más de 1 registro)
        duplicates = {k: v for k, v in duplicate_groups.items() if len(v) > 1}

        # Mostrar estadísticas
        print(f"Encontrados {len(duplicates)} grupos de duplicados")

        # Calcular total de registros que serán eliminados
        # De cada grupo, se elimina len(grupo) - 1 registros
        total_duplicates = sum(len(group) - 1 for group in duplicates.values())
        print(f"Total de registros duplicados a eliminar: {total_duplicates}")

        return duplicates
    
    def clean_data(self) -> pd.DataFrame:
        """
        Ejecuta el pipeline completo de limpieza de datos.
        
        Este es el método principal que orquesta todo el proceso de limpieza:
        1. Limpia texto en columnas principales
        2. Elimina registros con títulos vacíos
        3. Identifica duplicados usando hashing
        4. Elimina duplicados manteniendo primer registro de cada grupo
        5. Resetea índices del DataFrame
        6. Calcula y almacena estadísticas
        
        Returns:
            pd.DataFrame: DataFrame limpio sin duplicados ni registros inválidos.
                También se almacena en self.df_clean para acceso posterior.
        
        Raises:
            ValueError: Si load_data() no se ha ejecutado previamente
                (self.df_original is None)
        
        Side Effects:
            - Modifica self.df_clean (lo crea/actualiza)
            - Modifica self.duplicate_info (guarda metadata de eliminados)
            - Modifica self.cleaning_stats (actualiza contadores)
            - Imprime mensajes de progreso en consola
        
        Process Flow:
            PASO 1: Limpiar Texto
            ├─ Para cada columna de texto (title, abstract, authors, etc.)
            └─ Aplicar clean_text() para normalizar espacios y caracteres
            
            PASO 2: Eliminar Títulos Vacíos
            ├─ Filtrar registros donde title.strip() == ''
            └─ Actualizar cleaning_stats['empty_titles_removed']
            
            PASO 3: Identificar Duplicados
            ├─ Llamar identify_duplicates() con DataFrame filtrado
            └─ Obtener grupos de índices duplicados
            
            PASO 4: Preparar Eliminación de Duplicados
            ├─ Para cada grupo de duplicados:
            │  ├─ Mantener primer índice (menor)
            │  ├─ Marcar resto para eliminación
            │  └─ Guardar metadata en duplicate_info
            └─ Crear lista completa de índices a eliminar
            
            PASO 5: Eliminar y Resetear
            ├─ Eliminar filas usando df.drop()
            ├─ Resetear índices (reset_index)
            └─ Actualizar cleaning_stats
        
        Example:
            >>> cleaner = DataCleaner("ebsco_data.csv")
            >>> cleaner.load_data()
            ✅ Datos cargados exitosamente: 5,432 registros
            
            >>> df_clean = cleaner.clean_data()
            🧹 Iniciando limpieza de datos...
            🗑️ Eliminados 12 registros con títulos vacíos
            🔍 Identificando duplicados...
            📊 Encontrados 87 grupos de duplicados
            📊 Total de registros duplicados a eliminar: 234
            ✅ Limpieza completada:
               📊 Registros originales: 5,432
               📊 Títulos vacíos eliminados: 12
               📊 Duplicados eliminados: 234
               📊 Registros finales: 5,186
            
            >>> # Acceder al DataFrame limpio
            >>> print(f"Shape: {df_clean.shape}")
            >>> print(f"Columnas: {df_clean.columns.tolist()}")
        
        Duplicate Handling:
            De cada grupo de duplicados, se mantiene el registro con el
            índice más bajo (primer registro encontrado) y se eliminan
            todos los demás. Esto asegura que siempre hay un registro
            representativo mantenido.
        
        Metadata Tracking:
            Para cada registro eliminado, se guarda en duplicate_info:
            - reason: 'DUPLICADO'
            - kept_index: Índice del registro que se mantuvo
            - duplicate_of_title: Primeros 100 chars del título mantenido
            
            Esto permite trazabilidad completa de qué se eliminó y por qué.
        
        Performance:
            - Tiempo: O(n) donde n = número de registros
            - Espacio: O(d) donde d = número de duplicados
            - Eficiente para datasets de millones de registros
        
        Note:
            Este método NO modifica df_original. Trabaja en una copia
            y genera un nuevo DataFrame limpio.
        """
        print("Iniciando limpieza de datos...")
        
        # Verificar que los datos están cargados
        if self.df_original is None:
            raise ValueError("Datos no cargados. Ejecuta load_data() primero.")
        
        # ===== PASO 1: CREAR COPIA DE TRABAJO =====
        df_work: pd.DataFrame = self.df_original.copy()
        
        # ===== PASO 2: LIMPIAR TEXTO EN COLUMNAS PRINCIPALES =====
        text_columns = ['title', 'abstract', 'authors', 'journal', 'subjects']
        for col in text_columns:
            if col in df_work.columns:
                # Aplicar clean_text a cada celda de la columna
                df_work[col] = df_work[col].apply(self.clean_text)
        
        # ===== PASO 3: ELIMINAR REGISTROS CON TÍTULOS VACÍOS =====
        initial_count = len(df_work)
        
        # Filtrar registros donde el título no está vacío después de strip()
        df_work = df_work.loc[df_work['title'].str.strip() != '']  # type: ignore[assignment]
        
        # Calcular cuántos se eliminaron
        empty_titles_removed = initial_count - len(df_work)
        self.cleaning_stats['empty_titles_removed'] = empty_titles_removed
        
        if empty_titles_removed > 0:
            print(f"Eliminados {empty_titles_removed} registros con títulos vacíos")
        
        # ===== PASO 4: IDENTIFICAR DUPLICADOS =====
        duplicates = self.identify_duplicates(df_work)
        
        # Listas para almacenar información de eliminación
        indices_to_remove = []
        duplicate_info = {}
        
        # ===== PASO 5: PREPARAR ELIMINACIÓN DE DUPLICADOS =====
        for dup_key, indices in duplicates.items():
            if len(indices) > 1:
                # Estrategia: mantener el PRIMERO, eliminar el resto
                keep_idx = indices[0]           # Índice a mantener (primer registro)
                remove_indices = indices[1:]    # Índices a eliminar (resto)
                
                # Para cada índice a eliminar, guardar metadata
                for remove_idx in remove_indices:
                    indices_to_remove.append(remove_idx)
                    
                    # Obtener título del registro que se mantiene
                    kept_title_val = df_work.loc[keep_idx, 'title'] if 'title' in df_work.columns else ''
                    kept_title_str = '' if pd.isna(kept_title_val) else str(kept_title_val)
                    
                    # Truncar título a 100 caracteres para metadata
                    short_kept_title = kept_title_str[:100] + ("..." if len(kept_title_str) > 100 else "")
                    
                    # Guardar información del duplicado eliminado
                    duplicate_info[remove_idx] = {
                        'reason': 'DUPLICADO',
                        'kept_index': keep_idx,
                        'duplicate_of_title': short_kept_title
                    }
        
        # ===== PASO 6: ELIMINAR DUPLICADOS DEL DATAFRAME =====
        df_work = df_work.drop(indices_to_remove, errors='ignore')
        self.cleaning_stats['duplicates_removed'] = len(indices_to_remove)
        
        # ===== PASO 7: RESETEAR ÍNDICES =====
        # Después de eliminar filas, los índices quedan discontinuos
        # reset_index los hace continuos desde 0
        df_work = df_work.reset_index(drop=True)
        
        # ===== PASO 8: GUARDAR INFORMACIÓN Y ESTADÍSTICAS =====
        self.duplicate_info = duplicate_info
        self.cleaning_stats['clean_count'] = len(df_work)
        
        # Mostrar resumen de limpieza
        print("Limpieza completada:")
        print(f"   Registros originales: {self.cleaning_stats['original_count']:,}")
        print(f"   Títulos vacíos eliminados: {self.cleaning_stats['empty_titles_removed']:,}")
        print(f"   Duplicados eliminados: {self.cleaning_stats['duplicates_removed']:,}")
        print(f"   Registros finales: {self.cleaning_stats['clean_count']:,}")

        # Guardar DataFrame limpio en la instancia
        self.df_clean = df_work
        return df_work
    
    def create_removal_info_column(self) -> pd.DataFrame:
        """
        Crea versión del DataFrame original con columna de información de eliminación.
        
        Genera una nueva columna 'removal_info' en el DataFrame original que indica
        para cada registro si fue CONSERVADO o ELIMINADO, y en caso de eliminación,
        la razón específica (título vacío, duplicado, etc.).
        
        Esta funcionalidad es útil para:
        - Auditoría del proceso de limpieza
        - Trazabilidad de decisiones
        - Análisis de qué registros fueron eliminados
        - Recuperación manual de registros si es necesario
        
        Returns:
            pd.DataFrame: Copia del DataFrame original con nueva columna 'removal_info'
                que contiene una de las siguientes etiquetas:
                - "CONSERVADO": Registro se mantuvo en dataset limpio
                - "ELIMINADO - TÍTULO VACÍO": Registro sin título válido
                - "ELIMINADO - DUPLICADO: Duplicado del índice X": Es copia de otro registro
        
        Raises:
            ValueError: Si clean_data() no se ha ejecutado previamente
                (self.df_original is None)
        
        Column Values:
            - "CONSERVADO": Registros que pasaron todos los filtros
            - "ELIMINADO - TÍTULO VACÍO": title.strip() == ''
            - "ELIMINADO - DUPLICADO: Duplicado del índice X": 
              Donde X es el índice del registro original que se mantuvo
        
        Example:
            >>> cleaner = DataCleaner("articles.csv")
            >>> cleaner.load_data()
            >>> cleaner.clean_data()
            
            >>> df_with_info = cleaner.create_removal_info_column()
            >>> 
            >>> # Ver distribución de estados
            >>> print(df_with_info['removal_info'].value_counts())
            CONSERVADO                                  4,567
            ELIMINADO - DUPLICADO: Duplicado del...      234
            ELIMINADO - TÍTULO VACÍO                      12
            
            >>> # Ver solo registros eliminados
            >>> removed = df_with_info[df_with_info['removal_info'] != 'CONSERVADO']
            >>> print(removed[['title', 'removal_info']].head())
        
        Use Cases:
            1. **Auditoría**: Revisar qué y por qué se eliminó
            2. **Recuperación**: Encontrar registros eliminados por error
            3. **Estadísticas**: Analizar patrones de duplicación
            4. **Documentación**: Evidencia de proceso de limpieza
        
        Note:
            Este método NO modifica self.df_original. Retorna una nueva copia.
            Para guardar esta versión, usar save_files() que automáticamente
            genera el archivo *_COMPLETO.csv con esta información.
        """
        # Verificar que df_original existe
        if self.df_original is None:
            raise ValueError("df_original es None. Llama a load_data() antes de create_removal_info_column().")

        # Crear copia del DataFrame original
        df_with_info = self.df_original.copy()
        
        # Inicializar columna removal_info con strings vacíos
        df_with_info['removal_info'] = ''
        
        # ===== PASO 1: MARCAR DUPLICADOS ELIMINADOS =====
        for idx, info in self.duplicate_info.items():
            # Verificar que el índice existe en el DataFrame
            if idx < len(df_with_info):
                # Crear mensaje descriptivo con razón e índice conservado
                removal_message = (
                    f"ELIMINADO - {info['reason']}: "
                    f"Duplicado del índice {info['kept_index']}"
                )
                df_with_info.loc[idx, 'removal_info'] = removal_message
        
        # ===== PASO 2: MARCAR TÍTULOS VACÍOS =====
        # Identificar registros con títulos vacíos (después de strip)
        empty_title_mask = df_with_info['title'].str.strip() == ''
        df_with_info.loc[empty_title_mask, 'removal_info'] = 'ELIMINADO - TÍTULO VACÍO'
        
        # ===== PASO 3: MARCAR REGISTROS CONSERVADOS =====
        # Todos los registros que no tienen removal_info son conservados
        keep_mask = df_with_info['removal_info'] == ''
        df_with_info.loc[keep_mask, 'removal_info'] = 'CONSERVADO'
        
        return df_with_info
    
    def generate_cleaning_report(self) -> str:
        """
        Genera un reporte de texto detallado del proceso de limpieza.
        
        Crea un reporte formateado con todas las estadísticas del proceso
        de limpieza, incluyendo conteos, porcentajes y criterios utilizados.
        Este reporte es útil para documentación y auditoría.
        
        Returns:
            str: Reporte de limpieza formateado con múltiples secciones:
                - Fecha y hora de generación
                - Archivo procesado
                - Estadísticas generales (originales, finales, eliminados)
                - Detalle por tipo de eliminación
                - Tasas porcentuales de conservación/eliminación
                - Criterios de duplicación utilizados
        
        Report Sections:
            1. **Metadata**: Fecha, hora, archivo procesado
            2. **Estadísticas Generales**: Conteos totales
            3. **Detalle de Eliminaciones**: Por categoría
            4. **Tasas Porcentuales**: % conservado vs eliminado
            5. **Criterios**: Cómo se identificaron duplicados
        
        Example:
            >>> cleaner = DataCleaner("articles.csv")
            >>> cleaner.load_data()
            >>> cleaner.clean_data()
            >>> 
            >>> report = cleaner.generate_cleaning_report()
            >>> print(report)
            
            === REPORTE DE LIMPIEZA DE DATOS EBSCO ===
            Fecha: 2025-01-15 14:30:45
            Archivo procesado: articles.csv
            
            ESTADÍSTICAS:
            - Registros originales: 5,432
            - Registros finales (limpios): 5,186
            - Total eliminados: 246
            
            DETALLE DE ELIMINACIONES:
            - Títulos vacíos: 12
            - Duplicados: 234
            
            TASA DE LIMPIEZA:
            - Porcentaje conservado: 95.47%
            - Porcentaje eliminado: 4.53%
            
            CRITERIOS DE DUPLICACIÓN:
            - Título normalizado (sin puntuación, minúsculas)
            - DOI (si está disponible)
            - Autores
            ========================================
        
        Use Cases:
            - **Documentación**: Anexar a informes de proyecto
            - **Auditoría**: Evidencia de proceso de limpieza
            - **Versionamiento**: Registrar cada limpieza realizada
            - **Análisis**: Comparar resultados entre diferentes datasets
        
        Note:
            El reporte se genera basado en cleaning_stats que se popula
            durante clean_data(). Debe ejecutarse clean_data() antes de
            generar el reporte.
        """
        # Calcular totales y porcentajes
        total_removed = self.cleaning_stats['original_count'] - self.cleaning_stats['clean_count']
        
        # Calcular porcentajes con protección contra división por cero
        if self.cleaning_stats['original_count'] > 0:
            pct_conserved = (self.cleaning_stats['clean_count'] / 
                           self.cleaning_stats['original_count'] * 100)
            pct_removed = (total_removed / 
                         self.cleaning_stats['original_count'] * 100)
        else:
            pct_conserved = 0.0
            pct_removed = 0.0
        
        # Construir reporte formateado
        report = f"""
        === REPORTE DE LIMPIEZA DE DATOS EBSCO ===
        Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        Archivo procesado: {self.input_file}

        ESTADÍSTICAS:
        - Registros originales: {self.cleaning_stats['original_count']:,}
        - Registros finales (limpios): {self.cleaning_stats['clean_count']:,}
        - Total eliminados: {total_removed:,}

        DETALLE DE ELIMINACIONES:
        - Títulos vacíos: {self.cleaning_stats['empty_titles_removed']:,}
        - Duplicados: {self.cleaning_stats['duplicates_removed']:,}

        TASA DE LIMPIEZA:
        - Porcentaje conservado: {pct_conserved:.2f}%
        - Porcentaje eliminado: {pct_removed:.2f}%

        CRITERIOS DE DUPLICACIÓN:
        - Título normalizado (sin puntuación, minúsculas)
        - DOI (si está disponible)
        - Autores

        ========================================
        """
        return report
    
    def save_files(self, base_filename: Optional[str] = None) -> Tuple[str, str, str]:
        """
        Guarda los archivos procesados y el reporte de limpieza.
        
        Genera y guarda tres archivos en el directorio actual:
        1. *_LIMPIO.csv: Dataset limpio sin duplicados
        2. *_COMPLETO.csv: Dataset original con columna 'removal_info'
        3. *_REPORTE.txt: Reporte de texto con estadísticas
        
        Args:
            base_filename (Optional[str], optional): Nombre base para los archivos.
                Si no se proporciona, genera uno automático con timestamp.
                Ejemplo: "articles_2025" generará:
                - articles_2025_LIMPIO.csv
                - articles_2025_COMPLETO.csv
                - articles_2025_REPORTE.txt
        
        Returns:
            Tuple[str, str, str]: Tupla con las rutas de los tres archivos generados:
                (clean_file, full_file, report_file)
        
        Raises:
            ValueError: Si clean_data() no se ha ejecutado previamente
                (self.df_clean is None)
        
        Files Generated:
            1. **LIMPIO.csv**: 
               - Dataset final para análisis
               - Sin duplicados ni registros inválidos
               - Mismas columnas que el original
               - Índices reseteados 0 a N-1
            
            2. **COMPLETO.csv**:
               - Dataset original completo
               - Nueva columna 'removal_info' indica estado de cada registro
               - Útil para auditoría y trazabilidad
               - Permite recuperar registros eliminados si es necesario
            
            3. **REPORTE.txt**:
               - Reporte de texto plano
               - Estadísticas detalladas
               - Criterios de limpieza
               - Fecha y metadata del proceso
        
        Example:
            >>> cleaner = DataCleaner("raw_articles.csv")
            >>> cleaner.load_data()
            >>> cleaner.clean_data()
            >>> 
            >>> # Opción 1: Generar nombres automáticos con timestamp
            >>> clean, full, report = cleaner.save_files()
            💾 Archivo limpio guardado: ebsco_data_20250115_143045_LIMPIO.csv
            💾 Archivo completo guardado: ebsco_data_20250115_143045_COMPLETO.csv
            📋 Reporte guardado: ebsco_data_20250115_143045_REPORTE.txt
            
            >>> # Opción 2: Especificar nombre base personalizado
            >>> clean, full, report = cleaner.save_files("ml_articles_cleaned")
            💾 Archivo limpio guardado: ml_articles_cleaned_LIMPIO.csv
            💾 Archivo completo guardado: ml_articles_cleaned_COMPLETO.csv
            📋 Reporte guardado: ml_articles_cleaned_REPORTE.txt
            
            >>> # Usar rutas retornadas para procesamiento posterior
            >>> import pandas as pd
            >>> df_final = pd.read_csv(clean)
            >>> print(f"Cargado dataset limpio: {len(df_final)} registros")
        
        File Formats:
            - Encoding: UTF-8 (compatible con caracteres internacionales)
            - Separator: Comma (,)
            - Index: No incluido en CSV (index=False)
            - Header: Incluido (primera fila son nombres de columnas)
        
        Best Practices:
            - Especificar base_filename descriptivo para facilitar identificación
            - Mantener archivos _COMPLETO.csv para auditoría
            - Versionar archivos si se realizan múltiples limpiezas
            - Respaldar archivos originales antes de limpieza
        
        Note:
            Los archivos se guardan en el directorio actual. Para especificar
            otra ubicación, incluir la ruta en base_filename:
            cleaner.save_files("/home/user/data/articles")
        """
        # Verificar que clean_data() se ejecutó
        if self.df_clean is None:
            raise ValueError("Datos no limpiados. Ejecuta clean_data() primero.")
        
        # ===== GENERAR NOMBRE BASE SI NO SE PROPORCIONA =====
        if base_filename is None:
            # Crear timestamp en formato YYYYMMDD_HHMMSS
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            base_filename = f"ebsco_data_{timestamp}"
        
        # ===== CONSTRUIR RUTAS DE ARCHIVOS =====
        clean_file = f"{base_filename}_LIMPIO.csv"
        full_file = f"{base_filename}_COMPLETO.csv"
        report_file = f"{base_filename}_REPORTE.txt"
        
        # ===== GUARDAR ARCHIVO LIMPIO =====
        self.df_clean.to_csv(clean_file, index=False, encoding='utf-8')
        print(f"Archivo limpio guardado: {clean_file}")
        
        # ===== GUARDAR ARCHIVO COMPLETO CON INFO DE ELIMINACIÓN =====
        df_with_info = self.create_removal_info_column()
        df_with_info.to_csv(full_file, index=False, encoding='utf-8')
        print(f"Archivo completo guardado: {full_file}")
        
        # ===== GUARDAR REPORTE DE TEXTO =====
        report = self.generate_cleaning_report()
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report)
        print(f"Reporte guardado: {report_file}")
        
        # Retornar tupla con las tres rutas
        return clean_file, full_file, report_file
    
    def get_duplicate_analysis(self) -> pd.DataFrame:
        """
        Genera un análisis detallado de los duplicados encontrados.
        
        Crea un DataFrame con información completa sobre cada registro que
        fue eliminado por ser duplicado, incluyendo su índice, el índice del
        registro que se mantuvo, títulos, autores y DOI de ambos.
        
        Este análisis es útil para:
        - Verificar que la detección de duplicados funciona correctamente
        - Auditar decisiones de eliminación
        - Identificar patrones de duplicación
        - Recuperar registros específicos si es necesario
        
        Returns:
            pd.DataFrame: DataFrame con las siguientes columnas:
                - indice_eliminado: Índice del registro que fue eliminado
                - indice_conservado: Índice del registro que se mantuvo
                - titulo_eliminado: Título del registro eliminado (truncado a 100 chars)
                - titulo_conservado: Título del registro conservado (truncado)
                - autores_eliminado: Autores del registro eliminado (truncado a 50 chars)
                - doi_eliminado: DOI del registro eliminado
                - razon: Razón de eliminación (siempre 'DUPLICADO')
                
                Retorna DataFrame vacío si no hay duplicados.
        
        Example:
            >>> cleaner = DataCleaner("articles.csv")
            >>> cleaner.load_data()
            >>> cleaner.clean_data()
            >>> 
            >>> # Obtener análisis de duplicados
            >>> dup_analysis = cleaner.get_duplicate_analysis()
            >>> print(f"Total de duplicados analizados: {len(dup_analysis)}")
            Total de duplicados analizados: 234
            
            >>> # Ver primeros duplicados
            >>> print(dup_analysis.head())
            indice_eliminado  indice_conservado  titulo_eliminado  ...
            42                15                 Machine Learning...
            156               15                 Machine Learning...
            789               45                 Deep Neural Netw...
            
            >>> # Verificar un caso específico
            >>> case = dup_analysis[dup_analysis['indice_eliminado'] == 42].iloc[0]
            >>> print(f"Registro {case['indice_eliminado']} eliminado")
            >>> print(f"Era duplicado del registro {case['indice_conservado']}")
            >>> print(f"Título eliminado: {case['titulo_eliminado']}")
            >>> print(f"Título conservado: {case['titulo_conservado']}")
            
            >>> # Exportar análisis para revisión externa
            >>> dup_analysis.to_csv("duplicados_analisis.csv", index=False)
        
        Analysis Use Cases:
            1. **Verificación**: Confirmar que registros similares se agruparon
            2. **Recuperación**: Encontrar registros eliminados por error
            3. **Estadísticas**: Analizar patrones de duplicación por journal, autor, etc.
            4. **Documentación**: Evidencia detallada para informes
        
        Note:
            Los títulos y autores se truncan para mantener el DataFrame
            manejable. Para ver contenido completo, consultar df_original
            directamente usando los índices.
        """
        # Si no hay duplicados, retornar DataFrame vacío
        if not self.duplicate_info:
            return pd.DataFrame()
        
        # Verificar que df_original existe
        if self.df_original is None:
            raise ValueError("df_original es None. No se puede generar análisis de duplicados.")

        # Lista para almacenar datos de análisis
        analysis_data = []
        
        # Iterar sobre cada duplicado eliminado
        for idx, info in self.duplicate_info.items():
            # Verificar que el índice existe en df_original
            if idx < len(self.df_original):  # type: ignore[arg-type]
                # Obtener fila del registro eliminado
                row = self.df_original.iloc[idx]  # type: ignore[index]
                
                # Extraer y truncar información relevante
                titulo_eliminado = str(row.get('title', ''))[:100]
                if len(str(row.get('title', ''))) > 100:
                    titulo_eliminado += "..."
                
                autores_eliminado = str(row.get('authors', ''))[:50]
                if len(str(row.get('authors', ''))) > 50:
                    autores_eliminado += "..."
                
                # Construir registro para análisis
                analysis_data.append({
                    'indice_eliminado': idx,
                    'indice_conservado': info['kept_index'],
                    'titulo_eliminado': titulo_eliminado,
                    'titulo_conservado': info['duplicate_of_title'],
                    'autores_eliminado': autores_eliminado,
                    'doi_eliminado': row.get('doi', ''),
                    'razon': info['reason']
                })
        
        # Convertir lista a DataFrame
        return pd.DataFrame(analysis_data)


# ============================================================================
# FUNCIÓN DE CONVENIENCIA
# ============================================================================

def clean_ebsco_data(input_file: str, output_base_name: Optional[str] = None) -> Tuple[str, str, str]:
    """
    Función de conveniencia para limpiar datos de EBSCO con un solo comando.
    
    Esta función wrapper simplifica el proceso de limpieza ejecutando
    automáticamente todos los pasos necesarios: carga, limpieza, y exportación.
    Es la forma más rápida de limpiar un dataset si no se necesita acceso
    intermedio a los DataFrames o estadísticas.
    
    Args:
        input_file (str): Ruta al archivo CSV de entrada con datos de EBSCO.
            Debe existir y ser un CSV válido con encoding UTF-8.
        output_base_name (Optional[str], optional): Nombre base para archivos
            de salida. Si es None, genera nombre automático con timestamp.
            Por defecto None.
    
    Returns:
        Tuple[str, str, str]: Tupla con rutas de los tres archivos generados:
            (clean_file_path, full_file_path, report_file_path)
    
    Raises:
        FileNotFoundError: Si input_file no existe
        Exception: Si hay error al cargar los datos
    
    Process:
        1. Verificar que el archivo existe
        2. Crear instancia de DataCleaner
        3. Cargar datos con load_data()
        4. Limpiar datos con clean_data()
        5. Guardar archivos con save_files()
        6. Retornar rutas de archivos generados
    
    Example:
        >>> # Uso básico - nombres automáticos
        >>> clean, full, report = clean_ebsco_data("articles.csv")
        🚀 Iniciando proceso de limpieza de datos EBSCO...
        ✅ Datos cargados exitosamente: 5,432 registros
        🧹 Iniciando limpieza de datos...
        ...
        🎉 Proceso de limpieza completado exitosamente!
        📁 Archivos generados:
           📄 Datos limpios: ebsco_data_20250115_143045_LIMPIO.csv
           📄 Datos completos: ebsco_data_20250115_143045_COMPLETO.csv
           📄 Reporte: ebsco_data_20250115_143045_REPORTE.txt
        
        >>> # Uso con nombre personalizado
        >>> clean, full, report = clean_ebsco_data(
        ...     "raw_articles.csv",
        ...     output_base_name="ml_articles_2025"
        ... )
        
        >>> # Continuar con análisis posterior
        >>> import pandas as pd
        >>> df = pd.read_csv(clean)
        >>> print(f"Dataset limpio: {len(df)} artículos")
    
    Advanced Usage:
        Si necesitas más control sobre el proceso o acceso a estadísticas
        intermedias, usa la clase DataCleaner directamente:
        
        >>> cleaner = DataCleaner("articles.csv")
        >>> cleaner.load_data()
        >>> 
        >>> # Inspeccionar antes de limpiar
        >>> print(f"Registros originales: {len(cleaner.df_original)}")
        >>> 
        >>> # Limpiar
        >>> df_clean = cleaner.clean_data()
        >>> 
        >>> # Analizar duplicados
        >>> dup_analysis = cleaner.get_duplicate_analysis()
        >>> 
        >>> # Guardar
        >>> cleaner.save_files("custom_name")
    
    Best Practices:
        - Mantener backup del archivo original antes de limpiar
        - Revisar el reporte generado para verificar resultados
        - Examinar archivo _COMPLETO.csv para auditar eliminaciones
        - Usar nombres descriptivos para output_base_name
    
    Note:
        Esta función es ideal para scripts automatizados o uso interactivo
        rápido. Para workflows complejos, considera usar DataCleaner directamente.
    """
    print("🚀 Iniciando proceso de limpieza de datos EBSCO...")
    
    # ===== PASO 1: VERIFICAR EXISTENCIA DEL ARCHIVO =====
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"El archivo {input_file} no existe")
    
    # ===== PASO 2: CREAR INSTANCIA DE DATACLEANER =====
    cleaner = DataCleaner(input_file)
    
    # ===== PASO 3: CARGAR DATOS =====
    if not cleaner.load_data():
        raise Exception("Error cargando los datos")
    
    # ===== PASO 4: LIMPIAR DATOS =====
    cleaner.clean_data()
    
    # ===== PASO 5: GUARDAR ARCHIVOS =====
    clean_file, full_file, report_file = cleaner.save_files(output_base_name)
    
    # ===== PASO 6: MOSTRAR RESUMEN =====
    print("🎉 Proceso de limpieza completado exitosamente!")
    print(f"📁 Archivos generados:")
    print(f"   📄 Datos limpios: {clean_file}")
    print(f"   📄 Datos completos: {full_file}")
    print(f"   📄 Reporte: {report_file}")
    
    return clean_file, full_file, report_file


# ============================================================================
# EJEMPLO DE USO
# ============================================================================

if __name__ == "__main__":
    """
    Ejemplo de uso del limpiador de datos de EBSCO.
    
    Este bloque demuestra dos formas de usar el módulo:
    1. Función de conveniencia (rápida y simple)
    2. Clase DataCleaner (control completo)
    """
    
    # ========== OPCIÓN 1: FUNCIÓN DE CONVENIENCIA ==========
    # Forma más simple - un solo comando
    print("=" * 60)
    print("OPCIÓN 1: Usando función de conveniencia")
    print("=" * 60)
    
    clean, full, report = clean_ebsco_data(
        "ebsco_raw_data.csv",
        output_base_name="articles_cleaned_2025"
    )
    
    # ========== OPCIÓN 2: CLASE DATACLEANER (CONTROL TOTAL) ==========
    print("\n" + "=" * 60)
    print("OPCIÓN 2: Usando clase DataCleaner directamente")
    print("=" * 60)
    
    # Crear instancia
    cleaner = DataCleaner("ebsco_raw_data.csv")
    
    # Cargar datos
    if cleaner.load_data():
        # Inspeccionar antes de limpiar
        df = cleaner.df_original
        # df no será None porque load_data() devolvió True, pero ayudamos al type-checker
        assert df is not None
        print(f"\nDataset original: {len(df)} registros")
        print(f"Columnas: {df.columns.tolist()}")
        
        # Limpiar datos
        df_clean = cleaner.clean_data()
        
        # Obtener análisis de duplicados
        dup_analysis = cleaner.get_duplicate_analysis()
        if len(dup_analysis) > 0:
            print(f"\nPrimeros 5 duplicados encontrados:")
            print(dup_analysis.head())
        
        # Guardar archivos
        clean_file, full_file, report_file = cleaner.save_files("detailed_clean")
        
        # Mostrar reporte en consola
        print("\n" + cleaner.generate_cleaning_report())