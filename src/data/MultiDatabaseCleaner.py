"""
Multi-Database Academic Data Cleaner & Unifier
===============================================

Este módulo proporciona herramientas avanzadas para limpiar, normalizar, unificar
y eliminar duplicados de datasets extraídos de múltiples bases de datos académicas:
- EBSCO
- IEEE Xplore

El proceso incluye:
- Limpieza individual de cada base de datos
- Normalización de esquemas (columnas) entre bases de datos
- Detección inteligente de duplicados INTER-BASE e INTRA-BASE
- Consolidación de información: fusión de campos cuando hay duplicados
- Generación de dataset unificado con toda la información disponible
- Reportes detallados de limpieza y unificación

Criterios de Duplicación:
--------------------------
Dos artículos se consideran duplicados si comparten:
1. DOI (más confiable, si está disponible)
2. Título normalizado (sin puntuación, minúsculas)
3. Autores normalizados

Cuando se detecta un duplicado:
- Se mantiene UN SOLO registro
- Se consolida información de TODOS los duplicados
- Campos vacíos se rellenan con datos de duplicados
- Campos múltiples (autores, keywords) se fusionan sin repetir

Archivos Generados:
-------------------
1. *_UNIFICADO.csv: Dataset final unificado sin duplicados
2. *_LIMPIO_[DATABASE].csv: Cada base de datos limpia individualmente
3. *_REPORTE_UNIFICACION.txt: Reporte detallado con estadísticas
4. *_MAPA_DUPLICADOS.csv: Mapa de qué registros se fusionaron

Workflow Típico:
----------------
1. Cargar múltiples CSV (EBSCO, IEEE)
2. Normalizar esquemas a columnas comunes
3. Limpiar cada dataset individualmente
4. Identificar duplicados entre bases de datos
5. Consolidar información de duplicados
6. Exportar dataset unificado

Fecha: 2025
"""

import pandas as pd
import re
from typing import List, Dict, Tuple, Optional, Any, Set
import hashlib
from datetime import datetime
import os
from pathlib import Path


class MultiDatabaseCleaner:
    """
    Clase para limpiar y unificar datos de múltiples bases de datos académicas.
    
    Esta clase extiende la funcionalidad de DataCleaner para trabajar con
    múltiples fuentes de datos simultáneamente, detectando duplicados entre
    bases de datos y consolidando información.
    
    Soporta:
    - EBSCO
    - IEEE Xplore  
    
    Attributes:
        input_files (Dict[str, str]): Diccionario con archivos de entrada
            Key: nombre de base de datos ('ebsco', 'ieee')
            Value: ruta al archivo CSV
        dataframes (Dict[str, pd.DataFrame]): DataFrames originales por base de datos
        clean_dataframes (Dict[str, pd.DataFrame]): DataFrames limpios por base de datos
        unified_df (Optional[pd.DataFrame]): DataFrame unificado final
        duplicate_map (Dict[int, List[Tuple[str, int]]]): Mapa de duplicados
        statistics (Dict[str, Any]): Estadísticas del proceso
    
    Example:
        >>> cleaner = MultiDatabaseCleaner()
        >>> cleaner.add_database('ebsco', 'ebsco_articles.csv')
        >>> cleaner.add_database('ieee', 'ieee_articles.csv')
    >>> # Solo EBSCO e IEEE
        >>> 
        >>> cleaner.load_all()
        >>> unified_df = cleaner.clean_and_unify()
        >>> cleaner.save_files('academic_data_unified')
    """
    
    # Mapeo de columnas entre bases de datos a esquema unificado
    COLUMN_MAPPING = {
        'ebsco': {
            'title': 'title',
            'abstract': 'abstract',
            'authors': 'authors',
            'publication_date': 'publication_date',
            'journal': 'publication_title',
            'doi': 'doi',
            'subjects': 'keywords',
            'page_start': 'page_start',
            'page_end': 'page_end',
            'volume': 'volume',
            'issue': 'issue',
            'publisher': 'publisher',
            'pdf_links': 'pdf_url',
            'database': 'source_database',
            'peer_reviewed': 'peer_reviewed',
            'language': 'language',
            'issn': 'issn',
            'isbn': 'isbn',
        },
        'ieee': {
            'article_number': 'article_id',
            'title': 'title',
            'abstract': 'abstract',
            'authors': 'authors',
            'publication_title': 'publication_title',
            'publication_year': 'publication_year',
            'publication_date': 'publication_date',
            'doi': 'doi',
            'isbn': 'isbn',
            'issn': 'issn',
            'content_type': 'document_type',
            'publisher': 'publisher',
            'conference_location': 'conference_location',
            'volume': 'volume',
            'issue': 'issue',
            'start_page': 'page_start',
            'end_page': 'page_end',
            'index_terms': 'keywords',
            'pdf_url': 'pdf_url',
            'access_type': 'access_type',
            'is_open_access': 'is_open_access',
            'citing_paper_count': 'citation_count',
        },
        
    }
    
    # Columnas del esquema unificado
    UNIFIED_SCHEMA = [
        'article_id', 'title', 'subtitle', 'abstract', 'authors', 
        'publication_title', 'journal_title', 'publication_year', 'publication_date',
        'doi', 'isbn', 'issn', 'eissn', 'document_type', 'publisher',
        'language', 'volume', 'issue', 'page_start', 'page_end',
        'page_count', 'keywords', 'subject_area', 'pdf_url', 'url', 'stable_url',
        'access_type', 'is_open_access', 'peer_reviewed',
        'citation_count', 'conference_location',
        'source_databases',  # Lista de bases de datos de origen
        'original_indices',  # Índices originales en cada base de datos
    ]
    
    def __init__(self):
        """
        Inicializa el limpiador multi-base de datos.
        
        Configura estructuras de datos para manejar múltiples fuentes,
        mapeos de columnas y estadísticas de unificación.
        """
        # Diccionario de archivos de entrada por base de datos
        self.input_files: Dict[str, str] = {}
        
        # DataFrames originales por base de datos
        self.dataframes: Dict[str, pd.DataFrame] = {}
        
        # DataFrames limpios (sin duplicados intra-base) por base de datos
        self.clean_dataframes: Dict[str, pd.DataFrame] = {}
        
        # DataFrame unificado final
        self.unified_df: Optional[pd.DataFrame] = None
        
        # Mapa de duplicados: key = índice en unified_df, 
        # value = lista de (database, original_index) que se fusionaron
        self.duplicate_map: Dict[int, List[Tuple[str, int]]] = {}
        
        # Estadísticas del proceso
        self.statistics: Dict[str, Any] = {
            'databases_loaded': 0,
            'total_original_records': 0,
            'total_after_individual_cleaning': 0,
            'total_after_unification': 0,
            'duplicates_within_databases': {},
            'duplicates_between_databases': 0,
            'records_by_database': {},
            'consolidated_records': 0,
        }
    
    def add_database(self, db_name: str, file_path: str) -> bool:
        """
        Agrega una base de datos al proceso de limpieza.
        
        Args:
            db_name (str): Nombre de la base de datos ('ebsco', 'ieee')
            file_path (str): Ruta al archivo CSV
        
        Returns:
            bool: True si se agregó exitosamente, False si hay error
        """
        # Validar nombre de base de datos
        db_name_lower = db_name.lower()
        if db_name_lower not in ['ebsco', 'ieee']:
            print(f"❌ Base de datos '{db_name}' no soportada. Usa: ebsco, ieee")
            return False
        
        # Validar existencia del archivo
        if not os.path.exists(file_path):
            print(f"❌ Archivo no encontrado: {file_path}")
            return False
        
        # Agregar a diccionario
        self.input_files[db_name_lower] = file_path
        print(f"✅ Base de datos '{db_name_lower}' agregada: {file_path}")
        return True
    
    def load_all(self) -> bool:
        """
        Carga todos los archivos CSV agregados.
        
        Returns:
            bool: True si todos se cargaron exitosamente
        """
        print("\n📂 Cargando archivos de bases de datos...")
        
        if not self.input_files:
            print("❌ No hay archivos agregados. Usa add_database() primero.")
            return False
        
        success = True
        for db_name, file_path in self.input_files.items():
            try:
                df = pd.read_csv(file_path, encoding='utf-8')
                self.dataframes[db_name] = df
                
                # Actualizar estadísticas
                count = len(df)
                self.statistics['records_by_database'][db_name] = {
                    'original': count,
                    'after_cleaning': 0,
                    'in_unified': 0
                }
                self.statistics['total_original_records'] += count
                
                print(f"  ✅ {db_name.upper()}: {count:,} registros cargados")
                
            except Exception as e:
                print(f"  ❌ Error cargando {db_name}: {e}")
                success = False
        
        if success:
            self.statistics['databases_loaded'] = len(self.dataframes)
            print(f"\n✅ {len(self.dataframes)} bases de datos cargadas exitosamente")
            print(f"📊 Total de registros: {self.statistics['total_original_records']:,}")
        
        return success
    
    def normalize_schema(self, df: pd.DataFrame, db_name: str) -> pd.DataFrame:
        """
        Normaliza el esquema de un DataFrame al esquema unificado.
        
        Args:
            df (pd.DataFrame): DataFrame original
            db_name (str): Nombre de la base de datos
        
        Returns:
            pd.DataFrame: DataFrame con columnas normalizadas
        """
        # Obtener mapeo de columnas para esta base de datos
        column_map = self.COLUMN_MAPPING.get(db_name, {})
        
        # Crear DataFrame normalizado con columnas del esquema unificado
        normalized_df = pd.DataFrame()
        
        # Mapear columnas existentes
        for original_col, unified_col in column_map.items():
            if original_col in df.columns:
                normalized_df[unified_col] = df[original_col]
        
        # Agregar columna de base de datos de origen
        normalized_df['source_databases'] = db_name
        normalized_df['original_indices'] = df.index.astype(str) + f"@{db_name}"
        
        # Agregar columnas faltantes del esquema unificado con valores vacíos
        for col in self.UNIFIED_SCHEMA:
            if col not in normalized_df.columns:
                normalized_df[col] = ''
        
        # Reordenar columnas según esquema unificado
        normalized_df = normalized_df[self.UNIFIED_SCHEMA]
        
        return normalized_df
    
    def clean_text(self, text: str) -> str:
        """Limpia y normaliza texto."""
        if pd.isna(text) or text == "":
            return ""
        text = str(text)
        text = re.sub(r'[\r\n\t]+', ' ', text)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()
    
    def normalize_title(self, title: str) -> str:
        """Normaliza títulos para comparación."""
        if pd.isna(title) or title == "":
            return ""
        normalized = str(title).lower()
        normalized = re.sub(r'[^\w\s]', '', normalized)
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        return normalized
    
    def create_duplicate_key(self, row: pd.Series) -> str:
        """
        Crea clave única para identificar duplicados.
        
        Prioriza DOI, luego combina título + autores.
        """
        # Si hay DOI, usarlo como clave principal (más confiable)
        doi = str(row.get('doi', '')).lower().strip()
        if doi and doi != 'nan' and doi != '':
            return f"doi:{doi}"
        
        # Si no hay DOI, usar título + autores
        title = self.normalize_title(row.get('title', ''))
        authors = str(row.get('authors', '')).lower().strip()
        
        combined = f"{title}|{authors}"
        hash_object = hashlib.md5(combined.encode('utf-8'))
        return f"hash:{hash_object.hexdigest()}"
    
    def clean_individual_database(self, db_name: str) -> pd.DataFrame:
        """
        Limpia una base de datos individual (elimina duplicados intra-base).
        
        Args:
            db_name (str): Nombre de la base de datos
        
        Returns:
            pd.DataFrame: DataFrame limpio
        """
        print(f"\n🧹 Limpiando {db_name.upper()}...")
        
        df = self.dataframes[db_name].copy()
        original_count = len(df)
        
        # Normalizar esquema
        df_normalized = self.normalize_schema(df, db_name)
        
        # Limpiar texto en columnas principales
        text_columns = ['title', 'abstract', 'authors', 'keywords', 'publication_title', 'journal_title']
        for col in text_columns:
            if col in df_normalized.columns:
                df_normalized[col] = df_normalized[col].apply(self.clean_text)
        
        # Eliminar registros con títulos vacíos
        df_normalized = df_normalized[df_normalized['title'].str.strip() != '']
        empty_removed = original_count - len(df_normalized)
        
        # Identificar y eliminar duplicados intra-base
        duplicate_keys = {}
        indices_to_keep = []
        
        for idx, row in df_normalized.iterrows():
            key = self.create_duplicate_key(row)
            
            if key not in duplicate_keys:
                duplicate_keys[key] = idx
                indices_to_keep.append(idx)
        
        df_clean = df_normalized.loc[indices_to_keep].reset_index(drop=True)
        
        duplicates_removed = len(df_normalized) - len(df_clean)
        
        # Actualizar estadísticas
        self.statistics['duplicates_within_databases'][db_name] = duplicates_removed
        self.statistics['records_by_database'][db_name]['after_cleaning'] = len(df_clean)
        
        print(f"  📊 Original: {original_count:,} registros")
        print(f"  🗑️ Títulos vacíos eliminados: {empty_removed:,}")
        print(f"  🗑️ Duplicados eliminados: {duplicates_removed:,}")
        print(f"  ✅ Final: {len(df_clean):,} registros")
        
        return df_clean
    
    def consolidate_fields(self, records: List[pd.Series]) -> pd.Series:
        """
        Consolida información de múltiples registros duplicados.
        
        Estrategia:
        - Para campos simples: tomar el primero no vacío
        - Para campos múltiples (autores, keywords): fusionar sin duplicar
        - Para listas (source_databases): concatenar
        
        Args:
            records (List[pd.Series]): Lista de registros a consolidar
        
        Returns:
            pd.Series: Registro consolidado con toda la información
        """
        if len(records) == 1:
            return records[0]
        
        # Crear registro consolidado
        consolidated = pd.Series(index=self.UNIFIED_SCHEMA, dtype=object)
        
        # Campos que son listas de valores separados por ";"
        list_fields = ['authors', 'keywords']
        
        # Campos especiales
        special_fields = ['source_databases', 'original_indices']
        
        for col in self.UNIFIED_SCHEMA:
            if col in special_fields:
                # Concatenar todas las fuentes
                values = []
                for record in records:
                    val = record.get(col, '')
                    if val and str(val) != '' and str(val) != 'nan':
                        values.append(str(val))
                consolidated[col] = '; '.join(values)
                
            elif col in list_fields:
                # Consolidar listas sin duplicar
                all_items = set()
                for record in records:
                    val = record.get(col, '')
                    if val and str(val) != '' and str(val) != 'nan':
                        items = str(val).split(';')
                        for item in items:
                            cleaned = item.strip()
                            if cleaned:
                                all_items.add(cleaned)
                
                consolidated[col] = '; '.join(sorted(all_items))
                
            else:
                # Para campos simples: tomar el primero no vacío
                for record in records:
                    val = record.get(col, '')
                    if val and str(val) != '' and str(val) != 'nan':
                        consolidated[col] = val
                        break
                
                # Si no se encontró valor, dejar vacío
                if pd.isna(consolidated.get(col)):
                    consolidated[col] = ''
        
        return consolidated
    
    def unify_databases(self) -> pd.DataFrame:
        """
        Unifica todas las bases de datos en un solo DataFrame.
        
        Detecta duplicados entre bases de datos y consolida información.
        
        Returns:
            pd.DataFrame: DataFrame unificado
        """
        print("\n🔗 Unificando bases de datos...")
        
        if not self.clean_dataframes:
            raise ValueError("No hay bases de datos limpias. Ejecuta clean_all() primero.")
        
        # Concatenar todos los DataFrames limpios
        all_dfs = []
        for db_name, df in self.clean_dataframes.items():
            all_dfs.append(df)
        
        combined_df = pd.concat(all_dfs, ignore_index=True)
        print(f"  📊 Total de registros combinados: {len(combined_df):,}")
        
        # Identificar duplicados entre bases de datos
        print("\n  🔍 Identificando duplicados entre bases de datos...")
        duplicate_groups: Dict[str, List[int]] = {}
        
        for idx, row in combined_df.iterrows():
            key = self.create_duplicate_key(row)
            
            if key not in duplicate_groups:
                duplicate_groups[key] = []
            duplicate_groups[key].append(idx)  # type: ignore
        
        # Filtrar solo grupos con duplicados
        duplicates = {k: v for k, v in duplicate_groups.items() if len(v) > 1}
        
        total_duplicates = sum(len(group) - 1 for group in duplicates.values())
        print(f"  📊 Grupos de duplicados encontrados: {len(duplicates)}")
        print(f"  📊 Registros duplicados a consolidar: {total_duplicates}")
        
        self.statistics['duplicates_between_databases'] = total_duplicates
        
        # Consolidar duplicados
        print("\n  🔄 Consolidando información de duplicados...")
        unified_records = []
        processed_indices = set()
        
        for key, indices in duplicate_groups.items():
            if indices[0] in processed_indices:
                continue
            
            # Obtener todos los registros del grupo
            records = [combined_df.iloc[idx] for idx in indices]
            
            # Consolidar información
            consolidated = self.consolidate_fields(records)
            unified_records.append(consolidated)
            
            # Marcar índices como procesados
            for idx in indices:
                processed_indices.add(idx)
            
            # Guardar mapa de duplicados
            if len(indices) > 1:
                self.duplicate_map[len(unified_records) - 1] = [
                    (row['source_databases'], row['original_indices']) 
                    for row in records
                ]
        
        # Crear DataFrame unificado
        unified_df = pd.DataFrame(unified_records)
        
        # Actualizar estadísticas
        self.statistics['total_after_unification'] = len(unified_df)
        self.statistics['consolidated_records'] = len(self.duplicate_map)
        
        print(f"  ✅ Registros en dataset unificado: {len(unified_df):,}")
        print(f"  ✅ Registros consolidados (fusionados): {len(self.duplicate_map):,}")
        
        return unified_df
    
    def clean_and_unify(self) -> pd.DataFrame:
        """
        Ejecuta el pipeline completo de limpieza y unificación.
        
        Returns:
            pd.DataFrame: DataFrame unificado final
        """
        print("\n" + "="*70)
        print("🚀 INICIANDO PROCESO DE LIMPIEZA Y UNIFICACIÓN")
        print("="*70)
        
        # Verificar que hay datos cargados
        if not self.dataframes:
            raise ValueError("No hay datos cargados. Ejecuta load_all() primero.")
        
        # Limpiar cada base de datos individualmente
        for db_name in self.dataframes.keys():
            self.clean_dataframes[db_name] = self.clean_individual_database(db_name)
            self.statistics['total_after_individual_cleaning'] += len(self.clean_dataframes[db_name])
        
        # Unificar bases de datos
        self.unified_df = self.unify_databases()
        
        print("\n" + "="*70)
        print("🎉 PROCESO COMPLETADO EXITOSAMENTE")
        print("="*70)
        
        return self.unified_df
    
    def generate_report(self) -> str:
        """
        Genera reporte detallado del proceso de unificación.
        
        Returns:
            str: Reporte formateado
        """
        report = f"""
{'='*70}
REPORTE DE LIMPIEZA Y UNIFICACIÓN DE DATOS ACADÉMICOS
{'='*70}
Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

BASES DE DATOS PROCESADAS:
---------------------------
"""
        for db_name, stats in self.statistics['records_by_database'].items():
            report += f"""
{db_name.upper()}:
  - Registros originales: {stats['original']:,}
  - Duplicados intra-base eliminados: {self.statistics['duplicates_within_databases'].get(db_name, 0):,}
  - Registros después de limpieza: {stats['after_cleaning']:,}
"""
        
        report += f"""
ESTADÍSTICAS GENERALES:
-----------------------
- Total de registros originales: {self.statistics['total_original_records']:,}
- Total después de limpieza individual: {self.statistics['total_after_individual_cleaning']:,}
- Duplicados entre bases de datos: {self.statistics['duplicates_between_databases']:,}
- Total en dataset unificado: {self.statistics['total_after_unification']:,}
- Registros consolidados (fusionados): {self.statistics['consolidated_records']:,}

TASA DE REDUCCIÓN:
------------------
- Porcentaje final: {(self.statistics['total_after_unification'] / self.statistics['total_original_records'] * 100):.2f}%
- Registros eliminados/consolidados: {self.statistics['total_original_records'] - self.statistics['total_after_unification']:,}

CRITERIOS DE DUPLICACIÓN:
--------------------------
1. DOI (Digital Object Identifier) - prioridad máxima
2. Título normalizado (sin puntuación, minúsculas)
3. Autores normalizados

CONSOLIDACIÓN DE INFORMACIÓN:
------------------------------
- Campos simples: se toma el primer valor no vacío
- Campos múltiples (autores, keywords): se fusionan sin duplicar
- Source databases: se concatenan todas las fuentes

{'='*70}
"""
        return report
    
    def save_files(self, base_filename: Optional[str] = None) -> Dict[str, str]:
        """
        Guarda todos los archivos generados.
        
        Args:
            base_filename (Optional[str]): Nombre base para archivos
        
        Returns:
            Dict[str, str]: Diccionario con rutas de archivos generados
        """
        if self.unified_df is None:
            raise ValueError("No hay datos unificados. Ejecuta clean_and_unify() primero.")
        
        # Crear directorio de salida dentro de src/data/unified
        output_dir = Path(__file__).resolve().parent / "unified"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Generar nombre base si no se proporciona
        if base_filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            base_filename = f"academic_data_{timestamp}"
        
        base_path = output_dir / base_filename
        
        files_generated = {}
        
        # 1. Guardar dataset unificado
        # Antes de exportar, eliminar columnas solicitadas del CSV UNIFICADO
        columns_to_drop = [
            'subtitle', 'journal_title', 'isbn', 'issn', 'eissn', 'language',
            'page_count', 'subject_area', 'url', 'stable_url', 'conference_location'
        ]
        df_to_save = self.unified_df.drop(columns=columns_to_drop, errors='ignore')
        unified_file = f"{base_path}_UNIFICADO.csv"
        df_to_save.to_csv(unified_file, index=False, encoding='utf-8')
        files_generated['unified'] = unified_file
        print(f"💾 Dataset unificado: {unified_file}")
        
        # 2. Guardar datasets limpios individuales
        for db_name, df in self.clean_dataframes.items():
            clean_file = f"{base_path}_LIMPIO_{db_name.upper()}.csv"
            df.to_csv(clean_file, index=False, encoding='utf-8')
            files_generated[f'clean_{db_name}'] = clean_file
            print(f"💾 {db_name.upper()} limpio: {clean_file}")
        
        # 3. Guardar reporte
        report = self.generate_report()
        report_file = f"{base_path}_REPORTE_UNIFICACION.txt"
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report)
        files_generated['report'] = report_file
        print(f"📋 Reporte: {report_file}")
        
        # 4. Guardar mapa de duplicados
        if self.duplicate_map:
            dup_map_data = []
            for unified_idx, sources in self.duplicate_map.items():
                for db, original_idx in sources:
                    dup_map_data.append({
                        'unified_index': unified_idx,
                        'source_database': db,
                        'original_index': original_idx
                    })
            
            dup_map_df = pd.DataFrame(dup_map_data)
            dup_map_file = f"{base_path}_MAPA_DUPLICADOS.csv"
            dup_map_df.to_csv(dup_map_file, index=False, encoding='utf-8')
            files_generated['duplicate_map'] = dup_map_file
            print(f"🗺️  Mapa de duplicados: {dup_map_file}")
        
        print(f"\n✅ {len(files_generated)} archivos generados exitosamente")
        
        return files_generated


# ============================================================================
# FUNCIÓN DE CONVENIENCIA
# ============================================================================

def clean_and_unify_databases(
    ebsco_file: Optional[str] = None,
    ieee_file: Optional[str] = None,
    output_name: Optional[str] = None
) -> Tuple[pd.DataFrame, Dict[str, str]]:
    """
    Función de conveniencia para limpiar y unificar múltiples bases de datos.
    
    Args:
        ebsco_file (Optional[str]): Ruta al CSV de EBSCO
        ieee_file (Optional[str]): Ruta al CSV de IEEE
        output_name (Optional[str]): Nombre base para archivos de salida
    
    Returns:
        Tuple[pd.DataFrame, Dict[str, str]]: (DataFrame unificado, archivos generados)
    
    Example:
    >>> # Solo dos bases de datos
    >>> unified_df, files = clean_and_unify_databases(
    ...     ebsco_file='ebsco_articles.csv',
    ...     ieee_file='ieee_articles.csv'
    ... )
        
        >>> print(f"Dataset unificado: {len(unified_df)} artículos")
        >>> print(f"Archivo principal: {files['unified']}")
    """
    print("🚀 Iniciando limpieza y unificación de bases de datos académicas...\n")
    
    # Crear instancia del limpiador
    cleaner = MultiDatabaseCleaner()
    
    # Agregar bases de datos disponibles
    added_count = 0
    if ebsco_file:
        if cleaner.add_database('ebsco', ebsco_file):
            added_count += 1
    
    if ieee_file:
        if cleaner.add_database('ieee', ieee_file):
            added_count += 1
    
    # Solo EBSCO e IEEE están soportadas
    
    if added_count == 0:
        raise ValueError("No se agregó ninguna base de datos válida")
    
    # Cargar datos
    if not cleaner.load_all():
        raise Exception("Error cargando datos")
    
    # Limpiar y unificar
    unified_df = cleaner.clean_and_unify()
    
    # Guardar archivos
    files = cleaner.save_files(output_name)
    
    print("\n🎉 Proceso completado exitosamente!")
    
    return unified_df, files

