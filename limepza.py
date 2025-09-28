import pandas as pd
import re
from typing import List, Dict, Tuple, Optional, Any
import hashlib
from datetime import datetime
import os


class DataCleaner:
    """
    Clase para limpiar y procesar datos de EBSCO.
    Genera dos archivos:
    1. CSV limpio sin duplicados
    2. CSV normal con columna indicando registros eliminados
    """
    
    def __init__(self, input_file: str):
        """
        Inicializa el limpiador de datos
        
        Args:
            input_file (str): Ruta al archivo CSV de entrada
        """
        self.input_file = input_file
        # DataFrames principales (se inicializan como None hasta load_data / clean_data)
        self.df_original: Optional[pd.DataFrame] = None
        self.df_clean: Optional[pd.DataFrame] = None
        # Información de duplicados: índice eliminado -> metadata
        self.duplicate_info: Dict[int, Dict[str, Any]] = {}
        self.cleaning_stats: Dict[str, int] = {
            'original_count': 0,
            'clean_count': 0,
            'duplicates_removed': 0,
            'empty_titles_removed': 0,
            'invalid_records_removed': 0
        }
        
    def load_data(self) -> bool:
        """
        Carga los datos del archivo CSV
        
        Returns:
            bool: True si la carga fue exitosa, False en caso contrario
        """
        try:
            self.df_original = pd.read_csv(self.input_file, encoding='utf-8')
            self.cleaning_stats['original_count'] = len(self.df_original)
            print(f"✅ Datos cargados exitosamente: {len(self.df_original):,} registros")
            return True
        except Exception as e:
            print(f"❌ Error cargando el archivo: {e}")
            return False
    
    def clean_text(self, text: str) -> str:
        """
        Limpia texto eliminando caracteres especiales y normalizando espacios
        
        Args:
            text (str): Texto a limpiar
            
        Returns:
            str: Texto limpio
        """
        if pd.isna(text) or text == "":
            return ""
        
        # Convertir a string si no lo es
        text = str(text)
        
        # Eliminar caracteres de control y normalizar espacios
        text = re.sub(r'[\r\n\t]+', ' ', text)
        text = re.sub(r'\s+', ' ', text)
        text = text.strip()
        
        return text
    
    def normalize_title(self, title: str) -> str:
        """
        Normaliza títulos para comparación de duplicados
        
        Args:
            title (str): Título original
            
        Returns:
            str: Título normalizado
        """
        if pd.isna(title) or title == "":
            return ""
        
        # Convertir a minúsculas
        normalized = str(title).lower()
        
        # Eliminar puntuación y caracteres especiales
        normalized = re.sub(r'[^\w\s]', '', normalized)
        
        # Normalizar espacios
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        
        return normalized
    
    def create_duplicate_key(self, row: pd.Series) -> str:
        """
        Crea una clave única para identificar duplicados
        
        Args:
            row (pd.Series): Fila del DataFrame
            
        Returns:
            str: Clave hash para identificar duplicados
        """
        # Usar título normalizado, DOI, y autores para identificar duplicados
        title = self.normalize_title(row.get('title', ''))
        doi = str(row.get('doi', '')).lower().strip()
        authors = str(row.get('authors', '')).lower().strip()
        
        # Crear string combinado para hash
        combined = f"{title}|{doi}|{authors}"
        
        # Generar hash
        return hashlib.md5(combined.encode('utf-8')).hexdigest()
    
    def identify_duplicates(self, df: Optional[pd.DataFrame] = None) -> Dict[str, List[int]]:
        """
        Identifica registros duplicados
        
        Returns:
            Dict[str, List[int]]: Diccionario con claves de duplicados y listas de índices
        """
        print("🔍 Identificando duplicados...")
        # Permite pasar un DataFrame ya filtrado (ej: sin títulos vacíos)
        if df is None:
            df = self.df_original
        if df is None:
            raise ValueError("No hay DataFrame cargado para detección de duplicados.")

        duplicate_groups: Dict[str, List[int]] = {}

        for idx, row in df.iterrows():  # type: ignore[union-attr]
            key: str = self.create_duplicate_key(row)
            if key not in duplicate_groups:
                duplicate_groups[key] = []
            # idx es un entero estándar de iterrows
            duplicate_groups[key].append(idx)  # type: ignore[arg-type]

        # Filtrar solo grupos con duplicados (más de 1 registro)
        duplicates = {k: v for k, v in duplicate_groups.items() if len(v) > 1}

        print(f"📊 Encontrados {len(duplicates)} grupos de duplicados")

        total_duplicates = sum(len(group) - 1 for group in duplicates.values())
        print(f"📊 Total de registros duplicados a eliminar: {total_duplicates}")

        return duplicates
    
    def clean_data(self) -> pd.DataFrame:
        """
        Limpia los datos eliminando duplicados y registros inválidos
        
        Returns:
            pd.DataFrame: DataFrame limpio
        """
        print("🧹 Iniciando limpieza de datos...")
        
        if self.df_original is None:
            raise ValueError("Datos no cargados. Ejecuta load_data() primero.")
        
        # Crear copia para trabajar
        df_work: pd.DataFrame = self.df_original.copy()
        
        # 1. Limpiar texto en columnas principales
        text_columns = ['title', 'abstract', 'authors', 'journal', 'subjects']
        for col in text_columns:
            if col in df_work.columns:
                df_work[col] = df_work[col].apply(self.clean_text)
        
        # 2. Eliminar registros con títulos vacíos
        initial_count = len(df_work)
        # Usar loc para mantener tipo DataFrame explícito
        df_work = df_work.loc[df_work['title'].str.strip() != '']  # type: ignore[assignment]
        empty_titles_removed = initial_count - len(df_work)
        self.cleaning_stats['empty_titles_removed'] = empty_titles_removed
        
        if empty_titles_removed > 0:
            print(f"🗑️ Eliminados {empty_titles_removed} registros con títulos vacíos")
        
        # 3. Identificar y eliminar duplicados
        duplicates = self.identify_duplicates(df_work)
        
        indices_to_remove = []
        duplicate_info = {}
        
        for dup_key, indices in duplicates.items():
            if len(indices) > 1:
                # Mantener el primer registro, marcar el resto para eliminación
                keep_idx = indices[0]
                remove_indices = indices[1:]
                
                for remove_idx in remove_indices:
                    indices_to_remove.append(remove_idx)
                    kept_title_val = df_work.loc[keep_idx, 'title'] if 'title' in df_work.columns else ''
                    kept_title_str = '' if pd.isna(kept_title_val) else str(kept_title_val)
                    short_kept_title = kept_title_str[:100] + ("..." if len(kept_title_str) > 100 else "")
                    duplicate_info[remove_idx] = {
                        'reason': 'DUPLICADO',
                        'kept_index': keep_idx,
                        'duplicate_of_title': short_kept_title
                    }
        
        # Eliminar duplicados
        df_work = df_work.drop(indices_to_remove, errors='ignore')
        self.cleaning_stats['duplicates_removed'] = len(indices_to_remove)
        
        # 4. Resetear índices
        df_work = df_work.reset_index(drop=True)
        
        # Guardar información de limpieza
        self.duplicate_info = duplicate_info
        self.cleaning_stats['clean_count'] = len(df_work)
        
        print(f"✅ Limpieza completada:")
        print(f"   📊 Registros originales: {self.cleaning_stats['original_count']:,}")
        print(f"   📊 Títulos vacíos eliminados: {self.cleaning_stats['empty_titles_removed']:,}")
        print(f"   📊 Duplicados eliminados: {self.cleaning_stats['duplicates_removed']:,}")
        print(f"   📊 Registros finales: {self.cleaning_stats['clean_count']:,}")

        self.df_clean = df_work
        return df_work
    
    def create_removal_info_column(self) -> pd.DataFrame:
        """
        Crea DataFrame original con columna de información de eliminación
        
        Returns:
            pd.DataFrame: DataFrame original con columna 'removal_info'
        """
        if self.df_original is None:
            raise ValueError("df_original es None. Llama a load_data() antes de create_removal_info_column().")

        df_with_info = self.df_original.copy()
        df_with_info['removal_info'] = ''
        
        # Marcar registros que fueron eliminados
        for idx, info in self.duplicate_info.items():
            if idx < len(df_with_info):
                df_with_info.loc[idx, 'removal_info'] = f"ELIMINADO - {info['reason']}: Duplicado del índice {info['kept_index']}"
        
        # Marcar registros con títulos vacíos
        empty_title_mask = df_with_info['title'].str.strip() == ''
        df_with_info.loc[empty_title_mask, 'removal_info'] = 'ELIMINADO - TÍTULO VACÍO'
        
        # Marcar registros que se mantuvieron
        keep_mask = df_with_info['removal_info'] == ''
        df_with_info.loc[keep_mask, 'removal_info'] = 'CONSERVADO'
        
        return df_with_info
    
    def generate_cleaning_report(self) -> str:
        """
        Genera un reporte detallado de la limpieza
        
        Returns:
            str: Reporte de limpieza
        """
        report = f"""
        === REPORTE DE LIMPIEZA DE DATOS EBSCO ===
        Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        Archivo procesado: {self.input_file}

        ESTADÍSTICAS:
        - Registros originales: {self.cleaning_stats['original_count']:,}
        - Registros finales (limpios): {self.cleaning_stats['clean_count']:,}
        - Total eliminados: {self.cleaning_stats['original_count'] - self.cleaning_stats['clean_count']:,}

        DETALLE DE ELIMINACIONES:
        - Títulos vacíos: {self.cleaning_stats['empty_titles_removed']:,}
        - Duplicados: {self.cleaning_stats['duplicates_removed']:,}

        TASA DE LIMPIEZA:
        - Porcentaje conservado: {(self.cleaning_stats['clean_count'] / self.cleaning_stats['original_count'] * 100):.2f}%
        - Porcentaje eliminado: {((self.cleaning_stats['original_count'] - self.cleaning_stats['clean_count']) / self.cleaning_stats['original_count'] * 100):.2f}%

        CRITERIOS DE DUPLICACIÓN:
        - Título normalizado (sin puntuación, minúsculas)
        - DOI (si está disponible)
        - Autores

        ========================================
        """
        return report
    
    def save_files(self, base_filename: Optional[str] = None) -> Tuple[str, str, str]:
        """
        Guarda los archivos procesados y el reporte
        
        Args:
            base_filename (str, optional): Nombre base para los archivos
            
        Returns:
            Tuple[str, str, str]: Rutas de los archivos generados (limpio, completo, reporte)
        """
        if self.df_clean is None:
            raise ValueError("Datos no limpiados. Ejecuta clean_data() primero.")
        
        # Generar nombre base si no se proporciona
        if base_filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            base_filename = f"ebsco_data_{timestamp}"
        
        # Rutas de archivos
        clean_file = f"{base_filename}_LIMPIO.csv"
        full_file = f"{base_filename}_COMPLETO.csv"
        report_file = f"{base_filename}_REPORTE.txt"
        
        # Guardar archivo limpio
        self.df_clean.to_csv(clean_file, index=False, encoding='utf-8')
        print(f"💾 Archivo limpio guardado: {clean_file}")
        
        # Guardar archivo completo con información de eliminación
        df_with_info = self.create_removal_info_column()
        df_with_info.to_csv(full_file, index=False, encoding='utf-8')
        print(f"💾 Archivo completo guardado: {full_file}")
        
        # Guardar reporte
        report = self.generate_cleaning_report()
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report)
        print(f"📋 Reporte guardado: {report_file}")
        
        return clean_file, full_file, report_file
    
    def get_duplicate_analysis(self) -> pd.DataFrame:
        """
        Análisis detallado de duplicados encontrados
        
        Returns:
            pd.DataFrame: Análisis de duplicados
        """
        if not self.duplicate_info:
            return pd.DataFrame()
        
        if self.df_original is None:
            raise ValueError("df_original es None. No se puede generar análisis de duplicados.")

        analysis_data = []
        for idx, info in self.duplicate_info.items():
            if idx < len(self.df_original):  # type: ignore[arg-type]
                row = self.df_original.iloc[idx]  # type: ignore[index]
                analysis_data.append({
                    'indice_eliminado': idx,
                    'indice_conservado': info['kept_index'],
                    'titulo_eliminado': row.get('title', '')[:100] + "...",
                    'titulo_conservado': info['duplicate_of_title'],
                    'autores_eliminado': row.get('authors', '')[:50] + "...",
                    'doi_eliminado': row.get('doi', ''),
                    'razon': info['reason']
                })
        
        return pd.DataFrame(analysis_data)


# Función de uso fácil
def clean_ebsco_data(input_file: str, output_base_name: Optional[str] = None) -> Tuple[str, str, str]:
    """
    Función de conveniencia para limpiar datos de EBSCO
    
    Args:
        input_file (str): Archivo CSV de entrada
        output_base_name (str, optional): Nombre base para archivos de salida
        
    Returns:
        Tuple[str, str, str]: Rutas de archivos generados (limpio, completo, reporte)
    """
    print("🚀 Iniciando proceso de limpieza de datos EBSCO...")
    
    # Verificar que el archivo existe
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"El archivo {input_file} no existe")
    
    # Crear limpiador
    cleaner = DataCleaner(input_file)
    
    # Cargar datos
    if not cleaner.load_data():
        raise Exception("Error cargando los datos")
    
    # Limpiar datos
    cleaner.clean_data()
    
    # Guardar archivos
    clean_file, full_file, report_file = cleaner.save_files(output_base_name)
    
    print("🎉 Proceso de limpieza completado exitosamente!")
    print(f"📁 Archivos generados:")
    print(f"   📄 Datos limpios: {clean_file}")
    print(f"   📄 Datos completos: {full_file}")
    print(f"   📄 Reporte: {report_file}")
    
    return clean_file, full_file, report_file
