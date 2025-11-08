"""
Academic Analysis System - GUI con Webview
==========================================

Interfaz gráfica moderna para el sistema de análisis académico.
Diseño moderno con gradientes y animaciones suaves.

Características:
- Dashboard con menú lateral
- Validación automática de cookies
- Login con timeout 2FA (60 seg)
- Búsqueda de disponibilidad por query
- 4 secciones: Pipeline Completo, Scraping, Limpieza, Algoritmos
- Visualizaciones inline
- Botones de descarga
- Opción de mostrar/ocultar navegador
- HTML externo para mejor mantenimiento

Autor: 2025
"""

import webview
import sys
import os
from pathlib import Path
import json
import threading
import time
import base64
from io import BytesIO
from io import StringIO
import logging
from typing import Any, Dict, Optional, cast
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing

# ===== SUPRIMIR ERRORES DE PYWEBVIEW/WEBVIEW2 =====
import warnings
warnings.filterwarnings("ignore")

# Configurar logging para suprimir errores de pywebview
logging.getLogger('pywebview').setLevel(logging.CRITICAL)

# Aumentar límite de recursión
sys.setrecursionlimit(5000)

# Suprimir stderr temporalmente para WebView2
import contextlib

@contextlib.contextmanager
def suppress_stderr():
    """Suprimir stderr temporalmente."""
    with open(os.devnull, 'w') as devnull:
        old_stderr = sys.stderr
        sys.stderr = devnull
        try:
            yield
        finally:
            sys.stderr = old_stderr

# Agregar src al path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from scraper.EBSCO import EBSCOScraper
from scraper.IEEEScraper import IEEEScraper
from data.MultiDatabaseCleaner import MultiDatabaseCleaner, clean_and_unify_databases
from algoritmo.SimilitudTextualClasico import SimilitudTextualClasico
from algoritmo.SimilitudTextualIA import SimilitudTextualIA
import pandas as pd
from algoritmo.AcademicSortingAnalyzer import AcademicSortingAnalyzer
from algoritmo.ConceptsCategoryAnalyzer import ConceptsCategoryAnalyzer
from algoritmo.HierarchicalClusteringAnalyzer import HierarchicalClusteringAnalyzer
from algoritmo.CitationNetworkAnalyzer import CitationNetworkAnalyzer

import matplotlib.pyplot as plt
import seaborn as sns
from wordcloud import WordCloud
import folium
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.lib.utils import ImageReader
import tempfile
# Agrego imports para geocodificación y screenshots
try:
    import geopy
    from geopy.geocoders import Nominatim
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
except ImportError:
    geopy = None
    Nominatim = None
    webdriver = None
    Options = None

# Importar la clase en el encabezado
from src.algoritmo.BibliometricVisualizer import BibliometricVisualizer

# Directorios de datos centralizados bajo src/data
ROOT_DIR = Path(__file__).parent
DATA_DIR = ROOT_DIR / "src" / "data"
COOKIES_DIR = DATA_DIR / "cookies"
CSV_DIR = DATA_DIR / "csv"
UNIFIED_DIR = DATA_DIR / "unified"
for d in (COOKIES_DIR, CSV_DIR, UNIFIED_DIR):
    os.makedirs(d, exist_ok=True)


def _scrape_db_job(db: str, query: str, download_all: bool, custom_amount: int,
                    email: Optional[str], password: Optional[str], show_browser: bool,
                    threads_per_db: int = 4,
                    log_queue: Optional[Any] = None) -> Dict[str, Any]:
    """
    Proceso aislado para scrapear una base de datos.
    Retorna dict con {db, count, file, error?}
    """
    # Capturar toda la salida de consola de este proceso (incluye prints de scrapers)
    log_capture = StringIO()
    
    # Stream live logs to parent via a queue while also capturing them
    import io as _io
    class _StreamTee(_io.TextIOBase):
        def __init__(self, prefix: str, queue: Optional[Any], sink: StringIO):
            self.prefix = prefix
            self.queue = queue
            self.sink = sink
            self._buffer = ''
        def write(self, s: str) -> int:
            # write also to the capture sink
            written = self.sink.write(s)
            if self.queue is not None:
                self._buffer += s
                while '\n' in self._buffer:
                    line, self._buffer = self._buffer.split('\n', 1)
                    line = line.strip()
                    if line:
                        try:
                            self.queue.put(f"{self.prefix}{line}")
                        except Exception:
                            pass
            return written
        def flush(self):
            try:
                self.sink.flush()
            except Exception:
                pass
    from contextlib import redirect_stdout, redirect_stderr
    try:
        # Use a tee to both capture and stream
        tee = _StreamTee(prefix=f"[{db.upper()}] ", queue=log_queue, sink=log_capture)
        # El cast evita que el analizador estático se queje; en runtime cumple el protocolo de escritura
        with redirect_stdout(cast(Any, tee)), redirect_stderr(cast(Any, tee)):
            headless = not show_browser
            # Instanciar scraper por base
            if db == 'ebsco':
                scraper = EBSCOScraper(auto_login=False)
                cookie_file = 'ebsco_cookies.json'
            elif db == 'ieee':
                scraper = IEEEScraper(auto_login=False)
                cookie_file = 'ieee_cookies.json'
            else:
                return { 'db': db, 'error': f"Base desconocida: {db}", 'logs': log_capture.getvalue() }

            # Cargar cookies
            try:
                scraper.load_cookies(str(COOKIES_DIR / cookie_file))
            except Exception:
                pass

            # Validar cookies; intentar login si son inválidas y hay credenciales
            if not scraper.test_cookies():
                if email and password:
                    try:
                        scraper.login_and_get_cookies(email=email, password=password, headless=headless)
                        # Re-guardar cookies explícitamente en src/data/cookies
                        scraper.save_cookies(str(COOKIES_DIR / cookie_file))
                    except Exception as e:
                        return { 'db': db, 'error': f"Error de login: {e}", 'logs': log_capture.getvalue() }
                    if not scraper.test_cookies():
                        return { 'db': db, 'error': "No fue posible autenticar", 'logs': log_capture.getvalue() }
                else:
                    return { 'db': db, 'error': "No autenticado y sin credenciales", 'logs': log_capture.getvalue() }

            # Ejecutar scraping (con hilos por páginas para IEEE/JSTOR desde aquí para evitar firmas distintas)
            max_results = None if download_all else custom_amount
            articles: list[dict] = []
            if db == 'ebsco' or threads_per_db <= 1:
                # EBSCO o sin concurrencia: usar método propio
                articles = scraper.scrape_all(query, max_results=max_results)
            else:
                # Concurrencia por páginas para IEEE
                from concurrent.futures import ThreadPoolExecutor, as_completed
                # Determinar parámetros de paginación
                if db == 'ieee':
                    page_size_default = 25
                    total = scraper.get_total_items(query)
                    if total == 0:
                        articles = []
                    else:
                        target = min(max_results or total, total)
                        pages = []
                        remaining = target
                        page_number = 1
                        while remaining > 0:
                            current_records = min(page_size_default, remaining)
                            pages.append((page_number, current_records))
                            remaining -= current_records
                            page_number += 1
                        with ThreadPoolExecutor(max_workers=threads_per_db) as executor:
                            s = cast(Any, scraper)
                            def fetch_ieee(pn: int, rec: int):
                                return s.search(query, pn, rec, False)
                            futures = [executor.submit(fetch_ieee, pn, rec) for (pn, rec) in pages]
                            for fut in as_completed(futures):
                                data = fut.result()
                                arts = scraper.extract_articles(data)
                                articles.extend(arts)
                        articles = articles[:target]

            filename = f"{db}_{query.replace(' ', '_')}.csv"
            # Guardar CSV en src/data/csv
            full_csv_path = str(CSV_DIR / filename)
            scraper.save_to_csv(articles, full_csv_path)
            file_path = full_csv_path
            # ensure any remaining partial line is sent
            try:
                if log_queue is not None and getattr(tee, '_buffer', ''):
                    rem = tee._buffer.strip()
                    if rem:
                        log_queue.put(f"[{db.upper()}] {rem}")
            except Exception:
                pass
            return { 'db': db, 'count': len(articles), 'file': file_path, 'logs': log_capture.getvalue() }

    except Exception as e:
        # try send last buffer
        try:
            if log_queue is not None:
                log_queue.put(f"[{db.upper()}] ERROR: {e}")
        except Exception:
            pass
        return { 'db': db, 'error': str(e), 'logs': log_capture.getvalue() }
    finally:
        # signal end of stream
        try:
            if log_queue is not None:
                log_queue.put(None)
        except Exception:
            pass


class AcademicAnalysisAPI:
    """
    API Backend para la interfaz gráfica.
    Expone métodos Python al frontend JavaScript.
    """
    
    def __init__(self):
        self.window = None
        self.status = {
            'phase': 'idle',
            'progress': 0,
            'message': 'Listo para comenzar',
            'substatus': '',
            'results': {}
        }
        
        # Instancias de scrapers
        self.scrapers: Dict[str, Any] = {
            'ebsco': None,
            'ieee': None,
        }
        
        # Datos del proceso
        self.scraped_files = {}
        self.unified_file = None
        self.sorted_file = None
        self.analysis_results = {}
        # Buffer simple de logs para UI
        self.log_buffer: list[str] = []
        self.max_log_lines = 1000
        # Estado de red de citaciones
        self.citation_analyzer = None  # type: Optional[CitationNetworkAnalyzer]
        self.citation_nodes = []       # type: list[dict]
        
    def set_window(self, window):
        """Asignar referencia a la ventana."""
        self.window = window
    
    def update_status(self, phase, progress, message, substatus=''):
        """Actualizar estado y notificar al frontend."""
        self.status = {
            'phase': phase,
            'progress': progress,
            'message': message,
            'substatus': substatus,
            'results': self.status.get('results', {})
        }
        if self.window:
            self.window.evaluate_js(f'window.updateStatus({json.dumps(self.status)})')

    def log(self, message: str):
        """Añade un mensaje al log y lo envía a la UI."""
        try:
            ts = time.strftime('%H:%M:%S')
            line = f"[{ts}] {message}"
            self.log_buffer.append(line)
            if len(self.log_buffer) > self.max_log_lines:
                self.log_buffer = self.log_buffer[-self.max_log_lines:]
            if self.window:
                safe = json.dumps(line)
                self.window.evaluate_js(f'window.appendLog({safe})')
        except Exception:
            pass

    def log_blob(self, blob: str):
        """Añade múltiples líneas al log (por ejemplo, de subprocesos)."""
        if not blob:
            return
        for raw_line in blob.splitlines():
            self.log(raw_line)
    
    def get_status(self):
        """Obtener estado actual."""
        return self.status
    
    def check_cookies(self, databases):
        """
        Verificar si existen cookies válidas para las bases de datos seleccionadas.
        
        Args:
            databases: Lista de bases de datos ['ebsco', 'ieee']
        
        Returns:
            dict: {database: bool} indicando si las cookies son válidas
        """
        results = {}
        
        for db in databases:
            cookie_file = str(COOKIES_DIR / f"{db}_cookies.json")
            
            if not os.path.exists(cookie_file):
                results[db] = False
                continue
            
            try:
                # Intentar crear scraper SIN auto_login y validar cookies
                if db == 'ebsco':
                    scraper = EBSCOScraper(auto_login=False)
                    if scraper.load_cookies(str(COOKIES_DIR / f"{db}_cookies.json")):
                        results[db] = scraper.test_cookies()
                    else:
                        results[db] = False
                        
                elif db == 'ieee':
                    scraper = IEEEScraper(auto_login=False)
                    if scraper.load_cookies(str(COOKIES_DIR / f"{db}_cookies.json")):
                        results[db] = scraper.test_cookies()
                    else:
                        results[db] = False
                        
            except Exception as e:
                print(f"Error verificando cookies de {db}: {e}")
                results[db] = False
        
        return results
    
    def login_databases(self, databases, email, password, show_browser):
        """
        Realizar login en las bases de datos seleccionadas.
        
        Args:
            databases: Lista de bases de datos
            email: Email de usuario
            password: Contraseña
            show_browser: Bool - mostrar navegador (True) o headless (False)
        """
        thread = threading.Thread(
            target=self._login_worker,
            args=(databases, email, password, show_browser)
        )
        thread.daemon = True
        thread.start()
        return {'success': True, 'message': 'Login iniciado'}
    
    def _login_worker(self, databases, email, password, show_browser):
        """Worker para proceso de login."""
        try:
            total = len(databases)
            headless = not show_browser  # Invertir: si show=True, headless=False
            
            for i, db in enumerate(databases):
                progress = int((i / total) * 100)
                self.update_status('login', progress, f'Autenticando en {db.upper()}...', 
                                 'Espera aprox. 60 segundos para 2FA')
                
                try:
                    if db == 'ebsco':
                        scraper = EBSCOScraper(auto_login=False)
                        scraper.login_and_get_cookies(email, password, headless=headless)
                        scraper.save_cookies(str(COOKIES_DIR / "ebsco_cookies.json"))
                        self.scrapers['ebsco'] = scraper
                    elif db == 'ieee':
                        scraper = IEEEScraper(auto_login=False)
                        scraper.login_and_get_cookies(email, password, headless=headless)
                        scraper.save_cookies(str(COOKIES_DIR / "ieee_cookies.json"))
                        self.scrapers['ieee'] = scraper
                    
                    self.update_status('login', progress + int(100/total), 
                                     f'✅ {db.upper()} autenticado', '')
                    time.sleep(2)
                    
                except Exception as e:
                    self.update_status('error', 0, f'❌ Error en {db.upper()}: {str(e)}', '')
                    return
            
            self.update_status('login', 100, '✅ Login completado en todas las bases de datos', '')
            
        except Exception as e:
            self.update_status('error', 0, f'❌ Error general: {str(e)}', '')
    
    def get_availability(self, query, databases, email=None, password=None, show_browser=True):
        """
        Obtener cantidad de resultados disponibles en cada base de datos.
        REQUIERE login previo o cookies válidas.
        
        Args:
            query: Término de búsqueda
            databases: Lista de bases de datos
            email: Correo para autenticación (opcional)
            password: Contraseña para autenticación (opcional)
            show_browser: Si True muestra navegador (headless=False)
        
        Returns:
            dict: {database: total_items} (-1 si requiere login)
        """
        results = {}
        
        for db in databases:
            try:
                headless = not show_browser

                # Asegurar instancia del scraper
                if not self.scrapers.get(db):
                    if db == 'ebsco':
                        self.scrapers['ebsco'] = EBSCOScraper(auto_login=False)
                    elif db == 'ieee':
                        self.scrapers['ieee'] = IEEEScraper(auto_login=False)

                scraper = self.scrapers[db]

                # 1) Intentar cargar cookies si no hay sesión válida
                has_valid_session = False
                try:
                    # Cargar cookies según base
                    if db == 'ebsco':
                        scraper.load_cookies(str(COOKIES_DIR / "ebsco_cookies.json"))
                    elif db == 'ieee':
                        scraper.load_cookies(str(COOKIES_DIR / "ieee_cookies.json"))
                    has_valid_session = scraper.test_cookies()
                except Exception:
                    has_valid_session = False

                # 2) Si no hay sesión válida y tenemos credenciales, intentar login
                if not has_valid_session and email and password:
                    try:
                        scraper.login_and_get_cookies(email=email, password=password, headless=headless)
                        # Re-guardar cookies explícitamente en src/data/cookies
                        if db == 'ebsco':
                            scraper.save_cookies(str(COOKIES_DIR / "ebsco_cookies.json"))
                        elif db == 'ieee':
                            scraper.save_cookies(str(COOKIES_DIR / "ieee_cookies.json"))
                        has_valid_session = scraper.test_cookies()
                    except Exception as _:
                        has_valid_session = False

                # 3) Si sigue sin sesión válida, marcar -1 (requiere login)
                if not has_valid_session:
                    results[db] = -1
                    continue

                # 4) Con sesión válida, consultar disponibilidad
                total = scraper.get_total_items(query)
                results[db] = total
                        
            except Exception as e:
                results[db] = -1  # Error = requiere login
                print(f"Error obteniendo disponibilidad de {db}: {e}")
        
        return results
    
    def start_scraping(self, query, databases, download_all, custom_amount, email=None, password=None, show_browser=True):
        """
        Iniciar proceso de scraping.
        
        Args:
            query: Término de búsqueda
            databases: Lista de bases de datos
            download_all: Bool - descargar todo
            custom_amount: Int - cantidad personalizada
        """
        thread = threading.Thread(
            target=self._scraping_worker,
            args=(query, databases, download_all, custom_amount, email, password, show_browser)
        )
        thread.daemon = True
        thread.start()
        return {'success': True, 'message': 'Scraping iniciado'}
    
    def _scraping_worker(self, query, databases, download_all, custom_amount, email=None, password=None, show_browser=True, stop_on_auth_failure: bool = False):
        """Worker thread para scraping."""
        try:
            self.scraped_files = {}
            total_dbs = len(databases)

            # Multiproceso por base seleccionada
            threads_per_db = 4
            # Manager y colas para logs en vivo
            manager = multiprocessing.Manager()
            log_queues: Dict[str, Any] = { db: manager.Queue() for db in databases }
            # Consumidores de logs (hilos en el proceso padre)
            consumers: Dict[str, threading.Thread] = {}

            def _consume_logs(db_key: str, q: Any):
                try:
                    while True:
                        item = q.get()
                        if item is None:
                            break
                        # Pasar directamente a la UI
                        self.log(str(item))
                except Exception:
                    pass

            with ProcessPoolExecutor(max_workers=total_dbs) as executor:
                futures = {
                    executor.submit(
                        _scrape_db_job, db, query, download_all, custom_amount,
                        email, password, show_browser, threads_per_db, log_queues[db]
                    ): db for db in databases
                }

                # Iniciar consumidores por DB
                for db in databases:
                    t = threading.Thread(target=_consume_logs, args=(db, log_queues[db]), daemon=True)
                    t.start()
                    consumers[db] = t

                completed = 0
                for fut in as_completed(futures):
                    db = futures[fut]
                    base_progress = int((completed / total_dbs) * 100)
                    try:
                        self.update_status('scraping', base_progress, f'🔍 Scraping {db.upper()}...', 'Extrayendo artículos...')
                        result = fut.result()
                        # Los logs ya se streamearon en vivo; opcionalmente añadir resumen final si hubiese
                        if 'error' in result:
                            msg = f"❌ Error en {db.upper()}: {result['error']}"
                            self.update_status('error' if stop_on_auth_failure else 'scraping', base_progress, msg, '')
                            if stop_on_auth_failure and self.window:
                                safe_db = db.upper()
                                self.window.evaluate_js(f"alert('La autenticación o scraping falló en {safe_db}. Se detiene el pipeline.')")
                            if stop_on_auth_failure:
                                return
                        else:
                            count = result.get('count', 0)
                            file_path = result.get('file')
                            self.scraped_files[db] = file_path
                            progress = int(((completed + 1) / total_dbs) * 100)
                            self.update_status('scraping', progress, f'✅ {db.upper()}: {count} artículos', '')
                    except Exception as e:
                        msg = f"❌ Error en {db.upper()}: {str(e)}"
                        self.update_status('error' if stop_on_auth_failure else 'scraping', base_progress, msg, '')
                        if stop_on_auth_failure and self.window:
                            safe_db = db.upper()
                            self.window.evaluate_js(f"alert('La autenticación o scraping falló en {safe_db}. Se detiene el pipeline.')")
                        if stop_on_auth_failure:
                            return
                    finally:
                        completed += 1
                # Esperar cierre de consumidores
                for db, t in consumers.items():
                    try:
                        t.join(timeout=2.0)
                    except Exception:
                        pass
            
            if self.scraped_files:
                self.status['results']['scraped_files'] = self.scraped_files
                self.update_status('scraping', 100, '✅ Scraping completado', '')
            else:
                self.update_status('error', 0, '❌ No se pudo obtener datos', '')
                
        except Exception as e:
            self.update_status('error', 0, f'❌ Error: {str(e)}', '')
    
    def start_cleaning(self, output_name, csv_files=None):
        """
        Iniciar limpieza y unificación.
        
        Args:
            output_name: Nombre base para archivos de salida
            csv_files: Dict opcional con archivos a limpiar
        """
        thread = threading.Thread(
            target=self._cleaning_worker,
            args=(output_name, csv_files)
        )
        thread.daemon = True
        thread.start()
        return {'success': True, 'message': 'Limpieza iniciada'}
    
    def _cleaning_worker(self, output_name, csv_files):
        """Worker thread para limpieza."""
        try:
            self.update_status('cleaning', 10, '🧹 Iniciando limpieza...', 
                             'Cargando datos...')
            
            files_to_clean = csv_files if csv_files else self.scraped_files
            
            if not files_to_clean:
                self.update_status('error', 0, '❌ No hay archivos para limpiar', '')
                return
            
            self.update_status('cleaning', 30, '🔄 Unificando bases de datos...', 
                             'Eliminando duplicados...')
            
            unified_df, output_files = clean_and_unify_databases(
                ebsco_file=files_to_clean.get('ebsco'),
                ieee_file=files_to_clean.get('ieee'),
                output_name=output_name
            )
            
            self.unified_file = output_files['unified']
            
            self.update_status('cleaning', 100, 
                             f'✅ Limpieza completa: {len(unified_df)} artículos únicos', '')
            
            self.status['results']['unified_file'] = self.unified_file
            self.status['results']['total_records'] = len(unified_df)
            self.status['results']['output_files'] = output_files
            
        except Exception as e:
            self.update_status('error', 0, f'❌ Error: {str(e)}', '')
    
    def start_analysis(self, output_name, csv_file=None):
        """
        Iniciar análisis con algoritmos.
        
        Args:
            output_name: Nombre base para archivos de salida
            csv_file: Archivo CSV opcional a analizar
        """
        thread = threading.Thread(
            target=self._analysis_worker,
            args=(output_name, csv_file)
        )
        thread.daemon = True
        thread.start()
        return {'success': True, 'message': 'Análisis iniciado'}
    
    def _analysis_worker(self, output_name, csv_file):
        """Worker thread para análisis."""
        try:
            self.update_status('analysis', 10, '📊 Iniciando análisis...', 
                             'Cargando datos...')
            
            file_to_analyze = csv_file if csv_file else self.unified_file
            
            if not file_to_analyze:
                self.update_status('error', 0, '❌ No hay archivo para analizar', '')
                return
            
            self.update_status('analysis', 20, '🔢 Ejecutando algoritmos...', 
                             'Esto puede tomar varios minutos...')
            
            analyzer = AcademicSortingAnalyzer(file_to_analyze)
            results = analyzer.run_all_algorithms()
            
            self.update_status('analysis', 60, '📈 Generando visualizaciones...', 
                             'Creando gráficos...')
            
            chart_path = str(CSV_DIR / f"{output_name}_comparison.png")
            analyzer.create_time_comparison_chart(results, chart_path)
            
            self.update_status('analysis', 80, '👥 Analizando autores...', '')
            
            top_authors = analyzer.get_top_authors(15)
            authors_path = str(CSV_DIR / f"{output_name}_top_authors.csv")
            top_authors.to_csv(authors_path, index=False)
            
            # Convertir imagen a base64 para mostrar en interfaz
            with open(chart_path, 'rb') as f:
                img_data = base64.b64encode(f.read()).decode()
            
            # Preparar resultados
            times = {name: time*1000 for name, (df, time) in results.items() if time != float('inf')}
            sorted_times = dict(sorted(times.items(), key=lambda x: x[1]))
            
            self.status['results']['algorithm_times'] = sorted_times
            self.status['results']['chart_base64'] = img_data
            self.status['results']['chart_file'] = chart_path
            self.status['results']['authors_file'] = authors_path

            # Elegir el mejor algoritmo (menor tiempo) y guardar CSV ordenado para siguiente fase
            try:
                valid_results = {n: (df, t) for n, (df, t) in results.items() if df is not None and t != float('inf')}
                if valid_results:
                    best_name, (best_df, best_time) = min(valid_results.items(), key=lambda kv: kv[1][1])
                    # Remover columnas auxiliares si existen
                    drop_cols = [c for c in ['title_clean', 'year', 'sort_key'] if c in best_df.columns]
                    if drop_cols:
                        best_df_to_save = best_df.drop(columns=drop_cols)
                    else:
                        best_df_to_save = best_df

                    # Elegir nombre de salida según contexto:
                    # 1) Si csv_file fue pasado, guardar junto a ese archivo con sufijo _ORDENADO
                    # 2) Si no, pero existe self.unified_file, guardarlo junto a ese con sufijo _ORDENADO
                    # 3) En último caso, guardarlo en CSV_DIR con nombre base de output_name
                    from pathlib import Path as _Path
                    target_path = None
                    try:
                        if csv_file and os.path.exists(csv_file):
                            srcp = _Path(csv_file)
                            target_path = str(srcp.with_name(srcp.stem + "_ORDENADO.csv"))
                        elif self.unified_file:
                            up = _Path(self.unified_file)
                            target_path = str(up.with_name(up.stem + "_ORDENADO.csv"))
                        else:
                            target_path = str(CSV_DIR / f"{output_name}_ORDENADO.csv")
                    except Exception:
                        target_path = str(CSV_DIR / f"{output_name}_ORDENADO.csv")

                    best_df_to_save.to_csv(target_path, index=False, encoding='utf-8')
                    self.sorted_file = target_path

                    # Publicar en resultados para la UI
                    self.status['results']['best_sorted'] = {
                        'algorithm': best_name,
                        'time_ms': round(best_time*1000, 3),
                        'sorted_file': target_path
                    }
                    self.log(f"✅ CSV ordenado generado con {best_name} → {target_path}")
                else:
                    self.log('⚠️ No hubo resultados válidos de ordenamiento para generar CSV ordenado.')
            except Exception as _e:
                self.log(f"⚠️ No se pudo generar el CSV ordenado del mejor algoritmo: {_e}")
            
            self.update_status('analysis', 100, '✅ Análisis completado', '')
            
        except Exception as e:
            self.update_status('error', 0, f'❌ Error: {str(e)}', '')

    # ===================== NUEVO: Análisis de Conceptos (GAIE) =====================
    def analyze_concepts(self, csv_file: Optional[str] = None, top_k: int = 15):
        """
        Analiza la categoría GAIE: frecuencias de términos semilla, extrae términos asociados y
        estima la precisión de los términos generados.
        """
        try:
            filepath = csv_file or self.unified_file
            if not filepath:
                return {'success': False, 'message': 'No hay CSV seleccionado ni CSV unificado disponible.'}
            if not os.path.exists(filepath):
                return {'success': False, 'message': f'Archivo no encontrado: {filepath}'}

            df = pd.read_csv(filepath, encoding='utf-8')
            if 'abstract' not in df.columns:
                return {'success': False, 'message': 'El CSV no contiene columna "abstract".'}

            abstracts = [str(x) if pd.notna(x) else '' for x in df['abstract'].tolist()]

            analyzer = ConceptsCategoryAnalyzer()
            results = analyzer.analyze(abstracts, top_k=top_k)

            # Opcional: exponer en status para consumo de UI si esto se ejecuta en pipeline
            existing = self.status.get('results', {})
            existing['concepts'] = results
            self.status['results'] = existing

            return {'success': True, 'results': results, 'file': filepath}
        except Exception as e:
            return {'success': False, 'message': str(e)}

    # ===================== NUEVO: Clustering Jerárquico (dendrogramas) =====================
    def analyze_hierarchical_clustering(self, csv_file: Optional[str] = None, max_docs: int = 150,
                                        algorithms: Optional[list] = None):
        """
        Genera dendrogramas para varios enlaces (single/complete/average) sobre abstracts.
        Retorna imágenes en base64 y el coeficiente cophenético por algoritmo.
        """
        try:
            filepath = csv_file or self.unified_file or self.sorted_file
            if not filepath:
                return {'success': False, 'message': 'No hay CSV seleccionado ni CSV unificado/ordenado disponible.'}
            if not os.path.exists(filepath):
                return {'success': False, 'message': f'Archivo no encontrado: {filepath}'}

            df = pd.read_csv(filepath, encoding='utf-8')
            if 'abstract' not in df.columns:
                return {'success': False, 'message': 'El CSV no contiene columna "abstract".'}

            # Preparar datos
            data = df.copy()
            if 'title' in data.columns:
                labels = [str(t) if pd.notna(t) else f"Doc {i}" for i, t in enumerate(data['title'].tolist())]
            else:
                labels = [f"Doc {i}" for i in range(len(data))]
            abstracts = [str(x) if pd.notna(x) else '' for x in data['abstract'].tolist()]

            # Directorio para guardar imágenes
            out_dir = str(DATA_DIR / 'screenshots' / 'dendrograms')
            try:
                os.makedirs(out_dir, exist_ok=True)
            except Exception:
                pass

            from pathlib import Path as _P
            base_name = _P(filepath).stem

            hca = HierarchicalClusteringAnalyzer()
            res = hca.analyze(abstracts, algorithms=algorithms, labels=labels, max_docs=max_docs,
                              output_dir=out_dir, base_name=base_name)
            if 'error' in res:
                return {'success': False, 'message': res['error']}

            # Guardar en status para consumo de UI si se invoca desde pipeline
            existing = self.status.get('results', {})
            existing['clustering'] = res
            self.status['results'] = existing
            return {'success': True, 'results': res, 'file': filepath}
        except Exception as e:
            return {'success': False, 'message': str(e)}

    # ===================== NUEVO: Red de Citaciones =====================
    def analyze_citation_network(self, csv_file: Optional[str] = None,
                                 backend: str = 'classic', infer_threshold: float = 0.6,
                                 infer_top_k: int = 3, use_concepts: bool = False,
                                 text_field: str = 'abstract', limit: int = 120,
                                 classic_options: Optional[dict] = None,
                                 ai_options: Optional[dict] = None):
        """
        Construye la red de citaciones a partir de un CSV y retorna nodos/aristas/estadísticas.
        Si no hay relaciones explícitas, las infiere por similitud (título/autores/keywords).
        """
        try:
            filepath = csv_file or self.unified_file or self.sorted_file
            if not filepath:
                return {'success': False, 'message': 'No hay CSV disponible (selecciona uno o ejecuta pipeline).'}
            if not os.path.exists(filepath):
                return {'success': False, 'message': f'Archivo no encontrado: {filepath}'}

            df = pd.read_csv(filepath, encoding='utf-8')
            # Preparar artículos
            # Campos tolerantes: title, authors, keywords, citations (opcional), abstract
            rows = df.head(limit) if (limit and len(df) > limit) else df
            articles = []
            seen_titles = {}
            for idx, row in rows.iterrows():
                title = str(row.get('title', '') if pd.notna(row.get('title', '')) else '').strip()
                # Usar el título como identificador único del nodo (según requisito)
                if not title:
                    # si no hay título, saltar fila
                    continue
                # Asegurar unicidad del id basado en el título
                if title in seen_titles:
                    seen_titles[title] += 1
                    aid = f"{title} ({seen_titles[title]})"
                else:
                    seen_titles[title] = 1
                    aid = title
                # autores: admitir separador ';' o ','
                raw_auth = row.get('authors', None)
                if pd.isna(raw_auth):
                    authors = []
                else:
                    s = str(raw_auth)
                    if ';' in s:
                        authors = [a.strip() for a in s.split(';') if a.strip()]
                    elif ',' in s:
                        authors = [a.strip() for a in s.split(',') if a.strip()]
                    else:
                        authors = [s.strip()] if s.strip() else []
                # keywords
                raw_kw = row.get('keywords', None)
                if pd.isna(raw_kw):
                    keywords = []
                else:
                    s = str(raw_kw)
                    if ';' in s:
                        keywords = [k.strip() for k in s.split(';') if k.strip()]
                    elif ',' in s:
                        keywords = [k.strip() for k in s.split(',') if k.strip()]
                    else:
                        keywords = [s.strip()] if s.strip() else []
                # citations explícitas: si viene una columna con ids referenciados, intentar parsear lista
                citations = []
                for cand in ['citations', 'references', 'cited_ids']:
                    val = row.get(cand, None)
                    if val is not None and not pd.isna(val):
                        try:
                            # admitir JSON-like o lista separada por comas/; 
                            txt = str(val).strip()
                            if txt.startswith('[') and txt.endswith(']'):
                                import json as _json
                                parsed = _json.loads(txt)
                                citations = [str(x) for x in parsed]
                            else:
                                sep = ';' if ';' in txt else (',' if ',' in txt else None)
                                if sep:
                                    citations = [t.strip() for t in txt.split(sep) if t.strip()]
                                else:
                                    citations = [txt]
                        except Exception:
                            citations = []
                        break

                article = {
                    'id': aid,
                    'title': title,
                    'authors': authors,
                    'keywords': keywords,
                    'citations': citations,
                    'abstract': str(row.get('abstract', '') if pd.notna(row.get('abstract', '')) else ''),
                }
                articles.append(article)

            # Normalizar referencias de 'citations' para que apunten a nuestros IDs basados en título
            # Mapa: título exacto -> lista de IDs (por si hay duplicados)
            title_to_ids = {}
            for a in articles:
                title_to_ids.setdefault(a['title'], []).append(a['id'])

            def _map_citation_token(tok: str):
                s = (tok or '').strip()
                if not s:
                    return None
                # 1) Si ya coincide con un ID exacto (incluye sufijos), mantener
                for a in articles:
                    if s == a['id']:
                        return a['id']
                # 2) Si coincide con un título exacto, tomar el primer ID asociado a ese título
                ids = title_to_ids.get(s)
                if ids:
                    return ids[0]
                return None

            for a in articles:
                mapped = []
                for tok in (a.get('citations') or []):
                    mid = _map_citation_token(tok)
                    if mid and mid != a['id']:
                        mapped.append(mid)
                a['citations'] = mapped

            # Preparar listas de métodos según opciones
            classic_methods = []
            if backend != 'ia':
                co = classic_options or {}
                if co.get('levenshtein', True): classic_methods.append('levenshtein')
                if co.get('jarowinkler', True): classic_methods.append('jarowinkler')
                if co.get('tfidf', True): classic_methods.append('tfidf')
                if co.get('coseno', True): classic_methods.append('coseno')
                if not classic_methods:
                    classic_methods = ['coseno']
            ai_methods = []
            if backend == 'ia':
                ao = ai_options or {}
                if ao.get('sbert', True): ai_methods.append('sbert')
                if ao.get('hf', True): ai_methods.append('hf')
                if not ai_methods:
                    ai_methods = ['hf']

            cna = CitationNetworkAnalyzer(
                similarity_backend=('ia' if backend == 'ia' else 'classic'),
                classic_methods=classic_methods,
                ai_methods=ai_methods
            )
            # Aplicar parámetros dinámicos
            try:
                cna.infer_threshold = float(infer_threshold)
                cna.infer_top_k = int(infer_top_k) if infer_top_k is not None else None
                if backend == 'ia':
                    # establecer timeout razonable por comparación IA
                    _tmo = getattr(cna, 'ia_timeout_sec', None)
                    if not isinstance(_tmo, (int, float)) or _tmo <= 0:
                        cna.ia_timeout_sec = 15.0
            except Exception:
                pass

            # Progreso en vivo hacia la UI mientras se construye el grafo
            def _progress_cb(pct: float, msg: str):
                try:
                    self.update_status('citation', int(max(0, min(99, pct))), '🔗 Construyendo red de citaciones…', msg or '')
                    if pct % 10 < 1:  # log cada ~10%
                        self.log(msg)
                except Exception:
                    pass

            cna.build_graph(
                articles,
                infer_if_missing=True,
                enrich_with_concepts=bool(use_concepts),
                text_field=text_field,
                concepts_top_k=15,
                progress_callback=_progress_cb
            )

            # Exportar nodos y aristas (id y label serán el título)
            nodes = [{'id': str(nid), 'label': (cna.nodes[nid].get('title') or str(nid))} for nid in cna.nodes.keys()]
            edges = [{'source': str(u), 'target': str(v), 'weight': float(w)} for (u, v, w) in cna.edges()]
            sccs = cna.strongly_connected_components()

            # Persistir en estado para consultas subsecuentes
            self.citation_analyzer = cna
            self.citation_nodes = nodes

            # Top 10 caminos mínimos globales y Top SCCs desde el analizador
            top_paths_out = cna.top_k_shortest_paths_global(10)
            top_sccs_out = cna.top_sccs(10)

            # Resumen
            result = {
                'nodes': nodes,
                'edges': edges,
                'sccs': sccs,
                'node_count': len(nodes),
                'edge_count': len(edges),
                'scc_count': len(sccs),
                'top_paths': top_paths_out,
                'top_sccs': top_sccs_out,
            }

            # Generar imagen del grafo (como el gráfico de ordenamiento)
            try:
                from pathlib import Path as _P
                base_name = _P(filepath).stem
                out_dir = DATA_DIR / 'screenshots' / 'citation_graphs'
                os.makedirs(out_dir, exist_ok=True)
                img_path = str(out_dir / f'{base_name}_citation_graph.png')
                graph_b64 = self._render_citation_graph_image(cna, nodes, edges, sccs, img_path)
                result['graph_file'] = img_path
                result['graph_base64'] = graph_b64
            except Exception as _img_err:
                # No bloquear por errores de render; solo registrar en resultado
                result['graph_error'] = str(_img_err)

            # Adjuntar a status (por si lo llamamos desde pipeline)
            existing = self.status.get('results', {})
            existing['citation_network'] = result
            self.status['results'] = existing

            # Notificar finalización a la UI
            try:
                self.update_status('citation', 100, '✅ Red de citaciones construida', f"{len(nodes)} nodos, {len(edges)} aristas")
            except Exception:
                pass

            return {'success': True, 'results': result, 'file': filepath}
        except Exception as e:
            return {'success': False, 'message': str(e)}

    def _render_citation_graph_image(self, cna, nodes, edges, sccs, out_path: str) -> str:
        """
        Renderiza un PNG de la red de citaciones con layout por fuerzas (force-directed),
        colores por SCC, flechas y grosor/arrowsize según peso. Devuelve la imagen en base64.
        """
        import io
        import base64
        import math
        import numpy as np
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        from matplotlib.patches import FancyArrowPatch

        # Preparar layout por fuerzas (fallback a circular si el grafo es muy grande)
        N = max(1, len(nodes))
        pos = {}
        node_ids = [str(n['id']) for n in nodes]

        if N <= 300:
            # Construir índices y listas de aristas con pesos
            idx_of = {nid: i for i, nid in enumerate(node_ids)}
            E = []
            for e in edges:
                s = str(e.get('source'))
                t = str(e.get('target'))
                if s in idx_of and t in idx_of and s != t:
                    w = float(e.get('weight', 0.5) or 0.5)
                    E.append((idx_of[s], idx_of[t], w))

            # Estados iniciales
            rng = np.random.default_rng(42)
            P = rng.uniform(low=-1.0, high=1.0, size=(N, 2)).astype(float)  # posiciones
            V = np.zeros((N, 2), dtype=float)  # velocidades

            # Parámetros del sistema de fuerzas (calibrados para parecerse al canvas)
            iters = 200 if N <= 150 else (120 if N <= 250 else 90)
            k_rep = 0.04  # repulsión entre todos los pares
            k_attr = 0.015  # atracción por arista
            rest_len = 0.5  # longitud de reposo de los “resortes”
            damping = 0.85
            dt = 0.02
            eps = 1e-6

            # Precompute vecinos por nodo para acelerar fuerza atractiva
            neigh = [[] for _ in range(N)]
            for i, j, w in E:
                neigh[i].append((j, w))
                # Para el layout, también ayuda que la atracción sea “bidireccional”
                neigh[j].append((i, w))

            # Simulación de fuerzas
            for _ in range(iters):
                F = np.zeros((N, 2), dtype=float)
                # Fuerza de repulsión entre todos los pares
                for i in range(N):
                    pi = P[i]
                    d = P - pi
                    dist2 = np.sum(d*d, axis=1) + eps
                    inv = k_rep / dist2
                    inv[i] = 0.0  # sin auto-fuerza
                    # normalizar y acumular
                    F[i] += np.sum((d * inv[:, None]), axis=0)

                # Fuerza de atracción por aristas (grafos dirigidos pero usamos atracción simétrica)
                for i in range(N):
                    pi = P[i]
                    for j, w in neigh[i]:
                        if j == i:
                            continue
                        d = P[j] - pi
                        dist = math.sqrt(float(d[0]*d[0] + d[1]*d[1]) + eps)
                        # Hooke hacia longitud objetivo
                        force_mag = k_attr * w * (dist - rest_len)
                        F[i] += (d / dist) * force_mag

                # Integración simple con amortiguamiento
                V = (V + dt * F) * damping
                P = P + dt * V

            # Normalizar a [-1, 1] aproximadamente
            min_xy = P.min(axis=0)
            max_xy = P.max(axis=0)
            center = (min_xy + max_xy) / 2.0
            span = (max_xy - min_xy)
            max_span = float(max(span[0], span[1], 1e-6))
            P = (P - center) / max_span * 1.6  # escalar para que quepa bien

            for i, nid in enumerate(node_ids):
                pos[nid] = (float(P[i, 0]), float(P[i, 1]))
        else:
            # Fallback: circular para N grande
            angles = np.linspace(0, 2*math.pi, N, endpoint=False)
            r = 1.0
            for i, nid in enumerate(node_ids):
                pos[nid] = (r*math.cos(angles[i]), r*math.sin(angles[i]))

        # Mapa de SCC para colores
        scc_map = {}
        for idx, comp in enumerate(sccs or []):
            for nid in comp:
                scc_map[str(nid)] = idx
        try:
            _cmap = plt.get_cmap('tab10')
            colors = getattr(_cmap, 'colors', None) or [
                (0.368,0.507,0.710), (0.880,0.611,0.142), (0.560,0.692,0.195), (0.922,0.388,0.208),
                (0.528,0.470,0.701), (0.772,0.432,0.102), (0.364,0.619,0.782), (0.571,0.586,0.0),
                (0.916,0.596,0.478), (0.765,0.616,0.784)
            ]
        except Exception:
            colors = [
                (0.368,0.507,0.710), (0.880,0.611,0.142), (0.560,0.692,0.195), (0.922,0.388,0.208),
                (0.528,0.470,0.701), (0.772,0.432,0.102), (0.364,0.619,0.782), (0.571,0.586,0.0),
                (0.916,0.596,0.478), (0.765,0.616,0.784)
            ]

        fig, ax = plt.subplots(figsize=(10, 8), dpi=150)
        ax.set_facecolor('#0b1220')
        fig.patch.set_facecolor('#0b1220')
        ax.axis('off')

        # Dibujar aristas
        for e in edges:
            a = pos.get(e['source'])
            b = pos.get(e['target'])
            if not a or not b:
                continue
            w = float(e.get('weight', 0.5) or 0.5)
            lw = 0.8 + 2.2*w
            arr = FancyArrowPatch(a, b, arrowstyle='-|>', color=(0.75,0.8,0.9,0.85),
                                  mutation_scale=8 + 6*w, linewidth=lw, shrinkA=10, shrinkB=10)
            ax.add_patch(arr)
            # etiqueta de peso
            mx = (a[0]+b[0])/2; my = (a[1]+b[1])/2
            ax.text(mx, my, f"{w:.2f}", color='#cbd5e1', fontsize=7)

        # Dibujar nodos y etiquetas
        for i, n in enumerate(nodes):
            x, y = pos[n['id']]
            comp = scc_map.get(str(n['id']), i)
            color = colors[comp % len(colors)] if len(colors) else (0.39,0.4,0.95)
            ax.scatter([x], [y], s=90, c=[color], edgecolors='#e5e7eb', linewidths=0.7, zorder=3)
            label = (n.get('label') or n['id'])
            if len(label) > 48:
                label = label[:45] + '…'
            ax.text(x+0.05, y+0.02, label, color='#e2e8f0', fontsize=8)

        # Guardar a archivo
        plt.tight_layout()
        fig.savefig(out_path, facecolor=fig.get_facecolor(), bbox_inches='tight')
        plt.close(fig)

        # Devolver base64
        with open(out_path, 'rb') as f:
            b64 = base64.b64encode(f.read()).decode('utf-8')
        return b64

    def citation_shortest_path(self, source_id: str, target_id: str):
        try:
            if not self.citation_analyzer:
                return {'success': False, 'message': 'No hay red de citaciones cargada.'}
            dist, path = self.citation_analyzer.shortest_path_dijkstra(source_id, target_id)
            return {'success': True, 'distance': (None if dist == float('inf') else dist), 'path': path}
        except Exception as e:
            return {'success': False, 'message': str(e)}

    def citation_all_pairs_fw(self):
        try:
            if not self.citation_analyzer:
                return {'success': False, 'message': 'No hay red de citaciones cargada.'}
            n = len(self.citation_analyzer.nodes)
            if n > 180:
                return {'success': False, 'message': 'Demasiados nodos para Floyd–Warshall interactivo (>180).'}
            dist = self.citation_analyzer.all_pairs_floyd_warshall()
            return {'success': True, 'dist': dist}
        except Exception as e:
            return {'success': False, 'message': str(e)}
    
    def start_full_pipeline(self, query, databases, download_all, custom_amount, output_name, email=None, password=None, show_browser=True,
                            sim_enable: bool = False, sim_limit: int = 50, sim_only_abstracts: bool = True,
                            sim_use_classic: bool = True, sim_use_ai: bool = True,
                            sim_classic_options: Optional[dict] = None, sim_ai_options: Optional[dict] = None):
        """
        Ejecutar pipeline completo: Scraping → Limpieza → Análisis.
        """
        thread = threading.Thread(
        target=self._full_pipeline_worker,
        args=(query, databases, download_all, custom_amount, output_name, email, password, show_browser,
            sim_enable, sim_limit, sim_only_abstracts, sim_use_classic, sim_use_ai, sim_classic_options, sim_ai_options)
        )
        thread.daemon = True
        thread.start()
        return {'success': True, 'message': 'Pipeline completo iniciado'}
    
    def _full_pipeline_worker(self, query, databases, download_all, custom_amount, output_name, email=None, password=None, show_browser=True,
                              sim_enable: bool = False, sim_limit: int = 50, sim_only_abstracts: bool = True,
                              sim_use_classic: bool = True, sim_use_ai: bool = True,
                              sim_classic_options: Optional[dict] = None, sim_ai_options: Optional[dict] = None):
        """Worker para pipeline completo."""
        try:
            # FASE 1: Scraping (modo estricto: detener si una base falla)
            self._scraping_worker(query, databases, download_all, custom_amount, email, password, show_browser, stop_on_auth_failure=True)
            
            if not self.scraped_files:
                return
            
            time.sleep(2)
            
            # FASE 2: Limpieza
            self._cleaning_worker(output_name, None)
            
            if not self.unified_file:
                return
            
            time.sleep(2)
            
            # FASE 3: Análisis (algoritmos de ordenamiento, tiempos, autores)
            self._analysis_worker(output_name, None)

            # FASE 4 (opcional): Similitud textual sobre abstracts
            # Usar el CSV ORDENADO por el mejor algoritmo si existe; si no, usar el UNIFICADO
            data_source_for_next = self.sorted_file if (self.sorted_file and os.path.exists(self.sorted_file)) else self.unified_file
            if sim_enable and data_source_for_next and os.path.exists(data_source_for_next):
                try:
                    self.update_status('analysis', 70, '🧠 Calculando similitud textual…', 'Esto puede tardar según el número de artículos seleccionado')
                    df = pd.read_csv(data_source_for_next, encoding='utf-8')
                    if 'abstract' in df.columns:
                        # Filtrar por abstracts no vacíos si corresponde
                        if sim_only_abstracts:
                            df = df[~df['abstract'].isna() & (df['abstract'].astype(str).str.strip() != '')]
                        # Limitar cantidad
                        if sim_limit and sim_limit > 1:
                            df = df.head(sim_limit)

                        textos = [str(x) for x in (df['abstract'].tolist())]
                        labels = []
                        if 'title' in df.columns:
                            labels = [str(t).strip() if str(t).strip() else f'Artículo {i}' for i, t in enumerate(df['title'].tolist())]
                        else:
                            labels = [f'Artículo {i}' for i in range(len(textos))]

                        results = {}
                        if sim_use_classic:
                            try:
                                simc = SimilitudTextualClasico()
                                usar = None
                                if sim_classic_options and isinstance(sim_classic_options, dict):
                                    usar = {
                                        'levenshtein': bool(sim_classic_options.get('levenshtein', True)),
                                        'jaro': bool(sim_classic_options.get('jaro', True)),
                                        'tfidf': bool(sim_classic_options.get('tfidf', True)),
                                        'coseno': bool(sim_classic_options.get('coseno', True)),
                                    }
                                classic_res = simc.comparar_multiples(textos, corpus=None, usar=usar, top_k=10)
                                results.update(classic_res)
                            except Exception as e:
                                results['ClasicosError'] = str(e)

                        if sim_use_ai:
                            try:
                                siai = SimilitudTextualIA()
                                opts = sim_ai_options or {}
                                ai_res = siai.comparar_multiples(
                                    textos,
                                    usar_sbert=bool(opts.get('sbert', True)),
                                    usar_transformer=bool(opts.get('hf', True)),
                                    sbert_model=str(opts.get('sbert_model', 'sentence-transformers/all-MiniLM-L6-v2')),
                                    hf_model=str(opts.get('hf_model', 'thenlper/gte-small')),
                                    top_k=10
                                )
                                results.update(ai_res)
                            except Exception as e:
                                results['IAError'] = str(e)

                        # Inyectar a resultados del pipeline y notificar
                        existing = self.status.get('results', {})
                        existing['similarity'] = {
                            'labels': labels,
                            'results': results
                        }
                        # Anexar referencia del dataset usado para esta fase
                        existing['similarity_source'] = data_source_for_next
                        self.status['results'] = existing
                        self.update_status('analysis', 100, '✅ Análisis completado (incluye similitud textual)', '')
                    else:
                        self.log('⚠️ No se encontró columna "abstract" en el CSV unificado. Se omite la similitud textual.')
                except Exception as e:
                    self.log(f'⚠️ Error calculando similitud textual: {e}')

            # FASE 5: Análisis de Conceptos (GAIE) siempre que haya datos
            try:
                source_for_concepts = data_source_for_next or self.unified_file
                if source_for_concepts and os.path.exists(source_for_concepts):
                    self.update_status('analysis', 90, '🧩 Analizando conceptos (GAIE)…', '')
                    dfc = pd.read_csv(source_for_concepts, encoding='utf-8')
                    if 'abstract' in dfc.columns:
                        abstracts = [str(x) if pd.notna(x) else '' for x in dfc['abstract'].tolist()]
                        cca = ConceptsCategoryAnalyzer()
                        concepts_res = cca.analyze(abstracts, top_k=15)
                        existing = self.status.get('results', {})
                        existing['concepts'] = concepts_res
                        self.status['results'] = existing
                        self.update_status('analysis', 100, '✅ Análisis completado (incluye conceptos GAIE)', '')
            except Exception as e:
                self.log(f'⚠️ Error en Análisis de Conceptos: {e}')

            # FASE 6: Clustering jerárquico (dendrogramas)
            try:
                source_for_hc = data_source_for_next or self.unified_file
                if source_for_hc and os.path.exists(source_for_hc):
                    self.update_status('analysis', 95, '🌳 Generando dendrogramas (clustering jerárquico)…', '')
                    # Probar los 3 algoritmos clásicos
                    hc_res = self.analyze_hierarchical_clustering(source_for_hc, max_docs=120,
                                                                  algorithms=['single','complete','average'])
                    if hc_res.get('success'):
                        existing = self.status.get('results', {})
                        existing['clustering'] = hc_res['results']
                        self.status['results'] = existing
                        self.update_status('analysis', 100, '✅ Análisis completado (incluye dendrogramas)', '')
                    else:
                        self.log('⚠️ ' + hc_res.get('message','Error en clustering jerárquico'))
            except Exception as e:
                self.log(f'⚠️ Error en Clustering Jerárquico: {e}')

            # FASE 7: Red de Citaciones (limitada para visualización)
            try:
                source_for_cn = data_source_for_next or self.unified_file
                if source_for_cn and os.path.exists(source_for_cn):
                    self.update_status('analysis', 97, '🔗 Construyendo red de citaciones…', '')
                    cn_res = self.analyze_citation_network(source_for_cn, backend='classic', infer_threshold=0.6,
                                                           infer_top_k=3, use_concepts=False, text_field='abstract', limit=100)
                    if cn_res.get('success'):
                        existing = self.status.get('results', {})
                        existing['citation_network'] = cn_res['results']
                        self.status['results'] = existing
                        self.update_status('analysis', 100, '✅ Análisis completado (incluye red de citaciones)', '')
                    else:
                        self.log('⚠️ ' + cn_res.get('message', 'Error en red de citaciones'))
            except Exception as e:
                self.log(f'⚠️ Error en Red de Citaciones: {e}')
                
            # === NUEVO: Exportar visualizaciones científicas automáticamente ===
            try:
                if self.unified_file and os.path.exists(self.unified_file):
                    pdf_path = str(Path(self.unified_file).parent / f'{output_name}_visualizaciones.pdf')
                    visualizer = BibliometricVisualizer(self.unified_file, pdf_path)
                    visualizer.export_pdf()
                    self.log(f'Visualizaciones científicas exportadas a PDF: {pdf_path}')
            except Exception as e:
                self.log(f'Error exportando PDF de visualizaciones: {e}')
        except Exception as e:
            self.update_status('error', 0, f'❌ Error en pipeline: {str(e)}', '')



    def open_file(self, filepath):
        """Abrir archivo con aplicación predeterminada del sistema."""
        try:
            if os.path.exists(filepath):
                if sys.platform == 'win32':
                    os.startfile(filepath)
                elif sys.platform == 'darwin':
                    os.system(f'open "{filepath}"')
                else:
                    os.system(f'xdg-open "{filepath}"')
                return {'success': True}
            return {'success': False, 'error': 'Archivo no encontrado'}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def select_csv_file(self):
        """Abrir diálogo para seleccionar archivo CSV."""
        if self.window:
            file_types = ('CSV files (*.csv)',)
            result = self.window.create_file_dialog(
                webview.OPEN_DIALOG,
                allow_multiple=False,
                file_types=file_types
            )
            return result[0] if result else None
        return None

    # ===================== NUEVOS MÉTODOS: ALGORITMOS (SELECCIÓN MÚLTIPLE) =====================
    def get_articles_for_algorithms(self, csv_file: Optional[str] = None, limit: int = 500):
        """
        Cargar artículos (título y vista previa del abstract) desde un CSV para la sección de algoritmos.
        Usa el CSV unificado si no se especifica uno.
        """
        try:
            filepath = csv_file or self.unified_file
            if not filepath:
                return {'success': False, 'message': 'No hay CSV seleccionado ni CSV unificado disponible.'}
            if not os.path.exists(filepath):
                return {'success': False, 'message': f'Archivo no encontrado: {filepath}'}

            df = pd.read_csv(filepath, encoding='utf-8')
            if 'abstract' not in df.columns:
                return {'success': False, 'message': 'El CSV no contiene columna "abstract".'}

            articles = []
            count = 0
            for idx, row in df.iterrows():
                title = str(row.get('title', '')).strip()
                abstract = row.get('abstract', '')
                if pd.isna(abstract):
                    abstract = ''
                preview_src = str(abstract)
                preview = (preview_src[:180] + '…') if len(preview_src) > 180 else preview_src
                try:
                    index_val = int(idx)  # type: ignore
                except Exception:
                    try:
                        index_val = int(str(idx))
                    except Exception:
                        index_val = count
                articles.append({
                    'index': index_val,
                    'title': title if title else f'Artículo {idx}',
                    'has_abstract': bool(preview_src.strip()),
                    'abstract_preview': preview
                })
                count += 1
                if limit and count >= limit:
                    break

            return {'success': True, 'file': filepath, 'count': len(articles), 'articles': articles}
        except Exception as e:
            return {'success': False, 'message': str(e)}

    def analyze_similarity(self, indices: list, csv_file: Optional[str] = None,
                           use_classic: bool = True, use_ai: bool = True,
                           classic_options: Optional[dict] = None,
                           ai_options: Optional[dict] = None):
        """
        Analiza similitud entre artículos seleccionados (por índice) usando algoritmos clásicos y/o IA.
        """
        try:
            if not indices or len(indices) < 2:
                return {'success': False, 'message': 'Seleccione al menos 2 artículos.'}

            filepath = csv_file or self.unified_file
            if not filepath:
                return {'success': False, 'message': 'No hay CSV seleccionado ni CSV unificado disponible.'}
            if not os.path.exists(filepath):
                return {'success': False, 'message': f'Archivo no encontrado: {filepath}'}

            df = pd.read_csv(filepath, encoding='utf-8')
            if 'abstract' not in df.columns:
                return {'success': False, 'message': 'El CSV no contiene columna "abstract".'}

            # Extraer textos y etiquetas (títulos)
            textos = []
            labels = []
            for i in indices:
                row = df.iloc[int(i)]
                title = str(row.get('title', '')).strip()
                abstract = row.get('abstract', '')
                if pd.isna(abstract):
                    abstract = ''
                textos.append(str(abstract))
                labels.append(title if title else f'Artículo {i}')

            results = {}

            if use_classic:
                try:
                    simc = SimilitudTextualClasico()
                    usar = None
                    if classic_options and isinstance(classic_options, dict):
                        usar = {
                            'levenshtein': bool(classic_options.get('levenshtein', True)),
                            'jaro': bool(classic_options.get('jaro', True)),
                            'tfidf': bool(classic_options.get('tfidf', True)),
                            'coseno': bool(classic_options.get('coseno', True)),
                        }
                    classic_res = simc.comparar_multiples(textos, corpus=None, usar=usar, top_k=10)
                    results.update(classic_res)
                except Exception as e:
                    results['ClasicosError'] = str(e)

            if use_ai:
                try:
                    siai = SimilitudTextualIA()
                    opts = ai_options or {}
                    ai_res = siai.comparar_multiples(
                        textos,
                        usar_sbert=bool(opts.get('sbert', True)),
                        usar_transformer=bool(opts.get('hf', True)),
                        sbert_model=str(opts.get('sbert_model', 'sentence-transformers/all-MiniLM-L6-v2')),
                        hf_model=str(opts.get('hf_model', 'thenlper/gte-small')),
                        top_k=10
                    )
                    results.update(ai_res)
                except Exception as e:
                    results['IAError'] = str(e)

            return {'success': True, 'labels': labels, 'results': results}
        except Exception as e:
            return {'success': False, 'message': str(e)}





def load_html():
    """Cargar HTML desde archivo externo."""
    html_path = Path(__file__).parent / "interface.html"
    if html_path.exists():
        with open(html_path, 'r', encoding='utf-8') as f:
            return f.read()
    else:
        # Fallback: usar HTML básico si no se encuentra el archivo
        return """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Error</title>
        </head>
        <body>
            <h1>Error: No se encontró el archivo interface.html</h1>
            <p>Por favor, asegúrate de que el archivo interface.html esté en el mismo directorio que este script.</p>
        </body>
        </html>
        """


def main():
    """
    Iniciar aplicación GUI.
    """
    # Crear API
    api = AcademicAnalysisAPI()
    
    # Cargar HTML desde archivo
    html_content = load_html()
    
    # Crear ventana con suppress_stderr
    with suppress_stderr():
        window = webview.create_window(
            'Academic Analysis System',
            html=html_content,
            js_api=api,
            width=1400,
            height=900,
            resizable=True,
            frameless=False,
            background_color='#0f172a'
        )
    
    # Asignar ventana a API
    api.set_window(window)
    
    # Iniciar aplicación SIN DEBUG
    webview.start(debug=False)


if __name__ == '__main__':
    main()