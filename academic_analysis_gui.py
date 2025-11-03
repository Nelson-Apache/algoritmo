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
from scraper.JSTORScraper import JSTORScraper
from data.MultiDatabaseCleaner import MultiDatabaseCleaner, clean_and_unify_databases
from algoritmo.AcademicSortingAnalyzer import AcademicSortingAnalyzer

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
                    threads_per_db: int = 4) -> Dict[str, Any]:
    """
    Proceso aislado para scrapear una base de datos.
    Retorna dict con {db, count, file, error?}
    """
    # Capturar toda la salida de consola de este proceso (incluye prints de scrapers)
    log_capture = StringIO()
    from contextlib import redirect_stdout, redirect_stderr
    try:
        with redirect_stdout(log_capture), redirect_stderr(log_capture):
            headless = not show_browser
            # Instanciar scraper por base
            if db == 'ebsco':
                scraper = EBSCOScraper(auto_login=False)
                cookie_file = 'ebsco_cookies.json'
            elif db == 'ieee':
                scraper = IEEEScraper(auto_login=False)
                cookie_file = 'ieee_cookies.json'
            elif db == 'jstor':
                scraper = JSTORScraper(auto_login=False)
                cookie_file = 'jstor_cookies.json'
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
                # Concurrencia por páginas para IEEE/JSTOR
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
                elif db == 'jstor':
                    page_size_default = 25
                    total = scraper.get_total_items(query)
                    if total == 0:
                        articles = []
                    else:
                        target = min(max_results or total, total)
                        pages = []
                        remaining = target
                        page = 0
                        while remaining > 0:
                            current_page_size = min(page_size_default, remaining)
                            pages.append((page, current_page_size))
                            remaining -= current_page_size
                            page += 1
                        with ThreadPoolExecutor(max_workers=threads_per_db) as executor:
                            s = cast(Any, scraper)
                            def fetch_jstor(pg: int, sz: int):
                                return s.search(query, pg, sz, False)
                            futures = [executor.submit(fetch_jstor, pg, sz) for (pg, sz) in pages]
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
            return { 'db': db, 'count': len(articles), 'file': file_path, 'logs': log_capture.getvalue() }

    except Exception as e:
        return { 'db': db, 'error': str(e), 'logs': log_capture.getvalue() }


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
            'jstor': None
        }
        
        # Datos del proceso
        self.scraped_files = {}
        self.unified_file = None
        self.analysis_results = {}
        # Buffer simple de logs para UI
        self.log_buffer: list[str] = []
        self.max_log_lines = 1000
        
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
            databases: Lista de bases de datos ['ebsco', 'ieee', 'jstor']
        
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
                        
                elif db == 'jstor':
                    scraper = JSTORScraper(auto_login=False)
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
                    elif db == 'jstor':
                        scraper = JSTORScraper(auto_login=False)
                        scraper.login_and_get_cookies(email, password, headless=headless)
                        scraper.save_cookies(str(COOKIES_DIR / "jstor_cookies.json"))
                        self.scrapers['jstor'] = scraper
                    
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
                    elif db == 'jstor':
                        self.scrapers['jstor'] = JSTORScraper(auto_login=False)

                scraper = self.scrapers[db]

                # 1) Intentar cargar cookies si no hay sesión válida
                has_valid_session = False
                try:
                    # Cargar cookies según base
                    if db == 'ebsco':
                        scraper.load_cookies(str(COOKIES_DIR / "ebsco_cookies.json"))
                    elif db == 'ieee':
                        scraper.load_cookies(str(COOKIES_DIR / "ieee_cookies.json"))
                    elif db == 'jstor':
                        scraper.load_cookies(str(COOKIES_DIR / "jstor_cookies.json"))
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
                        elif db == 'jstor':
                            scraper.save_cookies(str(COOKIES_DIR / "jstor_cookies.json"))
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
            with ProcessPoolExecutor(max_workers=total_dbs) as executor:
                futures = {
                    executor.submit(_scrape_db_job, db, query, download_all, custom_amount, email, password, show_browser, threads_per_db): db
                    for db in databases
                }

                completed = 0
                for fut in as_completed(futures):
                    db = futures[fut]
                    base_progress = int((completed / total_dbs) * 100)
                    try:
                        self.update_status('scraping', base_progress, f'🔍 Scraping {db.upper()}...', 'Extrayendo artículos...')
                        result = fut.result()
                        # Volcar logs de subproceso si existen
                        self.log_blob(result.get('logs', ''))
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
                jstor_file=files_to_clean.get('jstor'),
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
            
            self.update_status('analysis', 100, '✅ Análisis completado', '')
            
        except Exception as e:
            self.update_status('error', 0, f'❌ Error: {str(e)}', '')
    
    def start_full_pipeline(self, query, databases, download_all, custom_amount, output_name, email=None, password=None, show_browser=True):
        """
        Ejecutar pipeline completo: Scraping → Limpieza → Análisis.
        """
        thread = threading.Thread(
            target=self._full_pipeline_worker,
            args=(query, databases, download_all, custom_amount, output_name, email, password, show_browser)
        )
        thread.daemon = True
        thread.start()
        return {'success': True, 'message': 'Pipeline completo iniciado'}
    
    def _full_pipeline_worker(self, query, databases, download_all, custom_amount, output_name, email=None, password=None, show_browser=True):
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
            
            # FASE 3: Análisis
            self._analysis_worker(output_name, None)
            
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