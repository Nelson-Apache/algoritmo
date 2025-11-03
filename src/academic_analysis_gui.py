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

    # ===================== NUEVO: Análisis de Grafos de Citaciones =====================
    def analyze_citation_graph(self, csv_file: Optional[str] = None):
        """
        Construye y analiza la red de citaciones entre artículos científicos,
        calcula métricas, caminos mínimos y componentes fuertemente conexas.
        """
        try:
            from src.grafos.seguimiento2_req1 import Seguimiento2Req1
            import matplotlib.pyplot as plt
            import base64, io, os
            import pandas as pd
            import networkx as nx

            # Seleccionar CSV
            filepath = csv_file or self.unified_file
            if not filepath:
                return {'success': False, 'message': 'No hay CSV seleccionado ni unificado disponible.'}
            if not os.path.exists(filepath):
                return {'success': False, 'message': f'Archivo no encontrado: {filepath}'}

            # Leer CSV
            df = pd.read_csv(filepath, encoding='utf-8')
            cols = list(df.columns)
            title_col = 'title' if 'title' in cols else 'titulo'
            authors_col = 'authors' if 'authors' in cols else 'autores'
            keywords_col = 'keywords' if 'keywords' in cols else 'palabras_clave'

            # Crear artículos
            articulos = []
            for i, row in df.iterrows():
                articulos.append({
                    'id': f"A{i}",
                    'titulo': str(row.get(title_col, '')),
                    'autores': [a.strip() for a in str(row.get(authors_col, '')).split(',') if a.strip()],
                    'palabras_clave': [k.strip() for k in str(row.get(keywords_col, '')).split(',') if k.strip()]
                })

            # === Construcción automática del grafo ===
            grafo = Seguimiento2Req1()
            grafo.construir_red(articulos)

            # === Cálculo de caminos mínimos (Dijkstra) ===
            caminos_minimos = {}
            nodos = list(grafo.grafo.nodes())
            for i in range(min(3, len(nodos))):  # solo algunos ejemplos
                for j in range(i + 1, min(3, len(nodos))):
                    origen, destino = nodos[i], nodos[j]
                    ruta, distancia = grafo.camino_minimo(origen, destino)
                    if ruta:
                        caminos_minimos[f"{origen} → {destino}"] = {
                            "ruta": ruta,
                            "distancia": round(distancia, 3)
                        }

            # === Componentes fuertemente conexas ===
            componentes = grafo.componentes_fuertemente_conexas()

            # === Dibujar el grafo ===
            plt.figure(figsize=(9, 7))
            pos = nx.spring_layout(grafo.grafo, seed=42)
            pesos = nx.get_edge_attributes(grafo.grafo, 'weight')
            nx.draw_networkx(
                grafo.grafo, pos,
                with_labels=True,
                node_size=700,
                node_color="#3b82f6",
                font_size=8,
                font_color="white",
                edge_color="#a5b4fc",
                arrows=True
            )
            nx.draw_networkx_edge_labels(grafo.grafo, pos, edge_labels={k: f"{v:.2f}" for k, v in pesos.items()}, font_size=6)
            plt.title("Red de Citaciones (Dirigida y Ponderada)", fontsize=12)
            plt.tight_layout()

            buf = io.BytesIO()
            plt.savefig(buf, format='png', bbox_inches='tight')
            plt.close()
            buf.seek(0)
            img_base64 = base64.b64encode(buf.read()).decode('utf-8')

            # === Resumen de métricas ===
            resumen = {
                "n_nodos": grafo.grafo.number_of_nodes(),
                "n_aristas": grafo.grafo.number_of_edges(),
                "n_componentes": len(componentes),
                "caminos_ejemplo": caminos_minimos
            }

            return {
                "success": True,
                "resumen": resumen,
                "componentes": componentes,
                "graph_base64": img_base64
            }

        except Exception as e:
            return {"success": False, "message": str(e)}

    """REQUERIMENTO 2 DEL SEGUIMIENTO 2"""
    def analyze_term_cooccurrence_graph(self, csv_file: str | None = None,
                                        top_k_terms: int = 40,
                                        min_cooc: int = 2):
        """
        Requerimiento 2:
        - Construye un grafo NO dirigido de coocurrencia de términos (a partir de abstracts).
        - Devuelve grados/fuerza por nodo y componentes conexas.

        Usa los términos producidos por ConceptsCategoryAnalyzer (Req. 3)
        como vocabulario base.
        """
        try:
            import os, io, base64
            from pathlib import Path as _P
            import pandas as pd
            from src.algoritmo.ConceptsCategoryAnalyzer import ConceptsCategoryAnalyzer
            from src.grafos.seguimiento2_req2 import Seguimiento2Req2

            filepath = csv_file or self.unified_file
            if not filepath:
                return {'success': False, 'message': 'No hay CSV seleccionado ni CSV unificado disponible.'}
            if not os.path.exists(filepath):
                return {'success': False, 'message': f'Archivo no encontrado: {filepath}'}

            df = pd.read_csv(filepath, encoding='utf-8')
            if 'abstract' not in df.columns:
                return {'success': False, 'message': 'El CSV no contiene columna "abstract".'}

            abstracts = [str(x) if pd.notna(x) else '' for x in df['abstract'].tolist()]

            # 1) Términos (Req. 3)
            cca = ConceptsCategoryAnalyzer()
            c_res = cca.analyze(abstracts, top_k=top_k_terms)
            # Combina semillas + generados si tu analyzer los expone por separado
            terms = []
            if 'all_terms' in c_res:
                terms = list(c_res['all_terms'])
            else:
                # alternativas razonables según tu implementacion
                if 'generated_terms' in c_res:
                    terms.extend([t['term'] if isinstance(t, dict) else str(t)
                                  for t in c_res['generated_terms']])
                if 'seed_stats' in c_res:
                    terms.extend([s['term'] if isinstance(s, dict) else str(s)
                                  for s in c_res['seed_stats']])
                # deduplicar preservando orden
                seen = set(); dedup = []
                for t in terms:
                    if t not in seen:
                        seen.add(t)
                        dedup.append(t)
                terms = dedup

            if not terms:
                return {'success': False, 'message': 'No se pudo obtener un vocabulario de términos desde ConceptsCategoryAnalyzer.'}

            # 2) Construir grafo de coocurrencia (Req. 2)
            co = Seguimiento2Req2(min_cooc=min_cooc)
            co.build_from_documents(abstracts, terms)

            summary = co.summary(top_k=15)

            # 3) Imagen a base64
            out_dir = _P(self.__class__.__module__).resolve()
            out_dir = _P(__file__).parent / "src" / "data" / "screenshots" / "cooc"
            try:
                os.makedirs(out_dir, exist_ok=True)
            except Exception:
                pass
            base_name = _P(filepath).stem
            out_png = out_dir / f"{base_name}_COOC.png"
            co.draw(str(out_png))

            with open(out_png, 'rb') as f:
                graph_b64 = base64.b64encode(f.read()).decode('utf-8')

            # Guardar en status si quieres que quede disponible desde pipeline
            existing = self.status.get('results', {})
            existing['cooccurrence'] = {
                'summary': summary,
                'image_file': str(out_png)
            }
            self.status['results'] = existing

            return {
                'success': True,
                'summary': summary,
                'graph_base64': graph_b64,
                'image_file': str(out_png)
            }

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