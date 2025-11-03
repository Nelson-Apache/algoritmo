"""
IEEE Xplore Database Scraper
=============================

Este módulo proporciona una clase para realizar web scraping de la base de datos
IEEE Xplore (https://ieeexplore.ieee.org/), permitiendo extraer artículos científicos,
papers de conferencias y documentación técnica de manera automatizada.

El scraper maneja automáticamente:
- Autenticación mediante navegador (Playwright)
- Gestión de cookies y sesiones
- Extracción de metadatos de artículos
- Exportación a CSV/JSON
- Rate limiting y manejo de errores

Requisitos:
-----------
- requests: Para realizar peticiones HTTP
- playwright: Para automatizar el navegador y manejar login
- pandas: Para manipulación de datos (opcional)

Fecha: 2025
"""

import requests
import json
import time
import csv
import pandas as pd
from typing import List, Dict, Optional, Any
from playwright.sync_api import sync_playwright
import os
import random
from pathlib import Path


class IEEEScraper:
    """
    Scraper para la base de datos IEEE Xplore.
    
    Esta clase proporciona métodos para autenticarse en IEEE Xplore mediante login
    institucional, realizar búsquedas de artículos científicos y técnicos, y extraer
    sus metadatos completos incluyendo títulos, autores, abstracts, DOIs, etc.
    
    Attributes:
        base_url (str): URL base de la API de búsqueda de IEEE
        session (requests.Session): Sesión HTTP para mantener cookies
        login_url (str): URL de inicio de sesión institucional
        headers (dict): Headers HTTP para las peticiones
        cookies (dict): Cookies de sesión para autenticación
        total_items (int): Número total de resultados disponibles
    
    Example:
        >>> scraper = IEEEScraper(auto_login=True)
        >>> articles = scraper.scrape_all("machine learning", max_results=100)
        >>> scraper.save_to_csv(articles, "ml_articles.csv")
    """
    
    def __init__(self, auto_login: bool = True):
        """
        Inicializa el scraper de IEEE Xplore.
        
        Configura la sesión HTTP, URLs, headers y opcionalmente realiza el
        login automático. Si auto_login es True, intentará cargar cookies
        existentes o iniciará un proceso de login manual si es necesario.
        
        Args:
            auto_login (bool, optional): Si es True, intenta autenticarse
                automáticamente al inicializar. Por defecto True.
        
        Raises:
            Exception: Si el auto_login falla y no se puede establecer sesión
        """
        # URL de la API de búsqueda de IEEE Xplore
        self.base_url = "https://ieeexplore-ieee-org.crai.referencistas.com/rest/search"
        
        # URL de la interfaz web (para navegación inicial)
        self.web_url = "https://ieeexplore-ieee-org.crai.referencistas.com"
        
        # Sesión HTTP para mantener cookies entre peticiones
        self.session = requests.Session()
        
        # URL de acceso institucional con proxy de autenticación
        self.login_url = "https://login.intelproxy.com/v2/inicio?cuenta=7Ah6RNpGWF22jjyq&url=ezp.2aHR0cHM6Ly9pZWVleHBsb3JlLmllZWUub3JnL1hsb3JlL2hvbWUuanNw"
        
        # Headers HTTP que simulan un navegador real
        self.headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "es-419,es;q=0.9",
            "Connection": "keep-alive",
            "Content-Type": "application/json",
            "Origin": "https://ieeexplore-ieee-org.crai.referencistas.com",
            "Referer": "https://ieeexplore-ieee-org.crai.referencistas.com/search/searchresult.jsp",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
            "X-Security-Request": "required",
            "sec-ch-ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
        }

        # Diccionario para almacenar cookies de sesión
        self.cookies = {}
        
        # Variable para almacenar el total de resultados disponibles
        self.total_items = None

        # Rutas centralizadas
        # DATA_ROOT -> <repo>/src/data ; BROWSER_PROFILE_ROOT -> <repo>/browser_profile
        self.DATA_ROOT = Path(__file__).parents[1] / "data"
        self.BROWSER_PROFILE_ROOT = Path(__file__).parents[2] / "browser_profile"

        # Proceso de autenticación automática
        if auto_login:
            # Intentar cargar cookies existentes primero
            if not (self.load_cookies("ieee_cookies.json") and self.test_cookies()):
                print("Cookies no válidas o no encontradas. Iniciando login manual...")
                self.manual_login()

    def manual_login(self):
        """
        Realiza el proceso de login completamente manual.
        
        Abre un navegador Chromium donde el usuario debe completar manualmente
        el proceso de autenticación institucional. Una vez completado, extrae
        las cookies de sesión y las guarda para futuros usos.
        
        Process:
            1. Abre navegador Chromium (headless=False)
            2. Navega a la URL de login institucional
            3. Espera a que el usuario complete el login
            4. Extrae cookies del contexto del navegador
            5. Guarda cookies en archivo JSON
        
        Raises:
            Exception: Si hay errores durante el proceso de navegación o
                extracción de cookies
        """
        print("=== LOGIN MANUAL REQUERIDO ===")
        print("Se abrirá un navegador. Por favor:")
        print("1. Completa el login manualmente")
        print("2. Navega hasta la página principal de IEEE Xplore")
        print("3. Presiona Enter en esta consola cuando estés listo")
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            context = browser.new_context(
                user_agent=self.headers["User-Agent"]
            )
            page = context.new_page()

            try:
                # Navegar a la página de login institucional
                page.goto(self.login_url)
                
                print("\nPor favor completa el login en el navegador...")
                print("Presiona Enter cuando hayas terminado y estés en IEEE Xplore:")
                input()
                
                # Verificar que estamos en la página correcta
                current_url = page.url
                if "ieeexplore" not in current_url.lower() and "crai.referencistas" not in current_url:
                    print("Navegando a IEEE Xplore...")
                    ieee_url = "https://ieeexplore-ieee-org.crai.referencistas.com/Xplore/home.jsp"
                    page.goto(ieee_url)
                    page.wait_for_timeout(3000)
                
                # Extraer todas las cookies del contexto del navegador
                cookies = context.cookies()
                safe_cookies: Dict[str, str] = {}
                for c in cookies:
                    name = c.get("name")
                    value = c.get("value")
                    if name and value:
                        safe_cookies[name] = value
                
                self.cookies = safe_cookies
                print(f"Cookies extraídas: {len(self.cookies)} cookies")
                
                # Guardar cookies en archivo para uso futuro
                self.save_cookies("ieee_cookies.json")
                
                print("✓ Login completado exitosamente")

            except Exception as e:
                print(f"Error durante el login manual: {e}")
                raise
            finally:
                browser.close()

    def login_with_persistent_browser(self):
        """
        Utiliza un perfil de navegador persistente para mantener la sesión.
        
        Features:
            - Guarda el estado del navegador en disco
            - Mantiene cookies entre sesiones
            - Evita repetir el proceso de login
        """
        print("=== LOGIN CON PERFIL PERSISTENTE ===")
        
        profile_dir = self.BROWSER_PROFILE_ROOT / "ieee"
        profile_dir.mkdir(parents=True, exist_ok=True)
        
        with sync_playwright() as p:
            browser = p.chromium.launch_persistent_context(
                user_data_dir=str(profile_dir),
                headless=False,
                user_agent=self.headers["User-Agent"]
            )
            
            try:
                page = browser.new_page()
                page.goto(self.login_url)
                
                print("Completa el login en el navegador...")
                print("El navegador guardará tu sesión para futuros usos.")
                print("Presiona Enter cuando hayas completado el login:")
                input()
                
                if "ieeexplore" not in page.url.lower():
                    page.goto("https://ieeexplore-ieee-org.crai.referencistas.com/Xplore/home.jsp")
                    page.wait_for_timeout(3000)
                
                cookies = browser.cookies()
                safe_cookies: Dict[str, str] = {}
                for c in cookies:
                    name = c.get("name")
                    value = c.get("value")
                    if name and value:
                        safe_cookies[name] = value
                
                self.cookies = safe_cookies
                self.save_cookies("ieee_cookies.json")
                
                print("✓ Login con perfil persistente completado")

            except Exception as e:
                print(f"Error con perfil persistente: {e}")
                raise
            finally:
                browser.close()

    def login_and_get_cookies(self, email: Optional[str] = None, password: Optional[str] = None, headless: bool = False):
        """
        Método avanzado de login con automatización completa y fallback manual.
        
        Args:
            email (Optional[str]): Email para login automático
            password (Optional[str]): Contraseña para login automático
            headless (bool, optional): Ejecutar en modo headless. Por defecto False.
        """
        print("Iniciando proceso de autenticación (IEEE)...")

        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=headless,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-sandbox',
                    '--disable-extensions',
                ]
            )

            context = browser.new_context(
                user_agent=self.headers["User-Agent"],
                viewport={'width': 1366, 'height': 768},
                extra_http_headers={
                    'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
                }
            )

            context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                window.chrome = { runtime: {} };
            """)

            page = context.new_page()

            try:
                print("➡️  Abriendo página de login del proxy institucional...")
                page.goto(self.login_url, wait_until='networkidle')
                page.wait_for_timeout(2000)

                # Intentar localizar y pulsar botón de Google SSO
                if email and password:
                    print("🔎 Buscando botón 'Continuar con Google'...")
                    clicked = False
                    for selector in [
                        "button:has-text('Google')",
                        "a:has-text('Google')",
                        "role=button[name*='Google' i]",
                        "text=/Google|Gmail|Acceder con Google|Continuar con Google/i",
                    ]:
                        try:
                            loc = page.locator(selector)
                            if loc.count() > 0:
                                loc.first.click()
                                clicked = True
                                break
                        except Exception:
                            pass

                    if not clicked:
                        print("⚠️ No se encontró botón de Google directamente, intentando detectar enlaces de IdP...")

                    # Flujo de Google
                    def fill_google_login():
                        if "accounts.google.com" not in page.url:
                            return False
                        print("✍️  Completando email de Google...")
                        page.wait_for_selector("input[type='email']", timeout=20000)
                        page.fill("input[type='email']", str(email))
                        # Botón siguiente (varía por idioma)
                        for next_sel in ["#identifierNext", "button:has-text('Siguiente')", "button:has-text('Next')"]:
                            try:
                                page.click(next_sel)
                                break
                            except Exception:
                                continue
                        page.wait_for_timeout(1500)

                        print("✍️  Completando contraseña de Google...")
                        page.wait_for_selector("input[type='password']", timeout=20000)
                        page.fill("input[type='password']", str(password))
                        for next_sel in ["#passwordNext", "button:has-text('Siguiente')", "button:has-text('Next')"]:
                            try:
                                page.click(next_sel)
                                break
                            except Exception:
                                continue
                        return True

                    # Si nos llevó a Google, llenar credenciales
                    try:
                        if "accounts.google.com" in page.url:
                            fill_google_login()
                    except Exception:
                        pass

                # Esperar redirección a IEEE (post login / o permitir completar manualmente si no hay credenciales)
                if not email or not password:
                    print("ℹ️ No se pasaron credenciales; esperando que completes el login si el navegador está visible...")
                
                # Esperar hasta 60s a que lleguemos a IEEE vía proxy
                print("⏳ Esperando redirección a IEEE Xplore...")
                arrived = False
                for _ in range(60):
                    current_url = page.url
                    if ("ieeexplore" in current_url.lower()) or ("crai.referencistas" in current_url.lower()):
                        arrived = True
                        break
                    page.wait_for_timeout(1000)

                if not arrived:
                    # Forzar navegación a home de IEEE via proxy como último recurso
                    try:
                        page.goto("https://ieeexplore-ieee-org.crai.referencistas.com/Xplore/home.jsp", wait_until='domcontentloaded')
                        page.wait_for_timeout(2000)
                    except Exception:
                        pass

                # Extraer cookies del contexto
                cookies = context.cookies()
                safe_cookies: Dict[str, str] = {}
                for c in cookies:
                    name = c.get("name")
                    value = c.get("value")
                    if name and value:
                        safe_cookies[name] = value

                if not safe_cookies:
                    raise RuntimeError("No se pudieron extraer cookies de sesión")

                self.cookies = safe_cookies
                self.save_cookies("ieee_cookies.json")
                print(f"✅ Login IEEE completado. Cookies: {len(self.cookies)}")

            except Exception as e:
                print(f"❌ Error durante el login IEEE: {e}")
                if not headless:
                    print("🔄 Fallback a modo manual: completa el login en la ventana abierta y presiona Enter aquí...")
                    try:
                        input()
                        cookies = context.cookies()
                        safe_cookies: Dict[str, str] = {}
                        for c in cookies:
                            name = c.get("name"); value = c.get("value")
                            if name and value:
                                safe_cookies[name] = value
                        if safe_cookies:
                            self.cookies = safe_cookies
                            self.save_cookies("ieee_cookies.json")
                            print(f"✅ Cookies capturadas tras login manual: {len(self.cookies)}")
                        else:
                            raise RuntimeError("No fue posible capturar cookies tras el login manual")
                    except Exception as e2:
                        print(f"❌ Falló el fallback manual: {e2}")
                        raise
                else:
                    raise
            finally:
                browser.close()

    def save_cookies(self, filename: str = "ieee_cookies.json"):
        """Guarda las cookies de sesión en un archivo JSON."""
        if not os.path.dirname(filename):
            cookies_dir = self.DATA_ROOT / "cookies"
            cookies_dir.mkdir(parents=True, exist_ok=True)
            fullpath = cookies_dir / filename
        else:
            fullpath = Path(filename)
            if fullpath.parent:
                fullpath.parent.mkdir(parents=True, exist_ok=True)

        with open(str(fullpath), 'w', encoding='utf-8') as f:
            json.dump(self.cookies, f, indent=2)
        print(f"Cookies guardadas en: {fullpath}")

    def load_cookies(self, filename: str = "ieee_cookies.json") -> bool:
        """Carga cookies de sesión desde un archivo JSON."""
        try:
            # Resolver ruta en src/data/cookies si es solo nombre de archivo
            if not os.path.dirname(filename):
                fullpath = self.DATA_ROOT / "cookies" / filename
            else:
                fullpath = Path(filename)
                
            if os.path.exists(str(fullpath)):
                with open(str(fullpath), 'r', encoding='utf-8') as f:
                    self.cookies = json.load(f)
                print(f"Cookies cargadas desde: {fullpath}")
                return True
            else:
                print(f"Archivo de cookies no encontrado: {fullpath}")
                return False
        except Exception as e:
            print(f"Error cargando cookies: {e}")
            return False

    def test_cookies(self) -> bool:
        """Verifica si las cookies actuales son válidas."""
        try:
            test_data = self.search("artificial intelligence", page_number=1, records_per_page=1, verbose=False)
            is_valid = test_data.get('totalRecords', 0) >= 0
            if is_valid:
                print("✓ Cookies válidas")
            else:
                print("✗ Cookies inválidas")
            return is_valid
        except Exception as e:
            print(f"✗ Cookies no válidas: {e}")
            return False

    def get_total_items(self, query: str) -> int:
        """
        Obtiene el número total de resultados disponibles para una búsqueda.
        
        Args:
            query (str): Término o términos de búsqueda
        
        Returns:
            int: Número total de resultados disponibles
        """
        payload = self._build_payload(query, page_number=1, records_per_page=1)

        try:
            response = self.session.post(
                self.base_url,
                headers=self.headers,
                cookies=self.cookies,
                json=payload,
            )
            response.raise_for_status()
            
            data = response.json()
            total = data.get("totalRecords", 0)
            print(f"Total de resultados disponibles para '{query}': {total:,}")
            return total

        except Exception as e:
            print(f"Error obteniendo total de items: {e}")
            if "401" in str(e) or "403" in str(e):
                print("Posible problema de autenticación. Cookies pueden haber expirado.")
            return 0

    def _build_payload(self, query: str, page_number: int = 1, records_per_page: int = 25) -> Dict:
        """
        Construye el payload JSON para las peticiones a la API de IEEE.
        
        Args:
            query (str): Término de búsqueda
            page_number (int): Número de página (1-indexed)
            records_per_page (int): Registros por página (máximo 100)
        
        Returns:
            Dict: Payload para la API
        """
        return {
            "newsearch": True,
            "queryText": query,
            "highlight": True,
            "returnFacets": ["ALL"],
            "returnType": "SEARCH",
            "matchPubs": True,
            "pageNumber": page_number,
            "rowsPerPage": records_per_page
        }

    def search(self, query: str, page_number: int = 1, records_per_page: int = 25, verbose: bool = True) -> Dict:
        """
        Realiza una búsqueda en IEEE Xplore.
        
        Args:
            query (str): Término de búsqueda
            page_number (int): Número de página (1-indexed)
            records_per_page (int): Registros por página (1-100)
            verbose (bool): Imprimir información de debug
        
        Returns:
            Dict: Respuesta JSON de la API
        """
        payload = self._build_payload(query, page_number, records_per_page)
        
        time.sleep(0.1)  # Rate limiting

        response = self.session.post(
            self.base_url,
            headers=self.headers,
            cookies=self.cookies,
            json=payload,
        )

        if verbose:
            print(f"📡 Query buscado: '{query}'")
            print(f"📡 Página: {page_number}, Registros/página: {records_per_page}")
            print(f"📡 Status code: {response.status_code}")

        response.raise_for_status()
        return response.json()

    def extract_articles(self, data: Dict) -> List[Dict]:
        """
        Extrae y procesa metadatos de artículos desde la respuesta JSON de IEEE.
        
        Args:
            data (Dict): Respuesta JSON de la API
        
        Returns:
            List[Dict]: Lista de artículos con metadatos
        """
        articles = []
        records = data.get("records", [])
        
        print(f"📄 Extrayendo {len(records)} artículos...")
        
        for record in records:
            # Extraer autores
            authors = []
            for author in record.get("authors", []):
                author_name = author.get("preferredName", "") or author.get("normalizedName", "")
                if author_name:
                    authors.append(author_name)
            
            # Extraer términos de índice (keywords)
            index_terms = []
            author_terms = record.get("authorTerms", [])
            ieee_terms = record.get("indexTerms", {}).get("IEEE Terms", {}).get("terms", [])
            
            for term in author_terms:
                index_terms.append(term)
            for term in ieee_terms:
                index_terms.append(term)
            
            # Construir diccionario con metadatos
            article = {
                "article_number": record.get("articleNumber", ""),
                "title": record.get("articleTitle", ""),
                "abstract": record.get("abstract", ""),
                "authors": "; ".join(authors),
                "publication_title": record.get("publicationTitle", ""),
                "publication_year": record.get("publicationYear", ""),
                "publication_date": record.get("publicationDate", ""),
                "doi": record.get("doi", ""),
                "isbn": record.get("isbn", ""),
                "issn": record.get("issn", ""),
                "eisbn": record.get("eisbn", ""),
                "eissn": record.get("eissn", ""),
                "content_type": record.get("contentType", ""),
                "publisher": record.get("publisher", ""),
                "conference_location": record.get("conferenceLocation", ""),
                "conference_dates": record.get("conferenceDates", ""),
                "volume": record.get("volume", ""),
                "issue": record.get("issue", ""),
                "start_page": record.get("startPage", ""),
                "end_page": record.get("endPage", ""),
                "index_terms": "; ".join(index_terms),
                "pdf_url": record.get("pdfUrl", ""),
                "html_url": record.get("htmlUrl", ""),
                "access_type": record.get("accessType", ""),
                "is_open_access": record.get("isOpenAccess", False),
                "citing_paper_count": record.get("citingPaperCount", 0),
                "download_count": record.get("downloadCount", 0),
            }
            articles.append(article)
            
        print(f"✅ {len(articles)} artículos extraídos exitosamente")
        return articles

    def scrape_all(
        self,
        query: str,
        max_results: Optional[int] = None,
        records_per_page: int = 25,
        delay: float = 0.5,
        threads: int = 1,
    ) -> List[Dict]:
        """
        Realiza scraping completo de múltiples páginas de resultados.
        
        Args:
            query (str): Término de búsqueda
            max_results (Optional[int]): Máximo de resultados a obtener
            records_per_page (int): Registros por página (1-100, recomendado 25-100)
            delay (float): Segundos entre peticiones
        
        Returns:
            List[Dict]: Lista de todos los artículos extraídos
        """
        print(f"🔍 Iniciando scraping para: '{query}'")
        
        if not self.test_cookies():
            print("Cookies inválidas. Iniciando re-autenticación...")
            self.manual_login()

        total_items = self.get_total_items(query)

        if total_items == 0:
            print("❌ No se encontraron resultados para la búsqueda")
            return []

        target_results = min(max_results or total_items, total_items)
        print(f"🎯 Objetivo: {target_results:,} resultados de {total_items:,} disponibles")

        # Modo concurrente por páginas
        if threads and threads > 1:
            from concurrent.futures import ThreadPoolExecutor, as_completed

            print(f"⚡ Scraping concurrente con {threads} hilos")
            pages = []
            remaining = target_results
            page_number = 1
            while remaining > 0:
                current_records = min(records_per_page, remaining)
                pages.append((page_number, current_records))
                remaining -= current_records
                page_number += 1

            all_articles: List[Dict] = []
            with ThreadPoolExecutor(max_workers=threads) as executor:
                futures = [
                    executor.submit(self.search, query, pn, rec, False)
                    for (pn, rec) in pages
                ]
                for fut in as_completed(futures):
                    try:
                        data = fut.result()
                        arts = self.extract_articles(data)
                        all_articles.extend(arts)
                    except Exception as e:
                        print(f"❌ Error en tarea concurrente: {e}")
            all_articles = all_articles[:target_results]
            print(f"🎉 Scraping completado: {len(all_articles):,} artículos obtenidos")
            return all_articles

        # Modo secuencial
        all_articles = []
        page_number = 1
        consecutive_errors = 0
        max_consecutive_errors = 3

        while len(all_articles) < target_results and consecutive_errors < max_consecutive_errors:
            remaining = target_results - len(all_articles)
            current_records = min(records_per_page, remaining)

            print(f"📡 Scraping página {page_number} "
                  f"({len(all_articles):,}/{target_results:,} completado)")

            try:
                data = self.search(query, page_number, current_records, verbose=True)
                articles = self.extract_articles(data)

                if not articles:
                    print("❌ No se encontraron más artículos")
                    break

                all_articles.extend(articles)
                page_number += 1
                consecutive_errors = 0

                if len(all_articles) < target_results:
                    sleep_time = delay + random.uniform(0, 0.5)
                    print(f"⏸️ Esperando {sleep_time:.1f} segundos...")
                    time.sleep(sleep_time)

            except requests.exceptions.RequestException as e:
                consecutive_errors += 1
                print(f"❌ Error de red ({consecutive_errors}/{max_consecutive_errors}): {e}")
                
                if "401" in str(e) or "403" in str(e):
                    print("🔑 Error de autenticación. Reautenticando...")
                    self.manual_login()
                    consecutive_errors = 0
                    continue
                
                wait_time = 5 * consecutive_errors
                print(f"⏳ Esperando {wait_time} segundos antes de reintentar...")
                time.sleep(wait_time)
                
            except Exception as e:
                consecutive_errors += 1
                print(f"❌ Error inesperado ({consecutive_errors}/{max_consecutive_errors}): {e}")
                time.sleep(5)

        print(f"🎉 Scraping completado: {len(all_articles):,} artículos obtenidos")
        return all_articles

    def save_to_csv(self, articles: List[Dict], filename: str):
        """Guarda los artículos en formato CSV."""
        if not articles:
            print("❌ No hay artículos para guardar")
            return
            
        all_columns = set()
        for article in articles:
            all_columns.update(article.keys())
        
        ordered_columns = sorted(all_columns)
        
        if not os.path.dirname(filename):
            csv_dir = self.DATA_ROOT / "csv"
            csv_dir.mkdir(parents=True, exist_ok=True)
            fullpath = csv_dir / filename
        else:
            fullpath = Path(filename)
            if fullpath.parent:
                fullpath.parent.mkdir(parents=True, exist_ok=True)

        with open(str(fullpath), 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=ordered_columns)
            writer.writeheader()

            for article in articles:
                clean_article = {}
                for col in ordered_columns:
                    value = article.get(col, "")
                    clean_value = str(value).replace('\n', ' ').replace('\r', ' ')
                    clean_article[col] = clean_value

                writer.writerow(clean_article)

        print(f"💾 Datos guardados en CSV: {fullpath}")
        print(f"📊 Total de registros: {len(articles)}")
        print(f"📋 Columnas incluidas: {len(ordered_columns)}")

    def save_to_json(self, articles: List[Dict], filename: str):
        """Guarda los artículos en formato JSON."""
        if not os.path.dirname(filename):
            json_dir = self.DATA_ROOT / "json"
            json_dir.mkdir(parents=True, exist_ok=True)
            fullpath = json_dir / filename
        else:
            fullpath = Path(filename)
            if fullpath.parent:
                fullpath.parent.mkdir(parents=True, exist_ok=True)

        with open(str(fullpath), "w", encoding="utf-8") as f:
            json.dump(articles, f, indent=2, ensure_ascii=False)
        print(f"💾 Datos guardados en JSON: {fullpath}")
