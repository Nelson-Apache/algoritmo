"""
JSTOR Database Scraper
======================

Este módulo proporciona una clase para realizar web scraping de la base de datos
académica JSTOR (https://www.jstor.org/), permitiendo extraer artículos científicos,
libros y documentación académica de manera automatizada.

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


class JSTORScraper:
    """
    Scraper para la base de datos académica JSTOR.
    
    Esta clase proporciona métodos para autenticarse en JSTOR mediante login
    institucional, realizar búsquedas de artículos académicos y extraer sus
    metadatos completos incluyendo títulos, autores, abstracts, DOIs, etc.
    
    Attributes:
        base_url (str): URL base de la API de búsqueda de JSTOR
        session (requests.Session): Sesión HTTP para mantener cookies
        login_url (str): URL de inicio de sesión institucional
        headers (dict): Headers HTTP para las peticiones
        cookies (dict): Cookies de sesión para autenticación
        total_items (int): Número total de resultados disponibles
    
    Example:
        >>> scraper = JSTORScraper(auto_login=True)
        >>> articles = scraper.scrape_all("artificial intelligence", max_results=100)
        >>> scraper.save_to_csv(articles, "ai_articles.csv")
    """
    
    def __init__(self, auto_login: bool = True):
        """
        Inicializa el scraper de JSTOR.
        
        Configura la sesión HTTP, URLs, headers y opcionalmente realiza el
        login automático. Si auto_login es True, intentará cargar cookies
        existentes o iniciará un proceso de login manual si es necesario.
        
        Args:
            auto_login (bool, optional): Si es True, intenta autenticarse
                automáticamente al inicializar. Por defecto True.
        
        Raises:
            Exception: Si el auto_login falla y no se puede establecer sesión
        """
        # URL de la API de búsqueda de JSTOR
        self.base_url = "https://www-jstor-org.crai.referencistas.com/search-results/grouped-search/"
        
        # URL de la interfaz web
        self.web_url = "https://www-jstor-org.crai.referencistas.com"
        
        # Sesión HTTP para mantener cookies entre peticiones
        self.session = requests.Session()
        
        # URL de acceso institucional con proxy de autenticación
        self.login_url = "https://login.intelproxy.com/v2/inicio?cuenta=7Ah6RNpGWF22jjyq&url=ezp.2aHR0cHM6Ly93d3cuanN0b3Iub3JnLw--"
        
        # Headers HTTP que simulan un navegador real
        self.headers = {
            "Accept": "*/*",
            "Accept-Language": "es-419,es;q=0.9",
            "Connection": "keep-alive",
            "Content-Type": "application/json",
            "Origin": "https://www-jstor-org.crai.referencistas.com",
            "Referer": "https://www-jstor-org.crai.referencistas.com/",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
            "sec-ch-ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
        }

        # Diccionario para almacenar cookies de sesión
        self.cookies = {}
        
        # Variable para almacenar el total de resultados disponibles
        self.total_items = None
        
        # Variable para almacenar el refreqid (request ID de JSTOR)
        self.refreqid = None

        # Proceso de autenticación automática
        if auto_login:
            # Intentar cargar cookies existentes primero
            if not (self.load_cookies("jstor_cookies.json") and self.test_cookies()):
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
        print("2. Navega hasta la página principal de JSTOR")
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
                print("Presiona Enter cuando hayas terminado y estés en JSTOR:")
                input()
                
                # Verificar que estamos en la página correcta
                current_url = page.url
                if "jstor" not in current_url.lower() and "crai.referencistas" not in current_url:
                    print("Navegando a JSTOR...")
                    jstor_url = "https://www-jstor-org.crai.referencistas.com/"
                    page.goto(jstor_url)
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
                self.save_cookies("jstor_cookies.json")
                
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
        
        profile_dir = "./browser_profile_jstor"
        os.makedirs(profile_dir, exist_ok=True)
        
        with sync_playwright() as p:
            browser = p.chromium.launch_persistent_context(
                user_data_dir=profile_dir,
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
                
                if "jstor" not in page.url.lower():
                    page.goto("https://www-jstor-org.crai.referencistas.com/")
                    page.wait_for_timeout(3000)
                
                cookies = browser.cookies()
                safe_cookies: Dict[str, str] = {}
                for c in cookies:
                    name = c.get("name")
                    value = c.get("value")
                    if name and value:
                        safe_cookies[name] = value
                
                self.cookies = safe_cookies
                self.save_cookies("jstor_cookies.json")
                
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
        print("Iniciando proceso de autenticación (JSTOR)...")

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

                    # Completar credenciales si estamos en Google
                    def fill_google_login():
                        if "accounts.google.com" not in page.url:
                            return False
                        print("✍️  Completando email de Google...")
                        page.wait_for_selector("input[type='email']", timeout=20000)
                        page.fill("input[type='email']", str(email))
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

                    try:
                        if "accounts.google.com" in page.url:
                            fill_google_login()
                    except Exception:
                        pass

                # Esperar redirección a JSTOR (o permitir completar manualmente sin credenciales)
                if not email or not password:
                    print("ℹ️ No se pasaron credenciales; si el navegador está visible, completa el login…")

                print("⏳ Esperando redirección a JSTOR...")
                arrived = False
                for _ in range(60):
                    current_url = page.url
                    if ("jstor" in current_url.lower()) or ("crai.referencistas" in current_url.lower()):
                        arrived = True
                        break
                    page.wait_for_timeout(1000)

                if not arrived:
                    try:
                        page.goto("https://www-jstor-org.crai.referencistas.com/", wait_until='domcontentloaded')
                        page.wait_for_timeout(2000)
                    except Exception:
                        pass

                # Extraer cookies
                cookies = context.cookies()
                safe_cookies: Dict[str, str] = {}
                for c in cookies:
                    name = c.get("name"); value = c.get("value")
                    if name and value:
                        safe_cookies[name] = value

                if not safe_cookies:
                    raise RuntimeError("No se pudieron extraer cookies de sesión")

                self.cookies = safe_cookies
                self.save_cookies("jstor_cookies.json")
                print(f"✅ Login JSTOR completado. Cookies: {len(self.cookies)}")

            except Exception as e:
                print(f"❌ Error durante el login JSTOR: {e}")
                if not headless:
                    print("🔄 Fallback a modo manual: completa el login en la ventana y presiona Enter aquí…")
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
                            self.save_cookies("jstor_cookies.json")
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

    def save_cookies(self, filename: str = "jstor_cookies.json"):
        """Guarda las cookies de sesión en un archivo JSON."""
        if not os.path.dirname(filename):
            cookies_dir = os.path.join("data", "cookies")
            os.makedirs(cookies_dir, exist_ok=True)
            fullpath = os.path.join(cookies_dir, filename)
        else:
            fullpath = filename
            parent = os.path.dirname(fullpath)
            if parent:
                os.makedirs(parent, exist_ok=True)

        with open(fullpath, 'w', encoding='utf-8') as f:
            json.dump(self.cookies, f, indent=2)
        print(f"Cookies guardadas en: {fullpath}")

    def load_cookies(self, filename: str = "jstor_cookies.json") -> bool:
        """Carga cookies de sesión desde un archivo JSON."""
        try:
            # Buscar en data/cookies si es solo nombre de archivo
            if not os.path.dirname(filename):
                fullpath = os.path.join("data", "cookies", filename)
            else:
                fullpath = filename
                
            if os.path.exists(fullpath):
                with open(fullpath, 'r', encoding='utf-8') as f:
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
            test_data = self.search("artificial intelligence", page=0, page_size=10, verbose=False)
            is_valid = test_data.get('totalResults', 0) >= 0
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
        payload = self._build_payload(query, page=0, page_size=1)

        try:
            response = self.session.post(
                self.base_url,
                headers=self.headers,
                cookies=self.cookies,
                json=payload,
            )
            response.raise_for_status()
            
            data = response.json()
            total = data.get("totalResults", 0)
            
            # Guardar refreqid para peticiones futuras
            if "refreqid" in data:
                self.refreqid = data["refreqid"]
            
            print(f"Total de resultados disponibles para '{query}': {total:,}")
            return total

        except Exception as e:
            print(f"Error obteniendo total de items: {e}")
            if "401" in str(e) or "403" in str(e):
                print("Posible problema de autenticación. Cookies pueden haber expirado.")
            return 0

    def _build_payload(self, query: str, page: int = 0, page_size: int = 25) -> Dict:
        """
        Construye el payload JSON para las peticiones a la API de JSTOR.
        
        Args:
            query (str): Término de búsqueda
            page (int): Número de página (0-indexed)
            page_size (int): Resultados por página
        
        Returns:
            Dict: Payload para la API
        """
        payload = {
            "acc": "",
            "allowEmptyQuery": False,
            "endDate": "",
            "forwardedAdvancedSearchParams": {
                "Query": query.replace(" ", "+")
            },
            "getFlagNames": ["genai_beta_search_results"],
            "isAdvancedSearch": False,
            "msFacetFields": [],
            "pageParams": {
                "page": page,
                "pageSize": page_size
            },
            "pageType": "basic_search_page",
            "referer": f"https://www-jstor-org.crai.referencistas.com/action/doBasicSearch?Query={query.replace(' ', '+')}",
            "searchTerm": query,
            "startDate": "",
            "theme": "",
            "filterQueries": [],
            "sort": "rel",  # rel = relevancia, puede ser también "date"
            "content_set_flags": []
        }
        
        # Agregar refreqid si existe
        if self.refreqid:
            payload["refreqid"] = self.refreqid
        
        return payload

    def search(self, query: str, page: int = 0, page_size: int = 25, verbose: bool = True) -> Dict:
        """
        Realiza una búsqueda en JSTOR.
        
        Args:
            query (str): Término de búsqueda
            page (int): Número de página (0-indexed)
            page_size (int): Resultados por página (1-100)
            verbose (bool): Imprimir información de debug
        
        Returns:
            Dict: Respuesta JSON de la API
        """
        payload = self._build_payload(query, page, page_size)
        
        time.sleep(0.1)  # Rate limiting

        response = self.session.post(
            self.base_url,
            headers=self.headers,
            cookies=self.cookies,
            json=payload,
        )

        if verbose:
            print(f"📡 Query buscado: '{query}'")
            print(f"📡 Página: {page}, Registros/página: {page_size}")
            print(f"📡 Status code: {response.status_code}")

        response.raise_for_status()
        data = response.json()
        
        # Actualizar refreqid si viene en la respuesta
        if "refreqid" in data:
            self.refreqid = data["refreqid"]
        
        return data

    def extract_articles(self, data: Dict) -> List[Dict]:
        """
        Extrae y procesa metadatos de artículos desde la respuesta JSON de JSTOR.
        
        Args:
            data (Dict): Respuesta JSON de la API
        
        Returns:
            List[Dict]: Lista de artículos con metadatos
        """
        articles = []
        docs = data.get("docs", [])
        
        print(f"📄 Extrayendo {len(docs)} artículos...")
        
        for doc in docs:
            # Extraer autores
            authors = []
            for author in doc.get("authors", []):
                author_name = author.get("name", "")
                if author_name:
                    authors.append(author_name)
            
            # Extraer keywords/subjects
            subjects = []
            for subj in doc.get("subjects", []):
                if isinstance(subj, str):
                    subjects.append(subj)
                elif isinstance(subj, dict):
                    subjects.append(subj.get("name", ""))
            
            # Construir diccionario con metadatos
            article = {
                "id": doc.get("id", ""),
                "stable_url": doc.get("stableUrl", ""),
                "title": doc.get("title", ""),
                "subtitle": doc.get("subtitle", ""),
                "abstract": doc.get("abstract", ""),
                "authors": "; ".join(authors),
                "publication_title": doc.get("publicationTitle", ""),
                "publication_year": doc.get("publicationYear", ""),
                "publication_date": doc.get("publicationDate", ""),
                "doi": doc.get("doi", ""),
                "isbn": doc.get("isbn", ""),
                "issn": doc.get("issn", ""),
                "content_type": doc.get("itemType", ""),  # article, book, review, etc.
                "publisher": doc.get("publisher", ""),
                "language": doc.get("language", ""),
                "volume": doc.get("volume", ""),
                "issue": doc.get("issue", ""),
                "page_start": doc.get("pageStart", ""),
                "page_end": doc.get("pageEnd", ""),
                "page_count": doc.get("pageCount", ""),
                "subjects": "; ".join(subjects),
                "pdf_url": doc.get("pdfUrl", ""),
                "access_type": doc.get("accessType", ""),
                "is_open_access": doc.get("isOpenAccess", False),
                "citation_count": doc.get("citationCount", 0),
                "has_full_text": doc.get("hasFullText", False),
                "preview_available": doc.get("previewAvailable", False),
            }
            articles.append(article)
            
        print(f"✅ {len(articles)} artículos extraídos exitosamente")
        return articles

    def scrape_all(
        self,
        query: str,
        max_results: Optional[int] = None,
        page_size: int = 25,
        delay: float = 0.5,
        threads: int = 1,
    ) -> List[Dict]:
        """
        Realiza scraping completo de múltiples páginas de resultados.
        
        Args:
            query (str): Término de búsqueda
            max_results (Optional[int]): Máximo de resultados a obtener
            page_size (int): Registros por página (1-100, recomendado 25-50)
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
            page = 0
            while remaining > 0:
                current_page_size = min(page_size, remaining)
                pages.append((page, current_page_size))
                remaining -= current_page_size
                page += 1

            all_articles: List[Dict] = []
            with ThreadPoolExecutor(max_workers=threads) as executor:
                futures = [
                    executor.submit(self.search, query, pg, sz, False)
                    for (pg, sz) in pages
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
        page = 0
        consecutive_errors = 0
        max_consecutive_errors = 3

        while len(all_articles) < target_results and consecutive_errors < max_consecutive_errors:
            remaining = target_results - len(all_articles)
            current_page_size = min(page_size, remaining)

            print(f"📡 Scraping página {page} "
                  f"({len(all_articles):,}/{target_results:,} completado)")

            try:
                data = self.search(query, page, current_page_size, verbose=True)
                articles = self.extract_articles(data)

                if not articles:
                    print("❌ No se encontraron más artículos")
                    break

                all_articles.extend(articles)
                page += 1
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
            csv_dir = os.path.join("data", "csv")
            os.makedirs(csv_dir, exist_ok=True)
            fullpath = os.path.join(csv_dir, filename)
        else:
            fullpath = filename
            parent = os.path.dirname(fullpath)
            if parent:
                os.makedirs(parent, exist_ok=True)

        with open(fullpath, 'w', newline='', encoding='utf-8') as csvfile:
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
            json_dir = os.path.join("data", "json")
            os.makedirs(json_dir, exist_ok=True)
            fullpath = os.path.join(json_dir, filename)
        else:
            fullpath = filename
            parent = os.path.dirname(fullpath)
            if parent:
                os.makedirs(parent, exist_ok=True)

        with open(fullpath, "w", encoding="utf-8") as f:
            json.dump(articles, f, indent=2, ensure_ascii=False)
        print(f"💾 Datos guardados en JSON: {fullpath}")
