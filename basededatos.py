import requests
import json
import time
import csv
import pandas as pd
from typing import List, Dict, Optional
from playwright.sync_api import sync_playwright
from typing import Any
import os
import random


class EBSCOScraper:
    def __init__(self, auto_login: bool = True):
        self.base_url = (
            "https://research-ebsco-com.crai.referencistas.com/api/search/v1/search"
        )
        self.session = requests.Session()
        self.login_url = "https://login.intelproxy.com/v2/inicio?cuenta=7Ah6RNpGWF22jjyq&url=ezp.2aHR0cHM6Ly9zZWFyY2guZWJzY29ob3N0LmNvbS9sb2dpbi5hc3B4PyZkaXJlY3Q9dHJ1ZSZzaXRlPWVkcy1saXZlJmF1dGh0eXBlPWlwJmN1c3RpZD1uczAwNDM2MyZnZW9jdXN0aWQ9Jmdyb3VwaWQ9bWFpbiZwcm9maWxlPWVkcyZicXVlcnk9Z2VuZXJhdGl2ZSthcnRpZmljaWFsK2ludGVsbGlnZW5jZQ--"
        
        self.headers = {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "Origin": "https://research-ebsco-com.crai.referencistas.com",
            "Referer": "https://research-ebsco-com.crai.referencistas.com/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "accept-language": "es;q=0.9, es-419;q=0.8, es;q=0.7",
            "sec-ch-ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "txn-route": "true",
            "x-eis-gateway-referrer-from-ui": "same-site",
            "x-initiated-by": "refresh",
        }

        self.cookies = {}
        self.total_items = None

        if auto_login:
            # Intentar cargar cookies existentes primero
            if not (self.load_cookies() and self.test_cookies()):
                print("Cookies no válidas o no encontradas. Iniciando login manual...")
                self.manual_login()

    def manual_login(self):
        """
        Login completamente manual - el usuario hace todo en el navegador
        """
        print("=== LOGIN MANUAL REQUERIDO ===")
        print("Se abrirá un navegador. Por favor:")
        print("1. Completa el login manualmente")
        print("2. Navega hasta la página principal de EBSCO")
        print("3. Presiona Enter en esta consola cuando estés listo")
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            context = browser.new_context(
                user_agent=self.headers["User-Agent"]
            )
            page = context.new_page()

            try:
                # Navegar a la página de login
                page.goto(self.login_url)
                
                print("\nPor favor completa el login en el navegador...")
                print("Presiona Enter cuando hayas terminado y estés en EBSCO:")
                input()
                
                # Verificar que estamos en EBSCO
                current_url = page.url
                if "ebsco" not in current_url.lower() and "crai.referencistas" not in current_url:
                    print("Navegando a EBSCO...")
                    ebsco_url = "https://research-ebsco-com.crai.referencistas.com/"
                    page.goto(ebsco_url)
                    page.wait_for_timeout(3000)
                
                # Extraer cookies
                cookies = context.cookies()
                safe_cookies: Dict[str, str] = {}
                for c in cookies:
                    name = c.get("name")
                    value = c.get("value")
                    if name and value:
                        safe_cookies[name] = value
                
                self.cookies = safe_cookies
                print(f"Cookies extraídas: {len(self.cookies)} cookies")
                
                # Guardar cookies
                self.save_cookies()
                
                print("✓ Login completado exitosamente")

            except Exception as e:
                print(f"Error durante el login manual: {e}")
                raise
            finally:
                browser.close()

    def login_with_persistent_browser(self):
        """
        Usa un perfil de navegador persistente para mantener la sesión
        """
        print("=== LOGIN CON PERFIL PERSISTENTE ===")
        
        # Crear directorio para el perfil del navegador
        profile_dir = "./browser_profile"
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
                
                # Navegar a EBSCO si no estamos ahí
                if "ebsco" not in page.url.lower():
                    page.goto("https://research-ebsco-com.crai.referencistas.com/")
                    page.wait_for_timeout(3000)
                
                # Extraer cookies
                cookies = browser.cookies()
                safe_cookies: Dict[str, str] = {}
                for c in cookies:
                    name = c.get("name")
                    value = c.get("value")
                    if name and value:
                        safe_cookies[name] = value
                
                self.cookies = safe_cookies
                self.save_cookies()
                
                print("✓ Login con perfil persistente completado")

            except Exception as e:
                print(f"Error con perfil persistente: {e}")
                raise
            finally:
                browser.close()

    def login_and_get_cookies(self, email: Optional[str] = None, password: Optional[str] = None, headless: bool = False):
        """
        Método mejorado con login automático completo y fallback manual
        """
        print("Iniciando proceso de autenticación...")
        
        with sync_playwright() as p:
            # Usar argumentos adicionales para evitar detección
            browser = p.chromium.launch(
                headless=headless,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-sandbox',
                    '--disable-extensions',
                    '--disable-plugins-discovery',
                    '--disable-web-security',
                    '--disable-features=VizDisplayCompositor'
                ]
            )
            
            context = browser.new_context(
                user_agent=self.headers["User-Agent"],
                viewport={'width': 1920, 'height': 1080},
                extra_http_headers={
                    'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
                }
            )
            
            # Ocultar las propiedades de automatización
            context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined,
                });
                window.chrome = {
                    runtime: {}
                };
            """)
            
            page = context.new_page()

            try:
                print("Navegando a la página de login...")
                page.goto(self.login_url, wait_until='networkidle')
                
                # Esperar más tiempo para que cargue completamente
                page.wait_for_timeout(5000)
                
                print("Buscando botón de Google...")
                
                # Tomar screenshot para debug
                page.screenshot(path="login_page_debug.png")
                print("Screenshot guardado como 'login_page_debug.png'")
                
                # Buscar botón de Google con selectores mejorados
                google_selectors = [
                    'button:has-text("Google")',
                    'a:has-text("Google")',
                    'button:has-text("Gmail")',
                    'a:has-text("Gmail")',
                    '[data-provider="google"]',
                    '.google-login',
                    '#google-login',
                    'button[title*="Google"]',
                    'a[href*="google"]',
                    'button[class*="google"]',
                    'a[class*="google"]',
                    # Selectores más específicos
                    'button:has([class*="google"])',
                    'a:has([class*="google"])',
                    'div[role="button"]:has-text("Google")',
                ]
                
                google_button = None
                for selector in google_selectors:
                    try:
                        element = page.wait_for_selector(selector, timeout=3000)
                        if element and element.is_visible():
                            print(f"✓ Botón de Google encontrado: {selector}")
                            google_button = element
                            break
                    except:
                        continue
                
                if not google_button:
                    print("❌ No se encontró botón de Google")
                    if not headless:
                        print("Cambiando a modo manual...")
                        input("Por favor, realiza el login manualmente y presiona Enter...")
                    else:
                        browser.close()
                        return self.manual_login()
                else:
                    # ===== LOGIN AUTOMÁTICO DE GOOGLE =====
                    print("🚀 Iniciando login automático...")
                    
                    # Hacer clic en botón de Google
                    google_button.scroll_into_view_if_needed()
                    page.wait_for_timeout(1000)
                    google_button.click()
                    print("✓ Click en botón de Google")
                    
                    # Esperar redirección a Google
                    page.wait_for_timeout(3000)
                    
                    if "google" in page.url.lower() or "accounts.google.com" in page.url:
                        print("✓ Redirigido a Google")
                        
                        if email and password:
                            # === AUTOMATIZAR LOGIN COMPLETO ===
                            print("🔑 Automatizando login con credenciales...")
                            
                            try:
                                # Paso 1: Ingresar email
                                print("Ingresando email...")
                                email_selectors = [
                                    'input[type="email"]',
                                    'input[name="identifier"]',
                                    'input[id="identifierId"]',
                                    '#Email',
                                    'input[aria-label*="email"]',
                                    'input[aria-label*="correo"]'
                                ]
                                
                                email_input = None
                                for selector in email_selectors:
                                    try:
                                        email_input = page.wait_for_selector(selector, timeout=5000)
                                        if email_input and email_input.is_visible():
                                            print(f"✓ Campo email encontrado: {selector}")
                                            break
                                    except:
                                        continue
                                
                                if email_input:
                                    # Limpiar campo y escribir email
                                    email_input.click()
                                    page.keyboard.press("Control+a")
                                    email_input.fill(email)
                                    page.wait_for_timeout(1000)
                                    
                                    # Buscar botón "Siguiente"
                                    next_selectors = [
                                        'button:has-text("Next")',
                                        'button:has-text("Siguiente")',
                                        'input[type="submit"]',
                                        '#identifierNext',
                                        'button[id*="next"]',
                                        'button[class*="next"]'
                                    ]
                                    
                                    next_button = None
                                    for selector in next_selectors:
                                        try:
                                            next_button = page.wait_for_selector(selector, timeout=3000)
                                            if next_button and next_button.is_visible():
                                                print(f"✓ Botón siguiente encontrado: {selector}")
                                                break
                                        except:
                                            continue
                                    
                                    if next_button:
                                        next_button.click()
                                        print("✓ Email enviado")
                                    else:
                                        # Fallback: presionar Enter
                                        page.keyboard.press("Enter")
                                        print("✓ Enter presionado para email")
                                    
                                    page.wait_for_timeout(3000)
                                else:
                                    raise Exception("No se encontró campo de email")
                                
                                # Paso 2: Ingresar contraseña
                                print("Esperando campo de contraseña...")
                                password_selectors = [
                                    'input[type="password"]',
                                    'input[name="password"]',
                                    'input[aria-label*="password"]',
                                    'input[aria-label*="contraseña"]',
                                    '#password',
                                    'input[name="Passwd"]'
                                ]
                                
                                password_input = None
                                for selector in password_selectors:
                                    try:
                                        password_input = page.wait_for_selector(selector, timeout=10000)
                                        if password_input and password_input.is_visible():
                                            print(f"✓ Campo contraseña encontrado: {selector}")
                                            break
                                    except:
                                        continue
                                
                                if password_input:
                                    # Escribir contraseña
                                    password_input.click()
                                    password_input.fill(password)
                                    page.wait_for_timeout(1000)
                                    
                                    # Buscar botón para enviar contraseña
                                    login_selectors = [
                                        'button:has-text("Next")',
                                        'button:has-text("Siguiente")',
                                        'button:has-text("Sign in")',
                                        'button:has-text("Iniciar sesión")',
                                        'input[type="submit"]',
                                        '#passwordNext',
                                        'button[id*="next"]'
                                    ]
                                    
                                    login_button = None
                                    for selector in login_selectors:
                                        try:
                                            login_button = page.wait_for_selector(selector, timeout=3000)
                                            if login_button and login_button.is_visible():
                                                print(f"✓ Botón login encontrado: {selector}")
                                                break
                                        except:
                                            continue
                                    
                                    if login_button:
                                        login_button.click()
                                        print("✓ Contraseña enviada")
                                    else:
                                        page.keyboard.press("Enter")
                                        print("✓ Enter presionado para contraseña")
                                    
                                    print("⏳ Esperando completar autenticación...")
                                    page.wait_for_timeout(5000)
                                    
                                else:
                                    raise Exception("No se encontró campo de contraseña")
                                
                            except Exception as e:
                                print(f"❌ Error en login automático: {e}")
                                if not headless:
                                    print("🔄 Cambiando a modo manual...")
                                    input("Completa el login manualmente y presiona Enter...")
                                else:
                                    raise
                        else:
                            # Sin credenciales - modo manual
                            print("📝 Sin credenciales - completar manualmente...")
                            if not headless:
                                input("Por favor completa el login de Google y presiona Enter...")
                            else:
                                browser.close()
                                return self.login_and_get_cookies(email, password, headless=False)
                    else:
                        print("❌ No se redirigió a Google correctamente")
                        if not headless:
                            input("Por favor completa el login manualmente y presiona Enter...")
                
                # ===== VERIFICAR LLEGADA A EBSCO =====
                print("🔍 Esperando llegada a EBSCO...")
                for i in range(30):  # 30 segundos máximo
                    current_url = page.url
                    if "ebsco" in current_url.lower() or "crai.referencistas" in current_url:
                        print(f"✅ Llegamos a EBSCO: {current_url}")
                        break
                    page.wait_for_timeout(1000)
                    if i % 5 == 0:
                        print(f"⏳ Esperando... URL actual: {current_url[:100]}...")
                else:
                    print("⚠️ No llegamos a EBSCO automáticamente, intentando navegar...")
                    try:
                        page.goto("https://research-ebsco-com.crai.referencistas.com/")
                        page.wait_for_timeout(3000)
                    except:
                        pass
                
                # ===== EXTRAER COOKIES =====
                cookies = context.cookies()
                safe_cookies: Dict[str, str] = {}
                for c in cookies:
                    name = c.get("name")
                    value = c.get("value")
                    if name and value:
                        safe_cookies[name] = value
                
                self.cookies = safe_cookies
                print(f"🍪 {len(self.cookies)} cookies extraídas")
                
                # Guardar cookies
                self.save_cookies()
                
                print("🎉 Login completado exitosamente!")

            except Exception as e:
                print(f"❌ Error durante el login: {e}")
                if not headless:
                    print("🔄 Fallback a modo manual...")
                    input("Por favor completa el login manualmente y presiona Enter...")
                    
                    # Extraer cookies después del login manual
                    try:
                        cookies = context.cookies()
                        safe_cookies: Dict[str, str] = {}
                        for c in cookies:
                            name = c.get("name")
                            value = c.get("value")
                            if name and value:
                                safe_cookies[name] = value
                        
                        self.cookies = safe_cookies
                        self.save_cookies()
                        print(f"🍪 {len(self.cookies)} cookies guardadas desde login manual")
                    except:
                        pass
                else:
                    raise
            finally:
                browser.close()

    def save_cookies(self, filename: str = "ebsco_cookies.json"):
        """Guarda las cookies en un archivo JSON"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.cookies, f, indent=2)
        print(f"Cookies guardadas en: {filename}")

    def load_cookies(self, filename: str = "ebsco_cookies.json") -> bool:
        """Carga cookies desde un archivo JSON"""
        try:
            if os.path.exists(filename):
                with open(filename, 'r', encoding='utf-8') as f:
                    self.cookies = json.load(f)
                print(f"Cookies cargadas desde: {filename}")
                return True
            else:
                print(f"Archivo de cookies no encontrado: {filename}")
                return False
        except Exception as e:
            print(f"Error cargando cookies: {e}")
            return False

    def test_cookies(self) -> bool:
        """Prueba si las cookies actuales funcionan"""
        try:
            test_data = self.search("artificial intelligence", offset=0, count=1, verbose=False)
            is_valid = test_data.get('totalItems', 0) >= 0
            if is_valid:
                print("✓ Cookies válidas")
            else:
                print("✗ Cookies inválidas")
            return is_valid
        except Exception as e:
            print(f"✗ Cookies no válidas: {e}")
            return False

    def get_total_items(self, query: str) -> int:
        """Obtiene el número total de resultados disponibles"""
        payload = self._build_payload(query, offset=0, count=1)

        try:
            response = self.session.post(
                self.base_url,
                headers=self.headers,
                cookies=self.cookies,
                json=payload,
                params={
                    "applyAllLimiters": "true",
                    "includeSavedItems": "false",
                    "excludeLinkValidation": "true",
                },
            )
            response.raise_for_status()
            
            data = response.json()
            total = data.get("search", {}).get("totalItems", 0)
            print(f"Total de resultados disponibles para '{query}': {total:,}")
            return total

        except Exception as e:
            print(f"Error obteniendo total de items: {e}")
            if "401" in str(e) or "403" in str(e):
                print("Posible problema de autenticación. Cookies pueden haber expirado.")
            return 0

    def _build_payload(self, query: str, offset: int = 0, count: int = 50) -> Dict:
        """Construye el payload para la petición"""
        return {
            "advancedSearchStrategy": "NONE",
            "query": query,
            "autoCorrect": False,
            "profileIdentifier": "q46rpe",
            "expanders": ["thesaurus", "concept"],
            "filters": [
                {"id": "FT", "values": ["true"]},
                {"id": "FT1", "values": ["true"]},
            ],
            "searchMode": "all",
            "sort": "relevance",
            "isNovelistEnabled": False,
            "includePlacards": True,
            "offset": offset,
            "count": count,
            "highlightTag": "mark",
            "userDirectAction": False,
        }

    def search(self, query: str, offset: int = 0, count: int = 50, verbose: bool = True) -> Dict:
        """Realiza una búsqueda específica"""
        payload = self._build_payload(query, offset, count)
        
        # Añadir delay aleatorio para evitar detección
        time.sleep(0.1)

        response = self.session.post(
            self.base_url,
            headers=self.headers,
            cookies=self.cookies,
            json=payload,
            params={
                "applyAllLimiters": "true",
                "includeSavedItems": "false",
                "excludeLinkValidation": "true",
            },
        )

        if verbose:
            print(f"📡 Query buscado: '{query}'")
            print(f"📡 Status code: {response.status_code}")

        response.raise_for_status()
        return response.json()

    def extract_articles(self, data: Dict) -> List[Dict]:
        """Extrae artículos de la respuesta JSON"""
        articles = []
        items = data.get("search", {}).get("items", [])
        
        print(f"📄 Extrayendo {len(items)} artículos...")
        
        for item in items:
            title = item.get("title", {}).get("value", "")
            title = title.replace("<mark>", "").replace("</mark>", "")

            abstract = item.get("abstract", {}).get("value", "")
            abstract = abstract.replace("<mark>", "").replace("</mark>", "")

            # Extraer enlaces PDF
            pdf_links = []
            full_text_links = item.get("links", {}).get("fullTextLinks", [])
            for link in full_text_links:
                if link.get("type") == "pdfFullText":
                    pdf_links.append(link.get("url"))

            # Procesar autores
            authors = []
            for contrib in item.get("contributors", []):
                author_name = contrib.get("name", "")
                if author_name:
                    authors.append(author_name)

            # Procesar temas
            subjects = []
            for subj in item.get("subjects", []):
                subject_name = subj.get("name", {}).get("value", "")
                if subject_name:
                    subjects.append(subject_name)

            article = {
                "id": item.get("id", ""),
                "title": title,
                "abstract": abstract,
                "authors": "; ".join(authors),  # Convertir lista a string para CSV
                "publication_date": item.get("publicationDate", ""),
                "journal": item.get("source", ""),
                "doi": item.get("doi", ""),
                "subjects": "; ".join(subjects),  # Convertir lista a string para CSV
                "page_start": item.get("pageStart", ""),
                "page_end": item.get("pageEnd", ""),
                "volume": item.get("volume", ""),
                "issue": item.get("issue", ""),
                "publisher": item.get("publisherName", ""),
                "pdf_links": "; ".join(pdf_links),  # Convertir lista a string para CSV
                "database": item.get("longDBName", ""),
                "peer_reviewed": item.get("peerReviewed", False),
                "language": item.get("language", ""),
                "document_type": item.get("documentType", ""),
                "isbn": item.get("isbn", ""),
                "issn": item.get("issn", ""),
            }
            articles.append(article)
            
        print(f"✅ {len(articles)} artículos extraídos exitosamente")
        return articles

    def scrape_all(
        self,
        query: str,
        max_results: Optional[int] = None,
        batch_size: int = 50,
        delay: float = 0.0,
    ) -> List[Dict]:
        """Scraping completo con mejor manejo de errores"""
        
        print(f"🔍 Iniciando scraping para: '{query}'")
        
        # Verificar cookies
        if not self.test_cookies():
            print("Cookies inválidas. Iniciando re-autenticación...")
            self.manual_login()

        # Obtener total con el query real
        total_items = self.get_total_items(query)

        if total_items == 0:
            print("❌ No se encontraron resultados para la búsqueda")
            return []

        target_results = min(max_results or total_items, total_items)
        print(f"🎯 Objetivo: {target_results:,} resultados de {total_items:,} disponibles")

        all_articles = []
        offset = 0
        consecutive_errors = 0
        max_consecutive_errors = 3

        while len(all_articles) < target_results and consecutive_errors < max_consecutive_errors:
            remaining = target_results - len(all_articles)
            current_batch_size = min(batch_size, remaining)

            print(f"📡 Scraping offset {offset:,} - {offset + current_batch_size:,} "
                  f"({len(all_articles):,}/{target_results:,} completado)")

            try:
                data = self.search(query, offset, current_batch_size, verbose=True)
                articles = self.extract_articles(data)

                if not articles:
                    print("❌ No se encontraron más artículos")
                    break

                all_articles.extend(articles)
                offset += current_batch_size
                consecutive_errors = 0  # Reset error counter

                # Rate limiting con variación
                if len(all_articles) < target_results:
                    sleep_time = delay + random.uniform(0, 1)
                    print(f"⏸️ Esperando {sleep_time:.1f} segundos...")
                    time.sleep(sleep_time)

            except requests.exceptions.RequestException as e:
                consecutive_errors += 1
                print(f"❌ Error de red ({consecutive_errors}/{max_consecutive_errors}): {e}")
                
                if "401" in str(e) or "403" in str(e):
                    print("🔑 Error de autenticación. Reautenticando...")
                    self.manual_login()
                    consecutive_errors = 0  # Reset después de reautenticar
                    continue
                
                # Esperar más tiempo antes de reintentar
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
        """Guarda los artículos en un archivo CSV"""
        if not articles:
            print("❌ No hay artículos para guardar")
            return
            
        # Obtener todas las columnas únicas
        all_columns = set()
        for article in articles:
            all_columns.update(article.keys())
        
        # Ordenar columnas para consistencia
        ordered_columns = sorted(all_columns)
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=ordered_columns)
            writer.writeheader()
            
            for article in articles:
                # Asegurar que todos los valores sean strings para evitar errores
                clean_article = {}
                for col in ordered_columns:
                    value = article.get(col, "")
                    # Convertir a string y limpiar caracteres problemáticos
                    clean_value = str(value).replace('\n', ' ').replace('\r', ' ')
                    clean_article[col] = clean_value
                
                writer.writerow(clean_article)
        
        print(f"💾 Datos guardados en CSV: {filename}")
        print(f"📊 Total de registros: {len(articles)}")
        print(f"📋 Columnas incluidas: {len(ordered_columns)}")

    # Mantener método JSON como respaldo
    def save_to_json(self, articles: List[Dict], filename: str):
        """Guarda los artículos en un archivo JSON (método alternativo)"""
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(articles, f, indent=2, ensure_ascii=False)
        print(f"💾 Datos guardados en JSON: {filename}")
