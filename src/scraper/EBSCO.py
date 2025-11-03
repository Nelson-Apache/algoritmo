"""
EBSCO Academic Database Scraper
================================

Este módulo proporciona una clase para realizar web scraping de la base de datos
académica EBSCO (https://www.ebsco.com/), permitiendo extraer artículos científicos,
papers y documentación académica de manera automatizada.

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
from typing import List, Dict, Optional
from playwright.sync_api import sync_playwright
from typing import Any
import os
import random


class EBSCOScraper:
    """
    Scraper para la base de datos académica EBSCO.
    
    Esta clase proporciona métodos para autenticarse en EBSCO mediante login
    institucional, realizar búsquedas de artículos académicos y extraer sus
    metadatos completos incluyendo títulos, autores, abstracts, DOIs, etc.
    
    Attributes:
        base_url (str): URL base de la API de búsqueda de EBSCO
        session (requests.Session): Sesión HTTP para mantener cookies
        login_url (str): URL de inicio de sesión institucional
        headers (dict): Headers HTTP para las peticiones
        cookies (dict): Cookies de sesión para autenticación
        total_items (int): Número total de resultados disponibles
    
    Example:
        >>> scraper = EBSCOScraper(auto_login=True)
        >>> articles = scraper.scrape_all("machine learning", max_results=100)
        >>> scraper.save_to_csv(articles, "ml_articles.csv")
    """
    
    def __init__(self, auto_login: bool = True):
        """
        Inicializa el scraper de EBSCO.
        
        Configura la sesión HTTP, URLs, headers y opcionalmente realiza el
        login automático. Si auto_login es True, intentará cargar cookies
        existentes o iniciará un proceso de login manual si es necesario.
        
        Args:
            auto_login (bool, optional): Si es True, intenta autenticarse
                automáticamente al inicializar. Por defecto True.
        
        Raises:
            Exception: Si el auto_login falla y no se puede establecer sesión
        """
        # URL de la API de búsqueda de EBSCO
        self.base_url = (
            "https://research-ebsco-com.crai.referencistas.com/api/search/v1/search"
        )
        
        # Sesión HTTP para mantener cookies entre peticiones
        self.session = requests.Session()
        
        # URL de acceso institucional con proxy de autenticación
        self.login_url = "https://login.intelproxy.com/v2/inicio?cuenta=7Ah6RNpGWF22jjyq&url=ezp.2aHR0cHM6Ly9zZWFyY2guZWJzY29ob3N0LmNvbS9sb2dpbi5hc3B4PyZkaXJlY3Q9dHJ1ZSZzaXRlPWVkcy1saXZlJmF1dGh0eXBlPWlwJmN1c3RpZD1uczAwNDM2MyZnZW9jdXN0aWQ9Jmdyb3VwaWQ9bWFpbiZwcm9maWxlPWVkcyZicXVlcnk9Z2VuZXJhdGl2ZSthcnRpZmljaWFsK2ludGVsbGlnZW5jZQ--"
        
        # Headers HTTP que simulan un navegador real
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

        # Diccionario para almacenar cookies de sesión
        self.cookies = {}
        
        # Variable para almacenar el total de resultados disponibles
        self.total_items = None

        # Proceso de autenticación automática
        if auto_login:
            # Intentar cargar cookies existentes primero
            if not (self.load_cookies() and self.test_cookies()):
                print("Cookies no válidas o no encontradas. Iniciando login manual...")
                self.manual_login()

    def manual_login(self):
        """
        Realiza el proceso de login completamente manual.
        
        Abre un navegador Chromium donde el usuario debe completar manualmente
        el proceso de autenticación institucional. Una vez completado, extrae
        las cookies de sesión y las guarda para futuros usos.
        
        Este método es útil cuando:
        - Las cookies han expirado
        - El login automático falla
        - Se requiere autenticación de dos factores
        - Es la primera vez que se usa el scraper
        
        Process:
            1. Abre navegador Chromium (headless=False)
            2. Navega a la URL de login institucional
            3. Espera a que el usuario complete el login
            4. Extrae cookies del contexto del navegador
            5. Guarda cookies en archivo JSON
        
        Raises:
            Exception: Si hay errores durante el proceso de navegación o
                extracción de cookies
        
        Note:
            El navegador permanecerá abierto hasta que el usuario presione
            Enter en la consola, indicando que el login está completo.
        """
        print("=== LOGIN MANUAL REQUERIDO ===")
        print("Se abrirá un navegador. Por favor:")
        print("1. Completa el login manualmente")
        print("2. Navega hasta la página principal de EBSCO")
        print("3. Presiona Enter en esta consola cuando estés listo")
        
        # Iniciar Playwright para automatizar el navegador
        with sync_playwright() as p:
            # Lanzar navegador Chromium visible (headless=False)
            browser = p.chromium.launch(headless=False)
            
            # Crear contexto con user agent personalizado
            context = browser.new_context(
                user_agent=self.headers["User-Agent"]
            )
            page = context.new_page()

            try:
                # Navegar a la página de login institucional
                page.goto(self.login_url)
                
                # Esperar confirmación del usuario
                print("\nPor favor completa el login en el navegador...")
                print("Presiona Enter cuando hayas terminado y estés en EBSCO:")
                input()
                
                # Verificar que estamos en la página correcta de EBSCO
                current_url = page.url
                if "ebsco" not in current_url.lower() and "crai.referencistas" not in current_url:
                    print("Navegando a EBSCO...")
                    ebsco_url = "https://research-ebsco-com.crai.referencistas.com/"
                    page.goto(ebsco_url)
                    page.wait_for_timeout(3000)
                
                # Extraer todas las cookies del contexto del navegador
                cookies = context.cookies()
                safe_cookies: Dict[str, str] = {}
                for c in cookies:
                    name = c.get("name")
                    value = c.get("value")
                    if name and value:
                        safe_cookies[name] = value
                
                # Almacenar cookies en la instancia
                self.cookies = safe_cookies
                print(f"Cookies extraídas: {len(self.cookies)} cookies")
                
                # Guardar cookies en archivo para uso futuro
                self.save_cookies()
                
                print("✓ Login completado exitosamente")

            except Exception as e:
                print(f"Error durante el login manual: {e}")
                raise
            finally:
                # Cerrar navegador siempre, incluso si hay errores
                browser.close()

    def login_with_persistent_browser(self):
        """
        Utiliza un perfil de navegador persistente para mantener la sesión.
        
        Este método crea y utiliza un directorio de perfil de usuario para
        Chromium, lo que permite que las cookies y la sesión persistan entre
        ejecuciones. Es útil para evitar tener que hacer login repetidamente.
        
        Features:
            - Guarda el estado del navegador en disco
            - Mantiene cookies entre sesiones
            - Evita repetir el proceso de login
            - Útil para desarrollo y pruebas
        
        Process:
            1. Crea directorio './browser_profile' si no existe
            2. Lanza navegador con perfil persistente
            3. Usuario completa login (solo primera vez)
            4. Extrae y guarda cookies
            5. Sesiones futuras reutilizan el perfil
        
        Note:
            El perfil del navegador puede crecer en tamaño con el tiempo.
            Se recomienda limpiarlo periódicamente.
        
        Warning:
            No compartir el directorio browser_profile ya que contiene
            datos sensibles de sesión.
        """
        print("=== LOGIN CON PERFIL PERSISTENTE ===")
        
        # Crear directorio para almacenar el perfil del navegador
        profile_dir = "./browser_profile"
        os.makedirs(profile_dir, exist_ok=True)
        
        with sync_playwright() as p:
            # Lanzar navegador con contexto persistente
            # Esto guarda cookies, localStorage, etc. en disco
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
                
                # Navegar a EBSCO si no estamos ahí ya
                if "ebsco" not in page.url.lower():
                    page.goto("https://research-ebsco-com.crai.referencistas.com/c/rfbjy2/search")
                    page.wait_for_timeout(3000)
                
                # Extraer cookies del contexto persistente
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
        Método avanzado de login con automatización completa y fallback manual.
        
        Este método intenta automatizar completamente el proceso de login mediante
        Google SSO. Si la automatización falla, hace fallback a login manual.
        Incluye detección anti-bot y manejo de múltiples escenarios de login.
        
        Args:
            email (Optional[str]): Email de Google para login automático.
                Si no se proporciona, el usuario debe ingresar manualmente.
            password (Optional[str]): Contraseña de Google para login automático.
                Si no se proporciona, el usuario debe ingresar manualmente.
            headless (bool, optional): Si es True, ejecuta el navegador en modo
                headless (sin interfaz gráfica). Por defecto False.
        
        Features:
            - Automatización completa del flujo de login de Google
            - Detección y clic en botón de Google SSO
            - Ingreso automático de credenciales
            - Anti-detección de bots (oculta webdriver)
            - Screenshots para debugging
            - Fallback a modo manual si falla automatización
            - Múltiples selectores para mayor compatibilidad
        
        Process:
            1. Configura navegador anti-detección
            2. Navega a página de login
            3. Detecta y hace clic en botón de Google
            4. Ingresa email y password si están disponibles
            5. Espera redirección a EBSCO
            6. Extrae y guarda cookies
        
        Raises:
            Exception: Si el proceso falla completamente y no es posible
                establecer sesión ni manual ni automáticamente
        
        Note:
            El modo headless puede ser detectado por algunos sistemas anti-bot.
            Se recomienda usar headless=False para mayor confiabilidad.
        
        Example:
            >>> scraper = EBSCOScraper(auto_login=False)
            >>> scraper.login_and_get_cookies(
            ...     email="usuario@universidad.edu",
            ...     password="contraseña_segura",
            ...     headless=False
            ... )
        """
        print("Iniciando proceso de autenticación...")
        
        with sync_playwright() as p:
            # Configurar navegador con argumentos anti-detección
            browser = p.chromium.launch(
                headless=headless,
                args=[
                    '--disable-blink-features=AutomationControlled',  # Oculta que es automatizado
                    '--disable-dev-shm-usage',  # Mejora rendimiento en Linux
                    '--no-sandbox',  # Necesario en algunos entornos
                    '--disable-extensions',  # Desactiva extensiones
                    '--disable-plugins-discovery',
                    '--disable-web-security',  # Solo para testing
                    '--disable-features=VizDisplayCompositor'
                ]
            )
            
            # Crear contexto con configuración realista
            context = browser.new_context(
                user_agent=self.headers["User-Agent"],
                viewport={'width': 1920, 'height': 1080},  # Resolución común
                extra_http_headers={
                    'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
                }
            )
            
            # Inyectar script para ocultar propiedades de automatización
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
                # Esperar a que la red esté inactiva (página completamente cargada)
                page.goto(self.login_url, wait_until='networkidle')
                
                # Esperar tiempo adicional para JavaScript dinámico
                page.wait_for_timeout(5000)
                
                print("Buscando botón de Google...")
                
                # Tomar screenshot para debugging (guardar en carpeta organizada)
                screenshots_dir = os.path.join("data", "screenshots")
                os.makedirs(screenshots_dir, exist_ok=True)
                screenshot_path = os.path.join(screenshots_dir, "login_page_debug.png")
                page.screenshot(path=screenshot_path)
                print(f"Screenshot guardado como '{screenshot_path}'")
                
                # Lista exhaustiva de selectores para encontrar el botón de Google
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
                    'button:has([class*="google"])',
                    'a:has([class*="google"])',
                    'div[role="button"]:has-text("Google")',
                ]
                
                # Intentar encontrar botón de Google con múltiples selectores
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
                    # No se encontró botón de Google
                    print("❌ No se encontró botón de Google")
                    if not headless:
                        print("Cambiando a modo manual...")
                        input("Por favor, realiza el login manualmente y presiona Enter...")
                    else:
                        # Si estamos en headless, reintentar en modo visible
                        browser.close()
                        return self.manual_login()
                else:
                    # ===== LOGIN AUTOMÁTICO DE GOOGLE =====
                    print("🚀 Iniciando login automático...")
                    
                    # Hacer scroll al botón si es necesario
                    google_button.scroll_into_view_if_needed()
                    page.wait_for_timeout(1000)
                    google_button.click()
                    print("✓ Click en botón de Google")
                    
                    # Esperar redirección a Google
                    page.wait_for_timeout(3000)
                    
                    # Verificar que estamos en la página de login de Google
                    if "google" in page.url.lower() or "accounts.google.com" in page.url:
                        print("✓ Redirigido a Google")
                        
                        if email and password:
                            # === AUTOMATIZAR LOGIN COMPLETO ===
                            print("🔑 Automatizando login con credenciales...")
                            
                            try:
                                # ===== PASO 1: INGRESAR EMAIL =====
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
                                
                                # ===== PASO 2: INGRESAR CONTRASEÑA =====
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
                    # Si no llegamos automáticamente, navegar manualmente
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
                
                # Guardar cookies para uso futuro
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
        """
        Guarda las cookies de sesión en un archivo JSON.
        
        Serializa el diccionario de cookies a formato JSON y lo guarda en disco
        para poder reutilizar la sesión en ejecuciones futuras sin necesidad
        de volver a hacer login.
        
        Args:
            filename (str, optional): Nombre del archivo donde guardar las cookies.
                Por defecto "ebsco_cookies.json".
        
        Note:
            Las cookies contienen tokens de sesión sensibles. No compartir
            ni subir a repositorios públicos.
        
        Security:
            Se recomienda agregar *.json al .gitignore para evitar exponer
            las cookies en control de versiones.
        
        Example:
            >>> scraper.save_cookies("mi_sesion.json")
            Cookies guardadas en: mi_sesion.json
        """
        # Si el usuario no pasó una ruta (solo nombre de archivo), guardamos
        # en data/cookies/<filename> para mantener el directorio raíz limpio.
        if not os.path.dirname(filename):
            cookies_dir = os.path.join("data", "cookies")
            os.makedirs(cookies_dir, exist_ok=True)
            fullpath = os.path.join(cookies_dir, filename)
        else:
            # Si se pasó una ruta, respetarla (crear carpeta si es necesaria)
            fullpath = filename
            parent = os.path.dirname(fullpath)
            if parent:
                os.makedirs(parent, exist_ok=True)

        with open(fullpath, 'w', encoding='utf-8') as f:
            json.dump(self.cookies, f, indent=2)
        print(f"Cookies guardadas en: {fullpath}")

    def load_cookies(self, filename: str = "ebsco_cookies.json") -> bool:
        """
        Carga cookies de sesión desde un archivo JSON.
        
        Intenta cargar cookies previamente guardadas desde un archivo. Si el
        archivo existe y se carga correctamente, retorna True. Si no existe
        o hay algún error, retorna False.
        
        Args:
            filename (str, optional): Nombre del archivo de cookies a cargar.
                Por defecto "ebsco_cookies.json".
        
        Returns:
            bool: True si las cookies se cargaron exitosamente, False en caso contrario.
        
        Raises:
            No lanza excepciones - captura errores y retorna False.
        
        Example:
            >>> scraper = EBSCOScraper(auto_login=False)
            >>> if scraper.load_cookies():
            ...     print("Cookies cargadas, sesión restaurada")
            ... else:
            ...     print("Cookies no disponibles, hacer login")
        """
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
        """
        Verifica si las cookies actuales son válidas.
        
        Realiza una búsqueda de prueba con 1 resultado para verificar que
        las cookies de sesión siguen siendo válidas y permiten acceso a la API.
        
        Returns:
            bool: True si las cookies son válidas y permiten búsquedas,
                  False si las cookies están expiradas o son inválidas.
        
        Note:
            Este método hace una petición real a la API, por lo que consume
            una llamada de tu cuota si existe límite de rate.
        
        Example:
            >>> if not scraper.test_cookies():
            ...     print("Cookies expiradas, re-autenticando...")
            ...     scraper.manual_login()
        """
        try:
            # Hacer una búsqueda mínima de prueba
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
        """
        Obtiene el número total de resultados disponibles para una búsqueda.
        
        Realiza una búsqueda solicitando solo 1 resultado para obtener el
        contador total de items disponibles sin consumir ancho de banda
        innecesariamente.
        
        Args:
            query (str): Término o términos de búsqueda.
        
        Returns:
            int: Número total de resultados disponibles para la búsqueda.
                 Retorna 0 si hay error o no hay resultados.
        
        Note:
            Este número representa el total de documentos que coinciden con
            la búsqueda, no cuántos puedes descargar (puede haber límites).
        
        Raises:
            No lanza excepciones - captura errores y retorna 0.
        
        Example:
            >>> total = scraper.get_total_items("machine learning")
            Total de resultados disponibles para 'machine learning': 45,321
            >>> print(f"Hay {total} artículos disponibles")
        """
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
        """
        Construye el payload JSON para las peticiones a la API de EBSCO.
        
        Método privado que genera la estructura JSON necesaria para realizar
        búsquedas en la API de EBSCO, incluyendo filtros, expansores y
        parámetros de paginación.
        
        Args:
            query (str): Término de búsqueda.
            offset (int, optional): Posición de inicio para paginación (0-indexed).
                Por defecto 0.
            count (int, optional): Número de resultados a retornar por página.
                Por defecto 50.
        
        Returns:
            Dict: Diccionario con la estructura completa del payload para la API.
        
        Payload Structure:
            - advancedSearchStrategy: Tipo de estrategia de búsqueda
            - query: Término de búsqueda
            - autoCorrect: Si se debe auto-corregir ortografía
            - profileIdentifier: ID del perfil institucional
            - expanders: Lista de expansores (tesauro, conceptos)
            - filters: Filtros aplicados (texto completo, etc.)
            - searchMode: Modo de búsqueda ("all" busca todas las palabras)
            - sort: Orden de resultados (relevancia, fecha, etc.)
            - offset: Posición inicial para paginación
            - count: Número de resultados por página
            - highlightTag: Tag HTML para resaltar coincidencias
        
        Note:
            Este método es privado (prefijo _) y normalmente no debe ser
            llamado directamente por usuarios de la clase.
        """
        return {
            "advancedSearchStrategy": "NONE",  # Búsqueda simple (no avanzada)
            "query": query,  # Término de búsqueda
            "autoCorrect": False,  # No corregir automáticamente errores
            "profileIdentifier": "q46rpe",  # ID del perfil institucional
            "expanders": ["thesaurus", "concept"],  # Expandir con sinónimos y conceptos
            "filters": [
                {"id": "FT", "values": ["true"]},  # Solo texto completo (Full Text)
                {"id": "FT1", "values": ["true"]},  # Texto completo disponible
            ],
            "searchMode": "all",  # Buscar TODAS las palabras (AND)
            "sort": "relevance",  # Ordenar por relevancia
            "isNovelistEnabled": False,  # No incluir contenido de Novelist
            "includePlacards": True,  # Incluir anuncios/destacados
            "offset": offset,  # Posición inicial (paginación)
            "count": count,  # Número de resultados a retornar
            "highlightTag": "mark",  # Tag HTML para resaltar coincidencias
            "userDirectAction": False,  # No es acción directa del usuario
        }

    def search(self, query: str, offset: int = 0, count: int = 50, verbose: bool = True) -> Dict:
        """
        Realiza una búsqueda en la base de datos EBSCO.
        
        Ejecuta una petición de búsqueda a la API de EBSCO y retorna los
        resultados en formato JSON. Incluye rate limiting automático para
        evitar bloqueos por exceso de peticiones.
        
        Args:
            query (str): Término o términos de búsqueda. Puede incluir
                operadores booleanos (AND, OR, NOT) y comillas para frases.
            offset (int, optional): Posición de inicio para paginación (0-indexed).
                Por defecto 0.
            count (int, optional): Número de resultados a retornar (1-50).
                Por defecto 50.
            verbose (bool, optional): Si es True, imprime información de debug.
                Por defecto True.
        
        Returns:
            Dict: Respuesta JSON de la API con los resultados de búsqueda.
        
        Raises:
            requests.exceptions.HTTPError: Si la petición falla (401, 403, 500, etc.)
            requests.exceptions.RequestException: Para errores de red
        
        Response Structure:
            {
                "search": {
                    "totalItems": int,  # Total de resultados
                    "items": [...]      # Lista de artículos
                }
            }
        
        Example:
            >>> results = scraper.search("artificial intelligence", offset=0, count=10)
            📡 Query buscado: 'artificial intelligence'
            📡 Status code: 200
            >>> print(f"Encontrados: {results['search']['totalItems']} artículos")
        """
        # Construir payload con parámetros de búsqueda
        payload = self._build_payload(query, offset, count)
        
        # Rate limiting: pequeño delay para evitar bloqueos
        time.sleep(0.1)

        # Realizar petición POST a la API
        response = self.session.post(
            self.base_url,
            headers=self.headers,
            cookies=self.cookies,
            json=payload,
            params={
                "applyAllLimiters": "true",  # Aplicar todos los filtros
                "includeSavedItems": "false",  # No incluir items guardados
                "excludeLinkValidation": "true",  # Excluir validación de enlaces
            },
        )

        if verbose:
            print(f"📡 Query buscado: '{query}'")
            print(f"📡 Status code: {response.status_code}")

        # Lanzar excepción si hay error HTTP
        response.raise_for_status()
        return response.json()

    def extract_articles(self, data: Dict) -> List[Dict]:
        """
        Extrae y procesa metadatos de artículos desde la respuesta JSON de la API.
        
        Parsea la respuesta JSON de EBSCO y extrae información estructurada
        de cada artículo, incluyendo título, autores, abstract, DOI, enlaces
        PDF, temas, fechas, y más metadatos bibliográficos.
        
        Args:
            data (Dict): Respuesta JSON de la API de EBSCO obtenida mediante
                el método search().
        
        Returns:
            List[Dict]: Lista de diccionarios, donde cada diccionario contiene
                los metadatos completos de un artículo.
        
        Article Structure:
            {
                'id': str,                    # ID único del artículo
                'title': str,                 # Título del artículo
                'abstract': str,              # Resumen/abstract
                'authors': str,               # Autores (separados por ;)
                'publication_date': str,      # Fecha de publicación
                'journal': str,               # Nombre de la revista
                'doi': str,                   # Digital Object Identifier
                'subjects': str,              # Temas (separados por ;)
                'page_start': str,            # Página inicial
                'page_end': str,              # Página final
                'volume': str,                # Volumen de la revista
                'issue': str,                 # Número de la revista
                'publisher': str,             # Editorial
                'pdf_links': str,             # Enlaces PDF (separados por ;)
                'database': str,              # Base de datos de origen
                'peer_reviewed': bool,        # Si está revisado por pares
                'language': str,              # Idioma del documento
                'document_type': str,         # Tipo de documento
                'isbn': str,                  # ISBN (para libros)
                'issn': str,                  # ISSN (para revistas)
            }
        
        Note:
            - Los campos múltiples (autores, temas, PDFs) se unen con ";" 
            - Las etiquetas <mark> de resaltado se eliminan automáticamente
            - Los campos faltantes se rellenan con string vacío ""
        
        Example:
            >>> response = scraper.search("quantum computing", count=5)
            >>> articles = scraper.extract_articles(response)
            📄 Extrayendo 5 artículos...
            ✅ 5 artículos extraídos exitosamente
        """
        articles = []
        # Obtener lista de items de la respuesta JSON
        items = data.get("search", {}).get("items", [])
        
        print(f"📄 Extrayendo {len(items)} artículos...")
        
        for item in items:
            # Extraer y limpiar título
            title = item.get("title", {}).get("value", "")
            title = title.replace("<mark>", "").replace("</mark>", "")

            # Extraer y limpiar abstract
            abstract = item.get("abstract", {}).get("value", "")
            abstract = abstract.replace("<mark>", "").replace("</mark>", "")

            # Extraer enlaces a PDF
            pdf_links = []
            full_text_links = item.get("links", {}).get("fullTextLinks", [])
            for link in full_text_links:
                if link.get("type") == "pdfFullText":
                    pdf_links.append(link.get("url"))

            # Procesar lista de autores
            authors = []
            for contrib in item.get("contributors", []):
                author_name = contrib.get("name", "")
                if author_name:
                    authors.append(author_name)

            # Procesar lista de temas/keywords
            subjects = []
            for subj in item.get("subjects", []):
                subject_name = subj.get("name", {}).get("value", "")
                if subject_name:
                    subjects.append(subject_name)

            # Construir diccionario con todos los metadatos
            article = {
                "id": item.get("id", ""),
                "title": title,
                "abstract": abstract,
                "authors": "; ".join(authors),  # Unir lista con punto y coma
                "publication_date": item.get("publicationDate", ""),
                "journal": item.get("source", ""),
                "doi": item.get("doi", ""),
                "subjects": "; ".join(subjects),
                "page_start": item.get("pageStart", ""),
                "page_end": item.get("pageEnd", ""),
                "volume": item.get("volume", ""),
                "issue": item.get("issue", ""),
                "publisher": item.get("publisherName", ""),
                "pdf_links": "; ".join(pdf_links),
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
        """
        Realiza scraping completo de múltiples páginas de resultados.
        
        Este es el método principal para extraer grandes cantidades de artículos.
        Itera sobre todas las páginas de resultados, manejando paginación,
        rate limiting, errores de red y re-autenticación automática si es necesario.
        
        Args:
            query (str): Término de búsqueda. Puede incluir operadores booleanos
                (AND, OR, NOT) y comillas para búsqueda de frases exactas.
            max_results (Optional[int], optional): Número máximo de resultados
                a obtener. Si es None, obtiene todos los disponibles. Por defecto None.
            batch_size (int, optional): Número de resultados por petición (1-50).
                Valores más altos son más eficientes pero pueden causar timeouts.
                Por defecto 50.
            delay (float, optional): Segundos de espera entre peticiones.
                Se agrega variación aleatoria para parecer más humano.
                Por defecto 0.0.
        
        Returns:
            List[Dict]: Lista de todos los artículos extraídos con sus metadatos
                completos. Ver extract_articles() para estructura de cada artículo.
        
        Features:
            - Paginación automática
            - Verificación de cookies antes de empezar
            - Re-autenticación automática si las cookies expiran
            - Rate limiting inteligente con variación aleatoria
            - Manejo robusto de errores de red
            - Reintentos automáticos con backoff exponencial
            - Límite de errores consecutivos para evitar loops infinitos
            - Progress tracking detallado
        
        Error Handling:
            - Máximo 3 errores consecutivos antes de abortar
            - Re-autenticación automática en errores 401/403
            - Backoff exponencial: 5 seg, 10 seg, 15 seg
            - Continúa desde donde se quedó después de errores
        
        Example:
            >>> # Extraer todos los resultados disponibles
            >>> articles = scraper.scrape_all("climate change")
            
            >>> # Extraer solo los primeros 100 resultados
            >>> articles = scraper.scrape_all(
            ...     query="machine learning",
            ...     max_results=100,
            ...     batch_size=50,
            ...     delay=1.0  # 1 segundo entre peticiones
            ... )
            
            >>> # Búsqueda con operadores booleanos
            >>> articles = scraper.scrape_all(
            ...     '"artificial intelligence" AND (healthcare OR medicine)',
            ...     max_results=500
            ... )
        
        Progress Output:
            🔍 Iniciando scraping para: 'machine learning'
            ✓ Cookies válidas
            Total de resultados disponibles para 'machine learning': 45,321
            🎯 Objetivo: 100 resultados de 45,321 disponibles
            📡 Scraping offset 0 - 50 (0/100 completado)
            📄 Extrayendo 50 artículos...
            ✅ 50 artículos extraídos exitosamente
            ⏸️ Esperando 1.2 segundos...
            📡 Scraping offset 50 - 100 (50/100 completado)
            🎉 Scraping completado: 100 artículos obtenidos
        
        Warning:
            - Respetar rate limits de la institución
            - No hacer scraping masivo sin permiso
            - Considerar agregar delay entre peticiones
            - Algunas instituciones limitan el número de descargas
        """
        
        print(f"🔍 Iniciando scraping para: '{query}'")
        
        # Verificar que las cookies son válidas antes de empezar
        if not self.test_cookies():
            print("Cookies inválidas. Iniciando re-autenticación...")
            self.manual_login()

        # Obtener número total de resultados disponibles
        total_items = self.get_total_items(query)

        if total_items == 0:
            print("❌ No se encontraron resultados para la búsqueda")
            return []

        # Determinar cuántos resultados queremos obtener
        target_results = min(max_results or total_items, total_items)
        print(f"🎯 Objetivo: {target_results:,} resultados de {total_items:,} disponibles")

        # Inicializar variables de control
        all_articles = []
        offset = 0
        consecutive_errors = 0
        max_consecutive_errors = 3

        # Loop principal de scraping
        while len(all_articles) < target_results and consecutive_errors < max_consecutive_errors:
            # Calcular cuántos resultados quedan por obtener
            remaining = target_results - len(all_articles)
            current_batch_size = min(batch_size, remaining)

            print(f"📡 Scraping offset {offset:,} - {offset + current_batch_size:,} "
                  f"({len(all_articles):,}/{target_results:,} completado)")

            try:
                # Realizar búsqueda para el batch actual
                data = self.search(query, offset, current_batch_size, verbose=True)
                articles = self.extract_articles(data)

                if not articles:
                    print("❌ No se encontraron más artículos")
                    break

                # Agregar artículos a la lista completa
                all_articles.extend(articles)
                offset += current_batch_size
                consecutive_errors = 0  # Reset del contador de errores

                # Rate limiting con variación aleatoria para parecer humano
                if len(all_articles) < target_results:
                    sleep_time = delay + random.uniform(0, 1)
                    print(f"⏸️ Esperando {sleep_time:.1f} segundos...")
                    time.sleep(sleep_time)

            except requests.exceptions.RequestException as e:
                # Manejo de errores de red
                consecutive_errors += 1
                print(f"❌ Error de red ({consecutive_errors}/{max_consecutive_errors}): {e}")
                
                # Verificar si es error de autenticación
                if "401" in str(e) or "403" in str(e):
                    print("🔑 Error de autenticación. Reautenticando...")
                    self.manual_login()
                    consecutive_errors = 0  # Reset después de reautenticar
                    continue
                
                # Backoff exponencial: esperar más tiempo con cada error
                wait_time = 5 * consecutive_errors
                print(f"⏳ Esperando {wait_time} segundos antes de reintentar...")
                time.sleep(wait_time)
                
            except Exception as e:
                # Manejo de errores inesperados
                consecutive_errors += 1
                print(f"❌ Error inesperado ({consecutive_errors}/{max_consecutive_errors}): {e}")
                time.sleep(5)

        print(f"🎉 Scraping completado: {len(all_articles):,} artículos obtenidos")
        return all_articles

    def save_to_csv(self, articles: List[Dict], filename: str):
        """
        Guarda los artículos extraídos en un archivo CSV.
        
        Exporta la lista de artículos con todos sus metadatos a formato CSV,
        que puede ser abierto en Excel, Google Sheets, pandas, etc. Maneja
        automáticamente la limpieza de caracteres problemáticos y asegura
        compatibilidad con diferentes aplicaciones.
        
        Args:
            articles (List[Dict]): Lista de artículos obtenida de scrape_all()
                o extract_articles().
            filename (str): Ruta y nombre del archivo CSV a crear.
                Si no incluye extensión .csv, se recomienda agregarla.
        
        Features:
            - Detecta automáticamente todas las columnas presentes
            - Ordena columnas alfabéticamente para consistencia
            - Limpia caracteres especiales problemáticos
            - Convierte saltos de línea a espacios
            - Maneja valores None/faltantes
            - Encoding UTF-8 para caracteres internacionales
        
        CSV Structure:
            Las columnas incluirán (si están disponibles):
            - id, title, abstract, authors, publication_date
            - journal, doi, subjects, page_start, page_end
            - volume, issue, publisher, pdf_links
            - database, peer_reviewed, language
            - document_type, isbn, issn
        
        Note:
            - Los campos múltiples están separados por ";"
            - Compatible con Excel y Google Sheets
            - Usar encoding UTF-8 al abrir en Excel para ver acentos
        
        Example:
            >>> articles = scraper.scrape_all("quantum physics", max_results=50)
            >>> scraper.save_to_csv(articles, "quantum_physics_2025.csv")
            💾 Datos guardados en CSV: quantum_physics_2025.csv
            📊 Total de registros: 50
            📋 Columnas incluidas: 18
            
            >>> # También funciona con rutas completas
            >>> scraper.save_to_csv(articles, "/home/user/data/articles.csv")
        
        Opening in Excel:
            1. Abrir Excel
            2. Data > From Text/CSV
            3. Seleccionar archivo
            4. Asegurar encoding UTF-8
            5. Delimiter: Comma
        
        Reading with pandas:
            >>> import pandas as pd
            >>> df = pd.read_csv("articles.csv", encoding='utf-8')
            >>> print(df.head())
        """
        if not articles:
            print("❌ No hay artículos para guardar")
            return
            
        # Obtener todas las columnas únicas de todos los artículos
        all_columns = set()
        for article in articles:
            all_columns.update(article.keys())
        
        # Ordenar columnas alfabéticamente para consistencia
        ordered_columns = sorted(all_columns)
        
        # Preparar ruta: si el usuario solo pasa un nombre de archivo, guardarlo
        # en data/csv/<filename> para mantener el directorio raíz limpio.
        if not os.path.dirname(filename):
            csv_dir = os.path.join("data", "csv")
            os.makedirs(csv_dir, exist_ok=True)
            fullpath = os.path.join(csv_dir, filename)
        else:
            fullpath = filename
            parent = os.path.dirname(fullpath)
            if parent:
                os.makedirs(parent, exist_ok=True)

        # Escribir archivo CSV
        with open(fullpath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=ordered_columns)
            writer.writeheader()

            for article in articles:
                # Limpiar cada valor para evitar errores en CSV
                clean_article = {}
                for col in ordered_columns:
                    value = article.get(col, "")
                    # Convertir a string y eliminar caracteres problemáticos
                    clean_value = str(value).replace('\n', ' ').replace('\r', ' ')
                    clean_article[col] = clean_value

                writer.writerow(clean_article)

        print(f"Datos guardados en CSV: {fullpath}")
        print(f"Total de registros: {len(articles)}")
        print(f"Columnas incluidas: {len(ordered_columns)}")

    def save_to_json(self, articles: List[Dict], filename: str):
        """
        Guarda los artículos extraídos en un archivo JSON.
        
        Método alternativo de exportación que guarda los datos en formato JSON,
        preservando la estructura completa de los datos incluyendo listas y
        objetos anidados. Útil para procesamiento programático posterior.
        
        Args:
            articles (List[Dict]): Lista de artículos obtenida de scrape_all()
                o extract_articles().
            filename (str): Ruta y nombre del archivo JSON a crear.
                Si no incluye extensión .json, se recomienda agregarla.
        
        Features:
            - Preserva estructura completa de datos
            - Indentación de 2 espacios para legibilidad
            - Encoding UTF-8 con caracteres Unicode
            - Formato JSON estándar compatible con cualquier lenguaje
        
        JSON Structure:
            [
                {
                    "id": "...",
                    "title": "...",
                    "authors": "Author1; Author2",
                    "abstract": "...",
                    ...
                },
                ...
            ]
        
        Advantages over CSV:
            - Preserva tipos de datos (bool, null, etc.)
            - Mejor para datos anidados
            - Fácil de parsear en cualquier lenguaje
            - No necesita escapar comillas o caracteres especiales
        
        Example:
            >>> articles = scraper.scrape_all("neural networks", max_results=100)
            >>> scraper.save_to_json(articles, "neural_networks.json")
            💾 Datos guardados en JSON: neural_networks.json
            
            >>> # Leer con Python
            >>> import json
            >>> with open("neural_networks.json", 'r') as f:
            ...     data = json.load(f)
            >>> print(f"Loaded {len(data)} articles")
        
        Reading in Other Languages:
            JavaScript:
                const data = require('./articles.json');
            
            R:
                library(jsonlite)
                data <- fromJSON("articles.json")
            
            Julia:
                using JSON
                data = JSON.parsefile("articles.json")
        
        Note:
            Para datasets muy grandes (>100MB), considerar usar CSV que
            es más eficiente en espacio y puede cargarse parcialmente.
        """
        # Guardar JSON en data/json si no se especifica ruta
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
        print(f"Datos guardados en JSON: {fullpath}")

