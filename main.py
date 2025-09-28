import argparse
import os
import sys
from datetime import datetime

from basededatos import EBSCOScraper
from limepza import DataCleaner, clean_ebsco_data


def run_scraping(query: str, max_results: int | None, batch_size: int, delay: float, output_csv: str, login_mode: str):
    print("=== FASE 1: SCRAPING ===")
    auto_login = False
    scraper = EBSCOScraper(auto_login=auto_login)

    # 0. Intentar reutilizar cookies si existen (independiente del modo)
    reused = False
    if scraper.load_cookies() and scraper.test_cookies():
        print("✅ Cookies válidas reutilizadas. Saltando login.")
        reused = True
    else:
        print("⚠️ No se pudieron reutilizar cookies. Procediendo con login según modo.")

    if not reused:
        # Login unificado: por defecto solicitar correo y contraseña
        if login_mode == 'persistente':
            scraper.login_with_persistent_browser()
        elif login_mode == 'cookies':
            # Ya se intentó arriba, aquí solo forzamos credenciales si falló
            print("Cookies inválidas. Solicitando credenciales...")
            email = input("Email: ").strip()
            password = input("Contraseña: ").strip()
            scraper.login_and_get_cookies(email=email, password=password, headless=False)
        else:
            # modo por defecto: credenciales directas
            print("=== LOGIN CON CREDENCIALES ===")
            email_env = os.getenv('GOOGLE_EMAIL')
            password_env = os.getenv('GOOGLE_PASSWORD')
            if email_env and password_env:
                use_env = input("Se detectaron GOOGLE_EMAIL / GOOGLE_PASSWORD en el entorno. ¿Usarlas? (s/N): ").strip().lower() == 's'
            else:
                use_env = False
            if use_env:
                scraper.login_and_get_cookies(email=email_env, password=password_env, headless=False)
            else:
                email = input("Email: ").strip()
                password = input("Contraseña: ").strip()
                headless = input("¿Modo headless? (s/n): ").strip().lower() == 's'
                scraper.login_and_get_cookies(email=email, password=password, headless=headless)

    articles = scraper.scrape_all(
        query=query,
        max_results=max_results,
        batch_size=batch_size,
        delay=delay,
    )

    if not articles:
        print("❌ No se generó ningún artículo. Abortando limpieza.")
        return None

    scraper.save_to_csv(articles, output_csv)
    return output_csv


def run_cleaning(input_csv: str, base_name: str | None):
    print("=== FASE 2: LIMPIEZA ===")
    if not os.path.exists(input_csv):
        print(f"❌ Archivo no encontrado: {input_csv}")
        return None
    clean_file, full_file, report_file = clean_ebsco_data(input_csv, base_name)
    return clean_file, full_file, report_file


def build_arg_parser():
    p = argparse.ArgumentParser(description="Pipeline EBSCO: scraping + limpieza")
    sub = p.add_subparsers(dest='command', required=False)

    # Comando completo
    p.add_argument('-q', '--query', help='Término de búsqueda', default='generative artificial intelligence')
    p.add_argument('-m', '--max-results', type=int, help='Máximo de resultados (por defecto todos)', default=None)
    p.add_argument('-b', '--batch-size', type=int, default=50, help='Tamaño de lote por petición')
    p.add_argument('-d', '--delay', type=float, default=0.0, help='Delay base entre lotes')
    p.add_argument('-o', '--output-csv', help='Nombre CSV de salida del scraping', default=None)
    p.add_argument('--login-mode', choices=['credenciales','persistente','cookies'], default='credenciales', help='Modo de login (credenciales por defecto)')
    p.add_argument('--skip-scrape', action='store_true', help='Saltar scraping y solo limpiar CSV existente')
    p.add_argument('--input-csv', help='CSV ya existente para limpiar (si se usa --skip-scrape)')
    p.add_argument('--base-name', help='Nombre base para archivos de limpieza')
    p.add_argument('--interactive', action='store_true', help='Activar modo menú interactivo (similar a basededatos.py)')

    return p


def interactive_pipeline():
    """Ejecuta el flujo completo en modo interactivo (login + scraping + limpieza)."""
    print("=== MODO INTERACTIVO EBSCO ===")
    print("Opciones de login:")
    print("1. Credenciales (email + contraseña)")
    print("2. Perfil persistente")
    print("3. Cargar cookies existentes")

    opcion = input("Selecciona una opción (1-4): ").strip()

    scraper = EBSCOScraper(auto_login=False)

    # Intento inicial de reutilizar cookies
    if scraper.load_cookies() and scraper.test_cookies():
        print("✅ Cookies válidas reutilizadas. Saltando selección de login.")
    else:
        print("⚠️ No se pudieron reutilizar cookies. Selecciona método de login.")
        if opcion == '1':
            email = input("Email: ").strip()
            password = input("Contraseña: ").strip()
            headless = input("¿Modo headless? (s/n): ").strip().lower() == 's'
            scraper.login_and_get_cookies(email=email, password=password, headless=headless)
        elif opcion == '2':
            scraper.login_with_persistent_browser()
        elif opcion == '3':
            if not (scraper.load_cookies() and scraper.test_cookies()):
                print("Cookies inválidas. Necesitas ingresar credenciales.")
                email = input("Email: ").strip()
                password = input("Contraseña: ").strip()
                scraper.login_and_get_cookies(email=email, password=password, headless=False)
        else:
            print("Opción no válida. Usando modo credenciales...")
            email = input("Email: ").strip()
            password = input("Contraseña: ").strip()
            scraper.login_and_get_cookies(email=email, password=password, headless=False)

    # Parámetros de scraping
    print("\nConfigura tu búsqueda:")
    query = input("Término de búsqueda (Enter = generative artificial intelligence): ").strip() or "generative artificial intelligence"
    max_results_str = input("Máximo de resultados (Enter para todos): ").strip()
    max_results = int(max_results_str) if max_results_str.isdigit() else None
    batch_size_str = input("Tamaño de lote (Enter=50): ").strip() or '50'
    delay_str = input("Delay base entre lotes en segundos (Enter=0): ").strip() or '0'
    try:
        batch_size = int(batch_size_str)
    except ValueError:
        batch_size = 50
    try:
        delay = float(delay_str)
    except ValueError:
        delay = 0.0

    print(f"\n🚀 Iniciando scraping para: '{query}'")
    articles = scraper.scrape_all(
        query=query,
        max_results=max_results,
        batch_size=batch_size,
        delay=delay,
    )

    if not articles:
        print("❌ No se obtuvieron artículos. Saliendo.")
        return

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    scrape_csv = f"{query.replace(' ', '_')}_articles_{timestamp}.csv"
    scraper.save_to_csv(articles, scrape_csv)

    print("\n=== LIMPIEZA ===")
    base_name = input("Nombre base para archivos limpios (Enter para automático): ").strip() or None
    clean_file, full_file, report_file = clean_ebsco_data(scrape_csv, base_name)

    print("\n=== RESUMEN ===")
    print(f"📄 CSV scraping: {scrape_csv}")
    print(f"📄 CSV limpio: {clean_file}")
    print(f"📄 CSV completo: {full_file}")
    print(f"📝 Reporte: {report_file}")
    print("✅ Proceso completado.")


def main():
    parser = build_arg_parser()
    args = parser.parse_args()

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_csv = args.output_csv or f"scrape_{timestamp}.csv"

    # Si modo interactivo, ejecutar y salir
    if args.interactive:
        interactive_pipeline()
        return

    # 1. Scraping (si no se omite)
    if not args.skip_scrape:
        # Si el usuario dejó valores por defecto y no pasó explícitamente --query, ofrecer prompt
        # Detectar si el usuario escribió la query (mirando sys.argv)
        passed_query = any(a in ('-q', '--query') for a in sys.argv)
        passed_max = any(a in ('-m', '--max-results') for a in sys.argv)

        query = args.query
        max_results = args.max_results

        if not passed_query:
            q_input = input(f"Término de búsqueda (Enter = {query}): ").strip()
            if q_input:
                query = q_input

        if not passed_max:
            m_input = input("Máximo de resultados (Enter = todos): ").strip()
            if m_input.isdigit():
                max_results = int(m_input)

        scraped_csv = run_scraping(
            query=query,
            max_results=max_results,
            batch_size=args.batch_size,
            delay=args.delay,
            output_csv=output_csv,
            login_mode=args.login_mode,
        )
        if scraped_csv is None:
            sys.exit(1)
        input_csv = scraped_csv
    else:
        if not args.input_csv:
            print("❌ Debes proporcionar --input-csv al usar --skip-scrape")
            sys.exit(1)
        input_csv = args.input_csv

    # 2. Limpieza
    cleaning_result = run_cleaning(input_csv, args.base_name)
    if cleaning_result is None:
        sys.exit(1)
    clean_file, full_file, report_file = cleaning_result

    print("\n=== RESUMEN PIPELINE ===")
    print(f"📄 CSV scraping: {input_csv}")
    print(f"📄 CSV limpio: {clean_file}")
    print(f"📄 CSV completo: {full_file}")
    print(f"📝 Reporte: {report_file}")


if __name__ == '__main__':
    main()
