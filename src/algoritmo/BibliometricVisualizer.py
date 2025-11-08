"""
BibliometricVisualizer
======================
Requerimiento 5 - Proyecto Análisis de Algoritmos (2025-2)
Universidad del Quindío

Genera visualizaciones bibliométricas:
1. 🌍 Mapa de calor por país del primer autor.
2. ☁️ Nube de palabras (abstracts + keywords).
3. 📈 Línea temporal (por año y revista).
4. 📄 Exportación a PDF con las tres figuras.

INTEGRACIÓN CON GUI:
- Se ejecuta automáticamente al finalizar el pipeline completo
- Usa el CSV unificado generado por el sistema
- Genera PDF con visualizaciones geográficas y temporales
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import tempfile
import time
import folium
import seaborn as sns
from reportlab.lib.pagesizes import letter, A4
from reportlab.platypus import SimpleDocTemplate, Image, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader
from datetime import datetime
import requests
import re
import pycountry


class BibliometricVisualizer:
    """
    Genera visualizaciones y exporta PDF de la producción científica.
    """
    def __init__(self, csv_path: str, output_pdf: str, geonames_username: str = None):
        self.csv_path = csv_path
        self.output_pdf = output_pdf
        self.df = pd.read_csv(csv_path, encoding='utf-8')
        self._country_cache = {}
        self.geonames_username = geonames_username  # Opcional: registrarse en geonames.org
        
    def normalize_country(self, name):
        """
        Normaliza nombres de países a formato estándar usando pycountry.
        """
        if not name:
            return None
        name = str(name).strip()
        
        # Diccionario de aliases comunes
        aliases = {
            "Estados Unidos": "United States",
            "EEUU": "United States",
            "USA": "United States",
            "Reino Unido": "United Kingdom",
            "UK": "United Kingdom",
            "Alemania": "Germany",
            "España": "Spain",
            "México": "Mexico",
            "Brasil": "Brazil",
            "Japón": "Japan",
            "Corea del Sur": "South Korea",
            "Países Bajos": "Netherlands",
            "Holanda": "Netherlands",
            "Federación de Rusia": "Russia",
            "República Popular China": "China",
            "Irán": "Iran",
            "Viet Nam": "Vietnam",
            "United States of America": "United States",
            "Russian Federation": "Russia",
            "Republic of Korea": "South Korea",
            "People's Republic of China": "China",
            "Czech Republic": "Czechia",
            "República Checa": "Czechia",
        }
        
        name = aliases.get(name, name)
        
        try:
            match = pycountry.countries.lookup(name)
            return match.name
        except:
            return name

    def get_author_country_wikidata(self, author_name):
        """
        Busca el país del autor usando Wikidata API (SPARQL).
        Estrategia 1: Búsqueda más confiable.
        """
        if not author_name:
            return None
            
        try:
            # SPARQL query para buscar la nacionalidad o país de trabajo
            query = f"""
            SELECT ?countryLabel WHERE {{
              ?person ?label "{author_name}"@en .
              {{ ?person wdt:P27 ?country . }}  # P27 = country of citizenship
              UNION
              {{ ?person wdt:P937 ?country . }}  # P937 = work location
              UNION
              {{ ?person wdt:P108 ?employer . ?employer wdt:P17 ?country . }}  # P108 = employer, P17 = country
              SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
            }}
            LIMIT 1
            """
            
            url = "https://query.wikidata.org/sparql"
            headers = {'User-Agent': 'BibliometricVisualizer/1.0'}
            response = requests.get(
                url, 
                params={'query': query, 'format': 'json'},
                headers=headers,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                bindings = data.get('results', {}).get('bindings', [])
                if bindings:
                    country = bindings[0].get('countryLabel', {}).get('value')
                    return self.normalize_country(country)
        except Exception as e:
            print(f"Error en Wikidata para {author_name}: {e}")
        
        return None

    def get_author_country_crossref(self, author_name):
        """
        Busca afiliaciones del autor usando CrossRef API.
        Estrategia 2: Basada en publicaciones académicas.
        """
        if not author_name:
            return None
            
        try:
            url = "https://api.crossref.org/works"
            params = {
                'query.author': author_name,
                'rows': 5,
                'select': 'author'
            }
            headers = {'User-Agent': 'BibliometricVisualizer/1.0 (mailto:example@university.edu)'}
            
            response = requests.get(url, params=params, headers=headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                items = data.get('message', {}).get('items', [])
                
                for item in items:
                    authors = item.get('author', [])
                    for author in authors:
                        if author_name.lower() in author.get('given', '').lower() + ' ' + author.get('family', '').lower():
                            affiliation = author.get('affiliation', [])
                            if affiliation:
                                aff_name = affiliation[0].get('name', '')
                                # Extraer país de la afiliación
                                country = self._extract_country_from_text(aff_name)
                                if country:
                                    return country
        except Exception as e:
            print(f"Error en CrossRef para {author_name}: {e}")
        
        return None

    def get_author_country_orcid(self, author_name):
        """
        Busca información del autor usando ORCID API.
        Estrategia 3: Base de datos de investigadores.
        """
        if not author_name:
            return None
            
        try:
            # Buscar ORCID del autor
            search_url = "https://pub.orcid.org/v3.0/search/"
            params = {'q': f'family-name:{author_name.split()[-1]} AND given-names:{author_name.split()[0]}'}
            headers = {'Accept': 'application/json'}
            
            response = requests.get(search_url, params=params, headers=headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                results = data.get('result', [])
                
                if results:
                    orcid_id = results[0].get('orcid-identifier', {}).get('path')
                    
                    # Obtener detalles del perfil
                    profile_url = f"https://pub.orcid.org/v3.0/{orcid_id}/person"
                    profile_response = requests.get(profile_url, headers=headers, timeout=10)
                    
                    if profile_response.status_code == 200:
                        profile_data = profile_response.json()
                        addresses = profile_data.get('addresses', {}).get('address', [])
                        
                        if addresses:
                            country_code = addresses[0].get('country', {}).get('value')
                            if country_code:
                                try:
                                    country = pycountry.countries.get(alpha_2=country_code)
                                    return country.name
                                except:
                                    pass
        except Exception as e:
            print(f"Error en ORCID para {author_name}: {e}")
        
        return None

    def _extract_country_from_text(self, text):
        """
        Extrae nombre de país de un texto (afiliación, biografía, etc).
        """
        if not text:
            return None
            
        text = str(text)
        
        # Buscar coincidencias con países conocidos
        for country in pycountry.countries:
            if country.name.lower() in text.lower():
                return country.name
            # Buscar también por código alpha-2
            if hasattr(country, 'alpha_2') and country.alpha_2.lower() in text.lower():
                return country.name
        
        return None

    def _validate_country_for_name(self, author_name, country):
        """
        Valida si el país tiene sentido para el nombre del autor.
        Detecta inconsistencias obvias (ej: nombre chino con país latinoamericano).
        """
        if not author_name or not country:
            return True
        
        author_lower = author_name.lower()
        
        # Patrones de nombres por región
        asian_patterns = {
            'chinese': ['wang', 'li', 'zhang', 'liu', 'chen', 'yang', 'huang', 'zhao', 'wu', 'zhou', 
                       'xu', 'sun', 'ma', 'zhu', 'hu', 'guo', 'he', 'gao', 'lin', 'xiao', 'wei', 'niu'],
            'japanese': ['sato', 'suzuki', 'takahashi', 'tanaka', 'watanabe', 'ito', 'yamamoto', 'nakamura'],
            'korean': ['kim', 'lee', 'park', 'choi', 'jung', 'kang', 'cho', 'yoon', 'jang'],
            'indian': ['kumar', 'singh', 'sharma', 'patel', 'gupta', 'reddy', 'krishnan', 'rao', 'chadhari']
        }
        
        # Regiones esperadas por patrón de nombre
        expected_regions = {
            'chinese': ['China', 'Taiwan', 'Hong Kong', 'Singapore', 'Macao'],
            'japanese': ['Japan'],
            'korean': ['South Korea', 'Korea', 'North Korea'],
            'indian': ['India', 'Pakistan', 'Bangladesh', 'Nepal', 'Sri Lanka']
        }
        
        # Verificar si hay un patrón asiático detectado
        for origin, patterns in asian_patterns.items():
            if any(pattern in author_lower for pattern in patterns):
                # Si es asiático pero el país no es de esa región
                if country not in expected_regions.get(origin, []):
                    # Países que tienen comunidades asiáticas grandes (permitir)
                    if country not in ['United States', 'United Kingdom', 'Canada', 'Australia', 'Singapore']:
                        print(f"⚠️  Inconsistencia detectada: {author_name} → {country} (parece {origin})")
                        return False
        
        return True

    def get_author_location(self, author_name):
        """
        Busca el país del autor usando múltiples APIs en cascada.
        Estrategias en orden de prioridad:
        1. Wikidata (más precisa para académicos conocidos)
        2. ORCID (base de datos de investigadores)
        3. CrossRef (publicaciones académicas)
        4. Validación de coherencia nombre-país
        """
        if not author_name:
            return None
            
        # Verificar caché
        if author_name in self._country_cache:
            return self._country_cache[author_name]
        
        # Intentar múltiples estrategias
        strategies = [
            self.get_author_country_wikidata,
            self.get_author_country_orcid,
            self.get_author_country_crossref
        ]
        
        for strategy in strategies:
            country = strategy(author_name)
            if country:
                # Validar si el país tiene sentido para el nombre
                if self._validate_country_for_name(author_name, country):
                    self._country_cache[author_name] = country
                    print(f"✓ {author_name} → {country}")
                    return country
                else:
                    # Si no pasa validación, intentar siguiente estrategia
                    continue
            time.sleep(0.5)  # Respetar rate limits
        
        # Si todas las APIs fallan, intentar inferir desde el nombre
        inferred_country = self._infer_country_from_name(author_name)
        if inferred_country:
            self._country_cache[author_name] = inferred_country
            print(f"🔍 {author_name} → {inferred_country} (inferido)")
            return inferred_country
        
        # Si no se encuentra, guardar en caché como None
        self._country_cache[author_name] = None
        print(f"✗ {author_name} → No encontrado")
        return None
    
    def _infer_country_from_name(self, author_name):
        """
        Infiere el país más probable basándose en el nombre del autor.
        Usa patrones lingüísticos y culturales.
        """
        if not author_name:
            return None
        
        name_lower = author_name.lower()
        
        # Apellidos chinos comunes
        chinese_surnames = ['wang', 'li', 'zhang', 'liu', 'chen', 'yang', 'huang', 'zhao', 
                          'wu', 'zhou', 'xu', 'sun', 'ma', 'zhu', 'hu', 'guo', 'he', 
                          'gao', 'lin', 'xiao', 'wei', 'niu', 'song', 'qian', 'feng']
        
        # Nombres japoneses
        japanese_names = ['sato', 'suzuki', 'takahashi', 'tanaka', 'watanabe', 'ito', 
                         'yamamoto', 'nakamura', 'kobayashi', 'kato', 'yoshida']
        
        # Nombres coreanos
        korean_names = ['kim', 'lee', 'park', 'choi', 'jung', 'kang', 'cho', 'yoon', 'jang']
        
        # Nombres indios
        indian_names = ['kumar', 'singh', 'sharma', 'patel', 'gupta', 'reddy', 'krishnan', 
                       'rao', 'chadhari', 'desai', 'joshi', 'iyer', 'bansal', 'harika']
        
        # Nombres árabes/persas
        arabic_names = ['mohammed', 'ahmad', 'hassan', 'ali', 'hussein', 'abdullah', 'omar']
        
        # Nombres hispanos (apellidos comunes)
        hispanic_names = ['garcia', 'rodriguez', 'martinez', 'lopez', 'gonzalez', 'hernandez',
                         'perez', 'sanchez', 'ramirez', 'torres', 'flores', 'rivera']
        
        # Detectar patrones
        for surname in chinese_surnames:
            if surname in name_lower:
                return 'China'
        
        for surname in japanese_names:
            if surname in name_lower:
                return 'Japan'
        
        for surname in korean_names:
            if surname in name_lower:
                return 'South Korea'
        
        for surname in indian_names:
            if surname in name_lower:
                return 'India'
        
        for surname in arabic_names:
            if surname in name_lower:
                return None  # Muy amplio, mejor no inferir
        
        for surname in hispanic_names:
            if surname in name_lower:
                return None  # Podría ser España, México, etc.
        
        return None

    def generate_heatmap(self):
        """
        Genera un mapa de calor mundial basado en el país del primer autor.
        """
        from geopy.geocoders import Nominatim
        
        # Preparar DataFrame
        if 'authors' not in self.df.columns:
            print("⚠️ No se encontró columna 'authors'")
            return None

        print("\n🔍 Buscando países de autores...")
        self.df['first_author'] = self.df['authors'].apply(
            lambda x: str(x).split(';')[0].strip() if pd.notna(x) else ''
        )
        self.df['author_country'] = None

        # Obtener países de autores
        for idx, row in self.df.iterrows():
            author = row['first_author']
            if not author:
                continue
            country = self.get_author_location(author)
            if country:
                self.df.at[idx, 'author_country'] = country

        # Agrupar por país
        country_counts = self.df['author_country'].dropna().value_counts()
        
        if country_counts.empty:
            print("⚠️ No se encontraron países para los autores")
            return None

        print(f"\n📊 Distribución por países:\n{country_counts}")

        # Obtener coordenadas
        geolocator = Nominatim(user_agent="bibliometric_visualizer")
        locations = {}
        
        for country in country_counts.index:
            try:
                loc = geolocator.geocode(country, timeout=10)
                if loc:
                    locations[country] = (loc.latitude, loc.longitude)
                time.sleep(1)  # Respetar rate limit de Nominatim
            except Exception as e:
                print(f"Error geocodificando {country}: {e}")

        if not locations:
            print("⚠️ No se pudieron obtener coordenadas")
            return None

        # Generar mapa con folium
        m = folium.Map(location=[20, 0], zoom_start=2, tiles='OpenStreetMap')
        
        max_count = country_counts.max()
        for country, count in country_counts.items():
            if country in locations:
                # Radio proporcional al número de publicaciones
                radius = 5 + (count / max_count) * 15
                folium.CircleMarker(
                    location=locations[country],
                    radius=radius,
                    popup=f"<b>{country}</b><br>Publicaciones: {count}",
                    tooltip=f"{country}: {count}",
                    color='crimson',
                    fill=True,
                    fill_color='red',
                    fill_opacity=0.6,
                    weight=2
                ).add_to(m)

        # Guardar mapa como HTML temporal y capturar como imagen
        map_img_path = None
        try:
            with tempfile.NamedTemporaryFile(suffix='.html', delete=False, mode='w') as tmpf:
                html_path = tmpf.name
                m.save(html_path)
                
                # Intentar captura con selenium si está disponible
                try:
                    from selenium import webdriver
                    from selenium.webdriver.chrome.options import Options
                    
                    options = Options()
                    options.add_argument('--headless')
                    options.add_argument('--disable-gpu')
                    options.add_argument('--no-sandbox')
                    
                    driver = webdriver.Chrome(options=options)
                    driver.set_window_size(1200, 800)
                    driver.get('file://' + html_path)
                    time.sleep(3)
                    
                    png_path = html_path.replace('.html', '.png')
                    driver.save_screenshot(png_path)
                    driver.quit()
                    map_img_path = png_path
                    
                except ImportError:
                    print("⚠️ Selenium no disponible, guardando solo HTML")
                    print(f"   Mapa guardado en: {html_path}")
                    
        except Exception as e:
            print(f"Error generando mapa: {e}")

        return map_img_path

    def generate_wordcloud(self):
        """Genera nube de palabras desde abstracts y keywords."""
        text = ''
        for col in ['abstract', 'keywords', 'keyword', 'palabras_clave']:
            if col in self.df.columns:
                text += ' '.join([str(x) for x in self.df[col] if pd.notna(x)]) + ' '
        
        wc_img_path = None
        if text.strip():
            wc = WordCloud(
                width=800, 
                height=400, 
                background_color='white', 
                colormap='viridis',
                max_words=100
            ).generate(text)
            
            with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmpf:
                wc.to_file(tmpf.name)
                wc_img_path = tmpf.name
        
        return wc_img_path

    def generate_timeline(self):
        """Genera línea temporal de publicaciones."""
        year_col = None
        for col in self.df.columns:
            if 'year' in col.lower() or 'año' in col.lower():
                year_col = col
                break
        
        journal_col = None
        for col in self.df.columns:
            if 'journal' in col.lower() or 'revista' in col.lower():
                journal_col = col
                break
        
        timeline_img_path = None
        if year_col:
            plt.figure(figsize=(10, 5))
            if journal_col:
                sns.countplot(data=self.df, x=year_col, hue=journal_col, palette='Set2')
                plt.legend(title='Revista', bbox_to_anchor=(1.05, 1), loc='upper left')
            else:
                sns.countplot(data=self.df, x=year_col, color='steelblue')
            
            plt.title('Publicaciones por Año y Revista', fontsize=14, fontweight='bold')
            plt.xlabel('Año', fontsize=12)
            plt.ylabel('Número de Publicaciones', fontsize=12)
            plt.xticks(rotation=45)
            plt.tight_layout()
            
            with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmpf:
                plt.savefig(tmpf.name, dpi=150)
                timeline_img_path = tmpf.name
            plt.close()
        
        return timeline_img_path

    def export_pdf(self):
        """Genera el PDF con todas las visualizaciones."""
        print("\n📊 Generando visualizaciones...")
        
        map_img_path = self.generate_heatmap()
        wc_img_path = self.generate_wordcloud()
        timeline_img_path = self.generate_timeline()
        
        print(f"\n📄 Creando PDF: {self.output_pdf}")
        
        c = canvas.Canvas(self.output_pdf, pagesize=A4)
        width, height = A4
        y = height - 60
        
        # Título principal
        c.setFont('Helvetica-Bold', 22)
        c.drawString(40, y, 'Análisis Bibliométrico')
        y -= 35
        
        c.setFont('Helvetica', 12)
        c.drawString(40, y, f'Fecha de generación: {datetime.now().strftime("%d/%m/%Y %H:%M")}')
        y -= 18
        c.drawString(40, y, f'Total de publicaciones analizadas: {len(self.df)}')
        y -= 50
        
        # === VISUALIZACIÓN 1: Mapa de Calor Geográfico ===
        if map_img_path and os.path.exists(map_img_path):
            c.setFont('Helvetica-Bold', 16)
            c.drawString(40, y, '🌍 Distribución Geográfica de Autores')
            y -= 25
            try:
                c.drawImage(ImageReader(map_img_path), 40, y - 320, width=520, height=320)
                y -= 340
            except Exception as e:
                c.setFont('Helvetica', 11)
                c.drawString(40, y, f'⚠️ Error al cargar mapa: {str(e)[:60]}')
                y -= 30
        
        # === VISUALIZACIÓN 2: Nube de Palabras ===
        if wc_img_path and os.path.exists(wc_img_path):
            if y < 280:
                c.showPage()
                y = height - 60
            
            c.setFont('Helvetica-Bold', 16)
            c.drawString(40, y, '☁️ Nube de Palabras (Keywords y Abstracts)')
            y -= 25
            try:
                c.drawImage(ImageReader(wc_img_path), 40, y - 220, width=520, height=220)
                y -= 250
            except Exception as e:
                c.setFont('Helvetica', 11)
                c.drawString(40, y, f'⚠️ Error al cargar nube: {str(e)[:60]}')
                y -= 30
        
        # === VISUALIZACIÓN 3: Línea Temporal ===
        if timeline_img_path and os.path.exists(timeline_img_path):
            if y < 300:
                c.showPage()
                y = height - 60
            
            c.setFont('Helvetica-Bold', 16)
            c.drawString(40, y, '📈 Evolución Temporal de Publicaciones')
            y -= 25
            try:
                c.drawImage(ImageReader(timeline_img_path), 40, y - 260, width=520, height=260)
            except Exception as e:
                c.setFont('Helvetica', 11)
                c.drawString(40, y, f'⚠️ Error al cargar gráfico: {str(e)[:60]}')
        
        c.save()
        print(f"\n✅ PDF generado exitosamente: {self.output_pdf}")

