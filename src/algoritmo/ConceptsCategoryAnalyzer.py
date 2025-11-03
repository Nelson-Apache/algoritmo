"""
Concepts Category Analyzer (GAIE)
=================================

Analiza abstracts para:
1) Contar frecuencia de un conjunto de términos semilla (frases) para la categoría
   "Concepts of Generative AI in Education".
2) Extraer automáticamente hasta top_k términos asociados (unigramas/bigramas) usando TF-IDF.
3) Calcular una métrica de "precisión" de los términos generados basada en co-ocurrencia
   documentaria con los términos semilla: P(cat|t) ≈ docs(cat ∧ t) / docs(t).

Sin dependencias extra (usa scikit-learn ya presente).
"""
from __future__ import annotations
from typing import List, Dict, Any, Tuple
import re
import math
from collections import Counter, defaultdict
import numpy as np

# scikit-learn para TF-IDF
from sklearn.feature_extraction.text import TfidfVectorizer


class ConceptsCategoryAnalyzer:
    """Analizador de conceptos para la categoría GAIE."""

    CATEGORY_NAME = "Concepts of Generative AI in Education"

    # Lista de términos semilla (variantes normalizadas incluidas)
    SEED_TERMS = [
        "generative models",
        "prompting",
        "machine learning",
        "multimodality",
        "fine-tuning", "fine tuning",
        "training data",
        "algorithmic bias",
        "explainability",
        "transparency",
        "ethics",
        "privacy",
        "personalization",
        "human-ai interaction", "human ai interaction",
        "ai literacy",
        "co-creation", "co creation",
    ]

    def __init__(self, extra_stopwords: List[str] | None = None):
        self.extra_stopwords = set((extra_stopwords or []))

    @staticmethod
    def _normalize_text(text: str) -> str:
        if not isinstance(text, str):
            text = "" if text is None else str(text)
        t = text.lower()
        # Mantener guiones para frases como "fine-tuning" o "human-ai"
        t = re.sub(r"[\r\n\t]", " ", t)
        t = re.sub(r"\s+", " ", t).strip()
        return t

    @staticmethod
    def _compile_phrase_regex(phrase: str) -> re.Pattern:
        # Palabras con espacios o guiones; permitir variantes de espacios múltiples
        # Usar límites de palabra aproximados: \b para extremos de tokens alfanuméricos y '-'
        esc = re.escape(phrase.lower()).replace("\\ ", r"\s+")
        # Permitir coincidencias con guión o espacio entre tokens (p.ej., human-ai vs human ai)
        esc = esc.replace("human\\s+ai", r"(?:human-?ai)")
        esc = esc.replace("fine\\s+tuning", r"(?:fine-?tuning)")
        esc = esc.replace("co\\s+creation", r"(?:co-?creation)")
        pat = rf"(?<![\w-]){esc}(?![\w-])"
        return re.compile(pat, re.IGNORECASE)

    def _doc_contains_any_seed(self, text: str, compiled: List[Tuple[str, re.Pattern]] ) -> bool:
        for _, rx in compiled:
            if rx.search(text):
                return True
        return False

    def count_seed_frequencies(self, abstracts: List[str]) -> Dict[str, Any]:
        """Cuenta ocurrencias de términos semilla por documento y totales."""
        norm_docs = [self._normalize_text(a) for a in abstracts]
        compiled = [(term, self._compile_phrase_regex(term)) for term in self.SEED_TERMS]

        term_total_occ = Counter()
        term_doc_occ = Counter()

        for doc in norm_docs:
            found_this_doc = set()
            for term, rx in compiled:
                hits = rx.findall(doc)
                if hits:
                    term_total_occ[term] += len(hits)
                    found_this_doc.add(term)
            for term in found_this_doc:
                term_doc_occ[term] += 1

        results = []
        for term in self.SEED_TERMS:
            results.append({
                'term': term,
                'total_occurrences': int(term_total_occ.get(term, 0)),
                'docs_with_term': int(term_doc_occ.get(term, 0))
            })

        return {
            'category': self.CATEGORY_NAME,
            'documents': len(abstracts),
            'seed_stats': results
        }

    def extract_keywords(self, abstracts: List[str], top_k: int = 15) -> List[Dict[str, Any]]:
        """Extrae términos asociados (unigramas y bigramas) con TF-IDF (top_k)."""
        texts = [self._normalize_text(a) for a in abstracts]

        # Vectorizador: permitir guiones, usar stopwords en inglés por defecto
        vectorizer = TfidfVectorizer(
            lowercase=True,
            stop_words='english',
            token_pattern=r'(?u)\b[\w-]{3,}\b',
            ngram_range=(1, 2),
            max_df=0.85,
            min_df=2
        )
        X = vectorizer.fit_transform(texts)
        terms = vectorizer.get_feature_names_out()
        # Sumar TF-IDF por término evitando advertencias de tipado en matrices dispersas
        scores = np.asarray(X.sum(axis=0)).ravel()  # type: ignore

        # Filtrar términos que ya sean semillas (normalizando variantes: espacios vs guiones)
        seed_norm = {t.replace('-', ' ').strip() for t in self.SEED_TERMS}
        pairs = [(terms[i], scores[i]) for i in range(len(terms))]
        pairs.sort(key=lambda x: x[1], reverse=True)

        selected = []
        for t, s in pairs:
            base = t.replace('-', ' ').strip()
            if base in seed_norm:
                continue
            # Evitar quedarse solo con tokens numéricos
            if re.fullmatch(r"[0-9\-]+", t):
                continue
            selected.append({'term': t, 'score': float(s)})
            if len(selected) >= top_k:
                break

        return selected

    def _phrase_doc_presence(self, abstracts: List[str], phrases: List[str]) -> Dict[str, Tuple[int, int]]:
        """Para cada frase, retorna (docs_con_frase, total_ocurrencias)."""
        norm_docs = [self._normalize_text(a) for a in abstracts]
        compiled = [(p, self._compile_phrase_regex(p)) for p in phrases]
        stats: Dict[str, Tuple[int, int]] = {}
        for p, rx in compiled:
            docs = 0
            occs = 0
            for doc in norm_docs:
                hits = rx.findall(doc)
                if hits:
                    docs += 1
                    occs += len(hits)
            stats[p] = (docs, occs)
        return stats

    def precision_of_generated(self, abstracts: List[str], gen_terms: List[str]) -> List[Dict[str, Any]]:
        """Calcula P(cat|t) ≈ docs(cat ∧ t) / docs(t)."""
        norm_docs = [self._normalize_text(a) for a in abstracts]
        seed_compiled = [(term, self._compile_phrase_regex(term)) for term in self.SEED_TERMS]
        gen_stats = self._phrase_doc_presence(norm_docs, gen_terms)

        # Determinar cuáles documentos cumplen la categoría (contienen al menos una semilla)
        cat_docs = []
        for i, doc in enumerate(norm_docs):
            if self._doc_contains_any_seed(doc, seed_compiled):
                cat_docs.append(i)
        cat_set = set(cat_docs)

        # Presencia por documento para términos generados
        gen_presence: Dict[str, set] = defaultdict(set)
        for term in gen_terms:
            rx = self._compile_phrase_regex(term)
            for i, doc in enumerate(norm_docs):
                if rx.search(doc):
                    gen_presence[term].add(i)

        # Calcular métrica por término
        results = []
        for term in gen_terms:
            docs_with_t = len(gen_presence.get(term, set()))
            if docs_with_t == 0:
                p = 0.0
                both = 0
            else:
                both = len(gen_presence[term] & cat_set)
                p = both / docs_with_t
            # total ocurrencias (además de documentos)
            _, total_occs = gen_stats.get(term, (0, 0))
            results.append({
                'term': term,
                'docs_with_term': docs_with_t,
                'total_occurrences': int(total_occs),
                'precision_cat_given_term': round(p, 4),
                'docs_with_term_and_category': both
            })
        return results

    def analyze(self, abstracts: List[str], top_k: int = 15) -> Dict[str, Any]:
        """Pipeline completo: semillas + extracción + precisión."""
        seed = self.count_seed_frequencies(abstracts)
        extracted = self.extract_keywords(abstracts, top_k=top_k)
        gen_terms = [x['term'] for x in extracted]
        gen_prec = self.precision_of_generated(abstracts, gen_terms) if gen_terms else []

        # Fusionar info para generados por término
        gen_map = {x['term']: x for x in gen_prec}
        merged = []
        for x in extracted:
            base = gen_map.get(x['term'], {})
            merged.append({
                'term': x['term'],
                'tfidf_score': round(float(x['score']), 6),
                'docs_with_term': int(base.get('docs_with_term', 0)),
                'total_occurrences': int(base.get('total_occurrences', 0)),
                'precision_cat_given_term': float(base.get('precision_cat_given_term', 0.0)),
                'docs_with_term_and_category': int(base.get('docs_with_term_and_category', 0)),
            })

        avg_precision = 0.0
        if merged:
            avg_precision = sum(m['precision_cat_given_term'] for m in merged) / len(merged)

        return {
            'category': self.CATEGORY_NAME,
            'documents': seed['documents'],
            'seed_stats': seed['seed_stats'],
            'generated_terms': merged,
            'avg_precision_generated': round(avg_precision, 4)
        }
