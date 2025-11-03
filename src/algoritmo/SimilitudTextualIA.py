"""
Similitud textual con modelos de IA
-----------------------------------

Esta clase implementa dos enfoques modernos para similitud textual usando
modelos de lenguaje pre-entrenados:

1) Sentence-BERT (SentenceTransformers):
   - Modelo por defecto: "sentence-transformers/all-MiniLM-L6-v2"
   - Uso recomendado para similitud semántica de oraciones/párrafos.

2) Transformers (Hugging Face) con mean pooling manual:
   - Modelo por defecto: "thenlper/gte-small" (ligero y efectivo)
   - Alternativas: cualquier modelo de tipo encoder (p. ej., roberta, mpnet). 

Dependencias opcionales (no estrictas para correr el resto del proyecto):
- sentence-transformers
- transformers
- torch

Ejemplo rápido:
>>> ia = SimilitudTextualIA()
>>> ia.similitud_sbert("La IA avanza rápido", "La inteligencia artificial progresa velozmente")
0.85  # (aprox)

>>> ia.similitud_transformer("hola mundo", "buenos días mundo")
0.74  # (aprox)
"""
from __future__ import annotations  # Permite referirse a tipos definidos más adelante sin comillas en Python 3.11-

import math  # Funciones matemáticas básicas (no crítico aquí, pero disponible)
from typing import Optional, Dict, Any  # Tipado opcional para mayor claridad

import numpy as np  # Librería numérica para manejar vectores y operaciones de álgebra lineal


def _cosine_sim(a: np.ndarray, b: np.ndarray) -> float:  # Función auxiliar para calcular similitud del coseno
    """Similitud del coseno entre dos vectores 1D."""  # Documenta el propósito
    a = a.astype(np.float32)  # Convierte el vector A a float32 para estabilidad y rendimiento
    b = b.astype(np.float32)  # Convierte el vector B a float32
    na = np.linalg.norm(a)  # Calcula la norma (magnitud) de A
    nb = np.linalg.norm(b)  # Calcula la norma (magnitud) de B
    if na == 0.0 or nb == 0.0:  # Si alguno es vector cero, el coseno es 0 por definición práctica
        return 0.0  # Evita división por cero devolviendo 0
    return float(np.dot(a, b) / (na * nb))  # Producto punto normalizado por las normas: coseno ∈ [-1,1]


class SimilitudTextualIA:  # Clase principal para similitud textual usando modelos de IA
    """
    Algoritmos de similitud textual basados en IA.

    Métodos:
    - similitud_sbert: embeddings con Sentence-BERT (sentence-transformers)
    - similitud_transformer: embeddings con Transformers + mean pooling
    - comparar: ejecuta ambos y retorna un dict con las puntuaciones
    """

    def __init__(self, device: Optional[str] = None):  # Constructor con opción de forzar dispositivo
        """
        device: 'cuda', 'cpu' o None para autodetectar (si torch está disponible).  # Explica argumento device
        """
        self._device = device or self._auto_device()  # Selecciona el dispositivo a usar (cuda/cpu)
        self._models: Dict[str, Any] = {}  # Cache interno de modelos/tokenizers ya cargados

    def _auto_device(self) -> str:  # Detecta automáticamente si hay GPU disponible
        try:
            import torch  # type: ignore  # Importa PyTorch si está instalado
            if torch.cuda.is_available():  # Verifica disponibilidad de CUDA
                return "cuda"  # Usa GPU
            return "cpu"  # Si no hay CUDA, usa CPU
        except Exception:
            return "cpu"  # Si no está torch, se usa CPU por defecto

    # -------------------- SBERT (Sentence-Transformers) --------------------
    def _load_sbert(self, model_name: str = "sentence-transformers/all-MiniLM-L6-v2"):  # Carga diferida de SBERT
        """Carga perezosa del modelo SBERT."""  # Solo carga cuando se necesita
        if "sbert" in self._models:  # Si ya está en cache, reutiliza
            return self._models["sbert"]  # Devuelve instancia cacheada
        try:
            from sentence_transformers import SentenceTransformer  # type: ignore  # Importa clase del paquete
        except ImportError as e:
            raise ImportError(
                "Falta 'sentence-transformers'. Instala con: pip install sentence-transformers"  # Mensaje guía
            ) from e
        model = SentenceTransformer(model_name, device=self._device)  # Crea el modelo en el dispositivo deseado
        self._models["sbert"] = model  # Guarda en cache
        return model  # Retorna el modelo listo para usar

    def similitud_sbert(self, texto1: str, texto2: str,
                         model_name: str = "sentence-transformers/all-MiniLM-L6-v2") -> float:  # API pública SBERT
        """
        Similitud con Sentence-BERT (coseno de embeddings).
        """
        model = self._load_sbert(model_name)  # Asegura que el modelo esté cargado
        embeddings = model.encode([texto1, texto2], convert_to_numpy=True, normalize_embeddings=False)  # Calcula embeddings
        return _cosine_sim(embeddings[0], embeddings[1])  # Devuelve el coseno entre los dos vectores

    # -------------------- Transformers + mean pooling --------------------
    def _load_transformer(self, model_name: str = "thenlper/gte-small"):  # Carga diferida de tokenizer+modelo HF
        """Carga perezosa de tokenizer y modelo HF Transformers."""  # Reutiliza si ya está cargado
        if "hf_model" in self._models and self._models.get("hf_name") == model_name:  # Verifica cache por nombre
            return self._models["hf_tok"], self._models["hf_model"]  # Devuelve tokenizer y modelo cacheados
        try:
            from transformers import AutoTokenizer, AutoModel  # type: ignore  # Importa clases de HF
        except ImportError as e:
            raise ImportError(
                "Falta 'transformers'. Instala con: pip install transformers torch"  # Mensaje guía
            ) from e
        tok = AutoTokenizer.from_pretrained(model_name)  # Descarga/carga tokenizer
        mdl = AutoModel.from_pretrained(model_name)  # Descarga/carga modelo encoder
        mdl.eval()  # Pone el modelo en modo evaluación (sin dropout)
        try:
            import torch  # type: ignore  # Importa torch para mover a GPU si procede
            if self._device == "cuda":  # Si se seleccionó GPU
                mdl.to("cuda")  # Mueve el modelo a la GPU
        except Exception:
            pass  # Si no hay torch o falla, continúa en CPU
        self._models["hf_tok"] = tok  # Cachea tokenizer
        self._models["hf_model"] = mdl  # Cachea modelo
        self._models["hf_name"] = model_name  # Guarda el nombre para validar cache
        return tok, mdl  # Retorna ambos objetos

    def _mean_pooling(self, token_embeddings, attention_mask):  # Hace promedio ponderado por la máscara de atención
        """Mean pooling con atención (torch.Tensor)."""  # Describe la técnica
        import torch  # type: ignore  # Importa torch localmente
        input_mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()  # Expande máscara
        sum_embeddings = torch.sum(token_embeddings * input_mask_expanded, 1)  # Suma embeddings por posición válida
        sum_mask = torch.clamp(input_mask_expanded.sum(1), min=1e-9)  # Evita división por cero
        return sum_embeddings / sum_mask  # Devuelve el promedio ponderado

    def similitud_transformer(self, texto1: str, texto2: str,
                              model_name: str = "thenlper/gte-small",
                              max_length: int = 256) -> float:  # API pública con Transformers
        """
        Similitud con modelo HF (encoder) y mean pooling de los estados ocultos.
        """
        tok, mdl = self._load_transformer(model_name)  # Asegura tokenizer y modelo cargados
        import torch  # type: ignore  # Importa torch para tensores
        batch = tok([texto1, texto2], padding=True, truncation=True, max_length=max_length, return_tensors="pt")  # Tokeniza
        if self._device == "cuda":  # Si se usa GPU
            batch = {k: v.to("cuda") for k, v in batch.items()}  # Mueve los tensores a GPU
        with torch.no_grad():  # Desactiva gradientes para inferencia
            out = mdl(**batch)  # Pasa por el modelo: obtiene estados ocultos
            last_hidden = out.last_hidden_state  # [B, T, H]  # Extrae la última capa
            pooled = self._mean_pooling(last_hidden, batch["attention_mask"])  # [B, H]  # Aplica mean pooling
            vecs = pooled.detach().cpu().numpy()  # Trae a CPU y convierte a numpy
        return _cosine_sim(vecs[0], vecs[1])  # Coseno entre los dos embeddings resultantes

    # -------------------- Comparador --------------------
    def comparar(self, texto1: str, texto2: str,
                 usar_sbert: bool = True,
                 usar_transformer: bool = True,
                 sbert_model: str = "sentence-transformers/all-MiniLM-L6-v2",
                 hf_model: str = "thenlper/gte-small") -> Dict[str, float]:  # Ejecuta múltiples métodos a la vez
        """
        Ejecuta los métodos seleccionados y retorna sus puntajes.
        """
        resultados: Dict[str, float] = {}  # Diccionario de resultados
        if usar_sbert:  # Si se solicita SBERT
            try:
                resultados["SBERT"] = self.similitud_sbert(texto1, texto2, model_name=sbert_model)  # Calcula SBERT
            except Exception as e:  # Si falta dependencia o falla descarga
                resultados["SBERT"] = float("nan")  # Marca como NaN para indicar indisponible
        if usar_transformer:  # Si se solicita Transformers
            try:
                resultados["HF-MeanPooling"] = self.similitud_transformer(texto1, texto2, model_name=hf_model)  # Calcula HF
            except Exception as e:  # Maneja errores (deps/modelos)
                resultados["HF-MeanPooling"] = float("nan")  # Marca como NaN
        return resultados  # Devuelve el dict con las puntuaciones

    def comparar_multiples(self, textos: list[str],
                           usar_sbert: bool = True,
                           usar_transformer: bool = True,
                           sbert_model: str = "sentence-transformers/all-MiniLM-L6-v2",
                           hf_model: str = "thenlper/gte-small",
                           max_length: int = 256,
                           top_k: int = 10) -> Dict[str, Any]:
        """
        Calcula similitud entre múltiples textos (>=2) usando modelos de IA.

        Retorna matrices NxN por modelo y lista de pares top por algoritmo.
        """
        n = len(textos)
        if n < 2:
            raise ValueError("Se requieren al menos 2 textos para comparar.")

        resultados: Dict[str, Any] = {}

        # SBERT batch
        if usar_sbert:
            try:
                model = self._load_sbert(sbert_model)
                # Normalizamos embeddings para usar producto punto como coseno
                embs = model.encode(textos, convert_to_numpy=True, normalize_embeddings=True)
                sim_mat = (embs @ embs.T).astype(np.float32)
                # Construir pares top
                pairs = []
                for i in range(n):
                    for j in range(i + 1, n):
                        pairs.append({'i': i, 'j': j, 'score': float(sim_mat[i, j])})
                pairs.sort(key=lambda x: x['score'], reverse=True)
                if top_k is not None:
                    pairs = pairs[:top_k]
                resultados['SBERT'] = {
                    'matrix': sim_mat.tolist(),
                    'pairs': pairs
                }
            except Exception:
                resultados['SBERT'] = {
                    'matrix': [[1.0 if i == j else 0.0 for j in range(n)] for i in range(n)],
                    'pairs': []
                }

        # HF mean pooling batch
        if usar_transformer:
            try:
                tok, mdl = self._load_transformer(hf_model)
                import torch  # type: ignore
                batch = tok(textos, padding=True, truncation=True, max_length=max_length, return_tensors="pt")
                if self._device == 'cuda':
                    batch = {k: v.to('cuda') for k, v in batch.items()}
                with torch.no_grad():
                    out = mdl(**batch)
                    pooled = self._mean_pooling(out.last_hidden_state, batch['attention_mask'])
                    vecs = pooled.detach().cpu().numpy().astype(np.float32)
                # Normalizar por fila
                norms = np.linalg.norm(vecs, axis=1, keepdims=True)
                norms[norms == 0] = 1e-9
                vecs_norm = vecs / norms
                sim_mat = (vecs_norm @ vecs_norm.T).astype(np.float32)
                pairs = []
                for i in range(n):
                    for j in range(i + 1, n):
                        pairs.append({'i': i, 'j': j, 'score': float(sim_mat[i, j])})
                pairs.sort(key=lambda x: x['score'], reverse=True)
                if top_k is not None:
                    pairs = pairs[:top_k]
                resultados['HF-MeanPooling'] = {
                    'matrix': sim_mat.tolist(),
                    'pairs': pairs
                }
            except Exception:
                resultados['HF-MeanPooling'] = {
                    'matrix': [[1.0 if i == j else 0.0 for j in range(n)] for i in range(n)],
                    'pairs': []
                }

        return resultados
