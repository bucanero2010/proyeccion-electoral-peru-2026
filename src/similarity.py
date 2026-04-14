#!/usr/bin/env python3
"""
District similarity based on 2021 voting patterns.
Uses cosine similarity on vote-share vectors to find districts
that vote alike, regardless of geographic proximity.
"""

import os
import csv
import numpy as np
from collections import defaultdict

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
FILE_2021 = os.path.join(DATA_DIR, "2021_presidencial-resultados-partidos.csv")


def load_2021_vectors():
    """
    Build a vote-share vector per distrito from 2021 results.
    Returns:
        ubigeos: list of ubigeo codes
        partidos: list of partido names (columns)
        vectors: numpy array (n_distritos x n_partidos), each row sums to ~1
    """
    with open(FILE_2021, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    # Aggregate votos by (ubigeo, partido)
    votos = defaultdict(lambda: defaultdict(float))
    for r in rows:
        votos[r["ubigeo"]][r["partido"]] += float(r["total_votos"] or 0)

    partidos = sorted({r["partido"] for r in rows})
    ubigeos = sorted(votos.keys())
    partido_idx = {p: i for i, p in enumerate(partidos)}

    vectors = np.zeros((len(ubigeos), len(partidos)))
    for i, ub in enumerate(ubigeos):
        for p, v in votos[ub].items():
            vectors[i, partido_idx[p]] = v
        total = vectors[i].sum()
        if total > 0:
            vectors[i] /= total  # normalize to vote shares

    return ubigeos, partidos, vectors


def cosine_similarity_matrix(vectors):
    """Compute pairwise cosine similarity. Returns (n x n) matrix."""
    norms = np.linalg.norm(vectors, axis=1, keepdims=True)
    norms = np.where(norms == 0, 1, norms)
    normalized = vectors / norms
    sim = normalized @ normalized.T
    np.nan_to_num(sim, copy=False, nan=0.0, posinf=1.0, neginf=0.0)
    return sim


def build_similarity_index(k=20):
    """
    Build a similarity index: for each distrito, the top-K most similar
    distritos based on 2021 voting patterns.

    Returns:
        sim_index: dict {ubigeo: [(neighbor_ubigeo, similarity_score), ...]}
        ubigeos: list of all ubigeos
    """
    ubigeos, partidos, vectors = load_2021_vectors()
    sim_matrix = cosine_similarity_matrix(vectors)
    ubigeo_to_idx = {ub: i for i, ub in enumerate(ubigeos)}

    sim_index = {}
    for i, ub in enumerate(ubigeos):
        # Get top K+1 (includes self), exclude self
        scores = sim_matrix[i]
        top_indices = np.argsort(scores)[::-1][1:k+1]
        sim_index[ub] = [(ubigeos[j], float(scores[j])) for j in top_indices]

    return sim_index, ubigeos


def get_similar_district_proportions(target_ubigeo, sim_index, district_props, district_pcts, threshold, k=10):
    """
    For a target distrito with insufficient data, compute vote proportions
    as a weighted average of its most similar districts that DO have enough data.

    Args:
        target_ubigeo: ubigeo of the distrito needing fallback
        sim_index: {ubigeo: [(neighbor_ubigeo, score), ...]}
        district_props: {ubigeo: {partido: proportion}} from 2026 data
        district_pcts: {ubigeo: pct_actas} from 2026 data
        threshold: minimum pct_actas to consider a district "reliable"
        k: max neighbors to use

    Returns:
        props: {partido: weighted_proportion} or None if no similar districts found
    """
    if target_ubigeo not in sim_index:
        return None

    neighbors = sim_index[target_ubigeo]
    weighted_props = defaultdict(float)
    total_weight = 0

    for neighbor_ub, score in neighbors:
        # Only use neighbors that have enough 2026 data
        if neighbor_ub in district_pcts and district_pcts[neighbor_ub] >= threshold:
            if neighbor_ub in district_props:
                for partido, prop in district_props[neighbor_ub].items():
                    weighted_props[partido] += prop * score
                total_weight += score
                if total_weight > 0 and len([1 for n, s in neighbors[:k] if n in district_pcts and district_pcts[n] >= threshold]) >= 3:
                    # We have at least 3 reliable neighbors, good enough
                    pass

        if total_weight > 0 and sum(1 for n, s in neighbors if n in district_pcts and district_pcts.get(n, 0) >= threshold) >= k:
            break

    if total_weight == 0:
        return None

    # Normalize
    return {p: v / total_weight for p, v in weighted_props.items()}
