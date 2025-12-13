import logging
from typing import Any, Dict, List

from elasticsearch import Elasticsearch
from ranx import Run, fuse

log = logging.getLogger(__name__)


class HybridSearcher:
    """
    Direct client for Elasticsearch operations, including client-side RRF fusion
    using the 'ranx' library.
    """

    def __init__(
        self,
        index_name: str,
        es_url: str = "http://localhost:9200",
    ):
        """
        Initializes the HybridSearchClient.
        """
        self.index_name = index_name
        self.RRF_K = 60  # Standard constant K for RRF fusion

        try:
            self.client = Elasticsearch(hosts=[es_url])
            self.client.info()
        except Exception:
            self.client = None
            log.error("Error initializing ES client.", exc_info=True)

    def full_text_search(self, query: str, k: int) -> List[Dict[str, Any]]:
        """
        Executes a full-text search (BM25) with minimum_should_match.
        """
        if not self.client:
            return []
        search_body = {
            "query": {
                "match": {
                    "text": {"query": query, "minimum_should_match": "80%"}
                }
            },
            "_source": {
                "includes": [
                    "text",
                    "metadata.title",
                    "metadata.source_uri",
                    "metadata.correlation_id",
                ]
            },
            "size": k * 3,
        }

        try:
            response = self.client.search(
                index=self.index_name, body=search_body
            )
            return response.get("hits", {}).get("hits", [])
        except Exception:
            raise

    def vector_knn_search(
        self, query_vector: List[float], k: int, num_candidates: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Executes a k-Nearest Neighbors (kNN) search for semantic similarity.
        """
        if not self.client:
            return []
        search_body = {
            "knn": {
                "field": "vector",
                "query_vector": query_vector,
                "k": k,
                "num_candidates": num_candidates,
            },
            "_source": {
                "includes": [
                    "text",
                    "metadata.title",
                    "metadata.source_uri",
                    "metadata.correlation_id",
                ]
            },
        }

        try:
            response = self.client.search(
                index=self.index_name, body=search_body
            )
            return response.get("hits", {}).get("hits", [])
        except Exception:
            raise

    # --- RRF Implementation using ranx ---

    def _get_flat_ranking_dict(
        self, hits: List[Dict[str, Any]]
    ) -> Dict[str, float]:
        """
        Converts raw ES hits to a flat {doc_id: score} dictionary.
        """
        ranking = {}
        for hit in hits:
            doc_id = (
                hit.get("_source", {})
                .get("metadata", {})
                .get("correlation_id", hit.get("_id"))
            )
            if doc_id and doc_id not in ranking:
                ranking[doc_id] = 1.0
        return ranking

    def _hits_to_run(self, hits: List[Dict[str, Any]], run_name: str) -> Run:
        """
        Converts ES hits into a ranx Run object.
        """
        ranking = self._get_flat_ranking_dict(hits)
        return Run(name=run_name, run={"q1": ranking})

    def _apply_rrf(
        self,
        bm25_hits: List[Dict[str, Any]],
        knn_hits: List[Dict[str, Any]],
        final_k: int,
    ) -> List[Dict[str, Any]]:
        """
        Compute Reciprocal Rank Fusion (RRF) client-side using ranx.
        Handles cases where one or more runs are empty.
        """

        runs = []

        if bm25_hits:
            bm25_run = self._hits_to_run(bm25_hits, "bm25")
            runs.append(bm25_run)

        if knn_hits:
            knn_run = self._hits_to_run(knn_hits, "knn")
            runs.append(knn_run)

        # If no runs at all, return empty
        if not runs:
            return []

        # If only one run, no need to fuse — just return its hits
        if len(runs) == 1:
            single_run = runs[0]
            fused_ranks = single_run["q1"]
        else:
            fused_run = fuse(
                runs,
                norm="min-max",  # or "none" if you want no normalization
                method="rrf",
                params={"k": self.RRF_K},
            )
            fused_ranks = fused_run["q1"]

        ranked_doc_ids = sorted(fused_ranks, key=fused_ranks.get, reverse=True)

        all_hits_map = {}
        for hit in bm25_hits + knn_hits:
            doc_id = (
                hit.get("_source", {})
                .get("metadata", {})
                .get("correlation_id", hit.get("_id"))
            )
            if doc_id and doc_id not in all_hits_map:
                all_hits_map[doc_id] = hit

        fused_results = []
        for doc_id in ranked_doc_ids[:final_k]:
            hit = all_hits_map.get(doc_id)
            if hit:
                hit["_rrf_score"] = fused_ranks[doc_id]
                fused_results.append(hit)

        return fused_results

    def hybrid_search_rrf(
        self,
        query: str,
        query_vector: List[float],
        k: int,
        num_candidates: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Executes a hybrid search, combining BM25 and kNN results,
        then re-ranks them using RRF.
        """
        if not self.client:
            return []

        bm25_hits = self.full_text_search(query, k=k)
        knn_hits = self.vector_knn_search(
            query_vector, k=k, num_candidates=num_candidates
        )
        fused_hits = self._apply_rrf(bm25_hits, knn_hits, final_k=k)

        return fused_hits
