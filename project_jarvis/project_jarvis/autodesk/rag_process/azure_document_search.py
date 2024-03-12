import os
from abc import ABC
from dataclasses import dataclass
from typing import Any, AsyncGenerator, Awaitable, Callable, List, Optional, Union, cast
from urllib.parse import urljoin

import aiohttp
from azure.search.documents.aio import SearchClient
from azure.search.documents.models import (
    QueryCaptionResult,
    QueryType,
    VectorizedQuery,
    VectorQuery)

from langchain.docstore.document import Document
from openai import AzureOpenAI, AsyncOpenAI, AsyncAzureOpenAI


def nonewlines(s: str) -> str:
    return s.replace("\n", " ").replace("\r", " ")

@dataclass
class SourceDocument:
    id: Optional[str]
    content: Optional[str]
    embedding: Optional[List[float]]
    category: Optional[str]
    sourcepage: Optional[str]
    sourcefile: Optional[str]

    def document_store(self) -> Document:

        return Document(page_content = self.content, 
                        metadata={"source":self.sourcefile,
                                  "sourcepage":self.sourcepage,
                                  "category":self.category,
                                  "embeddings":self.embedding})


class AzureAISearchClient(ABC):
    def __init__(
        self,
        search_client: SearchClient,
        openai_client: AsyncOpenAI,
        embedding_deployment: Optional[str],  # Not needed for non-Azure OpenAI or for retrieval_mode="text"
        embedding_model: str,
        openai_host: str,
    ):
        self.search_client = search_client
        self.openai_client = openai_client
        self.embedding_deployment = embedding_deployment
        self.embedding_model = embedding_model
        self.openai_host = openai_host

    def build_filter(self, overrides: dict[str, Any]) -> Optional[str]:
        exclude_category = overrides.get("exclude_category")
        filters = []
        if exclude_category:
            filters.append("category ne '{}'".format(exclude_category.replace("'", "''")))
        return None if len(filters) == 0 else " and ".join(filters)

    async def search(
        self,
        top: int,
        query_text: Optional[str],
        filter: Optional[str],
        vectors: List[VectorQuery],
        use_semantic_ranker: bool,
        use_semantic_captions: bool,
    ) -> List[Document]:
        # Use semantic ranker if requested and if retrieval mode is text or hybrid (vectors + text)
        if use_semantic_ranker and query_text:
            results = await self.search_client.search(
                search_text=query_text,
                filter=filter,
                query_type=QueryType.SEMANTIC,
                semantic_configuration_name="default",
                top=top,
                query_caption="extractive|highlight-false" if use_semantic_captions else None,
                vector_queries=vectors,
            )
        else:
            results = await self.search_client.search(
                search_text=query_text or "", filter=filter, top=top, vector_queries=vectors
            )

        documents = []
        async for page in results.by_page():
            async for document in page:
                documents.append(
                    SourceDocument(
                        id=document.get("id"),
                        content=document.get("content"),
                        embedding=document.get("embedding"),
                        category=document.get("category"),
                        sourcepage=document.get("sourcepage"),
                        sourcefile=document.get("sourcefile"),
                    ).document_store()
                )
        return documents

    async def compute_text_embedding(self, q: str):
        embedding = await self.openai_client.embeddings.create(
            # Azure Open AI takes the deployment name as the model name
            model=self.embedding_deployment if self.embedding_deployment else self.embedding_model,
            input=q,
        )
        query_vector = embedding.data[0].embedding
        return VectorizedQuery(vector=query_vector, k_nearest_neighbors=50, fields="embedding")

    async def run(
        self, message: str, context: dict[str, Any] = {}
    ) -> Union[dict[str, Any], AsyncGenerator[dict[str, Any], None]]:
        overrides = context.get("overrides", {})
        has_text = overrides.get("retrieval_mode") in ["text", "hybrid", None]
        has_vector = overrides.get("retrieval_mode") in ["vectors", "hybrid", None]
        use_semantic_ranker = overrides.get("semantic_ranker") and has_text       
        use_semantic_captions = True if overrides.get("semantic_captions") and has_text else False
        top = overrides.get("top", 3)
        filter = self.build_filter(overrides)
        # If retrieval mode includes vectors, compute an embedding for the query
        vectors: list[VectorQuery] = []
        if has_vector:
            vectors.append(await self.compute_text_embedding(message))
        # Only keep the text query if the retrieval mode uses text, otherwise drop it
        query_text = message if has_text else None

        results = await self.search(top, query_text, filter, vectors, use_semantic_ranker, use_semantic_captions)

        return results
