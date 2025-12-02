import logging
import os
import time
from itertools import islice
from typing import Any

import bson
import bson.json_util
import requests
from pymongo.database import Database
from sqlalchemy import text
from sqlalchemy.engine.base import Engine
from clickhouse_driver import Client as CHClient


from kaiwadb.document import Document
from kaiwadb.models.forms import SearchForm, GenerationForm
from kaiwadb.models.responses import GenerationResponse
from kaiwadb.models.instance import (
    MSSQL,
    Instance,
    MariaDB,
    Mongo,
    MySQL,
    Oracle,
    PostgreSQL,
    SQLite,
    ClickHouse,
)
from kaiwadb.models.responses import SearchResponse
from kaiwadb.schema_mapping import map_documents_to_tables


class KaiwaDB:
    """
    A client for interfacing with the KaiwaDB API to generate and execute database queries.

    KaiwaDB provides natural language query generation for various database engines including
    MongoDB, PostgreSQL, MySQL, MSSQL, Oracle, SQLite, and MariaDB. It automatically maps
    document schemas to database tables and generates optimized queries based on natural
    language input.

    Example:
        # Define clean Python schema with field aliasing
        class Product(Document):
            __collection__ = "products"

            product_id: int = Field(..., db_name="id")
            product_name: str = Field(..., db_name="name")
            unit_price: float = Field(..., db_name="price")

        # Initialize KaiwaDB with schema
        kdb = KaiwaDB(
            identifier="ecommerce",
            documents=[Product],
            engine=PostgreSQL(),
            api_key="your-api-key"
        )

        # Use natural language queries
        response = kdb.generate("Find products with unit price over 100")
        # KaiwaDB automatically maps "unit_price" to "price" in generated SQL
    """

    def __init__(
        self,
        *,
        identifier: str,
        documents: list[type[Document]],
        engine: Mongo | PostgreSQL | MySQL | MSSQL | Oracle | SQLite | MariaDB | ClickHouse,
        description: str | None = None,
        api_key: str | None = os.environ.get("KAIWADB_API_KEY", None),
        api_base_url: str = "https://api.kaiwadb.com",
        verbose: bool = False,
    ):
        """
        Initialize a new KaiwaDB client instance.

        Sets up the client with your database schema, establishes API connection,
        and registers the schema with the KaiwaDB service for natural language
        query generation.

        Args:
            identifier: Unique identifier for this database instance. Used to register
                       and identify the schema with the KaiwaDB service. Should be
                       descriptive and unique across your organization.
            documents: List of Document classes that define the database schema.
                      These classes should inherit from Document and use Field()
                      for field definitions with optional db_name mapping.
            engine: Database engine configuration object specifying the target database
                   type and version (Mongo, PostgreSQL, MySQL, etc.).
            description: Optional human-readable description of this database instance.
                        Helps with natural language understanding and team documentation.
            api_key: API key for KaiwaDB service authentication. If not provided,
                    will attempt to read from KAIWADB_API_KEY environment variable.
            api_base_url: Base URL for the KaiwaDB API service. Use the default
                         production URL unless connecting to a development instance.
            verbose: Enable verbose logging for debugging purposes. Useful during
                    development and troubleshooting.

        Raises:
            KeyError: If api_key is not provided and KAIWADB_API_KEY environment
                     variable is not set.
            requests.HTTPError: If schema registration fails with the API due to
                               network issues, authentication problems, or server errors.

        Notes:
            - Schema registration happens automatically during initialization
            - The client validates API connectivity and schema compatibility
            - Field name mappings are processed and stored for query generation
            - All subsequent queries will use the registered schema information
        """
        self.identifier = identifier
        self.documents = documents
        self.engine = engine
        self.description = description
        self.api_key = api_key or os.environ["KAIWADB_API_KEY"]
        self.api_base_url = api_base_url

        if verbose:
            logging.basicConfig(
                level=logging.INFO,
                format="kaiwadb: %(levelname)s - %(message)s",
            )

        self.logger = logging.getLogger(__name__)

        self.logger.info("Initializing KaiwaDB")
        self.logger.info(f"Using apikey: {self.api_key[:5]}***** to connect to {self.api_base_url}")

        self.instance = Instance(
            name=self.identifier,
            description=self.description,
            engine=self.engine,
            tables=map_documents_to_tables(self.documents),
        )

        with open("tables.json", "w") as f:
            f.write(bson.json_util.dumps(self.instance.model_dump(mode="json")["tables"]))

        self._register_schema()

    @property
    def http_headers(self) -> dict[str, str]:
        return {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}",
        }

    def _register_schema(self):
        self.logger.info("Registering database schema")
        res = requests.post(
            f"{self.api_base_url}/schema",
            json=self.instance.model_dump(),
            headers=self.http_headers,
        )
        if res.status_code == 200:
            # TODO: check response if the schema is new or matches the registered one
            self.logger.info("Database schema registered")
            return
        if res.status_code == 409:
            # TODO: check based on the `self.allow_instance_overwrites`
            self.logger.info("Different database schema already registered with the same identifier")
            return
        res.raise_for_status()

    def search(self, query: str, limit: int) -> list:
        self.logger.info(f'Searching pipelines for "{query}"')
        payload = SearchForm(query=query, limit=limit)
        st = time.monotonic()
        res = requests.post(
            f"{self.api_base_url}/schema/{self.identifier}/search",
            json=payload.model_dump(),
            headers=self.http_headers,
        )
        res = SearchResponse(**res.json())
        duration = time.monotonic() - st
        self.logger.info(f"Found {len(res.pipelines)} pipelines in {duration:.2f} seconds")
        return res.pipelines

    def generate(self, query: str) -> GenerationResponse:
        """
        Generate a database query pipeline from a natural language query.

        This method processes natural language input and returns a structured
        query pipeline for your registered database schema and engine.
        The generated queries automatically handle field name translation from
        your clean Python names to the actual database field names.

        The natural language processing understands:
        - Field references using Python names (e.g., "first_name" → "firstName")
        - Aggregations and groupings
        - Filtering and sorting operations
        - Joins between related documents
        - Complex conditional logic

        Args:
            query: Natural language description of the desired database operation.
                  Examples:
                  - "Find all users created in the last 30 days"
                  - "Get average order value by product category"
                  - "Count customers by registration source"
                  - "Show top 10 products by sales volume"

        Returns:
            GenerationResponse: Object containing:
                - assembled.query: The generated database-specific query
                - assembled.target: Target database engine information
                - metadata: Query generation metadata and optimization info

        Raises:
            requests.HTTPError: If the API request fails due to network issues,
                               authentication problems, or server errors.
            bson.errors.InvalidBSON: If the API response contains invalid BSON data.
            ValueError: If the natural language query cannot be parsed or is ambiguous.

        Example:
            >>> kdb = KaiwaDB(identifier="shop", documents=[Product], engine=PostgreSQL())
            >>> response = kdb.generate("Find products with unit price over 100")
            >>> print(response.assembled.query)
            # Generated SQL with proper field mapping:
            # SELECT * FROM products WHERE price > 100
        """
        self.logger.info(f'Generating pipeline for "{query}"')
        payload = GenerationForm(query=query)
        st = time.monotonic()
        res = requests.post(
            f"{self.api_base_url}/schema/{self.identifier}/generate",
            json=payload.model_dump(),
            headers=self.http_headers,
        )
        json = bson.json_util.loads(res.content.decode(res.encoding or "utf-8"))
        res = GenerationResponse(**json)
        duration = time.monotonic() - st
        self.logger.info(f"Generated pipeline in {duration:.2f} seconds")
        return res

    def run(
        self,
        query: str,
        db: Database[Any] | Engine | CHClient,
        limit: int | None = None,
        verbose: bool = False,
    ):
        """
        Generate and execute a database query from natural language input.

        This convenience method combines query generation and execution in a single
        call. It generates the appropriate query pipeline using natural language
        processing, then executes it against your database connection with proper
        field name translation and result formatting.

        The method handles the complexity of:
        - Translating natural language to database queries
        - Converting Python field names to database field names
        - Executing engine-specific operations (MongoDB aggregation vs SQL)

        Args:
            query: Natural language description of the desired database operation.
                  Should reference fields using your clean Python names.
            db: Database connection object. Must match the configured engine type:
                - pymongo.database.Database for MongoDB operations
                - sqlalchemy.engine.base.Engine for SQL database operations
            limit: Optional maximum number of results to return. If None,
                  returns all matching results. Use with caution for large datasets
                  as this loads all results into memory.

        Returns:
            list: List of result documents/rows from the query execution.
                 Format depends on the database engine:
                 - MongoDB: List of dictionaries with document data
                 - SQL databases: List of SQLAlchemy Row objects

        Raises:
            NotImplementedError: If the database connection type doesn't match
                               the configured engine type, or if the engine type
                               combination is not yet supported.
            requests.HTTPError: If query generation fails (see generate() method).
            TypeError: If the db parameter type doesn't match expected types.
            Database-specific exceptions: Various exceptions depending on the
                                        database engine (connection errors, syntax
                                        errors, permission errors, timeout errors, etc.).

        Example:
            >>> # MongoDB example
            >>> import pymongo
            >>> mongo_client = pymongo.MongoClient("mongodb://localhost:27017")
            >>> db = mongo_client.shop_db
            >>> results = kdb.run("Find products with unit price over 100", db, limit=10)
            >>> # Results contain documents with Python field names

            >>> # PostgreSQL example
            >>> from sqlalchemy import create_engine
            >>> engine = create_engine("postgresql://user:pass@localhost/shop")
            >>> results = kdb.run("Get average price by category", engine, limit=50)
            >>> # Results contain Row objects with proper field mapping
        """
        pipeline = self.generate(query)
        if verbose:
            self.logger.info(f"assembled:\n{pipeline.assembled.query}")

        match (db, pipeline.assembled.target):
            case (Database(), Mongo()):
                cursor = db.get_collection(pipeline.assembled.query["collection"]).aggregate(
                    pipeline.assembled.query["pipeline"]
                )
                docs = list(islice(cursor, limit))
                return docs
            case (Engine(), PostgreSQL()):
                with db.connect() as conn:
                    cursor = conn.execute(text(pipeline.assembled.query))
                    docs = list(islice(cursor, limit))
                    return docs
            case (CHClient(), ClickHouse()):
                result = db.query_dataframe(pipeline.assembled.query).head(limit)
                return result
            case (db, target):
                raise NotImplementedError(
                    f"Cannot run pipeline assembled for `{repr(target)}` on `{type(db)}`"
                )
