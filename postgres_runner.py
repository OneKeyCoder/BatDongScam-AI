"""PostgreSQL database runner for Vanna AI."""
import pandas as pd
from typing import Optional
from vanna.capabilities.sql_runner import SqlRunner, RunSqlToolArgs
from vanna.core.tool import ToolContext
import asyncpg
import asyncio


class PostgresRunner(SqlRunner):
    """PostgreSQL implementation of SqlRunner using asyncpg."""
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 5433,
        database: str = "vanna",
        user: str = "postgres",
        password: str = "secret",
        **kwargs
    ):
        """Initialize PostgreSQL connection parameters.
        
        Args:
            host: Database host
            port: Database port
            database: Database name
            user: Database user
            password: Database password
            **kwargs: Additional connection parameters
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.kwargs = kwargs
        self._pool: Optional[asyncpg.Pool] = None
    
    async def _get_pool(self) -> asyncpg.Pool:
        """Get or create connection pool."""
        if self._pool is None:
            self._pool = await asyncpg.create_pool(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                **self.kwargs
            )
        return self._pool
    
    async def run_sql(self, args: RunSqlToolArgs, context: ToolContext) -> pd.DataFrame:
        """Execute SQL query and return results as DataFrame.
        
        Args:
            args: Tool arguments containing the SQL query
            context: Tool execution context
            
        Returns:
            pandas DataFrame with query results
        """
        pool = await self._get_pool()
        
        async with pool.acquire() as conn:
            # Determine query type
            query_type = args.sql.strip().upper().split()[0]
            
            if query_type == "SELECT":
                # For SELECT queries, fetch all rows
                rows = await conn.fetch(args.sql)
                
                if not rows:
                    # Return empty DataFrame with no columns
                    return pd.DataFrame()
                
                # Convert to DataFrame
                df = pd.DataFrame([dict(row) for row in rows])
                return df
            
            else:
                # For INSERT, UPDATE, DELETE, etc.
                result = await conn.execute(args.sql)
                
                # Extract number of affected rows from result string
                # e.g., "INSERT 0 5" means 5 rows inserted
                parts = result.split()
                rows_affected = int(parts[-1]) if parts else 0
                
                # Return DataFrame with affected row count
                return pd.DataFrame({'rows_affected': [rows_affected]})
    
    async def close(self):
        """Close the connection pool."""
        if self._pool:
            await self._pool.close()
            self._pool = None
    
    def __del__(self):
        """Cleanup on deletion."""
        if self._pool:
            try:
                asyncio.get_event_loop().run_until_complete(self.close())
            except:
                pass
