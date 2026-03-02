"""
Database Access Object (DAO) Module.

This module handles all interactions with the PostgreSQL database, including
connection management, query execution, and specific report retrieval.
It implements robust error handling, retry logic, and type safety.
"""

import os
import time
import logging
from typing import List, Dict, Any, Optional, Generator
from contextlib import contextmanager

import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
load_dotenv()


class DatabaseManager:
    """Manages database connections and query execution using a connection pool."""

    _pool: Optional[ThreadedConnectionPool] = None

    @classmethod
    def _get_pool(cls) -> ThreadedConnectionPool:
        """
        Initializes and returns the database connection pool.
        
        Returns:
            ThreadedConnectionPool: The active connection pool.
        
        Raises:
            Exception: If the pool cannot be created.
        """
        if cls._pool is None:
            try:
                cls._pool = ThreadedConnectionPool(
                    minconn=1,
                    maxconn=5,
                    host=os.getenv("DB_HOST"),
                    database=os.getenv("DB_NAME"),
                    user=os.getenv("DB_USER"),
                    password=os.getenv("DB_PASS"),
                    port=os.getenv("DB_PORT"),
                    connect_timeout=10
                )
                logger.info("✅ Database connection pool initialized.")
            except Exception as e:
                logger.critical(f"❌ Failed to initialize database pool: {e}")
                raise
        return cls._pool

    @classmethod
    @contextmanager
    def get_cursor(cls) -> Generator[Any, None, None]:
        """
        Context manager that yields a database cursor from the pool.
        
        Yields:
            psycopg2.extensions.cursor: A RealDictCursor for executing queries.
        """
        pool = cls._get_pool()
        conn = pool.getconn()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                yield cur
        except Exception as e:
            logger.error(f"❌ Database operation failed: {e}")
            raise
        finally:
            pool.putconn(conn)

    @classmethod
    def execute_query(cls, query: str, params: Optional[tuple] = None, max_retries: int = 3) -> List[Dict[str, Any]]:
        """
        Executes a SQL query with automatic retry logic.

        Args:
            query: The SQL query string.
            params: Optional parameters for the query.
            max_retries: Maximum number of retry attempts.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries representing the rows found.
        """
        for attempt in range(max_retries):
            try:
                with cls.get_cursor() as cur:
                    cur.execute(query, params)
                    return cur.fetchall()
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.warning(f"⚠️ Query attempt {attempt + 1} failed: {e}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"❌ Query failed after {max_retries} attempts.")
                    return []
            except Exception as e:
                logger.error(f"❌ Unexpected error executing query: {e}")
                return []
        return []

# ==========================================
# Report Functions
# ==========================================

def get_proactive_report() -> List[Dict[str, Any]]:
    """
    Retrieves the Proactive Inventory Report.
    
    Evaluates ONLY properties, ignoring reservations.
    
    Filter criteria:
    - Has an offboarding date.
    - Status is still ACTIVE.
    - Blockoff date was more than 30 days ago.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries representing the rows found.
    """
    query = """
        SELECT DISTINCT
            mv.country AS "country",
            mv.nickname AS "property",
            mv.offboarding_guesty AS "offboarding_date",
            gl.data ->> 'active' AS "status_json"
        FROM mv_listings mv
        -- INNER JOIN: Only properties that actually exist in Guesty
        JOIN guesty_listing gl ON mv.nickname = gl.nickname
        WHERE 
            mv.offboarding_guesty IS NOT NULL
            AND gl.data ->> '_id' IS NOT NULL
            
            -- CONDITION 1: Still ACTIVE in Guesty
            AND lower(trim(gl.data ->> 'active')) = 'true'
            
            -- CONDITION 2: Blockoff happened more than 30 days ago
            AND mv.offboarding_guesty < (CURRENT_DATE - INTERVAL '30 days')
            
        ORDER BY mv.offboarding_guesty ASC;
    """
    return DatabaseManager.execute_query(query)

def get_active_alerts_report() -> List[Dict[str, Any]]:
    """
    Retrieves the Active Alerts Report.
    
    Filter criteria:
    - Reservation status is 'confirmed'.
    - Property status is 'ACTIVE'.
    - VIOLATION: Check-out date is after the Offboarding date.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries representing the critical reservations.
    """
    query = """
        SELECT 
            mv.country AS "country",
            r."LISTING'S NICKNAME" AS "property",
            r."CONFIRMATION CODE" AS "confirmation_code",
            mv.offboarding_guesty AS "offboarding_date",
            gl.data ->> 'active' AS "status_json",
            TO_DATE(regexp_replace(r."CHECK IN", '[[:space:]]+', '', 'g'), 'DD/MM/YYYY') AS "check_in_date",
            TO_DATE(regexp_replace(r."CHECK OUT", '[[:space:]]+', '', 'g'), 'DD/MM/YYYY') AS "check_out_date"
        FROM reservation_gold r
        JOIN mv_listings mv ON r."LISTING'S NICKNAME" = mv.nickname
        -- INNER JOIN: Only properties that actually exist in Guesty
        JOIN guesty_listing gl ON r."LISTING'S NICKNAME" = gl.nickname
        WHERE 
            lower(trim(r."STATUS")) = 'confirmed'
            AND mv.offboarding_guesty IS NOT NULL
            AND gl.data ->> '_id' IS NOT NULL
            
            -- CONDITION 1: Still ACTIVE in Guesty
            AND lower(trim(gl.data ->> 'active')) = 'true'
            
            -- CONDITION 2: THE ALERT (Check out is greater than Blockoff)
            AND TO_DATE(regexp_replace(r."CHECK OUT", '[[:space:]]+', '', 'g'), 'DD/MM/YYYY') > mv.offboarding_guesty
            
        ORDER BY "check_out_date" ASC;
    """
    return DatabaseManager.execute_query(query)

def get_reactive_report() -> List[Dict[str, Any]]:
    """
    Retrieves the Reactive/Historical Report.
    
    Filter criteria:
    - Reservation status is 'confirmed'.
    - Property status is 'INACTIVE' (already offboarded).
    - VIOLATION: Check-out date was after the Offboarding date.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries representing historical violations.
    """
    query = """
        SELECT 
            mv.country AS "country",
            r."LISTING'S NICKNAME" AS "property",
            r."CONFIRMATION CODE" AS "confirmation_code",
            mv.offboarding_guesty AS "offboarding_date",
            gl.data ->> 'active' AS "status_json",
            TO_DATE(regexp_replace(r."CHECK IN", '[[:space:]]+', '', 'g'), 'DD/MM/YYYY') AS "check_in_date",
            TO_DATE(regexp_replace(r."CHECK OUT", '[[:space:]]+', '', 'g'), 'DD/MM/YYYY') AS "check_out_date"
        FROM reservation_gold r
        JOIN mv_listings mv ON r."LISTING'S NICKNAME" = mv.nickname
        -- INNER JOIN: Only properties that actually exist in Guesty
        JOIN guesty_listing gl ON r."LISTING'S NICKNAME" = gl.nickname
        WHERE 
            lower(trim(r."STATUS")) = 'confirmed'
            AND mv.offboarding_guesty IS NOT NULL
            AND gl.data ->> '_id' IS NOT NULL
            
            -- CONDITION 1: Already INACTIVE in Guesty (Historical Record)
            AND lower(trim(gl.data ->> 'active')) = 'false'
            
            -- CONDITION 2: THE ALERT (Check out was greater than Blockoff)
            AND TO_DATE(regexp_replace(r."CHECK OUT", '[[:space:]]+', '', 'g'), 'DD/MM/YYYY') > mv.offboarding_guesty
            
        ORDER BY "check_out_date" DESC;
    """
    return DatabaseManager.execute_query(query)