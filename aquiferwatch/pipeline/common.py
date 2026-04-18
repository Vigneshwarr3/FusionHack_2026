"""Shared pipeline utilities — retry, logging, HTTP session.

Mirrors the patterns in Agricultural_Data_Analysis/backend/etl/common.py so ingests
here feel familiar. The RDS engine + S3 client are imported from the parent project
once integration lands; for now we keep a local logger + retry session.
"""

from __future__ import annotations

import logging

import requests
from requests.adapters import HTTPAdapter
from tenacity import retry, stop_after_attempt, wait_exponential
from urllib3.util.retry import Retry

from aquiferwatch.config import settings


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s %(name)s [%(levelname)s] %(message)s",
                datefmt="%H:%M:%S",
            )
        )
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


def http_session(user_agent: str | None = None) -> requests.Session:
    """Session with backoff on 429/5xx. Use for every external API call."""
    session = requests.Session()
    session.headers["User-Agent"] = user_agent or settings.usgs_user_agent
    retry_cfg = Retry(
        total=5,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retry_cfg)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


tenacious = retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=30),
)
