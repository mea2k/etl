"""
Экстрактор для извлечения данных из REST API
"""
import os
import sys
import logging
from wsgiref import headers
import requests

from typing import Dict, Any, List, Optional

from airflow.providers.http.hooks.http import HttpHook

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '.'))

from base_extractor import BaseExtractor

logger = logging.getLogger(__name__)


class APIExtractor(BaseExtractor):
    """Класс для извлечения данных из REST API"""
    
    def __init__(self, conn_id: str = None, 
                 base_url: str = None, 
                 config: Optional[Dict[str, Any]] = None):
        super().__init__(conn_id)
        self.base_url = base_url
        self.hook = None
        logger.info(f"APIExtractor initialized: {self.conn_id}")

    def extract(self, endpoint: str, method: str = 'GET', 
                params: Optional[Dict] = None, headers: Optional[Dict] = None,
                **kwargs) -> List[Dict[str, Any]]:
        """Извлечение данных из API"""
        try:
            if self.conn_id:
                self.hook = HttpHook(http_conn_id=self.conn_id, method=method)
                normalized_headers = self._normalize_headers(headers)
                response = self.hook.run(endpoint, data=params, headers=normalized_headers)
                result = response.json()
            else:
                url = f"{self.base_url}{endpoint}"
                logger.info(f"Requesting: {url}")
                normalized_headers = self._normalize_headers(headers)
                response = requests.request(method, url, params=params, headers=normalized_headers)
                response.raise_for_status()
                result = response.json()
            
            # Преобразование в список если нужно
            if isinstance(result, dict):
                result = [result]
            elif not isinstance(result, list):
                result = []
            
            logger.info(f"Extracted {len(result)} records from API")
            return result
        except Exception as e:
            return []


    def _normalize_headers(self, headers: Optional[Dict]) -> Dict[str, str]:
        """
        Нормализует headers: все значения должны быть str.
        """
        if not headers:
            return {}
        normalized = {}
        for key, value in headers.items():
            if isinstance(key, str) and isinstance(value, str):
                normalized[key] = value
            elif isinstance(value, dict):
                logger.warning(f"Skipping nested header {key}: {value}")
            else:
                normalized[str(key)] = str(value)
        # Стандартный User-Agent для Airflow
        normalized.setdefault('User-Agent', 'Airflow-ETL/1.0')        
        return normalized