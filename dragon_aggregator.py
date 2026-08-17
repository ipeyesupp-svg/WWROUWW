#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dragon API Aggregator
Единый endpoint для поиска по всем API из dragon.py
Запуск: uvicorn dragon_aggregator:app --host 0.0.0.0 --port 8000 --workers 1
"""

import os
import time
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote
from typing import Dict, Any, List, Optional
import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

# ==================== CONFIG ====================

class Config:
    # Sherk
    SHERK_KEY = "skay-faebc94beacccb0c720246851be162bd"
    SHERK_URL = "https://sherk.pro/api/v1/search"
    SHERK_RPS = 1.1

    # Night
    NIGHT_KEY = "sk_66beac29ce86f915b184a9ddde7aecbfc6177ab265cf5c1f579ce53219422234"
    NIGHT_URL = "https://nightsearch.life/api/search"
    NIGHT_RPS = 1.1

    # Snusbase
    SNUS_KEYS = ["sby0b7crta98od7efbb8zr70788n2h"]
    SNUS_URL = "https://api.snusbase.com"
    SNUS_RPS = 1.1

    # Dep
    DEP_TOKEN = "OsMTcjyHTRtfABnWA4V3d12SYKVIYE8z"
    DEP_URL = "https://api.depsearch.sbs"
    DEP_RPS = 1.1

    # Ofdata
    OFDATA_KEY = "KBnpz1CHKNngFXxK"
    OFDATA_URL = "https://api.ofdata.ru/v2"
    OFDATA_RPS = 1.1


# ==================== BASE CLIENT ====================

class BaseClient:
    name: str = "base"
    _lock = threading.Lock()
    _last_request: float = 0.0
    _min_interval: float = 1.0

    def _rate_limit(self):
        with self._lock:
            elapsed = time.time() - self._last_request
            if elapsed < self._min_interval:
                time.sleep(self._min_interval - elapsed)
            self._last_request = time.time()

    def _headers(self) -> dict:
        return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        }

    def search(self, query: str, search_type: str) -> Dict[str, Any]:
        raise NotImplementedError


# ==================== API CLIENTS ====================

class SherkClient(BaseClient):
    name = "sherk"
    _min_interval = Config.SHERK_RPS

    def search(self, query: str, search_type: str) -> Dict[str, Any]:
        self._rate_limit()
        params = {"key": Config.SHERK_KEY, "query": query, "type": search_type}
        try:
            r = requests.get(Config.SHERK_URL, params=params, headers=self._headers(), timeout=30)
            r.raise_for_status()
            return {"ok": True, "data": r.json(), "status_code": r.status_code}
        except requests.exceptions.HTTPError as e:
            try:
                return {"ok": False, "error": e.response.json(), "status_code": e.response.status_code}
            except ValueError:
                return {"ok": False, "error": f"HTTP {e.response.status_code}: {e.response.text[:200]}", "status_code": e.response.status_code}
        except Exception as e:
            return {"ok": False, "error": str(e), "status_code": 0}


class NightClient(BaseClient):
    name = "night"
    _min_interval = Config.NIGHT_RPS

    def search(self, query: str, search_type: str) -> Dict[str, Any]:
        self._rate_limit()
        payload = {"query": query, "search_type": search_type}
        headers = {**self._headers(), "X-API-Key": Config.NIGHT_KEY, "Content-Type": "application/json"}
        try:
            r = requests.post(Config.NIGHT_URL, json=payload, headers=headers, timeout=30)
            r.raise_for_status()
            return {"ok": True, "data": r.json(), "status_code": r.status_code}
        except requests.exceptions.HTTPError as e:
            try:
                return {"ok": False, "error": e.response.json(), "status_code": e.response.status_code}
            except ValueError:
                return {"ok": False, "error": f"HTTP {e.response.status_code}: {e.response.text[:200]}", "status_code": e.response.status_code}
        except Exception as e:
            return {"ok": False, "error": str(e), "status_code": 0}


class SnusbaseClient(BaseClient):
    name = "snusbase"
    _min_interval = Config.SNUS_RPS

    def _request(self, endpoint: str, payload: dict) -> Dict[str, Any]:
        self._rate_limit()
        headers = {**self._headers(), "Auth": Config.SNUS_KEYS[0], "Content-Type": "application/json"}
        try:
            r = requests.post(f"{Config.SNUS_URL}{endpoint}", json=payload, headers=headers, timeout=30)
            out = {"_rate_reset": r.headers.get("X-Rate-Limit-Reset"), "_rate_remain": r.headers.get("X-Rate-Limit-Remaining")}
            if r.status_code == 429:
                try:
                    out.update(r.json())
                except:
                    pass
                out.update({"ok": False, "error": "RATE LIMIT 429", "status_code": 429})
                return out
            r.raise_for_status()
            try:
                j = r.json()
                if isinstance(j, dict):
                    out.update(j)
                out.update({"ok": True, "status_code": r.status_code})
                return out
            except:
                return {"ok": False, "error": "Invalid JSON", "status_code": r.status_code}
        except requests.exceptions.HTTPError as e:
            out = {"ok": False, "error": f"HTTP {e.response.status_code}: {e.response.text[:200]}", "status_code": e.response.status_code}
            reset = e.response.headers.get("X-Rate-Limit-Reset")
            if reset:
                out["retry_after"] = reset
            return out
        except Exception as e:
            return {"ok": False, "error": str(e), "status_code": 0}

    def search(self, query: str, search_type: str) -> Dict[str, Any]:
        if search_type == "email_search":
            return self._request("/data/search", {"terms": [query], "types": ["email"]})
        elif search_type == "email_combo":
            return self._request("/tools/combo-lookup", {"terms": [query], "types": ["username"]})
        elif search_type == "password_search":
            return self._request("/data/search", {"terms": [query], "types": ["password"]})
        elif search_type == "password_combo":
            return self._request("/tools/combo-lookup", {"terms": [query], "types": ["password"]})
        return {"ok": False, "error": f"Unknown snusbase search_type: {search_type}"}


class DepClient(BaseClient):
    name = "dep"
    _min_interval = Config.DEP_RPS

    def search(self, query: str, search_type: str) -> Dict[str, Any]:
        self._rate_limit()
        url = f"{Config.DEP_URL}/quest={quote(query, safe=':@')}&token={Config.DEP_TOKEN}"
        try:
            r = requests.get(url, headers=self._headers(), timeout=30)
            if r.status_code == 429:
                out = {"error": "RATE LIMIT 429", "status_code": 429}
                retry = r.headers.get("Retry-After")
                if retry:
                    out["retry_after"] = retry
                try:
                    out.update(r.json())
                except:
                    pass
                return out
            r.raise_for_status()
            return {"ok": True, "data": r.json(), "status_code": r.status_code}
        except requests.exceptions.HTTPError as e:
            try:
                return {"ok": False, "error": e.response.json(), "status_code": e.response.status_code}
            except ValueError:
                return {"ok": False, "error": f"HTTP {e.response.status_code}: {e.response.text[:200]}", "status_code": e.response.status_code}
        except Exception as e:
            return {"ok": False, "error": str(e), "status_code": 0}


class OfdataClient(BaseClient):
    name = "ofdata"
    _min_interval = Config.OFDATA_RPS

    def search(self, query: str, search_type: str, extra: Optional[dict] = None) -> Dict[str, Any]:
        self._rate_limit()
        params = {"key": Config.OFDATA_KEY}
        if extra:
            params.update(extra)
        try:
            r = requests.get(f"{Config.OFDATA_URL}{query}", params=params, headers=self._headers(), timeout=30)
            r.raise_for_status()
            return {"ok": True, "data": r.json(), "status_code": r.status_code}
        except requests.exceptions.HTTPError as e:
            try:
                return {"ok": False, "error": e.response.json(), "status_code": e.response.status_code}
            except ValueError:
                return {"ok": False, "error": f"HTTP {e.response.status_code}: {e.response.text[:200]}", "status_code": e.response.status_code}
        except Exception as e:
            return {"ok": False, "error": str(e), "status_code": 0}


# ==================== AGGREGATOR ====================

class DragonAggregator:
    def __init__(self):
        self.sherk = SherkClient()
        self.night = NightClient()
        self.snus = SnusbaseClient()
        self.dep = DepClient()
        self.ofdata = OfdataClient()

        self.routes = {
            "username": [
                (self.sherk.search, ["@{}", "username"], {}, "sherk"),
                (self.night.search, ["{}", "tg"], {}, "night"),
            ],
            "id": [
                (self.sherk.search, ["{}", "id"], {}, "sherk"),
                (self.night.search, ["{}", "tg"], {}, "night"),
            ],
            "phone": [
                (self.sherk.search, ["{}", "phone"], {}, "sherk"),
                (self.night.search, ["{}", "phone"], {}, "night"),
                (self.dep.search, ["{}", "phone"], {}, "dep"),
            ],
            "fio": [
                (self.sherk.search, ["{}", "fio"], {}, "sherk"),
                (self.night.search, ["{}", "fio"], {}, "night"),
                (self.dep.search, ["{}", "fio"], {}, "dep"),
            ],
            "email": [
                (self.sherk.search, ["{}", "email"], {}, "sherk"),
                (self.night.search, ["{}", "email"], {}, "night"),
                (self.dep.search, ["{}", "email"], {}, "dep"),
                (self.snus.search, ["{}", "email_search"], {}, "snus"),
                (self.snus.search, ["{}", "email_combo"], {}, "snus_combo"),
            ],
            "car": [
                (self.sherk.search, ["{}", "car"], {}, "sherk"),
                (self.dep.search, ["{}", "car"], {}, "dep"),
            ],
            "vin": [
                (self.sherk.search, ["{}", "vin"], {}, "sherk"),
                (self.night.search, ["{}", "car"], {}, "night"),
                (self.dep.search, ["{}", "vin"], {}, "dep"),
            ],
            "vk": [
                (self.sherk.search, ["{}", "vk"], {}, "sherk"),
                (self.night.search, ["{}", "vk"], {}, "night"),
                (self.dep.search, ["{}", "vk"], {}, "dep"),
            ],
            "passport": [
                (self.sherk.search, ["{}", "passport"], {}, "sherk"),
            ],
            "inn": [
                (self.sherk.search, ["{}", "inn"], {}, "sherk"),
                (self.night.search, ["{}", "inn"], {}, "night"),
                (self.dep.search, ["inn{}", "inn"], {}, "dep"),
                (self.ofdata.search, ["/company", "inn"], {"extra": {"inn": "{}"}}, "ofdata"),
            ],
            "snils": [
                (self.sherk.search, ["{}", "snils"], {}, "sherk"),
                (self.night.search, ["{}", "snils"], {}, "night"),
                (self.dep.search, ["snils{}", "snils"], {}, "dep"),
            ],
            "ogrn": [
                (self.sherk.search, ["{}", "ogrn"], {}, "sherk"),
                (self.ofdata.search, ["/company", "ogrn"], {"extra": {"ogrn": "{}"}}, "ofdata"),
            ],
            "ip": [
                (self.night.search, ["{}", "ip"], {}, "night"),
                (self.dep.search, ["{}", "ip"], {}, "dep"),
            ],
            "nick": [
                (self.night.search, ["{}", "nick"], {}, "night"),
                (self.dep.search, ["nick:{}", "nick"], {}, "dep"),
            ],
            "ok": [
                (self.night.search, ["{}", "ok"], {}, "night"),
            ],
            "fb": [
                (self.night.search, ["{}", "fb"], {}, "night"),
            ],
            "password": [
                (self.dep.search, ["pass:{}", "password"], {}, "dep"),
                (self.snus.search, ["{}", "password_search"], {}, "snus"),
                (self.snus.search, ["{}", "password_combo"], {}, "snus_combo"),
            ],
            "tiktok": [
                (self.dep.search, ["tt:{}", "tiktok"], {}, "dep"),
            ],
            "address": [
                (self.dep.search, ["addr:{}", "address"], {}, "dep"),
            ],
            "company": [
                (self.ofdata.search, ["/search", "company"], {"extra": {"by": "name", "obj": "org", "query": "{}"}}, "ofdata"),
            ],
        }

    def _format_args(self, template_args: list, query: str) -> list:
        out = []
        for arg in template_args:
            if isinstance(arg, str) and "{}" in arg:
                out.append(arg.replace("{}", query))
            else:
                out.append(arg)
        return out

    def _format_kwargs(self, template_kwargs: dict, query: str) -> dict:
        out = {}
        for k, v in template_kwargs.items():
            if isinstance(v, str) and "{}" in v:
                out[k] = v.replace("{}", query)
            elif isinstance(v, dict):
                out[k] = self._format_kwargs(v, query)
            else:
                out[k] = v
        return out

    def search(self, search_type: str, query: str, max_workers: int = 10) -> Dict[str, Any]:
        if search_type not in self.routes:
            raise ValueError(f"Unknown search type: {search_type}. Available: {list(self.routes.keys())}")

        tasks = self.routes[search_type]
        results = {}
        errors = {}
        start = time.time()

        def run_task(func, args, kwargs, alias):
            try:
                fmt_args = self._format_args(args, query)
                fmt_kwargs = self._format_kwargs(kwargs, query)
                res = func(*fmt_args, **fmt_kwargs)
                return alias, res
            except Exception as e:
                return alias, {"ok": False, "error": str(e)}

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(run_task, func, args, kwargs, alias): alias
                for func, args, kwargs, alias in tasks
            }
            for future in as_completed(futures):
                alias, res = future.result()
                if not res.get("ok") and "error" in res:
                    errors[alias] = res
                else:
                    results[alias] = res

        elapsed = round((time.time() - start) * 1000, 2)
        return {
            "query": query,
            "type": search_type,
            "results": results,
            "errors": errors,
            "sources_total": len(tasks),
            "sources_success": len(results),
            "sources_failed": len(errors),
            "elapsed_ms": elapsed,
        }


# ==================== FASTAPI APP ====================

app = FastAPI(
    title="Dragon API Aggregator",
    description="Единый endpoint для поиска по всем API из dragon.py",
    version="1.0.0",
)

aggregator = DragonAggregator()


class SearchRequest(BaseModel):
    type: str = Field(..., description="Тип поиска", examples=["phone", "email", "inn", "username"])
    query: str = Field(..., description="Запрос", examples=["+79991234567", "ivan@mail.ru"])


class SearchResponse(BaseModel):
    query: str
    type: str
    results: Dict[str, Any]
    errors: Dict[str, Any]
    sources_total: int
    sources_success: int
    sources_failed: int
    elapsed_ms: float


@app.post("/api/v1/search", response_model=SearchResponse, tags=["Search"])
def search(req: SearchRequest):
    try:
        return aggregator.search(req.type, req.query.strip())
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health", tags=["System"])
def health():
    return {"status": "ok", "timestamp": time.time()}


@app.get("/api/v1/types", tags=["System"])
def list_types():
    mapping = {}
    for t, tasks in aggregator.routes.items():
        mapping[t] = [alias for _, _, _, alias in tasks]
    return mapping


# ==================== ENTRYPOINT ====================

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("dragon_aggregator:app", host="0.0.0.0", port=port, reload=False)
