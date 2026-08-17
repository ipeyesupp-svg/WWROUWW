#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dragon OSINT API Aggregator
═══════════════════════════
FastAPI-приложение, которое параллельно опрашивает все OSINT-источники
и возвращает объединённый JSON.

Запуск:
  uvicorn app:app --host 0.0.0.0 --port 8000 --reload

Swagger UI:  http://localhost:8000/docs
ReDoc:       http://localhost:8000/redoc
"""

import asyncio
import re
import time
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# ─────────────────────────────────────────────────────────────────────────────
#  КОНФИГУРАЦИЯ API-КЛЮЧЕЙ
# ─────────────────────────────────────────────────────────────────────────────

SHERK_KEY  = "skay-faebc94beacccb0c720246851be162bd"
SHERK_URL  = "https://sherk.pro/api/v1/search"

NIGHT_KEY  = "sk_66beac29ce86f915b184a9ddde7aecbfc6177ab265cf5c1f579ce53219422234"
NIGHT_URL  = "https://nightsearch.life/api/search"

SNUS_KEY   = "sby0b7crta98od7efbb8zr70788n2h"
SNUS_URL   = "https://api.snusbase.com"

DEP_TOKEN  = "TKeRG1ONMsqUrGIUeuTPbXegGPiwMpJ5"
DEP_URL    = "https://api.depsearch.sbs"

OFDATA_KEY = "KBnpz1CHKNngFXxK"
OFDATA_URL = "https://api.ofdata.ru/v2"

REQUEST_TIMEOUT = 30.0   # сек на один upstream-запрос
RATE_INTERVAL   = 1.1    # мин. интервал между запросами к одному источнику

COMMON_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":     "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

# ─────────────────────────────────────────────────────────────────────────────
#  ДОПУСТИМЫЕ ТИПЫ ПОИСКА
# ─────────────────────────────────────────────────────────────────────────────

SEARCH_TYPES = {
    "username":  "Telegram username (@user)",
    "tg_id":     "Telegram numeric ID",
    "phone":     "Номер телефона",
    "fio":       "ФИО (и дата рождения)",
    "email":     "Email-адрес",
    "car":       "Гос. номер авто (ГРЗ)",
    "vin":       "VIN-код автомобиля",
    "vk":        "VKontakte URL или ID",
    "passport":  "Паспорт РФ (10 цифр)",
    "inn":       "ИНН (10 или 12 цифр)",
    "snils":     "СНИЛС (11 цифр)",
    "ogrn":      "ОГРН (13 или 15 цифр)",
    "ip":        "IP-адрес",
    "nick":      "Никнейм",
    "ok":        "Одноклассники ID / username",
    "fb":        "Facebook ID / username",
    "password":  "Пароль",
    "tiktok":    "TikTok username",
    "address":   "Адрес (город, улица, дом)",
    "company":   "Название компании",
}

# Какие источники вызывать для каждого типа поиска
# ключ → список source_id
SOURCE_MAP: Dict[str, List[str]] = {
    "username": ["sherk", "night"],
    "tg_id":    ["sherk", "night"],
    "phone":    ["sherk", "night", "depsearch"],
    "fio":      ["sherk", "night", "depsearch"],
    "email":    ["sherk", "night", "depsearch", "snusbase"],
    "car":      ["sherk", "depsearch"],
    "vin":      ["sherk", "night", "depsearch"],
    "vk":       ["sherk", "night", "depsearch"],
    "passport": ["sherk"],
    "inn":      ["sherk", "night", "depsearch", "ofdata"],
    "snils":    ["sherk", "night", "depsearch"],
    "ogrn":     ["sherk", "ofdata"],
    "ip":       ["night", "depsearch"],
    "nick":     ["night", "depsearch"],
    "ok":       ["night"],
    "fb":       ["night"],
    "password": ["depsearch", "snusbase"],
    "tiktok":   ["depsearch"],
    "address":  ["depsearch"],
    "company":  ["ofdata"],
}

# ─────────────────────────────────────────────────────────────────────────────
#  ASYNC RATE LIMITER
# ─────────────────────────────────────────────────────────────────────────────

class RateLimiter:
    """Обеспечивает минимальный интервал между запросами к одному источнику."""

    def __init__(self, interval: float = RATE_INTERVAL):
        self._interval = interval
        self._lock     = asyncio.Lock()
        self._last     = 0.0

    async def wait(self):
        async with self._lock:
            now   = time.monotonic()
            delta = now - self._last
            if delta < self._interval:
                await asyncio.sleep(self._interval - delta)
            self._last = time.monotonic()

_rate: Dict[str, RateLimiter] = {}   # заполняется в lifespan

# ─────────────────────────────────────────────────────────────────────────────
#  ASYNC HTTP-КЛИЕНТ (один на всё приложение)
# ─────────────────────────────────────────────────────────────────────────────

_http: httpx.AsyncClient = None   # type: ignore

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _http, _rate
    _http = httpx.AsyncClient(
        timeout=REQUEST_TIMEOUT,
        headers=COMMON_HEADERS,
        follow_redirects=True,
    )
    _rate = {src: RateLimiter() for src in ["sherk", "night", "snusbase", "depsearch", "ofdata"]}
    yield
    await _http.aclose()

# ─────────────────────────────────────────────────────────────────────────────
#  ФУНКЦИИ ЗАПРОСОВ К ИСТОЧНИКАМ
# ─────────────────────────────────────────────────────────────────────────────

async def _sherk(query: str, search_type: str) -> Dict[str, Any]:
    """Sherk.pro API — GET /api/v1/search?key=&query=&type="""
    await _rate["sherk"].wait()
    try:
        r = await _http.get(SHERK_URL, params={"key": SHERK_KEY, "query": query, "type": search_type})
        r.raise_for_status()
        return r.json()
    except httpx.HTTPStatusError as e:
        try:
            return e.response.json()
        except Exception:
            return {"ok": False, "error": f"HTTP {e.response.status_code}"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


async def _night(query: str, search_type: str) -> Dict[str, Any]:
    """NightSearch API — POST /api/search"""
    await _rate["night"].wait()
    try:
        r = await _http.post(
            NIGHT_URL,
            json={"query": query, "search_type": search_type},
            headers={"X-API-Key": NIGHT_KEY},
        )
        r.raise_for_status()
        return r.json()
    except httpx.HTTPStatusError as e:
        try:
            return e.response.json()
        except Exception:
            return {"ok": False, "error": f"HTTP {e.response.status_code}"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


async def _snus(endpoint: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """Snusbase API — POST /<endpoint>"""
    await _rate["snusbase"].wait()
    try:
        r = await _http.post(
            SNUS_URL + endpoint,
            json=payload,
            headers={"Auth": SNUS_KEY},
        )
        out: Dict[str, Any] = {
            "_rate_reset":   r.headers.get("X-Rate-Limit-Reset"),
            "_rate_remain":  r.headers.get("X-Rate-Limit-Remaining"),
        }
        if r.status_code == 429:
            out.update({"ok": False, "error": "RATE LIMIT 429"})
            try:
                out.update(r.json())
            except Exception:
                pass
            return out
        r.raise_for_status()
        out.update(r.json())
        return out
    except httpx.HTTPStatusError as e:
        return {"ok": False, "error": f"HTTP {e.response.status_code}: {e.response.text[:200]}"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


async def _dep(query: str) -> Dict[str, Any]:
    """DepSearch API — GET /quest=<query>&token=<token>"""
    await _rate["depsearch"].wait()
    encoded = quote(query, safe=":@")
    url     = f"{DEP_URL}/quest={encoded}&token={DEP_TOKEN}"
    try:
        r = await _http.get(url)
        if r.status_code == 429:
            out: Dict[str, Any] = {"error": "RATE LIMIT 429"}
            retry = r.headers.get("Retry-After")
            if retry:
                out["retry_after"] = retry
            try:
                out.update(r.json())
            except Exception:
                pass
            return out
        r.raise_for_status()
        return r.json()
    except httpx.HTTPStatusError as e:
        try:
            return e.response.json()
        except Exception:
            return {"error": f"HTTP {e.response.status_code}"}
    except Exception as e:
        return {"error": str(e)}


async def _ofdata(endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """OfData API — GET /v2/<endpoint>?key=&..."""
    await _rate["ofdata"].wait()
    params = {**params, "key": OFDATA_KEY}
    try:
        r = await _http.get(OFDATA_URL + endpoint, params=params)
        r.raise_for_status()
        return r.json()
    except httpx.HTTPStatusError as e:
        try:
            return e.response.json()
        except Exception:
            return {"ok": False, "error": f"HTTP {e.response.status_code}"}
    except Exception as e:
        return {"ok": False, "error": str(e)}

# ─────────────────────────────────────────────────────────────────────────────
#  НОРМАЛИЗАЦИЯ РЕЗУЛЬТАТОВ
# ─────────────────────────────────────────────────────────────────────────────

def _norm_sherk_night(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Нормализует ответ Sherk / NightSearch / Snusbase (формат {took, size, results})."""
    if not isinstance(raw, dict):
        return {"found": False, "count": 0, "data": [], "error": "Invalid response"}

    if raw.get("ok") is False or "error" in raw:
        err = raw.get("error") or raw.get("message") or "Unknown error"
        return {"found": False, "count": 0, "data": [], "error": str(err)}

    results = raw.get("results") or raw.get("data") or raw.get("result")
    if not results:
        return {"found": False, "count": 0, "data": [], "error": None}

    data: List[Any] = []
    if isinstance(results, dict):
        for db_name, records in results.items():
            if isinstance(records, list):
                for rec in records:
                    entry = rec if isinstance(rec, dict) else {"value": rec}
                    data.append({**entry, "_db": db_name})
            else:
                data.append({"value": records, "_db": db_name})
    elif isinstance(results, list):
        data = results
    else:
        data = [{"value": results}]

    extra: Dict[str, Any] = {}
    if raw.get("_rate_remain") is not None:
        extra["rate_remaining"] = raw["_rate_remain"]

    return {"found": bool(data), "count": raw.get("size") or len(data), "data": data, "error": None, **extra}


def _norm_dep(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Нормализует ответ DepSearch."""
    if not isinstance(raw, dict):
        return {"found": False, "count": 0, "data": [], "error": "Invalid response"}

    if "error" in raw:
        return {"found": False, "count": 0, "data": [], "error": str(raw["error"])}

    extra: Dict[str, Any] = {}
    if "phone_info" in raw:
        extra["phone_info"] = raw["phone_info"]
    if "ip_info" in raw:
        extra["ip_info"] = raw["ip_info"]

    results = raw.get("results")
    if results is None:
        # Иногда DepSearch возвращает плоский объект без "results"
        flat = {k: v for k, v in raw.items() if v not in (None, "", []) and not k.startswith("_")}
        if flat:
            return {"found": True, "count": 1, "data": [flat], "error": None, **extra}
        return {"found": False, "count": 0, "data": [], "error": None, **extra}

    if isinstance(results, list):
        return {"found": bool(results), "count": len(results), "data": results, "error": None, **extra}
    elif isinstance(results, dict):
        data = [results] if results else []
        return {"found": bool(data), "count": len(data), "data": data, "error": None, **extra}

    return {"found": False, "count": 0, "data": [], "error": None, **extra}


def _norm_ofdata(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Нормализует ответ OfData."""
    if not isinstance(raw, dict):
        return {"found": False, "count": 0, "data": [], "error": "Invalid response"}

    meta   = raw.get("meta") or {}
    status = meta.get("status", "")

    if raw.get("error") or status == "error":
        err = raw.get("error") or meta.get("message") or "Unknown error"
        return {"found": False, "count": 0, "data": [], "error": str(err)}

    extra: Dict[str, Any] = {}
    if meta.get("balance") is not None:
        extra["balance"] = meta["balance"]
    if meta.get("today_request_count") is not None:
        extra["requests_today"] = meta["today_request_count"]

    result_data = raw.get("data")
    if not result_data:
        return {"found": False, "count": 0, "data": [], "error": None, **extra}

    # OfData v2: {"data": {"Записи": [...], "СтрВсего": N}}
    if isinstance(result_data, dict) and "Записи" in result_data:
        records = result_data.get("Записи") or []
        total_pages = result_data.get("СтрВсего")
        if total_pages is not None:
            extra["total_pages"] = total_pages
        return {"found": bool(records), "count": len(records), "data": records, "error": None, **extra}

    if isinstance(result_data, list):
        return {"found": bool(result_data), "count": len(result_data), "data": result_data, "error": None, **extra}

    return {"found": True, "count": 1, "data": [result_data], "error": None, **extra}

# ─────────────────────────────────────────────────────────────────────────────
#  АВТО-ОПРЕДЕЛЕНИЕ ТИПА ЗАПРОСА
# ─────────────────────────────────────────────────────────────────────────────

_EMAIL = re.compile(r'^[^@\s]+@[^@\s]+\.[^@\s]+$')
_IP    = re.compile(r'^\d{1,3}(\.\d{1,3}){3}$')
_PHONE = re.compile(r'^[78]\d{10}$')
_VIN   = re.compile(r'^[A-HJ-NPR-Z0-9]{17}$', re.I)
_GRZ   = re.compile(r'^[АВЕКМНОРСТУХ]\d{3}[АВЕКМНОРСТУХ]{2}\d{2,3}$', re.I)
_CYR   = re.compile(r'^[А-ЯЁа-яё\s\.\-]+$')
_VK    = re.compile(r'vk\.com|vkid\d+', re.I)


def auto_detect(q: str) -> str:
    raw = q.strip()

    if raw.startswith("@"):
        return "username"

    if _EMAIL.match(raw):
        return "email"

    if _IP.match(raw):
        return "ip"

    # Нормализуем телефон
    digits = re.sub(r'[\s\-\(\)\+]', '', raw)
    if _PHONE.match(digits):
        return "phone"

    if _VIN.match(raw) and not raw.isdigit():
        return "vin"

    if _GRZ.match(raw):
        return "car"

    if _VK.search(raw):
        return "vk"

    only_digits = re.sub(r'[\s\-]', '', raw)
    if only_digits.isdigit():
        n = len(only_digits)
        if n == 11:  return "snils"
        if n == 12:  return "inn"
        if n in (13, 15): return "ogrn"
        if n == 10:  return "inn"   # 10 цифр = ИНН юрлица (или паспорт)
        if n >= 6:   return "tg_id"

    if _CYR.match(raw) and len(raw.split()) >= 2:
        return "fio"

    return "nick"

# ─────────────────────────────────────────────────────────────────────────────
#  ГЛАВНАЯ АГРЕГИРУЮЩАЯ ФУНКЦИЯ
# ─────────────────────────────────────────────────────────────────────────────

async def aggregate(
    query:   str,
    stype:   str,
    sources: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Параллельно запрашивает все релевантные источники и возвращает
    нормализованный словарь результатов.
    """
    wanted = SOURCE_MAP.get(stype, [])
    if sources:
        wanted = [s for s in wanted if s in sources]

    async def run_sherk():
        type_map = {
            "username": "username", "tg_id": "id", "phone": "phone",
            "fio": "fio", "email": "email", "car": "car", "vin": "vin",
            "vk": "vk", "passport": "passport", "inn": "inn",
            "snils": "snils", "ogrn": "ogrn",
        }
        st = type_map.get(stype, stype)
        q  = query.lstrip("@") if stype == "username" else query
        if stype == "passport":
            q = re.sub(r'\s', '', q)
        raw = await _sherk(q, st)
        return "sherk", _norm_sherk_night(raw)

    async def run_night():
        type_map = {
            "username": "tg", "tg_id": "tg", "phone": "phone",
            "fio": "fio", "email": "email", "vin": "car", "vk": "vk",
            "inn": "inn", "snils": "snils", "ip": "ip", "nick": "nick",
            "ok": "ok", "fb": "fb",
        }
        st  = type_map.get(stype, stype)
        raw = await _night(query, st)
        return "nightsearch", _norm_sherk_night(raw)

    async def run_dep():
        dep_query = query
        if stype == "inn":
            dep_query = "inn" + re.sub(r'\D', '', query)
        elif stype == "snils":
            dep_query = "snils" + re.sub(r'\D', '', query)
        elif stype == "nick":
            dep_query = "nick:" + query
        elif stype == "password":
            dep_query = "pass:" + query
        elif stype == "tiktok":
            dep_query = "tt:" + query
        elif stype == "address":
            if not re.match(r'^(addr:|адрес:|г\.)', query, re.I):
                dep_query = "addr:" + query
        raw = await _dep(dep_query)
        return "depsearch", _norm_dep(raw)

    async def run_snus_search():
        types_map = {
            "email":    ["email"],
            "password": ["password"],
            "nick":     ["username"],
            "username": ["username"],
        }
        types = types_map.get(stype, ["email", "username", "password", "lastip", "name"])
        raw = await _snus("/data/search", {"terms": [query], "types": types})
        norm = _norm_sherk_night(raw)
        # Добавляем combo-lookup для email и password
        return "snusbase", norm

    async def run_snus_combo():
        raw = await _snus("/tools/combo-lookup", {"terms": [query], "types": ["username"]})
        return "snusbase_combo", _norm_sherk_night(raw)

    async def run_ofdata():
        if stype == "inn":
            raw = await _ofdata("/company", {"inn": re.sub(r'\D', '', query)})
        elif stype == "ogrn":
            raw = await _ofdata("/company", {"ogrn": re.sub(r'\D', '', query)})
        elif stype == "company":
            raw = await _ofdata("/search", {"by": "name", "obj": "org", "query": query})
        else:
            raw = {"ok": False, "error": "No OfData handler for this type"}
        return "ofdata", _norm_ofdata(raw)

    # Строим список корутин
    coros = []
    if "sherk"    in wanted: coros.append(run_sherk())
    if "night"    in wanted: coros.append(run_night())
    if "depsearch" in wanted: coros.append(run_dep())
    if "snusbase" in wanted:
        coros.append(run_snus_search())
        if stype in ("email", "password"):
            coros.append(run_snus_combo())
    if "ofdata"   in wanted: coros.append(run_ofdata())

    if not coros:
        return {"error": f"No sources configured for type '{stype}'"}

    # Параллельный запуск — ошибка одного источника не блокирует остальных
    results_raw = await asyncio.gather(*coros, return_exceptions=True)

    sources_out: Dict[str, Any] = {}
    total_count  = 0
    for item in results_raw:
        if isinstance(item, Exception):
            continue
        src_name, norm = item
        sources_out[src_name] = norm
        if norm.get("found"):
            total_count += norm.get("count", 0)

    return {
        "total_results": total_count,
        "sources":       sources_out,
    }

# ─────────────────────────────────────────────────────────────────────────────
#  FASTAPI ПРИЛОЖЕНИЕ
# ─────────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Dragon OSINT API Aggregator",
    description=(
        "Агрегирует данные из **5 OSINT-источников** параллельно: "
        "Sherk, NightSearch, DepSearch, Snusbase, OfData.\n\n"
        "**Источники по типу запроса** указаны в `/types`.\n\n"
        "Используй `type=auto` или не указывай тип — он будет определён автоматически."
    ),
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── Pydantic-модели ответов ─────────────────────────────────────────────────

class SourceResult(BaseModel):
    found:    bool
    count:    int
    data:     List[Any]
    error:    Optional[str] = None

class SearchResponse(BaseModel):
    query:         str
    type:          str
    detected_type: Optional[str] = None
    elapsed_ms:    float
    total_results: int
    sources:       Dict[str, Any]

# ─── Эндпоинты ───────────────────────────────────────────────────────────────

@app.get("/health", tags=["System"])
async def health():
    """Проверка работоспособности агрегатора."""
    return {"status": "ok", "version": "2.0.0"}


@app.get("/types", tags=["System"])
async def list_types():
    """Список поддерживаемых типов поиска с указанием источников."""
    return {
        t: {"description": desc, "sources": SOURCE_MAP.get(t, [])}
        for t, desc in SEARCH_TYPES.items()
    }


@app.get("/sources", tags=["System"])
async def list_sources():
    """Список доступных источников данных."""
    return {
        "sherk":      {"url": SHERK_URL,  "method": "GET",  "description": "Sherk.pro OSINT"},
        "nightsearch":{"url": NIGHT_URL,  "method": "POST", "description": "NightSearch.life"},
        "depsearch":  {"url": DEP_URL,    "method": "GET",  "description": "DepSearch.sbs"},
        "snusbase":   {"url": SNUS_URL,   "method": "POST", "description": "Snusbase breach DB"},
        "ofdata":     {"url": OFDATA_URL, "method": "GET",  "description": "OfData.ru (компании)"},
    }


@app.get(
    "/search",
    response_model=SearchResponse,
    tags=["Search"],
    summary="Универсальный поиск",
    description=(
        "Параллельно запрашивает все релевантные OSINT-источники.\n\n"
        "Если `type` не указан или `type=auto`, тип определяется автоматически.\n\n"
        "Параметр `sources` позволяет ограничить набор источников "
        "(например: `sherk,depsearch`)."
    ),
)
async def search(
    q: str = Query(..., description="Поисковый запрос", example="79991234567"),
    type: Optional[str] = Query(
        None,
        description="Тип поиска. Оставь пустым для автоопределения.",
        example="phone",
    ),
    sources: Optional[str] = Query(
        None,
        description="Фильтр источников через запятую: sherk,night,depsearch,snusbase,ofdata",
        example="sherk,depsearch",
    ),
):
    if not q.strip():
        raise HTTPException(status_code=400, detail="Query cannot be empty")

    detected = None
    if not type or type.lower() in ("auto", ""):
        detected = auto_detect(q.strip())
        stype = detected
    else:
        stype = type.lower()
        if stype not in SEARCH_TYPES:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown type '{stype}'. Use GET /types for the full list.",
            )

    src_filter: Optional[List[str]] = None
    if sources:
        src_filter = [s.strip() for s in sources.split(",") if s.strip()]

    t0 = time.monotonic()
    result = await aggregate(q.strip(), stype, src_filter)
    elapsed = (time.monotonic() - t0) * 1000

    return {
        "query":         q.strip(),
        "type":          stype,
        "detected_type": detected,
        "elapsed_ms":    round(elapsed, 1),
        "total_results": result.get("total_results", 0),
        "sources":       result.get("sources", {}),
    }


@app.get(
    "/search/{stype}",
    response_model=SearchResponse,
    tags=["Search"],
    summary="Поиск по конкретному типу",
    description="Аналогично `/search?q=...&type=...` но тип указывается в URL.",
)
async def search_by_type(
    stype: str,
    q: str = Query(..., description="Поисковый запрос"),
    sources: Optional[str] = Query(None, description="Фильтр источников через запятую"),
):
    stype = stype.lower()
    if stype not in SEARCH_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown type '{stype}'. Use GET /types for the full list.",
        )

    src_filter: Optional[List[str]] = None
    if sources:
        src_filter = [s.strip() for s in sources.split(",") if s.strip()]

    t0     = time.monotonic()
    result = await aggregate(q.strip(), stype, src_filter)
    elapsed = (time.monotonic() - t0) * 1000

    return {
        "query":         q.strip(),
        "type":          stype,
        "detected_type": None,
        "elapsed_ms":    round(elapsed, 1),
        "total_results": result.get("total_results", 0),
        "sources":       result.get("sources", {}),
    }


@app.get(
    "/detect",
    tags=["Search"],
    summary="Авто-определение типа запроса",
    description="Возвращает предполагаемый тип без выполнения поиска.",
)
async def detect_type(
    q: str = Query(..., description="Запрос для анализа", example="user@mail.ru"),
):
    stype   = auto_detect(q.strip())
    sources = SOURCE_MAP.get(stype, [])
    return {
        "query":       q.strip(),
        "type":        stype,
        "description": SEARCH_TYPES.get(stype, ""),
        "sources":     sources,
    }

# ─────────────────────────────────────────────────────────────────────────────
#  ЗАПУСК
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
