# -*- coding: utf-8 -*-
"""
HTTP helpers with small retry/backoff.
"""
import time, random, requests

UA = "dex-snap/1.0 (+snapshots)"

def make_session(timeout=(10, 20)) -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept": "application/json"})
    s.request = _wrap_with_timeout(s.request, timeout)
    return s

def _wrap_with_timeout(fn, timeout):
    def inner(method, url, **kw):
        if "timeout" not in kw:
            kw["timeout"] = timeout
        return fn(method, url, **kw)
    return inner

def get_json(url, session=None, retries=2, backoff=(0.4, 1.6)):
    s = session or make_session()
    last = None
    for i in range(retries + 1):
        try:
            r = s.get(url)
            if r.status_code == 200:
                return r.json()
            last = f"{r.status_code}: {r.text[:200]}"
        except Exception as e:
            last = str(e)
        if i < retries:
            time.sleep(random.uniform(*backoff))
    raise RuntimeError(f"GET {url} failed: {last}")

def post_json(url, payload, session=None, retries=2, backoff=(0.4, 1.6)):
    s = session or make_session()
    last = None
    for i in range(retries + 1):
        try:
            r = s.post(url, json=payload)
            if r.status_code == 200:
                return r.json()
            last = f"{r.status_code}: {r.text[:200]}"
        except Exception as e:
            last = str(e)
        if i < retries:
            time.sleep(random.uniform(*backoff))
    raise RuntimeError(f"POST {url} failed: {last}")
