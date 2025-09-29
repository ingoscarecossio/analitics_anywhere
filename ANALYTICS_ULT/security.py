# -*- coding: utf-8 -*-
import pandas as pd
def octal_to_rwx(octal_str):
    s = str(octal_str).strip()
    if not s or s.lower()=="nan": return None
    s = s[-3:]
    try:
        n = int(s, 8)
    except Exception:
        try: n = int(s)
        except Exception: return None
    tri = [ (n // 64) % 8, (n // 8) % 8, n % 8 ]
    def bits(v): return ("r" if v & 4 else "-") + ("w" if v & 2 else "-") + ("x" if v & 1 else "-")
    return "".join(bits(v) for v in tri)
def world_writable(octal_str):
    rwx = octal_to_rwx(octal_str); 
    return bool(rwx and ("w" in rwx[-3:]))
def world_readable(octal_str):
    rwx = octal_to_rwx(octal_str); 
    return bool(rwx and ("r" in rwx[-3:]))
