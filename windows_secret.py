"""User-bound Windows DPAPI, using only Python stdlib and Windows system DLLs."""
import ctypes
from ctypes import wintypes
import os
from pathlib import Path


class Blob(ctypes.Structure):
    _fields_ = [("size", wintypes.DWORD), ("data", ctypes.POINTER(ctypes.c_ubyte))]


def _crypt(value, decrypt=False):
    if os.name != "nt":
        raise ValueError("Protected credentials require Windows")
    crypt32 = ctypes.WinDLL("crypt32", use_last_error=True)
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    function = crypt32.CryptUnprotectData if decrypt else crypt32.CryptProtectData
    function.argtypes = [ctypes.POINTER(Blob), ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p, wintypes.DWORD, ctypes.POINTER(Blob)]
    function.restype = wintypes.BOOL
    kernel32.LocalFree.argtypes = [ctypes.c_void_p]
    kernel32.LocalFree.restype = ctypes.c_void_p
    buffer = ctypes.create_string_buffer(value)
    source = Blob(len(value), ctypes.cast(buffer, ctypes.POINTER(ctypes.c_ubyte)))
    destination = Blob()
    if not function(ctypes.byref(source), None, None, None, None, 1, ctypes.byref(destination)):
        raise ValueError("Windows credential protection failed; use the Windows account that performed setup")
    try:
        return ctypes.string_at(destination.data, destination.size)
    finally:
        kernel32.LocalFree(ctypes.cast(destination.data, ctypes.c_void_p))
        ctypes.memset(buffer, 0, len(value))


def protect(value, path):
    encrypted = _crypt(value.encode("utf-8"))
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(encrypted)


def unprotect(path):
    return _crypt(Path(path).read_bytes(), decrypt=True).decode("utf-8")
