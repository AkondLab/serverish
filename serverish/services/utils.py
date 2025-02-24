import importlib.util
import pathlib
from functools import lru_cache
import socket

from serverish.services import Service

@lru_cache(maxsize=32)
def load_service_class(file_path: str | pathlib.Path, class_name: str) -> type[Service]:
    """Dynamically loads a class from a Python file"""
    file_path = pathlib.Path(file_path).resolve()
    module_name = file_path.stem  # Extracts filename without extension

    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load service module from {file_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # Load module dynamically

    if not hasattr(module, class_name):
        raise AttributeError(f"Service Module '{module_name}' does not have a service class '{class_name}'")

    cls = getattr(module, class_name)  # Get class reference

    if not issubclass(cls, Service):
        raise TypeError(f"Service class '{class_name}' is not a subclass of 'Service'")

    return cls

@lru_cache
def get_ext_ip_and_host() -> tuple[str, str | None]:
    """Get external IP and host name"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80)) # Fake connection to random IP (never actually sends data)
        ip = s.getsockname()[0]
        s.close()
        try:
            host = socket.gethostbyaddr(ip)[0]
        except Exception:
            host = None
    except Exception:
        ip = '127.0.0.1'
        host = None

    return ip, host

