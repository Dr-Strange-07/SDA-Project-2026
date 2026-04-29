from collections import deque
from hashlib import pbkdf2_hmac
from typing import Deque, Dict, Tuple


def normalize_metric_value(value: float) -> str:
    return f"{float(value):.2f}"


def generate_signature(raw_value_str: str, key: str, iterations: int) -> str:
    password_bytes = key.encode("utf-8")
    salt_bytes = raw_value_str.encode("utf-8")
    hash_bytes = pbkdf2_hmac(
        hash_name="sha256",
        password=password_bytes,
        salt=salt_bytes,
        iterations=iterations,
    )
    return hash_bytes.hex()


def verify_signature(
    packet: Dict,
    secret_key: str,
    iterations: int,
    value_field: str,
    signature_field: str,
) -> bool:
    metric_value = packet.get(value_field)
    incoming_signature = packet.get(signature_field, "")

    if metric_value is None or not incoming_signature:
        return False

    normalized_value = normalize_metric_value(float(metric_value))
    expected_signature = generate_signature(normalized_value, secret_key, iterations)
    return expected_signature == incoming_signature


def update_running_average(
    current_window: Deque[float],
    new_value: float,
    window_size: int,
) -> Tuple[Deque[float], float]:
    next_window = deque(current_window, maxlen=window_size)
    next_window.append(float(new_value))
    running_avg = sum(next_window) / len(next_window)
    return next_window, running_avg
