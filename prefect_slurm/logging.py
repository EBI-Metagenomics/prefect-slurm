import logging
import os
from typing import Any, Iterable, Mapping

from prefect.logging.filters import redact_substr

from prefect_slurm.config import LOG_MASK_PATTERNS


def mask_sensitive_data(
    value: Any, env: Mapping[str, Any], patterns: Iterable[str] = LOG_MASK_PATTERNS
) -> Any:
    secrets = (env.get(pattern) or pattern for pattern in patterns)

    for secret in sorted(filter(None, secrets), key=len, reverse=True):
        value = redact_substr(value, secret)
    return value


class RedactingFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = mask_sensitive_data(record.msg, env=os.environ)
        if record.args:
            record.args = tuple(
                mask_sensitive_data(arg, os.environ) for arg in record.args
            )

        return True
