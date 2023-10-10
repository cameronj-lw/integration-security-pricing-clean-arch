
from dataclasses import dataclass
from typing import Any

from app.domain.repositories import (
    SecurityRepository, PortfolioRepository
)


@dataclass
class SecurityNotFoundException(Exception):
    repository: SecurityRepository
    missing_field: str
    missing_value: Any


@dataclass
class PortfolioNotFoundException(Exception):
    repository: PortfolioRepository
    missing_field: str
    missing_value: Any


class InvalidDictError(Exception):
    pass


