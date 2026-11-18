"""Mage decorators with no-op fallbacks for local unit-test orchestration."""

try:
    from mage_ai.data_preparation.decorators import (  # type: ignore
        data_exporter,
        data_loader,
        test,
        transformer,
    )
except ModuleNotFoundError:
    def _identity(function):
        return function

    data_exporter = _identity
    data_loader = _identity
    test = _identity
    transformer = _identity

