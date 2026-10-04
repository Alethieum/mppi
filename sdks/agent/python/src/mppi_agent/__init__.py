"""MPPI agent payment and inference client."""

from mppi_protocol import (
    Catalog,
    Rates,
    TokenCounts,
    funding_amount,
    parse_catalog,
    required_coverage,
)
from mppi_protocol.signing import LocalSessionSigner

from .client import (
    Client,
    ClientError,
    CloseAccountingError,
    CompletionStream,
    InferenceError,
    PaymentError,
)
from .discovery import ProviderDiscovery, discover, verify_challenge
from .hooks import HookError, Hooks, ObserverStats
from .interfaces import AgentSessions, FundingWallet, OpenResult, SessionSigner
from .onboarding import OnboardingError, OnboardingResult, onboard

__all__ = [
    "HookError",
    "Hooks",
    "ObserverStats",
    "AgentSessions",
    "Catalog",
    "Client",
    "ClientError",
    "CloseAccountingError",
    "CompletionStream",
    "FundingWallet",
    "LocalSessionSigner",
    "InferenceError",
    "OpenResult",
    "OnboardingError",
    "OnboardingResult",
    "PaymentError",
    "ProviderDiscovery",
    "Rates",
    "SessionSigner",
    "TokenCounts",
    "funding_amount",
    "discover",
    "parse_catalog",
    "required_coverage",
    "verify_challenge",
    "onboard",
]
