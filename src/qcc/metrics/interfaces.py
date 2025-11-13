from __future__ import annotations

"""Strategy interfaces for Tagger metrics.

Protocols describe pure, deterministic, no-I/O strategy contracts. Use forward
references for domain types to avoid circular imports.
"""

from typing import Protocol, Any, Dict


class TaggingSpeedStrategy(Protocol):
    """Strategy interface for tagging speed estimation.

    Inputs:
        tagger: "Tagger" (forward ref) containing tagassignments
    Outputs:
        float: estimated mean seconds between tags
               (after trimming the slowest intervals)

    Invariants:
        - Pure: no mutation, no I/O
        - Deterministic for same input
    """

    def speed_seconds(self, tagger: "Tagger") -> float:  # pragma: no cover - interface
        ...


class AgreementStrategy(Protocol):
    """Strategy interface for pairwise agreement between taggers.

    Inputs:
        a, b: "Tagger"
        characteristic: "Characteristic"
    Outputs:
        float: agreement in [0.0, 1.0]

    Invariants:
        - Pure and deterministic
    """

    def pairwise(self, a: "Tagger", b: "Tagger", characteristic: "Characteristic") -> float:  # pragma: no cover - interface
        ...


class PatternSignalsStrategy(Protocol):
    """Strategy interface for pattern signal detection.

    Inputs:
        tagger: "Tagger"
        characteristic: "Characteristic"
    Outputs:
        dict: structured analysis results (pure JSON-serializable dict)

    Invariants:
        - Pure and deterministic
    """

    def analyze(self, tagger: "Tagger", characteristic: "Characteristic") -> Dict[str, Any]:  # pragma: no cover - interface
        ...
