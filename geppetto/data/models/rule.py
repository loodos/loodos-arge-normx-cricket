from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class Severity(str, Enum):
    INFO = "info"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class RuleDefinition(BaseModel):
    """
    Natural language definition of a specific data quality rule.
    This represents a concrete rule concept before it's translated into code.
    """

    id: int = Field(
        ..., description="Versioned numeric identifier (display_id * 1000 + version)"
    )
    display_id: int = Field(
        ..., description="Base numeric identifier for the rule (1, 2, 3...)"
    )
    version: int = Field(
        default=1, description="Version number of this rule definition"
    )
    predecessor_id: Optional[int] = Field(
        None, description="The ID of the version this was refined from"
    )
    improvement_message: Optional[str] = Field(
        None, description="Agent's reasoning for this refinement"
    )
    is_latest: bool = Field(
        True, description="Whether this is the current active version"
    )
    is_approved: bool = Field(False, description="Whether this rule has been approved")
    is_rejected: bool = Field(False, description="Whether this rule has been rejected")
    slug: str = Field(
        ...,
        description="Kebab-case identifier (e.g., 'venue-preparation-time-too-short')",
    )
    name: str = Field(
        ...,
        description="Human-readable name (e.g., 'Venue Preparation Time is too short')",
    )
    definition: str = Field(
        ..., description="Natural language description of what this rule detects"
    )
    parameters: Dict[str, Any] = Field(
        default_factory=dict,
        description="Parameters that would be used in this rule (e.g., thresholds, limits)",
    )


class DiscrepancyRule(BaseModel):
    rule_id: str = Field(
        ..., description="Unique snake_case identifier (e.g., 'late_delivery_check')."
    )
    definition_id: int = Field(
        ...,
        description="Unique numeric identifier for the rule definition that this rule is based on",
    )
    description: str = Field(
        ..., description="Human-readable explanation of why this is a discrepancy."
    )
    category: str = Field(
        ...,
        description="Links back to the 'Attention Framework' (e.g., 'time-based-irregularities').",
    )
    severity: Severity = Field(default=Severity.MEDIUM)
    logic: str = Field(..., description="Free text description of the logic.")
    code: str = Field(..., description="Raw Python function code using Polars.")
    explanation: str = Field(
        ..., description="Simple explanation of the code and logic."
    )
    parameters: Dict[str, Any] = Field(
        default_factory=dict, description="Adjustable parameters used in the code."
    )
    dependencies: List[str] = Field(
        default_factory=list,
        description="List of Python packages required (e.g. ['scikit-learn>=1.0']).",
    )
