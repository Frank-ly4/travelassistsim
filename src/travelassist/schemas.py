from __future__ import annotations

from pydantic import BaseModel, Field


class ProjectMetadata(BaseModel):
    name: str
    environment: str
    timezone: str
    owner: str
    version: str


class KPIItem(BaseModel):
    id: str
    name: str
    category: str
    grain: str
    description: str
    business_question: str
    formula_sql_hint: str = Field(default="")