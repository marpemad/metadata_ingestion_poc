from dataclasses import dataclass
from typing import Optional
import os

@dataclass
class Config:
    env: str = os.getenv("ENV", "dev")
    raw_base: Optional[str] = None
    hub_base: Optional[str] = None
    checkpoint_base: Optional[str] = None

    @staticmethod
    def from_defaults(raw_base: str, hub_base: str, checkpoint_base: str, env: str = None):
        return Config(env=env or os.getenv("ENV", "dev"),
                      raw_base=raw_base,
                      hub_base=hub_base,
                      checkpoint_base=checkpoint_base)
