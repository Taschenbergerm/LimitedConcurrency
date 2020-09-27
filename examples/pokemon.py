import dataclasses

@dataclasses.dataclass
class Pokemon:
    id: int
    name: str
    base_stat: int