"""
The Pipeline class: the thing that the user imports when using aggregreat.
"""

from typing import Mapping, Dict, Sequence, List, Optional

from aggregation_types import Stage


class Pipeline:
    """
    A container for an aggregation pipeline.
    """

    def __init__(self: 'Pipeline', stages: Optional[Sequence[Stage]] = None) -> None:
        self.stages = stages or []

    @property
    def stages(self: 'Pipeline') -> List[Dict]:
        """
        The Pipeline stages, returned as a list
        """
        return [stage.value for stage in self._stages]

    @stages.setter
    def stages(self: 'Pipeline', stages: Sequence[Mapping]) -> None:
        self._stages = [Stage(stage) for stage in stages]

    def _add_stage(self, stage: Mapping[str, dict]) -> 'Pipeline':
        return Pipeline(self.stages + [stage])

    def pop(self: 'Pipeline') -> List[Dict]:
        """
        The advertised alias for `stages`
        """
        return self.stages

    def match(self: 'Pipeline',  query: Mapping[str, Mapping]) -> 'Pipeline':
        """
        Input: query in standard Mongo query language. Output: Pipeline instance with an added
        $match stage.
        """
        return self._add_stage({"$match": query})
