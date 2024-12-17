from typing import Any, TypedDict
from pendulum import UTC, Date, DateTime, Time
from datetime import timedelta
from logging import getLogger

from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable


logger = getLogger(__name__)

class ContinuousAndCatchupIntervals(TypedDict):
    continuous_interval: float
    catchup_interval: float


class ContinuousAndCatchupTimetable(Timetable):
    def __init__(
        self, 
        continuous_interval: timedelta = timedelta(minutes=30), 
        catchup_interval: timedelta = timedelta(days=1)
    ):
        self._continuous_interval = continuous_interval
        self._catchup_interval = catchup_interval
        
        if self._continuous_interval > self._catchup_interval:
            raise ValueError("The continuous interval must be less than or equal to the catchup interval.")
        
        self.description = f"Runs catchup dag runs every {self._catchup_interval} and non-catchup dag runs every {self._continuous_interval}"
        
    def serialize(self) -> ContinuousAndCatchupIntervals:
        return ContinuousAndCatchupIntervals(
            continuous_interval = self._continuous_interval.total_seconds(),
            catchup_interval = self._catchup_interval.total_seconds()
        )
    
    @classmethod
    def deserialize(cls, value: ContinuousAndCatchupIntervals) -> Timetable:
        return cls(
            continuous_interval = timedelta(seconds=value["continuous_interval"]), 
            catchup_interval = timedelta(seconds=value["catchup_interval"])
        )
        
    @property
    def summary(self) -> str:
        return f"Continuous interval: {self._continuous_interval}, Catchup interval: {self._catchup_interval}"
    
    def infer_manual_data_interval(self, run_after: DateTime) -> DataInterval:
        """Trigger the most recent interval, even if it's a partial interval."""
        start = DateTime.combine(run_after.date(), Time.min).replace(tzinfo=UTC)
        
        if (
            start + self._catchup_interval > DateTime.now(UTC)
            and start + self._continuous_interval > DateTime.now(UTC)
            ):
            return DataInterval(start=start, end=DateTime.now(UTC))
        elif start + self._catchup_interval > DateTime.now(UTC):
            return DataInterval(start=start, end=(start + self._continuous_interval))
        else:
            return DataInterval(start=start, end=(start + self._catchup_interval))
        
    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        
        now = DateTime.now(UTC)
        logger.info(f"last_automated_data_interval: {last_automated_data_interval}, restriction: {restriction}")

        if last_automated_data_interval is None:
            # First run
            next_start = restriction.earliest or now
        else:
            next_start = last_automated_data_interval.end

        if restriction.latest and next_start >= restriction.latest:
            return None  # Over the DAG's end

        # Determine interval
        if next_start + self._catchup_interval <= now:
            # Catchup phase
            next_end = next_start + self._catchup_interval
        elif next_start + self._continuous_interval <= now:
            # Continuous phase
            next_end = next_start + self._continuous_interval
        else:
            # Too soon for the next interval
            return None

        # Enforce latest restriction
        if restriction.latest and next_end > restriction.latest:
            next_end = restriction.latest

        return DagRunInfo.interval(start=next_start, end=next_end)
