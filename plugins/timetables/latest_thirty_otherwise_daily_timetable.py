from pendulum import UTC, Date, DateTime, Time
from datetime import timedelta

from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable

class LatestHalfHourOtherwiseDailyTimetable(Timetable):
    def infer_manual_data_interval(self, run_after: DateTime) -> DataInterval:
        """Trigger the most recent interval, even if it's a partial interval."""
        start = DateTime.combine(run_after.date(), Time.min).replace(tzinfo=UTC)
        
        if (
            start + timedelta(days=1) > DateTime.now(UTC)
            and start + timedelta(minutes=30) > DateTime.now(UTC)
            ):
            # sub 30 minute interval
            return DataInterval(start=start, end=DateTime.now(UTC))
        elif start + timedelta(days=1) > DateTime.now(UTC):
            return DataInterval(start=start, end=(start + timedelta(minutes=30)))
        else:
            return DataInterval(start=start, end=(start + timedelta(days=1)))
        
    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        
        # Prevent race conditions by setting now once
        now = DateTime.now(UTC)
        
        # Set start datetime
        if last_automated_data_interval is not None:  
            # There was a previous run on the regular schedule.
            next_start = last_automated_data_interval.end
        else:  
            # This is the first ever run on the regular schedule.
            next_start = restriction.earliest
            if next_start is None:  
                # No start_date. Don't schedule.
                return None
            if not restriction.catchup:
                # If the DAG has catchup=False, today is the earliest to consider.
                next_start = max(next_start, DateTime.combine(now.date(), Time.min).replace(tzinfo=UTC))
                
        # Set end datetime
        if (
            next_start + timedelta(days=1) > now
            and next_start + timedelta(minutes=30) > now
            ):
            # The data interval should be 30 minutes, but 30 minutes hasn't passed since the last run
            # skip run
            return None
        elif (
            next_start + timedelta(days=1) > now
            and next_start + (timedelta(minutes=30) * 2) < now
            ):
            # There is less than a day difference between the last run time interval
            # and now, but more than two 30 minute windows between the last run time
            # interval and now, so run everything in the time interval up until an 
            # even half hour
            if now.minute < 30:
                next_end = now.set(minute=0, second=0, microsecond=0)
            else:
                next_end = now.set(minute=30, second=0, microsecond=0)
            
        elif next_start + timedelta(days=1) > now:
            # There is only one 30 minute window between the last run and now
            # Treat like a normal half hour interval
            next_end = next_start + timedelta(minutes=30)
        else:
            # There is more than a day gap between that last run and now
            # Still need to make sure our day intervals are going from midnight
            # to midnight utc in the case of a paused dag
            next_end = next_start.add(days=1).set(hour=0, minute=0, second=0, microsecond=0)
        
        if restriction.latest is not None:
            if next_start >= restriction.latest:
                # Over the DAG's scheduled end; don't schedule.
                return None
            if next_end > restriction.latest:
                # Truncate the end to avoid running past the restriction
                next_end = restriction.latest
        
        return DagRunInfo.interval(start=next_start, end=next_end)

