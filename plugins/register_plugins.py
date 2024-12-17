from airflow.plugins_manager import AirflowPlugin
from operators.aws_glue_operator import AwsGlueJobOperator
from operators.aws_glue_run_crawler_operator import AwsGlueCrawlerOperator
from timetables.continuous_and_catchup_timetable import ContinuousAndCatchupTimetable
from timetables.latest_thirty_otherwise_daily_timetable import LatestHalfHourOtherwiseDailyTimetable

class RuggableTimetablePlugin(AirflowPlugin):
    name = "latest_half_hour_otherwise_daily_timetable_plugin"
    timetables = [LatestHalfHourOtherwiseDailyTimetable, ContinuousAndCatchupTimetable]
    
class RuggableCustomOperatorsPlugin(AirflowPlugin):
    name = "ruggable_custom_operators_plugin"
    operators = [AwsGlueCrawlerOperator, AwsGlueJobOperator]