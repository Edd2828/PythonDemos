from pyspark.sql.functions import (
    col, lit, unix_timestamp, from_unixtime, make_timestamp, to_date, dayofweek, round, when, year, month, dayofmonth, date_add, expr
)
from pyspark.sql import DataFrame
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType


def operational_hours_difference(df: DataFrame, CreatedDate: col, EndDate: col, op_start: str, op_end: str) -> DataFrame:
    '''
    Returns calculates operational time difference between two dates and excludes out of hours times and weekends.

    Parameters:
        - dataframe (pyspark DataFrame): must contain [CreatedDate, EndDate] as timestamps
        - CreatedDate (pyspark column): must be a start date of type timestamp
        - EndDate (pyspark column): must be an end date of type timestamp
        - op_start (str): operational start time with format HH:MM
        - op_end (str): operational end time with format HH:MM
    
    Returns:
        Original pyspark dataframe with additional column OperationalMinutestoClosure of type long represented in minutes
    '''

    time_format = '%H:%M'
    full_op_date_minutes = (datetime.strptime(op_end, time_format) - datetime.strptime(op_start, time_format)).seconds / 60

    op_start_hour = datetime.strptime(op_start, time_format).hour
    op_start_minute = datetime.strptime(op_start, time_format).minute
    op_end_hour = datetime.strptime(op_end, time_format).hour
    op_end_minute = datetime.strptime(op_end, time_format).minute

    print(f"Minutes in full day of operation: {full_op_date_minutes}")
    
    MONDAY = 2
    TUESDAY = 3
    WEDNESDAY = 4
    THURSDAY = 5
    FRIDAY = 6
    SATURDAY = 7
    SUNDAY = 1

    def op_start(created: col) -> col:
        'Returns timestamp with same date but with time set to 08:00:00'
        return make_timestamp(year(created), month(created), dayofmonth(created), lit(op_start_hour), lit(op_start_minute), lit(0), lit('UTC'))

    def op_end(created: col) -> col:
        return make_timestamp(year(created), month(created), dayofmonth(created), lit(op_end_hour), lit(op_end_minute), lit(0), lit('UTC'))

    def minute_diff(end: col, created: col) -> col:
        return (unix_timestamp(end) - unix_timestamp(created)) / 60
    
    df = df \
    .withColumn("OperationalCreatedDate", # moving CreatedDate forward to next starting operational timestamp if created out of hours
        when(
            dayofweek(df.CreatedDate) == lit(SATURDAY), 
                op_start(date_add(to_date(df.CreatedDate), 2)) # Sat to Mon
        ) 
        .when(
            dayofweek(df.CreatedDate) == lit(SUNDAY), 
                op_start(date_add(to_date(df.CreatedDate), 1)) # Sun to Mon
        ) 
        .when(
            df.CreatedDate < op_start(df.CreatedDate), 
                op_start(df.CreatedDate) # early morning to 8:00
        ) 
        .when(
            (df.CreatedDate > op_end(df.CreatedDate)) & (dayofweek(df.CreatedDate) != lit(FRIDAY)), 
                op_start(date_add(to_date(df.CreatedDate), 1)) # late evening (not friday) to 8:00 next day
        ) 
        .when(
            (df.CreatedDate > op_end(df.CreatedDate)) & (dayofweek(df.CreatedDate) == lit(FRIDAY)), 
                op_start(date_add(to_date(df.CreatedDate), 3)) # late evening Fri to 8:00 next Mon
        ) 
        .otherwise(df.CreatedDate)
    ) \
    .withColumn("FirstLayerOperationalEndDate", # moving EndDate back to last ending operational timestamp if End out of hours
        when(
            dayofweek(df.EndDate) == lit(SATURDAY), 
                op_end(date_add(to_date(df.EndDate), -1)) # Sat back to Fri
        ) 
        .when(
            dayofweek(df.EndDate) == lit(SUNDAY), 
                op_end(date_add(to_date(df.EndDate), -2)) # Sun back to Fri
        ) 
        .when(
            df.EndDate > op_end(df.EndDate), 
                op_end(df.EndDate) # after 17:00 back to 17:00
        ) 
        .when(
            (df.EndDate < op_start(df.EndDate)) & (dayofweek(df.EndDate) != lit(MONDAY)), 
                op_end(date_add(to_date(df.EndDate), -1)) # early morning (not monday) back to previous day 17:00
        ) 
        .when(
            (df.EndDate < op_start(df.EndDate)) & (dayofweek(df.EndDate) == lit(MONDAY)), 
                op_end(date_add(to_date(df.EndDate), -3)) # early morning mon back to previous day 17:00
        ) 
        .otherwise(df.EndDate)
    )

    # ensure that End dates are set equal to created if they are before created date
    df = df.withColumn("OperationalEndDate", 
        when(
            unix_timestamp(df.FirstLayerOperationalEndDate) < unix_timestamp(df.OperationalCreatedDate),
                df.OperationalCreatedDate
            ) 
        .otherwise(df.FirstLayerOperationalEndDate)
    ).drop(col('FirstLayerOperationalEndDate'))

    # count the number of weekdays between OperationalCreatedDate and OperationalEndDate excluding weekends
    df = df \
    .withColumn('days', expr('sequence(OperationalCreatedDate, OperationalEndDate, interval 1 day)')) \
    .withColumn('weekdays', expr('filter(transform(days, day->(day, extract(dow_iso from day))), day -> day.col2 <=5).day')) \
    .withColumn('weekdays_size', expr('size(weekdays)')) \
    .withColumn('FullOperationalDaysDifference', 
                when(
                    expr('size(weekdays)') > 1,
                    expr('size(weekdays)') - lit(2)
                    ) \
                .otherwise(lit(0))
    ) \
    .drop(col('days'))

    return df \
    .withColumn("OperationalMinutestoClosure",
        when(
            to_date(df.OperationalCreatedDate) == to_date(df.OperationalEndDate), 
            round(minute_diff(df.OperationalEndDate, df.OperationalCreatedDate)) # same day difference
        ) \
        .when(
            df.weekdays_size == lit(2), 
            round(
                minute_diff(op_end(df.OperationalCreatedDate), df.OperationalCreatedDate) + # starting day
                minute_diff(df.OperationalEndDate, op_start(df.OperationalEndDate)) # ending day
            )
        ) \
        .otherwise(
            round(
                minute_diff(op_end(df.OperationalCreatedDate), df.OperationalCreatedDate) + # starting day
                (lit(full_op_date_minutes) * df.FullOperationalDaysDifference) +
                minute_diff(df.OperationalEndDate, op_start(df.OperationalEndDate)) # ending day
            ) 
        )
    ).drop(col('OperationalCreatedDate'), col('OperationalEndDate'), col('weekdays'), col('weekdays_size'), col('FullOperationalDaysDifference'))

spark = SparkSession.builder.appName("Sample DataFrame").getOrCreate()

# Define a schema for the DataFrame
schema = StructType([
    StructField("TicketNumber", IntegerType(), True),
    StructField("CreatedDate", TimestampType(), True),
    StructField("EndDate", TimestampType(), True)
])

# Create data for the DataFrame
data = [
    (1, datetime(2024, 1, 1, 6, 48, 0), datetime(2024, 1, 2, 17, 32, 0)),
    (2, datetime(2024, 1, 1, 6, 0, 0), datetime(2024, 1, 8, 18, 0, 0)),
    (3, datetime(2024, 1, 5, 5, 0, 0), datetime(2024, 1, 7, 18, 0, 0)),
]

# Create the DataFrame
df = spark.createDataFrame(data, schema)

df = operational_hours_difference(df, df.CreatedDate, df.EndDate, '09:00', '10:00')

# Show the DataFrame
df.show()