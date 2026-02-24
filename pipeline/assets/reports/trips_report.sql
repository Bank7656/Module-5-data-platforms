/* @bruin

name: reports.trips_report
type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: taxi_type
    type: string
    description: "Type of taxi (yellow, green)"
    primary_key: true
  - name: pickup_date
    type: date
    description: "The date of the trip pickup"
    primary_key: true
  - name: payment_type_name
    type: string
    description: "Name of the payment method"
    primary_key: true
  - name: trip_count
    type: bigint
    description: "Total number of trips for this group"
  - name: total_amount
    type: double
    description: "Total amount collected"
  - name: average_passenger_count
    type: double
    description: "Average number of passengers per trip"

@bruin */

SELECT
    taxi_type,
    CAST(pickup_datetime AS DATE) as pickup_date,
    payment_type_name,
    COUNT(*) as trip_count,
    SUM(total_amount) as total_amount,
    AVG(passenger_count) as average_passenger_count
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY 1, 2, 3
