package com.tw.apps

import java.sql.Timestamp

case class StationData(
    bikes_available: Integer,
    docks_available: Integer,
    is_renting: Boolean,
    is_returning: Boolean,
    timestamp: Timestamp,
    last_updated: Long,
    station_id: String,
    name: String,
    latitude: Double,
    longitude: Double
)