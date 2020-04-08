package com.tw.apps

import java.sql.Timestamp

case class StationData(
                        station_id: String,
                        timestamp: Timestamp,
                        bikes_available: Int,
                        docks_available: Int,
                        is_renting: Boolean,
                        is_returning: Boolean,
                        last_updated: Long,
                        name: String,
                        latitude: Double,
                        longitude: Double
                      )
