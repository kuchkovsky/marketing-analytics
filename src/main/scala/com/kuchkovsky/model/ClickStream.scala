package com.kuchkovsky.model

case class ClickStream(userId: String,
                       eventId: String,
                       eventTime: java.sql.Timestamp,
                       eventType: String,
                       attributes: String)
