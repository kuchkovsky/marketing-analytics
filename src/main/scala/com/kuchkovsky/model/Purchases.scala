package com.kuchkovsky.model

case class Purchases(purchaseId: String,
                     purchaseTime: java.sql.Timestamp,
                     billingCost: Double,
                     isConfirmed: Boolean)
