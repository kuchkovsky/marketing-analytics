package com.kuchkovsky.model

case class PurchasesAttribution(purchaseId: String,
                                purchaseTime: java.sql.Timestamp,
                                billingCost: Double,
                                isConfirmed: Boolean,
                                sessionId: Long,
                                campaignId: String,
                                channelId: String)
