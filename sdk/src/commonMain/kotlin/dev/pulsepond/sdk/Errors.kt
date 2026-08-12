package dev.pulsepond.sdk

/** Raised when the SDK cannot safely initialize from its configuration. */
public class PulsepondConfigurationException public constructor(message: String) :
    IllegalArgumentException(message)

/** Raised when an event does not satisfy the closed Pulsepond v1 contract. */
public class PulsepondValidationException public constructor(message: String) :
    IllegalArgumentException(message)
