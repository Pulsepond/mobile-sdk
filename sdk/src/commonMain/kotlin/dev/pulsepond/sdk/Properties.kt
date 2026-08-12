package dev.pulsepond.sdk

private const val maxSafeInteger: Long = 9_007_199_254_740_991
private const val maxProperties: Int = 32

internal sealed interface EventPropertyValue {
    data class Text(val value: String) : EventPropertyValue

    data class Integer(val value: Long) : EventPropertyValue

    data class Flag(val value: Boolean) : EventPropertyValue

    data object Null : EventPropertyValue
}

/** Swift-friendly builder for the closed, flat Pulsepond property model. */
public class PulsepondProperties public constructor() {
    private val values: MutableMap<String, EventPropertyValue> = linkedMapOf()

    public fun setString(key: String, value: String): PulsepondProperties = apply {
        validateKey(key)
        validatePrintableText("string property", value, 256)
        put(key, EventPropertyValue.Text(value))
    }

    public fun setInteger(key: String, value: Long): PulsepondProperties = apply {
        validateKey(key)
        if (value !in -maxSafeInteger..maxSafeInteger) {
            throw PulsepondValidationException("numeric properties must be safe integers")
        }
        put(key, EventPropertyValue.Integer(value))
    }

    public fun setBoolean(key: String, value: Boolean): PulsepondProperties = apply {
        validateKey(key)
        put(key, EventPropertyValue.Flag(value))
    }

    public fun setNull(key: String): PulsepondProperties = apply {
        validateKey(key)
        put(key, EventPropertyValue.Null)
    }

    public fun remove(key: String): PulsepondProperties = apply {
        values.remove(key)
    }

    public fun clear(): PulsepondProperties = apply {
        values.clear()
    }

    internal fun snapshot(): Map<String, EventPropertyValue> =
        values.entries
            .sortedBy { it.key }
            .associateTo(linkedMapOf()) { it.key to it.value }

    private fun put(key: String, value: EventPropertyValue) {
        if (key !in values && values.size >= maxProperties) {
            throw PulsepondValidationException("properties must contain at most $maxProperties values")
        }
        values[key] = value
    }

    private fun validateKey(key: String) {
        validateSlug("property name", key, 64)
    }
}
