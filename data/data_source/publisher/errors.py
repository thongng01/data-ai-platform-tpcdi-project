"""Define safe publisher errors that never include transport exception text."""


class PublisherError(Exception):
    """Describe a local contract or state failure with a safe fixed message."""


class DeliveryUncertain(PublisherError):
    """Mark a send whose remote outcome cannot be proved by the local journal."""
