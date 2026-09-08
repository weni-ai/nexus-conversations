class BillingError(Exception):
    """Base error for billing API calls."""

    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


class BillingPermanentError(BillingError):
    """The request will keep failing as sent. Retrying it is pointless."""


class BillingTransientError(BillingError):
    """The request may succeed on a later attempt."""
