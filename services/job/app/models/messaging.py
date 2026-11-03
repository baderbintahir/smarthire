"""Event tables from the chassis (Appendix E.10), declared here so this service's migrations own them."""

from smarthire_common.kafka import ProcessedEventMixin
from smarthire_common.models import Base
from smarthire_common.outbox import OutboxMixin


class OutboxEvent(OutboxMixin, Base):
    """Events waiting to be published (written in the same transaction as the change they describe)."""


class ProcessedEvent(ProcessedEventMixin, Base):
    """Events this service's consumers have already handled (redeliveries are skipped)."""
