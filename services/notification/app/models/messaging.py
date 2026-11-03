"""Event tables from the chassis (Appendix E.10), declared here so this service's migrations own them.
Notification only CONSUMES events (it publishes none), so it has processed_events but no outbox."""

from smarthire_common.kafka import ProcessedEventMixin
from smarthire_common.models import Base


class ProcessedEvent(ProcessedEventMixin, Base):
    """Events this service's consumers have already handled (redeliveries are skipped)."""
