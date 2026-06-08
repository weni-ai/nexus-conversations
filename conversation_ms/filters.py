from django.db.models import Q
from django_filters import rest_framework as filters

from conversation_ms.models import Conversation

TOPIC_UNCLASSIFIED_SENTINEL = "unclassified"


class ConversationFilter(filters.FilterSet):
    """
    Filter for Conversation model.
    """

    start_date = filters.IsoDateTimeFilter(
        field_name="start_date",
        lookup_expr="gte",
    )
    end_date = filters.IsoDateTimeFilter(
        field_name="start_date",
        lookup_expr="lte",
    )
    status = filters.NumberFilter(field_name="resolution")
    csat = filters.BaseInFilter(field_name="csat")
    resolution = filters.BaseInFilter(field_name="resolution")
    topics = filters.CharFilter(method="filter_topics")
    has_chats_room = filters.BooleanFilter(field_name="has_chats_room")
    nps = filters.NumberFilter(field_name="nps")
    project_uuid = filters.UUIDFilter(field_name="project__uuid")

    search = filters.CharFilter(method="search_filter")

    class Meta:
        model = Conversation
        fields = [
            "start_date",
            "end_date",
            "status",
            "resolution",
            "csat",
            "nps",
            "has_chats_room",
            "project_uuid",
        ]

    def filter_topics(self, queryset, name, value):
        """Filter by topic name(s). Use 'unclassified' (case-insensitive) for conversations with no topic."""
        values = [v.strip() for v in value.split(",") if v.strip()]
        if not values:
            return queryset

        include_unclassified = any(v.lower() == TOPIC_UNCLASSIFIED_SENTINEL for v in values)
        named_topics = [v for v in values if v.lower() != TOPIC_UNCLASSIFIED_SENTINEL]

        q = Q()
        if include_unclassified:
            q |= Q(classification__isnull=True) | Q(classification__topic__isnull=True)
        if named_topics:
            q |= Q(classification__topic__name__in=named_topics)

        return queryset.filter(q).distinct() if q else queryset

    def search_filter(self, queryset, name, value):
        """Custom search filter for contact_name and contact_urn"""
        return queryset.filter(Q(contact_name__icontains=value) | Q(contact_urn__icontains=value))
