import datetime

from django.db.models import Q
from django.utils import timezone
from django_filters import rest_framework as filters

from conversation_ms.models import Conversation


class CharInFilter(filters.BaseInFilter, filters.CharFilter):
    pass


class NumberInFilter(filters.BaseInFilter, filters.NumberFilter):
    pass


class ConversationFilter(filters.FilterSet):
    """
    Filter for Conversation model.
    """

    start_date = filters.DateFilter(
        field_name="start_date",
        lookup_expr="gte",
        input_formats=["%d-%m-%Y", "%Y-%m-%d", "iso-8601"],
    )
    end_date = filters.DateFilter(
        method="filter_end_date",
        input_formats=["%d-%m-%Y", "%Y-%m-%d", "iso-8601"],
    )
    status = filters.NumberFilter(field_name="resolution")
    csat = CharInFilter(field_name="csat")
    resolution = CharInFilter(field_name="resolution")
    topics = CharInFilter(field_name="classification__topic__name")
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

    def search_filter(self, queryset, name, value):
        """Custom search filter for contact_name and contact_urn"""
        return queryset.filter(Q(contact_name__icontains=value) | Q(contact_urn__icontains=value))

    def filter_end_date(self, queryset, name, value):
        """
        Filter end_date to include the entire day (up to 23:59:59).
        The filter should apply to the start_date field, not the end_date field of the conversation.
        """
        if not value:
            return queryset

        # Combine date with max time to get end of day
        dt = datetime.datetime.combine(value, datetime.time.max)
        if timezone.is_naive(dt):
            dt = timezone.make_aware(dt)

        return queryset.filter(start_date__lte=dt)
