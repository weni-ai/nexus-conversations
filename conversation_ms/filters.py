from django_filters import rest_framework as filters
from conversation_ms.models import Conversation


class ConversationFilter(filters.FilterSet):
    """
    Filter for Conversation model.
    """
    start_date = filters.IsoDateTimeFilter(field_name="start_date", lookup_expr="gte")
    end_date = filters.IsoDateTimeFilter(field_name="end_date", lookup_expr="lte")
    status = filters.NumberFilter(field_name="resolution")
    csat = filters.BaseInFilter(field_name="csat")
    resolution = filters.BaseInFilter(field_name="resolution")
    topics = filters.BaseInFilter(field_name="classification__topic__name")
    has_chats_room = filters.BooleanFilter(field_name="has_chats_room")
    nps = filters.NumberFilter(field_name="nps")
    
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
            "has_chats_room"
        ]

    def search_filter(self, queryset, name, value):
        """Custom search filter for contact_name and contact_urn"""
        from django.db.models import Q
        return queryset.filter(Q(contact_name__icontains=value) | Q(contact_urn__icontains=value))