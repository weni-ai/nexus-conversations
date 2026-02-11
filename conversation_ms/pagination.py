from rest_framework.pagination import PageNumberPagination


class MessagePagination(PageNumberPagination):
    page_size = 12
    page_size_query_param = "page_size"
    max_page_size = 50

    def get_paginated_response(self, data):
        return {
            "next": self.get_next_link(),
            "previous": self.get_previous_link(),
            "results": data,
        }
