from rest_framework import serializers

from conversation_ms.services.reconcile_cohort_export import parse_api_utc


def _is_provided(value) -> bool:
    return value is not None and str(value).strip() != ""


class ConversationsCountRequestSerializer(serializers.Serializer):
    """
    Body for POST ``/api/v1/projects/<uuid>/conversations-count/``.

    ``start_date`` and ``end_date`` are optional ISO datetimes. When both are omitted,
    the service uses yesterday in the project's timezone.
    """

    start_date = serializers.CharField(required=False, allow_null=True, default=None)
    end_date = serializers.CharField(required=False, allow_null=True, default=None)

    def validate(self, attrs):
        start_date = attrs.get("start_date")
        end_date = attrs.get("end_date")
        start_provided = _is_provided(start_date)
        end_provided = _is_provided(end_date)

        if start_provided != end_provided:
            raise serializers.ValidationError(
                "Both start_date and end_date must be provided together.",
            )

        if start_provided:
            try:
                start_bound = parse_api_utc(str(start_date).strip())
            except ValueError as e:
                raise serializers.ValidationError({"start_date": str(e)}) from e

            try:
                end_bound = parse_api_utc(str(end_date).strip())
            except ValueError as e:
                raise serializers.ValidationError({"end_date": str(e)}) from e

            if end_bound < start_bound:
                raise serializers.ValidationError(
                    {"end_date": "end_date must be on or after start_date"},
                )

        return attrs


class ConversationsCountResponseSerializer(serializers.Serializer):
    sampling_mode = serializers.CharField()
    total_count = serializers.IntegerField()
    target_date = serializers.CharField()


class ImprovementsCancelRequestSerializer(serializers.Serializer):
    target_date = serializers.CharField()


class ImprovementsCancelResponseSerializer(serializers.Serializer):
    run_key = serializers.CharField()
    cancel_requested = serializers.BooleanField()


class ImprovementListItemSerializer(serializers.Serializer):
    uuid = serializers.UUIDField()
    text = serializers.CharField()
    type = serializers.ChoiceField(
        choices=[
            "many_questions_before_answering",
            "wrong_behavior_due_to_instructions",
            "missing_static_knowledge",
            "personality_deviation",
            "mentions_competitors",
            "poor_product_search_results",
            "repetitive_response",
            "amazing_conversation",
        ],
    )
    conversations_count = serializers.IntegerField(min_value=0)


class ImprovementsListResponseSerializer(serializers.Serializer):
    improvements_count = serializers.IntegerField(min_value=0)
    improvements = ImprovementListItemSerializer(many=True)
