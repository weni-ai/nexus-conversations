from rest_framework import serializers

from conversation_ms.services.reconcile_cohort_export import parse_api_utc


def _is_provided(value) -> bool:
    return value is not None and str(value).strip() != ""


class ConversationsCountRequestSerializer(serializers.Serializer):
    """
    Body for POST ``/api/v1/projects/<uuid>/improvements/run/``.

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
            "poor_product_search_results",
            "repetitive_response",
            "custom_analysis",
        ],
    )
    recommended_action = serializers.CharField(allow_null=True)
    conversations_count = serializers.IntegerField(min_value=0)


class ImprovementsTaskSerializer(serializers.Serializer):
    is_running = serializers.BooleanField()
    progress = serializers.IntegerField(min_value=0)
    total = serializers.IntegerField(min_value=0)
    created_at = serializers.DateTimeField(allow_null=True)


class ImprovementsListResponseSerializer(serializers.Serializer):
    yesterday_conversations_count = serializers.IntegerField(min_value=0)
    improvements_task = ImprovementsTaskSerializer()
    improvements = ImprovementListItemSerializer(many=True)


class ImprovementAffectedMessageSerializer(serializers.Serializer):
    uuid = serializers.CharField()
    id = serializers.CharField()
    text = serializers.CharField(allow_null=True)
    source = serializers.ChoiceField(choices=["incoming", "outgoing"])
    created_at = serializers.CharField(allow_null=True)


class ImprovementAffectedConversationSerializer(serializers.Serializer):
    uuid = serializers.UUIDField()
    contact_urn = serializers.CharField()
    contact_name = serializers.CharField()
    messages = ImprovementAffectedMessageSerializer(many=True)


class ImprovementAffectedConversationsResponseSerializer(serializers.Serializer):
    count = serializers.IntegerField(min_value=0)
    next = serializers.CharField(allow_null=True)
    previous = serializers.CharField(allow_null=True)
    results = ImprovementAffectedConversationSerializer(many=True)


class ImprovementAffectedInstructionSerializer(serializers.Serializer):
    instruction_id = serializers.IntegerField()
    change_type = serializers.ChoiceField(choices=["add", "fix", "remove"])
    was_changed = serializers.BooleanField(allow_null=True)


class ImprovementDetailSerializer(serializers.Serializer):
    uuid = serializers.UUIDField()
    text = serializers.CharField()
    type = serializers.ChoiceField(
        choices=[
            "many_questions_before_answering",
            "wrong_behavior_due_to_instructions",
            "missing_static_knowledge",
            "personality_deviation",
            "poor_product_search_results",
            "repetitive_response",
            "custom_analysis",
        ],
    )
    recommended_action = serializers.CharField(allow_null=True)
    description = serializers.CharField()
    suggested_change = serializers.CharField(allow_null=True)
    status = serializers.ChoiceField(choices=["pending", "ignored", "resolved"])
    affected_instructions = ImprovementAffectedInstructionSerializer(many=True)


class ImprovementStatusUpdateSerializer(serializers.Serializer):
    status = serializers.ChoiceField(choices=["ignored", "resolved"])


class CustomAnalysisListItemSerializer(serializers.Serializer):
    uuid = serializers.UUIDField()
    title = serializers.CharField()
    conversations_count = serializers.IntegerField(min_value=0)


class CustomAnalysisDetailSerializer(serializers.Serializer):
    uuid = serializers.UUIDField()
    title = serializers.CharField()
    definition = serializers.CharField()
    exclusions = serializers.CharField()
    slug = serializers.SlugField()


class CustomAnalysisCreateSerializer(serializers.Serializer):
    title = serializers.CharField()
    definition = serializers.CharField()
    exclusions = serializers.CharField(required=False, allow_blank=True, default="")

    def validate_title(self, value):
        if not str(value).strip():
            raise serializers.ValidationError("Complete this field")
        return value

    def validate_definition(self, value):
        if not str(value).strip():
            raise serializers.ValidationError("Complete this field")
        return value


class CustomAnalysisUpdateSerializer(serializers.Serializer):
    title = serializers.CharField(required=False)
    definition = serializers.CharField(required=False)
    exclusions = serializers.CharField(required=False, allow_blank=True)

    def validate(self, attrs):
        if not attrs:
            raise serializers.ValidationError("At least one field must be provided")
        title = attrs.get("title")
        if title is not None and not str(title).strip():
            raise serializers.ValidationError({"title": "Complete this field"})
        definition = attrs.get("definition")
        if definition is not None and not str(definition).strip():
            raise serializers.ValidationError({"definition": "Complete this field"})
        return attrs


class OpenSupportTicketRequestSerializer(serializers.Serializer):
    improvement_uuid = serializers.UUIDField()
    project_name = serializers.CharField()


class OpenSupportTicketResponseSerializer(serializers.Serializer):
    """Pass-through response from the Nexus open-support-ticket endpoint."""

    def to_representation(self, instance):
        if isinstance(instance, dict):
            return instance
        return super().to_representation(instance)


class ImprovementsMetricsQuerySerializer(serializers.Serializer):
    start_date = serializers.DateField(required=False, allow_null=True, default=None)
    end_date = serializers.DateField(required=False, allow_null=True, default=None)

    def validate(self, attrs):
        from improvements.services.improvements_metrics_service import resolve_metrics_date_range

        start_date = attrs.get("start_date")
        end_date = attrs.get("end_date")
        try:
            resolve_metrics_date_range(start_date, end_date)
        except ValueError as exc:
            message = str(exc)
            raise serializers.ValidationError({"start_date": message, "end_date": message}) from exc
        return attrs


class ImprovementsMetricsUsageSerializer(serializers.Serializer):
    projects_with_runs = serializers.IntegerField()
    projects_with_more_than_one_run = serializers.IntegerField()
    total_runs = serializers.IntegerField()
    completed_runs = serializers.IntegerField()


class ImprovementsMetricsTypeCountSerializer(serializers.Serializer):
    dimension_id = serializers.CharField()
    count = serializers.IntegerField()


class ImprovementsMetricsDeliverySerializer(serializers.Serializer):
    avg_suggestions_per_completed_run = serializers.FloatField()
    projects_with_suggestions_count = serializers.IntegerField()
    top_improvement_types = ImprovementsMetricsTypeCountSerializer(many=True)


class ImprovementsMetricsActionsSerializer(serializers.Serializer):
    resolved_count = serializers.IntegerField()
    ignored_count = serializers.IntegerField()
    active_count = serializers.IntegerField()
    superseded_count = serializers.IntegerField()


class ImprovementsMetricsCustomAnalysisSerializer(serializers.Serializer):
    projects_with_active_custom_monitors = serializers.IntegerField()
    active_custom_monitors_total = serializers.IntegerField()
    project_uuids = serializers.ListField(child=serializers.UUIDField())


class ImprovementsMetricsOrphanRunSerializer(serializers.Serializer):
    run_uuid = serializers.UUIDField()
    project_uuid = serializers.UUIDField()
    status = serializers.CharField()
    started_at = serializers.DateTimeField()
    age_hours = serializers.FloatField()


class ImprovementsMetricsRuntimeSerializer(serializers.Serializer):
    avg_duration_seconds = serializers.FloatField(allow_null=True)
    p50_duration_seconds = serializers.FloatField(allow_null=True)
    p95_duration_seconds = serializers.FloatField(allow_null=True)
    avg_seconds_to_first_suggestion = serializers.FloatField(allow_null=True)
    orphan_runs_over_24h = ImprovementsMetricsOrphanRunSerializer(many=True)


class ImprovementsMetricsResponseSerializer(serializers.Serializer):
    start_date = serializers.DateField(allow_null=True)
    end_date = serializers.DateField(allow_null=True)
    usage = ImprovementsMetricsUsageSerializer()
    delivery = ImprovementsMetricsDeliverySerializer()
    actions = ImprovementsMetricsActionsSerializer()
    custom_analysis = ImprovementsMetricsCustomAnalysisSerializer()
    runtime = ImprovementsMetricsRuntimeSerializer()


class ImprovementsSuggestionsPerProjectQuerySerializer(ImprovementsMetricsQuerySerializer):
    page = serializers.IntegerField(required=False, default=1, min_value=1)
    page_size = serializers.IntegerField(required=False, default=50, min_value=1, max_value=100)


class ImprovementsSuggestionsPerProjectItemSerializer(serializers.Serializer):
    project_uuid = serializers.UUIDField()
    suggestions_count = serializers.IntegerField()
    completed_runs = serializers.IntegerField()


class ImprovementsSuggestionsPerProjectResponseSerializer(serializers.Serializer):
    count = serializers.IntegerField()
    next = serializers.CharField(allow_null=True)
    previous = serializers.CharField(allow_null=True)
    start_date = serializers.DateField(allow_null=True)
    end_date = serializers.DateField(allow_null=True)
    results = ImprovementsSuggestionsPerProjectItemSerializer(many=True)
