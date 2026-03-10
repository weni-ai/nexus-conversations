"""
Resolution entities adapter.
Copied from router.repositories.entities for standalone microservice.
"""


class ResolutionEntities:
    RESOLVED = "0"
    UNRESOLVED = "1"
    IN_PROGRESS = "2"
    UNCLASSIFIED = "3"
    HAS_CHAT_ROOM = "4"

    @staticmethod
    def resolution_mapping(resolution_status: str) -> str:
        resolution_choices = {
            ResolutionEntities.RESOLVED: "resolved",
            ResolutionEntities.UNRESOLVED: "unresolved",
            ResolutionEntities.IN_PROGRESS: "in progress",
            ResolutionEntities.UNCLASSIFIED: "unclassified",
            ResolutionEntities.HAS_CHAT_ROOM: "Has Chat Room",
        }

        return resolution_choices.get(resolution_status, "unclassified")

    @staticmethod
    def convert_resolution_string_to_int(resolution_string: str) -> str:
        if isinstance(resolution_string, int):
            return str(resolution_string)
        if str(resolution_string).isdigit():
            return str(resolution_string)

        resolution_mapping = {
            "resolved": ResolutionEntities.RESOLVED,
            "unresolved": ResolutionEntities.UNRESOLVED,
            "in progress": ResolutionEntities.IN_PROGRESS,
            "unclassified": ResolutionEntities.UNCLASSIFIED,
            "has chat room": ResolutionEntities.HAS_CHAT_ROOM,
        }
        return resolution_mapping.get(resolution_string.lower(), ResolutionEntities.IN_PROGRESS)
