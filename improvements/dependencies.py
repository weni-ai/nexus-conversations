from __future__ import annotations

from dataclasses import dataclass

from improvements.adapters.boto3 import (
    Boto3ImprovementsLambdaClient,
    Boto3S3Storage,
    RedBeatBatchCheckScheduler,
)
from improvements.adapters.nexus import NexusProjectDataClient
from improvements.ports import (
    BatchCheckScheduler,
    ImprovementsLambdaClient,
    ProjectDataClient,
    S3Storage,
)

_improvements_dependencies: ImprovementsDependencies | None = None


@dataclass
class ImprovementsDependencies:
    s3: S3Storage
    lambda_client: ImprovementsLambdaClient
    scheduler: BatchCheckScheduler
    project_data: ProjectDataClient


def build_default_improvements_dependencies() -> ImprovementsDependencies:
    return ImprovementsDependencies(
        s3=Boto3S3Storage(),
        lambda_client=Boto3ImprovementsLambdaClient(),
        scheduler=RedBeatBatchCheckScheduler(),
        project_data=NexusProjectDataClient(),
    )


def get_improvements_dependencies() -> ImprovementsDependencies:
    global _improvements_dependencies
    if _improvements_dependencies is None:
        _improvements_dependencies = build_default_improvements_dependencies()
    return _improvements_dependencies


def set_improvements_dependencies(deps: ImprovementsDependencies) -> None:
    global _improvements_dependencies
    _improvements_dependencies = deps


def reset_improvements_dependencies() -> None:
    global _improvements_dependencies
    _improvements_dependencies = None
