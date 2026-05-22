#!/usr/bin/env python3
"""Validate repository files that define the public runtime contract."""

from __future__ import annotations

import json
import os
import pathlib
import re
import sys


README_PATH = pathlib.Path("README.md")
DOCKERFILE_PATH = pathlib.Path("Dockerfile")
DOCKERIGNORE_PATH = pathlib.Path(".dockerignore")
GITIGNORE_PATH = pathlib.Path(".gitignore")
GITHUB_DIR = pathlib.Path(".github")
SONAR_WORKFLOW_PATH = GITHUB_DIR / "workflows" / "sonar.yml"
SONAR_PROJECT_PATH = pathlib.Path("sonar-project.properties")
COMPOSE_PATH = pathlib.Path("compose.yml")
CONFIG_DIR = pathlib.Path("config")
DOCS_DIR = pathlib.Path("docs")
ECS_DIR = pathlib.Path("ecs")
SCRIPTS_DIR = pathlib.Path("scripts")

CONTRACT_PATH = DOCS_DIR / "contracts" / "market-ingest-app-contract.md"
COST_CONFIG_PATH = CONFIG_DIR / "cost.paper.toml"
EXCHANGES_CONFIG_PATH = CONFIG_DIR / "exchanges.toml"
UNIVERSE_CONFIG_PATH = CONFIG_DIR / "universe.major-50.toml"
SERVICE_EXAMPLE_PATH = ECS_DIR / "service.example.json"
TASK_DEFINITION_EXAMPLE_PATH = ECS_DIR / "task-definition.example.json"
TASK_ROLE_POLICY_EXAMPLE_PATH = ECS_DIR / "task-role-policy.example.json"
DEPLOY_SCRIPT_PATH = SCRIPTS_DIR / "deploy.sh"
CHECK_RUNTIME_SCRIPT_PATH = SCRIPTS_DIR / "check-runtime.sh"
CHECK_ECR_SCAN_SCRIPT_PATH = SCRIPTS_DIR / "check-ecr-scan.sh"
RENDER_TASK_DEFINITION_SCRIPT_PATH = SCRIPTS_DIR / "render-ecs-task-definition.sh"
DIAGNOSE_L1_STALENESS_SCRIPT_PATH = SCRIPTS_DIR / "diagnose-l1-staleness.sh"
CHECK_REPOSITORY_CONTRACT_SCRIPT_PATH = SCRIPTS_DIR / "check-repository-contract.py"
CHECK_RELEASE_READINESS_SCRIPT_PATH = SCRIPTS_DIR / "check-release-readiness.sh"
PREPARE_RELEASE_ARTIFACTS_SCRIPT_PATH = SCRIPTS_DIR / "prepare-release-artifacts.sh"

REQUIRED_PATHS = [
    README_PATH,
    DOCKERFILE_PATH,
    DOCKERIGNORE_PATH,
    GITIGNORE_PATH,
    SONAR_WORKFLOW_PATH,
    SONAR_PROJECT_PATH,
    COMPOSE_PATH,
    COST_CONFIG_PATH,
    EXCHANGES_CONFIG_PATH,
    UNIVERSE_CONFIG_PATH,
    CONTRACT_PATH,
    SERVICE_EXAMPLE_PATH,
    TASK_DEFINITION_EXAMPLE_PATH,
    TASK_ROLE_POLICY_EXAMPLE_PATH,
    DEPLOY_SCRIPT_PATH,
    CHECK_RUNTIME_SCRIPT_PATH,
    CHECK_ECR_SCAN_SCRIPT_PATH,
    RENDER_TASK_DEFINITION_SCRIPT_PATH,
    DIAGNOSE_L1_STALENESS_SCRIPT_PATH,
    CHECK_REPOSITORY_CONTRACT_SCRIPT_PATH,
    CHECK_RELEASE_READINESS_SCRIPT_PATH,
    PREPARE_RELEASE_ARTIFACTS_SCRIPT_PATH,
]
EXECUTABLE_PATHS = [
    DEPLOY_SCRIPT_PATH,
    CHECK_RUNTIME_SCRIPT_PATH,
    CHECK_ECR_SCAN_SCRIPT_PATH,
    RENDER_TASK_DEFINITION_SCRIPT_PATH,
    DIAGNOSE_L1_STALENESS_SCRIPT_PATH,
    CHECK_REPOSITORY_CONTRACT_SCRIPT_PATH,
    CHECK_RELEASE_READINESS_SCRIPT_PATH,
    PREPARE_RELEASE_ARTIFACTS_SCRIPT_PATH,
]


def fail(message: str) -> None:
    print(message, file=sys.stderr)
    sys.exit(1)


def require_paths(paths: list[pathlib.Path]) -> None:
    missing_paths = [str(path) for path in paths if not path.exists()]
    if missing_paths:
        print("missing required repository files:")
        for path in missing_paths:
            print(f"- {path}")
        sys.exit(1)


def require_executable(paths: list[pathlib.Path]) -> None:
    non_executable_paths = [str(path) for path in paths if not os.access(path, os.X_OK)]
    if non_executable_paths:
        print("required scripts must be executable:")
        for path in non_executable_paths:
            print(f"- {path}")
        sys.exit(1)


def load_json(path: pathlib.Path) -> object:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def check_service_example(path: pathlib.Path) -> None:
    service = load_json(path)
    if not isinstance(service, dict):
        fail(f"{path} must contain a JSON object")

    capacity_providers = {
        item.get("capacityProvider"): item
        for item in service.get("capacityProviderStrategy", [])
        if isinstance(item, dict)
    }
    spot = capacity_providers.get("FARGATE_SPOT")
    if not spot or int(spot.get("weight", 0)) < 1 or int(spot.get("base", -1)) != 0:
        fail("ecs/service.example.json must use FARGATE_SPOT with weight>=1 and base=0")
    if int(service.get("desiredCount", 0)) != 1:
        fail("ecs/service.example.json desiredCount must be 1 for the single supervisor service")

    awsvpc = service.get("networkConfiguration", {}).get("awsvpcConfiguration", {})
    if awsvpc.get("assignPublicIp") != "DISABLED":
        fail("ecs/service.example.json must disable public IP assignment")
    if not awsvpc.get("subnets") or not awsvpc.get("securityGroups"):
        fail("ecs/service.example.json must include subnet and security group placeholders")


def check_task_definition_example(path: pathlib.Path) -> None:
    task_definition = load_json(path)
    if not isinstance(task_definition, dict):
        fail(f"{path} must contain a JSON object")

    runtime_platform = task_definition.get("runtimePlatform", {})
    if runtime_platform.get("cpuArchitecture") != "ARM64":
        fail("ecs/task-definition.example.json must target ARM64")
    if "FARGATE" not in task_definition.get("requiresCompatibilities", []):
        fail("ecs/task-definition.example.json must require FARGATE compatibility")

    containers = task_definition.get("containerDefinitions", [])
    if len(containers) != 1:
        fail("ecs/task-definition.example.json must contain one supervisor container")
    container = containers[0]
    if container.get("readonlyRootFilesystem") is not True:
        fail("ecs/task-definition.example.json container must use readonlyRootFilesystem=true")

    container_user = (container.get("user") or "").strip().lower()
    if container_user in {"", "0", "0:0", "root", "root:root"}:
        fail("ecs/task-definition.example.json container must set an explicit non-root user")

    capability_drops = {
        item.upper()
        for item in (
            container.get("linuxParameters", {})
            .get("capabilities", {})
            .get("drop", [])
        )
    }
    if "ALL" not in capability_drops:
        fail("ecs/task-definition.example.json container must drop all Linux capabilities")

    command = container.get("command", [])
    if "--l0-s3-bucket" not in command or "--l1-s3-bucket" not in command:
        fail("ecs/task-definition.example.json command must pass explicit L0/L1 buckets")


def check_required_phrases() -> None:
    readme_text = README_PATH.read_text(encoding="utf-8")
    dockerfile_text = DOCKERFILE_PATH.read_text(encoding="utf-8")
    compose_text = COMPOSE_PATH.read_text(encoding="utf-8")
    contract_text = CONTRACT_PATH.read_text(encoding="utf-8")

    contract_script_phrases = [
        str(CHECK_REPOSITORY_CONTRACT_SCRIPT_PATH),
        str(CHECK_RELEASE_READINESS_SCRIPT_PATH),
        str(PREPARE_RELEASE_ARTIFACTS_SCRIPT_PATH),
        str(RENDER_TASK_DEFINITION_SCRIPT_PATH),
        str(DIAGNOSE_L1_STALENESS_SCRIPT_PATH),
    ]

    required_contract_phrases = [
        (readme_text, "market-ingest-app은 현재 NATS subject를 직접 publish하지 않는다"),
        (readme_text, "linux/arm64 child digest"),
        (dockerfile_text, "ARG NANGMAN_GIT_SHA=unknown"),
        (dockerfile_text, "ARG NANGMAN_GIT_DIRTY=true"),
        (compose_text, "NANGMAN_GIT_SHA: ${NANGMAN_GIT_SHA:-unknown}"),
        (compose_text, "NANGMAN_GIT_DIRTY: ${NANGMAN_GIT_DIRTY:-true}"),
        (contract_text, "NATS subject emitted by market-ingest-app: none"),
        (contract_text, "downstream handoff contract is the success-only"),
        (contract_text, "capabilities.drop=[\"ALL\"]"),
        (contract_text, "runner_git_sha"),
        (contract_text, "runner_git_dirty"),
        (contract_text, "CloudWatch metrics"),
        (contract_text, "CPU/Memory utilization thresholds"),
        (contract_text, "must not register the task"),
        (contract_text, "must not update ECS services"),
        (contract_text, "resolve the linux/arm64 child digest"),
    ]
    required_contract_phrases.extend(
        (contract_text, phrase) for phrase in contract_script_phrases
    )
    for text, phrase in required_contract_phrases:
        if phrase not in text:
            fail(f"contract text missing required phrase: {phrase}")


def public_roots() -> list[pathlib.Path]:
    return [
        pathlib.Path(".env.example"),
        DOCKERIGNORE_PATH,
        GITIGNORE_PATH,
        GITHUB_DIR,
        COMPOSE_PATH,
        CONFIG_DIR,
        DOCKERFILE_PATH,
        DOCS_DIR,
        ECS_DIR,
        README_PATH,
        SCRIPTS_DIR,
        SONAR_PROJECT_PATH,
    ]


def public_leak_checks() -> list[tuple[str, re.Pattern[str]]]:
    return [
        ("aws_account_id", re.compile(r"\b\d{12}\b")),
        (
            "actual_account_suffix_bucket",
            re.compile(r"nangman-crypto-dev-[A-Za-z0-9-]+-\d{6}\b"),
        ),
        ("administrator_access_profile", re.compile("Administrator" + r"Access-")),
        (
            "private_ipv4",
            re.compile(
                r"\b(?:10|192\.168|172\.(?:1[6-9]|2[0-9]|3[0-1]))"
                r"\.\d{1,3}\.\d{1,3}(?:\.\d{1,3})?\b"
            ),
        ),
        ("private_ecr_uri", re.compile(r"\b\d{12}\.dkr\.ecr\.")),
    ]


def iter_public_files(roots: list[pathlib.Path]) -> list[pathlib.Path]:
    paths: list[pathlib.Path] = []
    for root in roots:
        if root.is_file():
            paths.append(root)
        else:
            paths.extend(sorted(path for path in root.rglob("*") if path.is_file()))
    return [path for path in paths if ".git" not in path.parts]


def path_leak_violations(
    path: pathlib.Path, checks: list[tuple[str, re.Pattern[str]]]
) -> list[str]:
    text = path.read_text(encoding="utf-8", errors="ignore")
    return [f"{path}: {label}" for label, pattern in checks if pattern.search(text)]


def find_public_leaks() -> list[str]:
    violations: list[str] = []
    checks = public_leak_checks()
    for path in iter_public_files(public_roots()):
        violations.extend(path_leak_violations(path, checks))
    return violations


def check_public_leaks() -> None:
    violations = find_public_leaks()
    if violations:
        print("public contract leak check failed:")
        for violation in violations:
            print(f"- {violation}")
        sys.exit(1)


def main() -> None:
    require_paths(REQUIRED_PATHS)
    require_executable(EXECUTABLE_PATHS)
    check_service_example(SERVICE_EXAMPLE_PATH)
    check_task_definition_example(TASK_DEFINITION_EXAMPLE_PATH)
    load_json(TASK_ROLE_POLICY_EXAMPLE_PATH)
    check_required_phrases()
    check_public_leaks()
    print("repository contract gate ok")


if __name__ == "__main__":
    main()
