#!/usr/bin/env python3
"""Validate repository files that define the public runtime contract."""

from __future__ import annotations

import json
import os
import pathlib
import re
import sys


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
    readme_text = pathlib.Path("README.md").read_text(encoding="utf-8")
    dockerfile_text = pathlib.Path("Dockerfile").read_text(encoding="utf-8")
    compose_text = pathlib.Path("compose.yml").read_text(encoding="utf-8")
    contract_text = pathlib.Path("docs/contracts/market-ingest-app-contract.md").read_text(
        encoding="utf-8"
    )

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
        (contract_text, "scripts/check-repository-contract.py"),
        (contract_text, "scripts/check-release-readiness.sh"),
        (contract_text, "scripts/prepare-release-artifacts.sh"),
        (contract_text, "scripts/render-ecs-task-definition.sh"),
        (contract_text, "scripts/diagnose-l1-staleness.sh"),
        (contract_text, "CloudWatch metrics"),
        (contract_text, "CPU/Memory utilization thresholds"),
        (contract_text, "must not register the task"),
        (contract_text, "must not update ECS services"),
        (contract_text, "resolve the linux/arm64 child digest"),
    ]
    for text, phrase in required_contract_phrases:
        if phrase not in text:
            fail(f"contract text missing required phrase: {phrase}")


def check_public_leaks() -> None:
    public_roots = [
        pathlib.Path(".env.example"),
        pathlib.Path(".dockerignore"),
        pathlib.Path(".gitignore"),
        pathlib.Path(".github"),
        pathlib.Path("compose.yml"),
        pathlib.Path("config"),
        pathlib.Path("Dockerfile"),
        pathlib.Path("docs"),
        pathlib.Path("ecs"),
        pathlib.Path("README.md"),
        pathlib.Path("scripts"),
        pathlib.Path("sonar-project.properties"),
    ]
    checks = [
        ("aws_account_id", re.compile(r"\b[0-9]{12}\b")),
        (
            "actual_account_suffix_bucket",
            re.compile(r"nangman-crypto-dev-[A-Za-z0-9-]+-[0-9]{6}\b"),
        ),
        ("administrator_access_profile", re.compile("Administrator" + r"Access-")),
        (
            "private_ipv4",
            re.compile(
                r"\b(?:10|192\.168|172\.(?:1[6-9]|2[0-9]|3[0-1]))"
                r"\.[0-9]{1,3}\.[0-9]{1,3}(?:\.[0-9]{1,3})?\b"
            ),
        ),
        ("private_ecr_uri", re.compile(r"\b[0-9]{12}\.dkr\.ecr\.")),
    ]

    violations: list[str] = []
    for root in public_roots:
        paths = [root] if root.is_file() else sorted(path for path in root.rglob("*") if path.is_file())
        for path in paths:
            if ".git" in path.parts:
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            for label, pattern in checks:
                if pattern.search(text):
                    violations.append(f"{path}: {label}")

    if violations:
        print("public contract leak check failed:")
        for violation in violations:
            print(f"- {violation}")
        sys.exit(1)


def main() -> None:
    required_paths = [
        pathlib.Path("README.md"),
        pathlib.Path("Dockerfile"),
        pathlib.Path(".dockerignore"),
        pathlib.Path(".gitignore"),
        pathlib.Path(".github/workflows/sonar.yml"),
        pathlib.Path("sonar-project.properties"),
        pathlib.Path("compose.yml"),
        pathlib.Path("config/cost.paper.toml"),
        pathlib.Path("config/exchanges.toml"),
        pathlib.Path("config/universe.major-50.toml"),
        pathlib.Path("docs/contracts/market-ingest-app-contract.md"),
        pathlib.Path("ecs/service.example.json"),
        pathlib.Path("ecs/task-definition.example.json"),
        pathlib.Path("ecs/task-role-policy.example.json"),
        pathlib.Path("scripts/deploy.sh"),
        pathlib.Path("scripts/check-runtime.sh"),
        pathlib.Path("scripts/check-ecr-scan.sh"),
        pathlib.Path("scripts/render-ecs-task-definition.sh"),
        pathlib.Path("scripts/diagnose-l1-staleness.sh"),
        pathlib.Path("scripts/check-repository-contract.py"),
        pathlib.Path("scripts/check-release-readiness.sh"),
        pathlib.Path("scripts/prepare-release-artifacts.sh"),
    ]
    executable_paths = [
        pathlib.Path("scripts/deploy.sh"),
        pathlib.Path("scripts/check-runtime.sh"),
        pathlib.Path("scripts/check-ecr-scan.sh"),
        pathlib.Path("scripts/render-ecs-task-definition.sh"),
        pathlib.Path("scripts/diagnose-l1-staleness.sh"),
        pathlib.Path("scripts/check-repository-contract.py"),
        pathlib.Path("scripts/check-release-readiness.sh"),
        pathlib.Path("scripts/prepare-release-artifacts.sh"),
    ]

    require_paths(required_paths)
    require_executable(executable_paths)
    check_service_example(pathlib.Path("ecs/service.example.json"))
    check_task_definition_example(pathlib.Path("ecs/task-definition.example.json"))
    load_json(pathlib.Path("ecs/task-role-policy.example.json"))
    check_required_phrases()
    check_public_leaks()
    print("repository contract gate ok")


if __name__ == "__main__":
    main()
