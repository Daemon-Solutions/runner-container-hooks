# v0.9.0 Release Notes

## Features

- Add WebSocket heartbeat to Kubernetes hook to keep connections alive during long-running job steps, preventing premature disconnections

## Bug Fixes

- Fix WebSocket connection stability issue causing exec sessions to drop mid-job
- Ensure WebSocket connections are properly closed and cleaned up after each step
- Fix callback firing order for WebSocket exec responses
- Increase pod exec timeouts to reduce spurious failures on slow clusters
- Overwrite `_runner_file_commands` correctly when merging temp directories between runner and pod
- Fix open handle leaks in Jest tests causing intermittent test failures

## Misc

- Reduce logging verbosity: demote several `core.info` calls to `core.debug` to avoid noisy runner logs
- Remove extraneous peer dependency entries from package manifests
- Add `--forceExit` to Jest configuration to ensure clean test teardown

## SHA-256 Checksums

The SHA-256 checksums for the packages included in this build are shown below:

- actions-runner-hooks-docker-<HOOK_VERSION>.zip <DOCKER_SHA>
- actions-runner-hooks-k8s-<HOOK_VERSION>.zip <K8S_SHA>
