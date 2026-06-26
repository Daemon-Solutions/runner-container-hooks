import * as core from '@actions/core'
import * as path from 'path'
import * as fs from 'fs'
import { spawn } from 'child_process'
import * as k8s from '@kubernetes/client-node'
import tar from 'tar-fs'
import * as stream from 'stream'
import { WritableStreamBuffer } from 'stream-buffers'
import { createHash } from 'crypto'
import type { ContainerInfo, Registry } from 'hooklib'
import {
  getSecretName,
  JOB_CONTAINER_NAME,
  RunnerInstanceLabel
} from '../hooks/constants'
import {
  PodPhase,
  formatError,
  mergePodSpecWithOptions,
  mergeObjectMeta,
  fixArgs,
  listDirAllCommand,
  sleep,
  EXTERNALS_VOLUME_NAME,
  GITHUB_VOLUME_NAME,
  WORK_VOLUME
} from './utils'
import * as shlex from 'shlex'
import { parsePositiveMsEnv, WebSocketHeartbeat } from './heartbeat'
import type { HeartbeatWebSocket } from './heartbeat'

const kc = new k8s.KubeConfig()

kc.loadFromDefault()

const k8sApi = kc.makeApiClient(k8s.CoreV1Api)
const k8sBatchV1Api = kc.makeApiClient(k8s.BatchV1Api)
const k8sAuthorizationV1Api = kc.makeApiClient(k8s.AuthorizationV1Api)

const DEFAULT_WAIT_FOR_POD_TIME_SECONDS = 10 * 60 // 10 min

export const requiredPermissions = [
  {
    group: '',
    verbs: ['get', 'list', 'create', 'delete'],
    resource: 'pods',
    subresource: ''
  },
  {
    group: '',
    verbs: ['get', 'create'],
    resource: 'pods',
    subresource: 'exec'
  },
  {
    group: '',
    verbs: ['get', 'list', 'watch'],
    resource: 'pods',
    subresource: 'log'
  },
  {
    group: '',
    verbs: ['create', 'delete', 'get', 'list'],
    resource: 'secrets',
    subresource: ''
  }
]

export async function createJobPod(
  name: string,
  jobContainer?: k8s.V1Container,
  services?: k8s.V1Container[],
  registry?: Registry,
  extension?: k8s.V1PodTemplateSpec
): Promise<k8s.V1Pod> {
  core.debug(`[createJobPod] Starting pod creation: ${name}`)
  core.debug(`[createJobPod] Has jobContainer: ${!!jobContainer}`)
  core.debug(`[createJobPod] Services count: ${services?.length || 0}`)
  core.debug(`[createJobPod] Has registry: ${!!registry}`)
  core.debug(`[createJobPod] Has extension: ${!!extension}`)

  const containers: k8s.V1Container[] = []
  if (jobContainer) {
    core.debug(`[createJobPod] Adding job container: ${jobContainer.name}`)
    core.debug(
      `[createJobPod] Job container volumeMounts: ${JSON.stringify(jobContainer.volumeMounts?.map(vm => ({ name: vm.name, mountPath: vm.mountPath })))}`
    )
    containers.push(jobContainer)
  }
  if (services?.length) {
    core.debug(`[createJobPod] Adding ${services.length} service containers`)
    for (const service of services) {
      core.debug(
        `[createJobPod] Service: ${service.name}, volumeMounts: ${JSON.stringify(service.volumeMounts?.map(vm => ({ name: vm.name, mountPath: vm.mountPath })))}`
      )
    }
    containers.push(...services)
  }

  const appPod = new k8s.V1Pod()

  appPod.apiVersion = 'v1'
  appPod.kind = 'Pod'

  appPod.metadata = new k8s.V1ObjectMeta()
  appPod.metadata.name = name

  const instanceLabel = new RunnerInstanceLabel()
  appPod.metadata.labels = {
    [instanceLabel.key]: instanceLabel.value
  }
  appPod.metadata.annotations = {}

  appPod.spec = new k8s.V1PodSpec()
  appPod.spec.containers = containers
  appPod.spec.securityContext = {
    fsGroup: 1001
  }

  // Extract working directory from GITHUB_WORKSPACE
  // GITHUB_WORKSPACE is like /__w/repo-name/repo-name
  const githubWorkspace = process.env.GITHUB_WORKSPACE
  const workingDirPath = githubWorkspace?.split('/').slice(-2).join('/') ?? ''
  core.debug(`[createJobPod] GITHUB_WORKSPACE: ${githubWorkspace}`)
  core.debug(`[createJobPod] Extracted workingDirPath: ${workingDirPath}`)

  const initCommands = [
    'mkdir -p /mnt/externals',
    'mkdir -p /mnt/work',
    'mkdir -p /mnt/github',
    'mv /home/runner/externals/* /mnt/externals/'
  ]

  if (workingDirPath) {
    initCommands.push(`mkdir -p /mnt/work/${workingDirPath}`)
  }

  core.debug(`[createJobPod] Init commands: ${initCommands.join(' && ')}`)

  appPod.spec.initContainers = [
    {
      name: 'fs-init',
      image:
        process.env.ACTIONS_RUNNER_IMAGE ||
        'ghcr.io/actions/actions-runner:latest',
      command: ['sh', '-c', initCommands.join(' && ')],
      securityContext: {
        runAsGroup: 1001,
        runAsUser: 1001
      },
      volumeMounts: [
        {
          name: EXTERNALS_VOLUME_NAME,
          mountPath: '/mnt/externals'
        },
        {
          name: WORK_VOLUME,
          mountPath: '/mnt/work'
        },
        {
          name: GITHUB_VOLUME_NAME,
          mountPath: '/mnt/github'
        }
      ]
    }
  ]

  appPod.spec.restartPolicy = 'Never'

  core.debug(`[createJobPod] Creating standard volumes`)
  appPod.spec.volumes = [
    {
      name: EXTERNALS_VOLUME_NAME,
      emptyDir: {}
    },
    {
      name: GITHUB_VOLUME_NAME,
      emptyDir: {}
    },
    {
      name: WORK_VOLUME,
      emptyDir: {}
    }
  ]
  core.debug(
    `[createJobPod] Initial volumes: ${appPod.spec.volumes.map(v => v.name).join(', ')}`
  )

  if (registry) {
    core.debug(`[createJobPod] Creating docker registry secret`)
    const secret = await createDockerSecret(registry)
    if (!secret?.metadata?.name) {
      throw new Error(`created secret does not have secret.metadata.name`)
    }
    const secretReference = new k8s.V1LocalObjectReference()
    secretReference.name = secret.metadata.name
    appPod.spec.imagePullSecrets = [secretReference]
    core.debug(`[createJobPod] Added imagePullSecret: ${secret.metadata.name}`)
  }

  if (extension?.metadata) {
    core.debug(`[createJobPod] Merging extension metadata`)
    core.debug(
      `[createJobPod] Extension labels: ${JSON.stringify(extension.metadata.labels)}`
    )
    core.debug(
      `[createJobPod] Extension annotations: ${JSON.stringify(extension.metadata.annotations)}`
    )
    mergeObjectMeta(appPod, extension.metadata)
  }

  if (extension?.spec) {
    core.debug(`[createJobPod] Merging extension spec`)
    core.debug(
      `[createJobPod] Extension volumes: ${extension.spec.volumes?.map(v => v.name).join(', ') || 'none'}`
    )
    core.debug(
      `[createJobPod] Extension containers: ${extension.spec.containers?.map(c => c.name).join(', ') || 'none'}`
    )
    core.debug(
      `[createJobPod] Volumes BEFORE merge: ${appPod.spec.volumes.map(v => v.name).join(', ')}`
    )
    mergePodSpecWithOptions(appPod.spec, extension.spec)
    core.debug(
      `[createJobPod] Volumes AFTER merge: ${appPod.spec.volumes?.map(v => v.name).join(', ') || 'none'}`
    )
    core.debug(
      `[createJobPod] Total containers after merge: ${appPod.spec.containers.length}`
    )
  }

  core.debug(`[createJobPod] Final pod configuration:`)
  core.debug(
    `[createJobPod] - Volumes (${appPod.spec.volumes?.length || 0}): ${appPod.spec.volumes?.map(v => v.name).join(', ') || 'none'}`
  )
  core.debug(`[createJobPod] - Containers (${appPod.spec.containers.length}):`)
  for (const container of appPod.spec.containers) {
    core.debug(`[createJobPod]   * ${container.name}:`)
    core.debug(`[createJobPod]     - Image: ${container.image}`)
    core.debug(
      `[createJobPod]     - VolumeMounts (${container.volumeMounts?.length || 0}): ${container.volumeMounts?.map(vm => `${vm.name}@${vm.mountPath}`).join(', ') || 'none'}`
    )
  }
  core.debug(
    `[createJobPod] - InitContainers (${appPod.spec.initContainers?.length || 0})`
  )

  core.debug(`[createJobPod] Creating pod in namespace: ${namespace()}`)
  const result = await k8sApi.createNamespacedPod({
    namespace: namespace(),
    body: appPod
  })

  core.debug(
    `[createJobPod] Pod created successfully: ${result.metadata?.name}`
  )
  core.debug(`[createJobPod] Pod UID: ${result.metadata?.uid}`)

  return result
}

export async function createContainerStepPod(
  name: string,
  container: k8s.V1Container,
  extension?: k8s.V1PodTemplateSpec
): Promise<k8s.V1Pod> {
  const appPod = new k8s.V1Pod()

  appPod.apiVersion = 'v1'
  appPod.kind = 'Pod'

  appPod.metadata = new k8s.V1ObjectMeta()
  appPod.metadata.name = name

  const instanceLabel = new RunnerInstanceLabel()
  appPod.metadata.labels = {
    [instanceLabel.key]: instanceLabel.value
  }
  appPod.metadata.annotations = {}

  appPod.spec = new k8s.V1PodSpec()
  appPod.spec.containers = [container]

  appPod.spec.restartPolicy = 'Never'

  appPod.spec.volumes = [
    {
      name: EXTERNALS_VOLUME_NAME,
      emptyDir: {}
    },
    {
      name: GITHUB_VOLUME_NAME,
      emptyDir: {}
    },
    {
      name: WORK_VOLUME,
      emptyDir: {}
    }
  ]

  if (extension?.metadata) {
    mergeObjectMeta(appPod, extension.metadata)
  }

  if (extension?.spec) {
    mergePodSpecWithOptions(appPod.spec, extension.spec)
  }

  return await k8sApi.createNamespacedPod({
    namespace: namespace(),
    body: appPod
  })
}

export async function deletePod(name: string): Promise<void> {
  await k8sApi.deleteNamespacedPod({
    name,
    namespace: namespace(),
    gracePeriodSeconds: 0
  })
}

export async function execPodStep(
  command: string[],
  podName: string,
  containerName: string,
  stdin?: stream.Readable
): Promise<number> {
  const exec = new k8s.Exec(kc)
  core.debug(
    `[execPodStep] Starting execPodStep with command: ${JSON.stringify(command)}, podName: ${podName}, containerName: ${containerName}`
  )

  command = fixArgs(command)
  core.debug(`[execPodStep] Fixed command: ${JSON.stringify(command)}`)

  const DEFAULT_PING_PERIOD_MS = 5000
  const pingPeriodMs = parsePositiveMsEnv(
    process.env.ACTIONS_RUNNER_HEARTBEAT_PERIOD_MS,
    DEFAULT_PING_PERIOD_MS
  )
  const pongDeadlineMs = parsePositiveMsEnv(
    process.env.ACTIONS_RUNNER_HEARTBEAT_DEADLINE_MS,
    pingPeriodMs * 12 + 1000
  )
  core.debug(
    `[execPodStep] Heartbeat config: pingPeriodMs=${pingPeriodMs}, pongDeadlineMs=${pongDeadlineMs}`
  )

  const heartbeat = new WebSocketHeartbeat(pingPeriodMs, pongDeadlineMs)

  return new Promise<number>((resolve, reject) => {
    core.debug('[execPodStep] About to call exec.exec')
    let ws: HeartbeatWebSocket | null = null

    exec
      .exec(
        namespace(),
        podName,
        containerName,
        command,
        process.stdout,
        process.stderr,
        stdin ?? null,
        false /* tty */,
        async resp => {
          core.debug(
            `[execPodStep] execPodStep response: ${JSON.stringify(resp)}`
          )

          heartbeat.stop()

          // Close WebSocket and wait for it before resolving/rejecting
          const closeWebSocket = async (): Promise<void> => {
            const socket = ws
            if (
              socket &&
              (socket.readyState === 1 || socket.readyState === 0)
            ) {
              return new Promise<void>(closeResolve => {
                const closeTimeout = setTimeout(() => {
                  core.warning(
                    '[execPodStep] WebSocket close timeout, forcing cleanup'
                  )
                  closeResolve()
                }, 5000)

                socket.once('close', () => {
                  clearTimeout(closeTimeout)
                  core.debug('[execPodStep] WebSocket closed cleanly')
                  closeResolve()
                })
                socket.close()
              })
            }
          }

          if (resp.status === 'Success') {
            core.debug(`[execPodStep] Success, code: ${resp.code}`)
            await closeWebSocket()
            resolve(resp.code || 0)
          } else {
            core.debug(
              `[execPodStep] Failure: ${JSON.stringify({ message: resp?.message, details: resp?.details })}`
            )
            await closeWebSocket()
            reject(new Error(resp?.message || 'execPodStep failed'))
          }
        }
      )
      .then(websocket => {
        core.debug('[execPodStep] exec.exec resolved, ws object received')
        ws = websocket
        if (ws) {
          heartbeat.start(ws, reject)
        } else {
          core.warning('[Heartbeat] WebSocket is null, heartbeat not started')
        }
      })
      .catch(async e => {
        heartbeat.stop()
        core.error(`[execPodStep] exec.exec threw error: ${e}`)

        // Close WebSocket before rejecting with timeout protection
        const socket = ws
        if (socket && (socket.readyState === 1 || socket.readyState === 0)) {
          await new Promise<void>(closeResolve => {
            const closeTimeout = setTimeout(() => {
              core.warning(
                '[execPodStep] WebSocket close timeout in error handler'
              )
              closeResolve()
            }, 5000)

            socket.once('close', () => {
              clearTimeout(closeTimeout)
              closeResolve()
            })
            socket.close()
          })
        }

        reject(e)
      })
  })
}

export async function execCalculateOutputHashSorted(
  podName: string,
  containerName: string,
  command: string[]
): Promise<string> {
  const exec = new k8s.Exec(kc)

  let output = ''
  const outputWriter = new stream.Writable({
    write(chunk, _enc, cb) {
      try {
        output += chunk.toString('utf8')
        cb()
      } catch (e) {
        cb(e as Error)
      }
    }
  })

  await new Promise<void>((resolve, reject) => {
    exec
      .exec(
        namespace(),
        podName,
        containerName,
        command,
        outputWriter, // capture stdout
        process.stderr,
        null,
        false /* tty */,
        resp => {
          core.debug(`internalExecOutput response: ${JSON.stringify(resp)}`)
          if (resp.status === 'Success') {
            resolve()
          } else {
            core.debug(
              JSON.stringify({
                message: resp?.message,
                details: resp?.details
              })
            )
            reject(new Error(resp?.message || 'internalExecOutput failed'))
          }
        }
      )
      .catch(e => reject(e))
  })

  outputWriter.end()

  // Sort lines for consistent ordering across platforms
  const sortedOutput =
    output
      .split('\n')
      .filter(line => line.length > 0)
      .sort()
      .join('\n') + '\n'

  const hash = createHash('sha256')
  hash.update(sortedOutput)
  return hash.digest('hex')
}

export async function localCalculateOutputHashSorted(
  commands: string[]
): Promise<string> {
  return await new Promise<string>((resolve, reject) => {
    const child = spawn(commands[0], commands.slice(1), {
      stdio: ['ignore', 'pipe', 'ignore']
    })

    let output = ''
    child.stdout.on('data', chunk => {
      output += chunk.toString('utf8')
    })
    child.on('error', reject)
    child.on('close', (code: number) => {
      if (code === 0) {
        // Sort lines for consistent ordering across distributions/platforms
        const sortedOutput =
          output
            .split('\n')
            .filter(line => line.length > 0)
            .sort()
            .join('\n') + '\n'

        const hash = createHash('sha256')
        hash.update(sortedOutput)
        resolve(hash.digest('hex'))
      } else {
        reject(new Error(`child process exited with code ${code}`))
      }
    })
  })
}

export async function execCpToPod(
  podName: string,
  runnerPath: string,
  containerPath: string
): Promise<void> {
  core.debug(`[execCpToPod] Starting copy operation`)
  core.debug(`[execCpToPod] Source (runnerPath): ${runnerPath}`)
  core.debug(`[execCpToPod] Destination (containerPath): ${containerPath}`)
  core.debug(`[execCpToPod] Target pod: ${podName}`)
  core.debug(`[execCpToPod] Target container: ${JOB_CONTAINER_NAME}`)

  // Diagnostic-only: log source path info before attempting the copy.
  // Do not throw here — if the path is missing the exec will fail and the
  // retry loop will surface a clear error after 30 attempts.
  try {
    const sourceExists = fs.existsSync(runnerPath)
    core.debug(`[execCpToPod] Source path exists: ${sourceExists}`)
    if (sourceExists) {
      const sourceStats = fs.statSync(runnerPath)
      core.debug(
        `[execCpToPod] Source is directory: ${sourceStats.isDirectory()}`
      )
      if (sourceStats.isDirectory()) {
        const files = fs.readdirSync(runnerPath)
        core.debug(
          `[execCpToPod] Source directory contains ${files.length} items`
        )
        core.debug(
          `[execCpToPod] First few items: ${files.slice(0, 5).join(', ')}`
        )
      }
    } else {
      core.warning(`[execCpToPod] Source path does not exist: ${runnerPath}`)
    }
  } catch (err) {
    core.warning(`[execCpToPod] Error checking source path: ${err}`)
  }

  core.debug(`Copying ${runnerPath} to pod ${podName} at ${containerPath}`)

  const DEFAULT_PING_PERIOD_MS = 5000
  const pingPeriodMs = parsePositiveMsEnv(
    process.env.ACTIONS_RUNNER_HEARTBEAT_PERIOD_MS,
    DEFAULT_PING_PERIOD_MS
  )
  const pongDeadlineMs = parsePositiveMsEnv(
    process.env.ACTIONS_RUNNER_HEARTBEAT_DEADLINE_MS,
    pingPeriodMs * 12 + 1000
  )
  // OpenShift HAProxy can kill idle WebSocket connections — default to 10 min
  // to survive large workspace copies while still detecting truly stale sockets.
  const EXEC_TIMEOUT_MS = parsePositiveMsEnv(
    process.env.ACTIONS_RUNNER_EXEC_TIMEOUT_MS,
    600000
  )
  core.debug(
    `[execCpToPod] Heartbeat config: pingPeriodMs=${pingPeriodMs}, pongDeadlineMs=${pongDeadlineMs}`
  )
  core.debug(`[execCpToPod] Using exec timeout: ${EXEC_TIMEOUT_MS}ms`)

  let attempt = 0
  while (true) {
    core.debug(`[execCpToPod] Attempt ${attempt + 1} starting...`)
    const heartbeat = new WebSocketHeartbeat(pingPeriodMs, pongDeadlineMs)
    try {
      const exec = new k8s.Exec(kc)
      // Use tar to extract with --no-same-owner to avoid ownership issues.
      // Then use find to fix permissions. The -m flag helps but we also need to fix permissions after.
      const command = [
        'sh',
        '-c',
        `tar xf - --no-same-owner -C ${shlex.quote(containerPath)} 2>/dev/null; ` +
          `find ${shlex.quote(containerPath)} -type f -exec chmod u+rw {} \\; 2>/dev/null; ` +
          `find ${shlex.quote(containerPath)} -type d -exec chmod u+rwx {} \\; 2>/dev/null`
      ]
      core.debug(`[execCpToPod] Command to execute: ${JSON.stringify(command)}`)

      const readStream = tar.pack(runnerPath)
      const errStream = new WritableStreamBuffer()
      core.debug(`[execCpToPod] Executing tar extraction in pod...`)

      const execPromise = new Promise<void>((resolve, reject) => {
        let callbackFired = false
        let resolved = false
        let websocket: HeartbeatWebSocket | null = null

        exec
          .exec(
            namespace(),
            podName,
            JOB_CONTAINER_NAME,
            command,
            null,
            errStream,
            readStream,
            false,
            async status => {
              if (resolved) return
              callbackFired = true
              core.debug(`[execCpToPod] Exec callback invoked`)
              core.debug(
                `[execCpToPod] Exec completed with status: ${JSON.stringify(status)}`
              )

              const errStreamSize = errStream.size()
              core.debug(`[execCpToPod] Error stream size: ${errStreamSize}`)

              heartbeat.stop()

              const socket = websocket
              const closeWs = async (): Promise<void> => {
                if (
                  socket &&
                  (socket.readyState === 1 || socket.readyState === 0)
                ) {
                  return new Promise<void>(closeResolve => {
                    const t = setTimeout(() => {
                      core.warning(
                        '[execCpToPod] WebSocket close timeout after callback'
                      )
                      closeResolve()
                    }, 5000)
                    socket.once('close', () => {
                      clearTimeout(t)
                      core.debug(
                        '[execCpToPod] WebSocket closed after callback'
                      )
                      closeResolve()
                    })
                    socket.close()
                  })
                }
              }

              if (errStreamSize) {
                const errContent = errStream.getContentsAsString()
                core.error(`[execCpToPod] Error stream content: ${errContent}`)
                resolved = true
                await closeWs()
                reject(
                  new Error(
                    `Error from execCpToPod - status: ${status.status}, details: \n ${errContent}`
                  )
                )
                return
              }

              core.debug(`[execCpToPod] Exec successful, resolving...`)
              resolved = true
              await closeWs()
              resolve()
            }
          )
          .then(ws => {
            core.debug(`[execCpToPod] exec.exec() promise resolved`)
            core.debug(`[execCpToPod] WebSocket exists: ${!!ws}`)

            if (ws) {
              websocket = ws
              core.debug(`[execCpToPod] WebSocket readyState: ${ws.readyState}`)

              // Start heartbeat — rejects the outer promise if pong deadline
              // is missed, which forces a retry rather than an infinite hang.
              // Critical for OpenShift where HAProxy can silently drop idle
              // WebSocket connections mid-transfer.
              heartbeat.start(ws, (err: Error) => {
                if (!resolved) {
                  resolved = true
                  reject(err)
                }
              })

              const closeHandler = (code: number, reason: string): void => {
                core.debug(
                  `[execCpToPod] WebSocket closed: code=${code}, reason=${reason}`
                )
                // On OpenShift, the exec WebSocket can close cleanly (code 1000)
                // without the status callback firing if the command exits quickly.
                if (
                  code === 1000 &&
                  !callbackFired &&
                  !resolved &&
                  errStream.size() === 0
                ) {
                  core.debug(
                    `[execCpToPod] WebSocket closed normally without callback, resolving`
                  )
                  resolved = true
                  heartbeat.stop()
                  resolve()
                }
              }

              const errorHandler = (err: Error): void => {
                core.error(`[execCpToPod] WebSocket error: ${err.message}`)
                if (!callbackFired && !resolved) {
                  resolved = true
                  heartbeat.stop()
                  reject(err)
                }
              }

              ws.on('close', closeHandler)
              ws.on('error', errorHandler)
            } else {
              core.warning(
                '[execCpToPod] WebSocket is null, heartbeat not started'
              )
            }
          })
          .catch(e => {
            if (resolved) return
            core.error(`[execCpToPod] Exec threw error: ${e}`)
            core.error(`[execCpToPod] Error type: ${typeof e}`)
            core.error(`[execCpToPod] Error message: ${e?.message}`)
            core.error(`[execCpToPod] Error stack: ${e?.stack}`)
            core.error(`[execCpToPod] Error details: ${JSON.stringify(e)}`)
            if (!callbackFired) {
              resolved = true
              heartbeat.stop()
              const socket = websocket
              if (socket && socket.readyState === 1) {
                socket.close()
              }
              reject(e)
            }
          })
      })

      await Promise.race([
        execPromise,
        new Promise<void>((_, timeoutReject) =>
          setTimeout(
            () =>
              timeoutReject(
                new Error(`Tar extraction timed out after ${EXEC_TIMEOUT_MS}ms`)
              ),
            EXEC_TIMEOUT_MS
          )
        )
      ])

      core.debug(
        `[execCpToPod] Attempt ${attempt + 1} succeeded, breaking retry loop`
      )
      break
    } catch (error) {
      heartbeat.stop()
      core.error(`[execCpToPod] Attempt ${attempt + 1} failed: ${error}`)
      core.error(`[execCpToPod] Error type: ${typeof error}`)
      core.error(`[execCpToPod] Error message: ${(error as Error)?.message}`)
      core.error(`[execCpToPod] Error stack: ${(error as Error)?.stack}`)
      core.error(`[execCpToPod] Error details: ${JSON.stringify(error)}`)

      attempt++
      if (attempt >= 30) {
        core.error(`[execCpToPod] All 30 attempts failed, giving up`)
        throw new Error(
          `cpToPod failed after ${attempt} attempts: ${formatError(error)}`
        )
      }
      core.debug(`[execCpToPod] Sleeping 1 second before retry...`)
      await sleep(1000)
    }
  }

  core.debug(
    `[execCpToPod] Copy operation completed, starting hash verification...`
  )

  let attempts = 15
  const delay = 1000
  for (let i = 0; i < attempts; i++) {
    try {
      core.debug(`[execCpToPod] Hash verification attempt ${i + 1}/${attempts}`)

      core.debug(`[execCpToPod] Calculating local hash for: ${runnerPath}`)
      const want = await localCalculateOutputHashSorted([
        'sh',
        '-c',
        listDirAllCommand(runnerPath)
      ])
      core.debug(`[execCpToPod] Local hash: ${want}`)

      core.debug(`[execCpToPod] Calculating remote hash for: ${containerPath}`)
      const got = await execCalculateOutputHashSorted(
        podName,
        JOB_CONTAINER_NAME,
        ['sh', '-c', listDirAllCommand(containerPath)]
      )
      core.debug(`[execCpToPod] Remote hash: ${got}`)

      if (got !== want) {
        core.warning(
          `[execCpToPod] Hash mismatch on attempt ${i + 1}: want='${want}' got='${got}'`
        )
        await sleep(delay)
        continue
      }

      core.debug(`[execCpToPod] Hash verification successful!`)
      break
    } catch (error) {
      core.error(
        `[execCpToPod] Hash verification attempt ${i + 1} failed: ${error}`
      )
      await sleep(delay)
    }
  }

  core.debug(`[execCpToPod] execCpToPod completed successfully`)
}

export async function execCpFromPod(
  podName: string,
  containerPath: string,
  parentRunnerPath: string
): Promise<void> {
  const targetRunnerPath = `${parentRunnerPath}/${path.basename(containerPath)}`
  core.debug(`[execCpFromPod] Starting copy from pod`)
  core.debug(
    `[execCpFromPod] Copying from pod ${podName}: ${containerPath} -> ${targetRunnerPath}`
  )

  const DEFAULT_PING_PERIOD_MS = 5000
  const pingPeriodMs = parsePositiveMsEnv(
    process.env.ACTIONS_RUNNER_HEARTBEAT_PERIOD_MS,
    DEFAULT_PING_PERIOD_MS
  )
  const pongDeadlineMs = parsePositiveMsEnv(
    process.env.ACTIONS_RUNNER_HEARTBEAT_DEADLINE_MS,
    pingPeriodMs * 12 + 1000
  )
  core.debug(
    `[execCpFromPod] Heartbeat config: pingPeriodMs=${pingPeriodMs}, pongDeadlineMs=${pongDeadlineMs}`
  )

  let attempt = 0
  while (true) {
    core.debug(`[execCpFromPod] Attempt ${attempt + 1} starting`)
    const heartbeat = new WebSocketHeartbeat(pingPeriodMs, pongDeadlineMs)
    try {
      const exec = new k8s.Exec(kc)
      const containerPaths = containerPath.split('/')
      const dirname = containerPaths.pop() as string
      const command = [
        'tar',
        'cf',
        '-',
        '-C',
        containerPaths.join('/') || '/',
        dirname
      ]
      core.debug(`[execCpFromPod] Command: ${JSON.stringify(command)}`)

      const writerStream = tar.extract(parentRunnerPath)
      const errStream = new WritableStreamBuffer()

      await new Promise<void>((resolve, reject) => {
        let resolved = false
        let callbackFired = false
        let websocket: HeartbeatWebSocket | null = null

        exec
          .exec(
            namespace(),
            podName,
            JOB_CONTAINER_NAME,
            command,
            writerStream,
            errStream,
            null,
            false,
            async status => {
              if (resolved) return
              callbackFired = true
              core.debug(
                `[execCpFromPod] Exec callback: ${JSON.stringify(status)}`
              )

              heartbeat.stop()

              const socket = websocket
              const closeWs = async (): Promise<void> => {
                if (
                  socket &&
                  (socket.readyState === 1 || socket.readyState === 0)
                ) {
                  return new Promise<void>(closeResolve => {
                    const t = setTimeout(() => {
                      core.warning('[execCpFromPod] WebSocket close timeout')
                      closeResolve()
                    }, 5000)
                    socket.once('close', () => {
                      clearTimeout(t)
                      core.debug('[execCpFromPod] WebSocket closed cleanly')
                      closeResolve()
                    })
                    socket.close()
                  })
                }
              }

              if (errStream.size()) {
                const errContent = errStream.getContentsAsString()
                core.error(`[execCpFromPod] Error stream: ${errContent}`)
                resolved = true
                await closeWs()
                reject(
                  new Error(`Error from cpFromPod - details: \n ${errContent}`)
                )
                return
              }

              core.debug(`[execCpFromPod] Exec successful`)
              resolved = true
              await closeWs()
              resolve()
            }
          )
          .then(ws => {
            core.debug(`[execCpFromPod] WebSocket received: ${!!ws}`)
            if (ws) {
              websocket = ws
              core.debug(
                `[execCpFromPod] WebSocket readyState: ${ws.readyState}`
              )
              // Heartbeat keeps the connection alive through OpenShift HAProxy
              // and rejects (triggering retry) if the connection goes stale.
              heartbeat.start(ws, (err: Error) => {
                if (!resolved) {
                  resolved = true
                  reject(err)
                }
              })
            } else {
              core.warning(
                '[execCpFromPod] WebSocket is null, heartbeat not started'
              )
            }
          })
          .catch(e => {
            if (resolved) return
            core.error(`[execCpFromPod] exec.exec threw: ${e}`)
            core.error(`[execCpFromPod] Error type: ${typeof e}`)
            core.error(`[execCpFromPod] Error message: ${e?.message}`)
            core.error(`[execCpFromPod] Error details: ${JSON.stringify(e)}`)
            if (!callbackFired) {
              resolved = true
              heartbeat.stop()
              reject(e)
            }
          })
      })
      break
    } catch (error) {
      heartbeat.stop()
      core.debug(`[execCpFromPod] Attempt ${attempt + 1} failed: ${error}`)
      attempt++
      if (attempt >= 30) {
        throw new Error(
          `execCpFromPod failed after ${attempt} attempts: ${formatError(error)}`
        )
      }
      await sleep(1000)
    }
  }

  let attempts = 15
  const delay = 1000
  for (let i = 0; i < attempts; i++) {
    try {
      const want = await execCalculateOutputHashSorted(
        podName,
        JOB_CONTAINER_NAME,
        ['sh', '-c', listDirAllCommand(containerPath)]
      )

      const got = await localCalculateOutputHashSorted([
        'sh',
        '-c',
        listDirAllCommand(targetRunnerPath)
      ])

      if (got !== want) {
        core.debug(
          `The hash of the directory does not match the expected value; want='${want}' got='${got}'`
        )
        await sleep(delay)
        continue
      }

      break
    } catch (error) {
      core.debug(`Attempt ${i + 1} failed: ${error}`)
      await sleep(delay)
    }
  }
}

export async function waitForJobToComplete(jobName: string): Promise<void> {
  const backOffManager = new BackOffManager()
  while (true) {
    try {
      if (await isJobSucceeded(jobName)) {
        return
      }
    } catch (error) {
      throw new Error(`job ${jobName} has failed: ${formatError(error)}`)
    }
    await backOffManager.backOff()
  }
}

export async function createDockerSecret(
  registry: Registry
): Promise<k8s.V1Secret> {
  const authContent = {
    auths: {
      [registry.serverUrl || 'https://index.docker.io/v1/']: {
        username: registry.username,
        password: registry.password,
        auth: Buffer.from(`${registry.username}:${registry.password}`).toString(
          'base64'
        )
      }
    }
  }

  const runnerInstanceLabel = new RunnerInstanceLabel()

  const secretName = getSecretName()
  const secret = new k8s.V1Secret()
  secret.immutable = true
  secret.apiVersion = 'v1'
  secret.metadata = new k8s.V1ObjectMeta()
  secret.metadata.name = secretName
  secret.metadata.namespace = namespace()
  secret.metadata.labels = {
    [runnerInstanceLabel.key]: runnerInstanceLabel.value
  }
  secret.type = 'kubernetes.io/dockerconfigjson'
  secret.kind = 'Secret'
  secret.data = {
    '.dockerconfigjson': Buffer.from(JSON.stringify(authContent)).toString(
      'base64'
    )
  }

  return await k8sApi.createNamespacedSecret({
    namespace: namespace(),
    body: secret
  })
}

export async function createSecretForEnvs(envs: {
  [key: string]: string
}): Promise<string> {
  const runnerInstanceLabel = new RunnerInstanceLabel()

  const secret = new k8s.V1Secret()
  const secretName = getSecretName()
  secret.immutable = true
  secret.apiVersion = 'v1'
  secret.metadata = new k8s.V1ObjectMeta()
  secret.metadata.name = secretName

  secret.metadata.labels = {
    [runnerInstanceLabel.key]: runnerInstanceLabel.value
  }
  secret.kind = 'Secret'
  secret.data = {}
  for (const [key, value] of Object.entries(envs)) {
    secret.data[key] = Buffer.from(value).toString('base64')
  }

  await k8sApi.createNamespacedSecret({
    namespace: namespace(),
    body: secret
  })
  return secretName
}

export async function deleteSecret(name: string): Promise<void> {
  await k8sApi.deleteNamespacedSecret({
    name,
    namespace: namespace()
  })
}

export async function pruneSecrets(): Promise<void> {
  const secretList = await k8sApi.listNamespacedSecret({
    namespace: namespace(),
    labelSelector: new RunnerInstanceLabel().toString()
  })
  if (!secretList.items.length) {
    return
  }

  await Promise.all(
    secretList.items.map(
      async secret =>
        secret.metadata?.name && (await deleteSecret(secret.metadata.name))
    )
  )
}

export async function waitForPodPhases(
  podName: string,
  awaitingPhases: Set<PodPhase>,
  backOffPhases: Set<PodPhase>,
  maxTimeSeconds = DEFAULT_WAIT_FOR_POD_TIME_SECONDS
): Promise<void> {
  const backOffManager = new BackOffManager(maxTimeSeconds)
  let phase: PodPhase = PodPhase.UNKNOWN
  try {
    while (true) {
      phase = await getPodPhase(podName)
      if (awaitingPhases.has(phase)) {
        return
      }

      if (!backOffPhases.has(phase)) {
        throw new Error(
          `Pod ${podName} is unhealthy with phase status ${phase}`
        )
      }
      await backOffManager.backOff()
    }
  } catch (error) {
    throw new Error(
      `Pod ${podName} is unhealthy with phase status ${phase}: ${formatError(error)}`
    )
  }
}

export function getPrepareJobTimeoutSeconds(): number {
  const envTimeoutSeconds =
    process.env['ACTIONS_RUNNER_PREPARE_JOB_TIMEOUT_SECONDS']

  if (!envTimeoutSeconds) {
    return DEFAULT_WAIT_FOR_POD_TIME_SECONDS
  }

  const timeoutSeconds = parseInt(envTimeoutSeconds, 10)
  if (!timeoutSeconds || timeoutSeconds <= 0) {
    core.warning(
      `Prepare job timeout is invalid ("${timeoutSeconds}"): use an int > 0`
    )
    return DEFAULT_WAIT_FOR_POD_TIME_SECONDS
  }

  return timeoutSeconds
}

async function getPodPhase(name: string): Promise<PodPhase> {
  const podPhaseLookup = new Set<string>([
    PodPhase.PENDING,
    PodPhase.RUNNING,
    PodPhase.SUCCEEDED,
    PodPhase.FAILED,
    PodPhase.UNKNOWN
  ])
  const pod = await k8sApi.readNamespacedPod({
    name,
    namespace: namespace()
  })

  if (!pod.status?.phase || !podPhaseLookup.has(pod.status.phase)) {
    return PodPhase.UNKNOWN
  }
  return pod.status?.phase as PodPhase
}

async function isJobSucceeded(name: string): Promise<boolean> {
  const job = await k8sBatchV1Api.readNamespacedJob({
    name,
    namespace: namespace()
  })
  if (job.status?.failed) {
    throw new Error(`job ${name} has failed`)
  }
  return !!job.status?.succeeded
}

export async function getPodLogs(
  podName: string,
  containerName: string
): Promise<void> {
  const log = new k8s.Log(kc)
  const logStream = new stream.PassThrough()
  logStream.on('data', chunk => {
    // use write rather than console.log to prevent double line feed
    process.stdout.write(chunk)
  })

  logStream.on('error', err => {
    process.stderr.write(err.message)
  })

  await log.log(namespace(), podName, containerName, logStream, {
    follow: true,
    pretty: false,
    timestamps: false
  })
  await new Promise(resolve => logStream.on('end', () => resolve(null)))
}

export async function prunePods(): Promise<void> {
  const podList = await k8sApi.listNamespacedPod({
    namespace: namespace(),
    labelSelector: new RunnerInstanceLabel().toString()
  })
  if (!podList.items.length) {
    return
  }

  await Promise.all(
    podList.items.map(
      async pod => pod.metadata?.name && (await deletePod(pod.metadata.name))
    )
  )
}

export async function getPodStatus(
  name: string
): Promise<k8s.V1PodStatus | undefined> {
  const pod = await k8sApi.readNamespacedPod({
    name,
    namespace: namespace()
  })
  return pod.status
}

export async function isAuthPermissionsOK(): Promise<boolean> {
  const sar = new k8s.V1SelfSubjectAccessReview()
  const asyncs: Promise<k8s.V1SelfSubjectAccessReview>[] = []
  for (const resource of requiredPermissions) {
    for (const verb of resource.verbs) {
      sar.spec = new k8s.V1SelfSubjectAccessReviewSpec()
      sar.spec.resourceAttributes = new k8s.V1ResourceAttributes()
      sar.spec.resourceAttributes.verb = verb
      sar.spec.resourceAttributes.namespace = namespace()
      sar.spec.resourceAttributes.group = resource.group
      sar.spec.resourceAttributes.resource = resource.resource
      sar.spec.resourceAttributes.subresource = resource.subresource
      asyncs.push(
        k8sAuthorizationV1Api.createSelfSubjectAccessReview({ body: sar })
      )
    }
  }
  const responses = await Promise.all(asyncs)
  return responses.every(resp => resp.status?.allowed)
}

export async function isPodContainerAlpine(
  podName: string,
  containerName: string
): Promise<boolean> {
  let isAlpine = true
  try {
    await execPodStep(
      [
        'sh',
        '-c',
        `[ $(cat /etc/*release* | grep -i -e "^ID=*alpine*" -c) != 0 ] || exit 1`
      ],
      podName,
      containerName
    )
  } catch {
    isAlpine = false
  }

  return isAlpine
}

export function namespace(): string {
  if (process.env['ACTIONS_RUNNER_KUBERNETES_NAMESPACE']) {
    return process.env['ACTIONS_RUNNER_KUBERNETES_NAMESPACE']
  }

  const context = kc.getContexts().find(ctx => ctx.namespace)
  if (!context?.namespace) {
    throw new Error(
      'Failed to determine namespace, falling back to `default`. Namespace should be set in context, or in env variable "ACTIONS_RUNNER_KUBERNETES_NAMESPACE"'
    )
  }
  return context.namespace
}

class BackOffManager {
  private backOffSeconds = 1
  totalTime = 0
  constructor(private throwAfterSeconds?: number) {
    if (!throwAfterSeconds || throwAfterSeconds < 0) {
      this.throwAfterSeconds = undefined
    }
  }

  async backOff(): Promise<void> {
    await new Promise(resolve =>
      setTimeout(resolve, this.backOffSeconds * 1000)
    )
    this.totalTime += this.backOffSeconds
    if (this.throwAfterSeconds && this.throwAfterSeconds < this.totalTime) {
      throw new Error('backoff timeout')
    }
    if (this.backOffSeconds < 20) {
      this.backOffSeconds *= 2
    }
    if (this.backOffSeconds > 20) {
      this.backOffSeconds = 20
    }
  }
}

export function containerPorts(
  container: ContainerInfo
): k8s.V1ContainerPort[] {
  const ports: k8s.V1ContainerPort[] = []
  if (!container.portMappings?.length) {
    return ports
  }
  for (const portDefinition of container.portMappings) {
    const portProtoSplit = portDefinition.split('/')
    if (portProtoSplit.length > 2) {
      throw new Error(`Unexpected port format: ${portDefinition}`)
    }

    const port = new k8s.V1ContainerPort()
    port.protocol =
      portProtoSplit.length === 2 ? portProtoSplit[1].toUpperCase() : 'TCP'

    const portSplit = portProtoSplit[0].split(':')
    if (portSplit.length > 2) {
      throw new Error('ports should have at most one ":" separator')
    }

    const parsePort = (p: string): number => {
      const num = Number(p)
      if (!Number.isInteger(num) || num < 1 || num > 65535) {
        throw new Error(`invalid container port: ${p}`)
      }
      return num
    }

    if (portSplit.length === 1) {
      port.containerPort = parsePort(portSplit[0])
    } else {
      port.hostPort = parsePort(portSplit[0])
      port.containerPort = parsePort(portSplit[1])
    }

    ports.push(port)
  }
  return ports
}

export async function getPodByName(name): Promise<k8s.V1Pod> {
  return await k8sApi.readNamespacedPod({
    name,
    namespace: namespace()
  })
}
