import * as core from '@actions/core'
import * as path from 'path'
import { spawn } from 'child_process'
import * as k8s from '@kubernetes/client-node'
import tar from 'tar-fs'
import * as stream from 'stream'
import { WritableStreamBuffer } from 'stream-buffers'
import { createHash } from 'crypto'
import * as fs from 'fs'
import type { ContainerInfo, Registry } from 'hooklib'
import {
  getSecretName,
  JOB_CONTAINER_NAME,
  RunnerInstanceLabel
} from '../hooks/constants'
import {
  PodPhase,
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
  core.info(`[createJobPod] Starting pod creation: ${name}`)
  core.info(`[createJobPod] Has jobContainer: ${!!jobContainer}`)
  core.info(`[createJobPod] Services count: ${services?.length || 0}`)
  core.info(`[createJobPod] Has registry: ${!!registry}`)
  core.info(`[createJobPod] Has extension: ${!!extension}`)

  const containers: k8s.V1Container[] = []
  if (jobContainer) {
    core.info(`[createJobPod] Adding job container: ${jobContainer.name}`)
    core.info(
      `[createJobPod] Job container volumeMounts: ${JSON.stringify(jobContainer.volumeMounts?.map(vm => ({ name: vm.name, mountPath: vm.mountPath })))}`
    )
    containers.push(jobContainer)
  }
  if (services?.length) {
    core.info(`[createJobPod] Adding ${services.length} service containers`)
    for (const service of services) {
      core.info(
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
  core.info(`[createJobPod] GITHUB_WORKSPACE: ${githubWorkspace}`)
  core.info(`[createJobPod] Extracted workingDirPath: ${workingDirPath}`)

  const initCommands = [
    'mkdir -p /mnt/externals',
    'mkdir -p /mnt/work',
    'mkdir -p /mnt/github',
    'mv /home/runner/externals/* /mnt/externals/'
  ]

  if (workingDirPath) {
    initCommands.push(`mkdir -p /mnt/work/${workingDirPath}`)
  }

  core.info(`[createJobPod] Init commands: ${initCommands.join(' && ')}`)

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

  core.info(`[createJobPod] Creating standard volumes`)
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
  core.info(
    `[createJobPod] Initial volumes: ${appPod.spec.volumes.map(v => v.name).join(', ')}`
  )

  if (registry) {
    core.info(`[createJobPod] Creating docker registry secret`)
    const secret = await createDockerSecret(registry)
    if (!secret?.metadata?.name) {
      throw new Error(`created secret does not have secret.metadata.name`)
    }
    const secretReference = new k8s.V1LocalObjectReference()
    secretReference.name = secret.metadata.name
    appPod.spec.imagePullSecrets = [secretReference]
    core.info(`[createJobPod] Added imagePullSecret: ${secret.metadata.name}`)
  }

  if (extension?.metadata) {
    core.info(`[createJobPod] Merging extension metadata`)
    core.info(
      `[createJobPod] Extension labels: ${JSON.stringify(extension.metadata.labels)}`
    )
    core.info(
      `[createJobPod] Extension annotations: ${JSON.stringify(extension.metadata.annotations)}`
    )
    mergeObjectMeta(appPod, extension.metadata)
  }

  if (extension?.spec) {
    core.info(`[createJobPod] Merging extension spec`)
    core.info(
      `[createJobPod] Extension volumes: ${extension.spec.volumes?.map(v => v.name).join(', ') || 'none'}`
    )
    core.info(
      `[createJobPod] Extension containers: ${extension.spec.containers?.map(c => c.name).join(', ') || 'none'}`
    )
    core.info(
      `[createJobPod] Volumes BEFORE merge: ${appPod.spec.volumes.map(v => v.name).join(', ')}`
    )
    mergePodSpecWithOptions(appPod.spec, extension.spec)
    core.info(
      `[createJobPod] Volumes AFTER merge: ${appPod.spec.volumes?.map(v => v.name).join(', ') || 'none'}`
    )
    core.info(
      `[createJobPod] Total containers after merge: ${appPod.spec.containers.length}`
    )
  }

  core.info(`[createJobPod] Final pod configuration:`)
  core.info(
    `[createJobPod] - Volumes (${appPod.spec.volumes?.length || 0}): ${appPod.spec.volumes?.map(v => v.name).join(', ') || 'none'}`
  )
  core.info(`[createJobPod] - Containers (${appPod.spec.containers.length}):`)
  for (const container of appPod.spec.containers) {
    core.info(`[createJobPod]   * ${container.name}:`)
    core.info(`[createJobPod]     - Image: ${container.image}`)
    core.info(
      `[createJobPod]     - VolumeMounts (${container.volumeMounts?.length || 0}): ${container.volumeMounts?.map(vm => `${vm.name}@${vm.mountPath}`).join(', ') || 'none'}`
    )
  }
  core.info(
    `[createJobPod] - InitContainers (${appPod.spec.initContainers?.length || 0})`
  )

  core.info(`[createJobPod] Creating pod in namespace: ${namespace()}`)
  const result = await k8sApi.createNamespacedPod({
    namespace: namespace(),
    body: appPod
  })

  core.info(`[createJobPod] Pod created successfully: ${result.metadata?.name}`)
  core.info(`[createJobPod] Pod UID: ${result.metadata?.uid}`)

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
  core.info(
    `[execPodStep] Starting execPodStep with command: ${JSON.stringify(command)}, podName: ${podName}, containerName: ${containerName}`
  )

  command = fixArgs(command)
  core.debug(`[execPodStep] Fixed command: ${JSON.stringify(command)}`)

  // Heartbeat constants matching kubectl's Go implementation
  const PING_PERIOD_MS = parseInt(
    process.env.ACTIONS_RUNNER_HEARTBEAT_PERIOD_MS || '5000',
    10
  )
  const PING_READ_DEADLINE_MS = parseInt(
    process.env.ACTIONS_RUNNER_HEARTBEAT_DEADLINE_MS ||
      String(PING_PERIOD_MS * 12 + 1000),
    10
  )
  core.debug(
    `[execPodStep] Heartbeat config: PING_PERIOD_MS=${PING_PERIOD_MS}, PING_READ_DEADLINE_MS=${PING_READ_DEADLINE_MS}`
  )

  let pingInterval: ReturnType<typeof setInterval> | null = null
  let pongTimeout: ReturnType<typeof setTimeout> | null = null

  const stopHeartbeat = (): void => {
    core.info('[Heartbeat] stopHeartbeat called')
    if (pingInterval) {
      clearInterval(pingInterval)
      pingInterval = null
    }
    if (pongTimeout) {
      clearTimeout(pongTimeout)
      pongTimeout = null
    }
  }

  const resetPongTimeout = (): void => {
    core.info('[Heartbeat] resetPongTimeout called')
    if (pongTimeout) {
      clearTimeout(pongTimeout)
    }
    pongTimeout = setTimeout(() => {
      core.warning(
        `[Heartbeat] No pong received in ${PING_READ_DEADLINE_MS}ms, connection may be stale`
      )
    }, PING_READ_DEADLINE_MS)
  }

  const startHeartbeat = (ws: any): void => {
    core.info(
      `[Heartbeat] Starting with period=${PING_PERIOD_MS}ms, deadline=${PING_READ_DEADLINE_MS}ms`
    )

    // Handle pong responses
    ws.on('pong', () => {
      core.info('[Heartbeat] Pong received')
      resetPongTimeout()
    })

    // Handle errors
    ws.on('error', (err: Error) => {
      core.error(`[Heartbeat] WebSocket error: ${err.message}`)
      stopHeartbeat()
    })

    // Cleanup on close
    ws.on('close', () => {
      core.info('[Heartbeat] WebSocket closed, stopping heartbeat')
      stopHeartbeat()
    })

    // Set initial pong timeout
    resetPongTimeout()

    // Start ping loop
    pingInterval = setInterval(() => {
      // WebSocket readyState: 0 = CONNECTING, 1 = OPEN, 2 = CLOSING, 3 = CLOSED
      core.info(`[Heartbeat] Ping loop, ws.readyState=${ws.readyState}`)
      if (ws.readyState === 1) {
        try {
          ws.ping()
          core.info('[Heartbeat] Ping sent')
        } catch (err) {
          core.error(`[Heartbeat] Ping failed: ${err}`)
          stopHeartbeat()
        }
      } else {
        core.info(
          `[Heartbeat] WebSocket not open (readyState=${ws.readyState}), stopping`
        )
        stopHeartbeat()
      }
    }, PING_PERIOD_MS)
  }

  return new Promise<number>((resolve, reject) => {
    core.info('[execPodStep] About to call exec.exec')
    let ws: any | null = null

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
          core.info(
            `[execPodStep] execPodStep response: ${JSON.stringify(resp)}`
          )

          // Stop heartbeat immediately
          stopHeartbeat()

          // Close WebSocket and wait for it before resolving/rejecting
          const closeWebSocket = async (): Promise<void> => {
            if (ws && (ws.readyState === 1 || ws.readyState === 0)) {
              return new Promise<void>(closeResolve => {
                // Set a timeout to ensure we don't hang forever
                const closeTimeout = setTimeout(() => {
                  core.warning(
                    '[execPodStep] WebSocket close timeout, forcing cleanup'
                  )
                  closeResolve()
                }, 5000)

                ws.once('close', () => {
                  clearTimeout(closeTimeout)
                  core.info('[execPodStep] WebSocket closed cleanly')
                  closeResolve()
                })
                ws.close()
              })
            }
          }

          if (resp.status === 'Success') {
            core.info(`[execPodStep] Success, code: ${resp.code}`)
            await closeWebSocket()
            resolve(resp.code || 0)
          } else {
            core.error(
              `[execPodStep] Failure: ${JSON.stringify({ message: resp?.message, details: resp?.details })}`
            )
            await closeWebSocket()
            reject(new Error(resp?.message || 'execPodStep failed'))
          }
        }
      )
      .then(websocket => {
        core.info('[execPodStep] exec.exec resolved, ws object received')
        ws = websocket
        // Start heartbeat once WebSocket is connected
        if (ws) {
          startHeartbeat(ws)
        } else {
          core.warning('[Heartbeat] WebSocket is null, heartbeat not started')
        }
      })
      .catch(async e => {
        stopHeartbeat()
        core.error(`[execPodStep] exec.exec threw error: ${e}`)

        // Close WebSocket before rejecting with timeout protection
        if (ws && (ws.readyState === 1 || ws.readyState === 0)) {
          await new Promise<void>(closeResolve => {
            const closeTimeout = setTimeout(() => {
              core.warning(
                '[execPodStep] WebSocket close timeout in error handler'
              )
              closeResolve()
            }, 5000)

            ws.once('close', () => {
              clearTimeout(closeTimeout)
              closeResolve()
            })
            ws.close()
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
  core.info(`[execCpToPod] Starting copy operation`)
  core.info(`[execCpToPod] Source (runnerPath): ${runnerPath}`)
  core.info(`[execCpToPod] Destination (containerPath): ${containerPath}`)
  core.info(`[execCpToPod] Target pod: ${podName}`)
  core.info(`[execCpToPod] Target container: ${JOB_CONTAINER_NAME}`)

  // Check if source path exists
  try {
    const sourceExists = fs.existsSync(runnerPath)
    core.info(`[execCpToPod] Source path exists: ${sourceExists}`)

    if (sourceExists) {
      const sourceStats = fs.statSync(runnerPath)
      core.info(
        `[execCpToPod] Source is directory: ${sourceStats.isDirectory()}`
      )
      core.info(`[execCpToPod] Source is file: ${sourceStats.isFile()}`)

      if (sourceStats.isDirectory()) {
        const files = fs.readdirSync(runnerPath)
        core.info(
          `[execCpToPod] Source directory contains ${files.length} items`
        )
        core.info(
          `[execCpToPod] First few items: ${files.slice(0, 5).join(', ')}`
        )
      }
    } else {
      core.error(`[execCpToPod] Source path does not exist: ${runnerPath}`)
      throw new Error(`Source path does not exist: ${runnerPath}`)
    }
  } catch (err) {
    core.error(`[execCpToPod] Error checking source path: ${err}`)
    throw err
  }

  core.debug(`Copying ${runnerPath} to pod ${podName} at ${containerPath}`)
  // await new Promise(resolve => setTimeout(resolve, 300000)) // 5 minute sleep [DISABLED]
  let attempt = 0
  while (true) {
    try {
      core.info(`[execCpToPod] Attempt ${attempt + 1} starting...`)

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

      core.info(`[execCpToPod] Command to execute: ${JSON.stringify(command)}`)
      core.info(`[execCpToPod] Creating tar pack from: ${runnerPath}`)

      const readStream = tar.pack(runnerPath)
      const errStream = new WritableStreamBuffer()

      core.info(`[execCpToPod] Executing tar extraction in pod...`)

      // Create a timeout promise
      // Create a timeout promise with configurable timeout
      const EXEC_TIMEOUT_MS = parseInt(
        process.env.ACTIONS_RUNNER_EXEC_TIMEOUT_MS || '600000',
        10
      ) // 10 minutes default
      core.info(`[execCpToPod] Using timeout: ${EXEC_TIMEOUT_MS}ms`)

      const execPromise = new Promise((resolve, reject) => {
        core.info(`[execCpToPod] About to call exec.exec()`)

        let callbackFired = false
        let resolved = false
        let websocket: any | null = null

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
              core.info(`[execCpToPod] Exec callback invoked`)
              core.info(
                `[execCpToPod] Exec completed with status: ${JSON.stringify(status)}`
              )

              const errStreamSize = errStream.size()
              core.info(`[execCpToPod] Error stream size: ${errStreamSize}`)

              if (errStreamSize) {
                const errContent = errStream.getContentsAsString()
                core.error(`[execCpToPod] Error stream content: ${errContent}`)
                resolved = true

                // Close WebSocket and wait for close event before rejecting
                if (websocket && websocket.readyState === 1) {
                  await new Promise<void>(closeResolve => {
                    websocket.once('close', () => {
                      core.info('[execCpToPod] WebSocket closed after error')
                      closeResolve()
                    })
                    websocket.close()
                  })
                }

                reject(
                  new Error(
                    `Error from execCpToPod - status: ${status.status}, details: \n ${errContent}`
                  )
                )
                return
              }
              core.info(`[execCpToPod] Exec successful, resolving...`)
              resolved = true

              // Close WebSocket and wait for close event before resolving
              if (websocket && websocket.readyState === 1) {
                await new Promise<void>(closeResolve => {
                  websocket.once('close', () => {
                    core.info('[execCpToPod] WebSocket closed cleanly')
                    closeResolve()
                  })
                  websocket.close()
                })
              }

              resolve(status)
            }
          )
          .then(ws => {
            core.info(`[execCpToPod] exec.exec() promise resolved`)
            core.info(`[execCpToPod] WebSocket object type: ${typeof ws}`)
            core.info(`[execCpToPod] WebSocket exists: ${!!ws}`)

            if (ws) {
              websocket = ws
              core.info(`[execCpToPod] WebSocket readyState: ${ws.readyState}`)

              const closeHandler = (code: number, reason: string): void => {
                core.info(
                  `[execCpToPod] WebSocket closed: code=${code}, reason=${reason}`
                )

                // If WebSocket closes normally and callback hasn't fired, resolve immediately
                if (
                  code === 1000 &&
                  !callbackFired &&
                  !resolved &&
                  errStream.size() === 0
                ) {
                  core.info(
                    `[execCpToPod] WebSocket closed normally without callback, resolving immediately`
                  )
                  resolved = true
                  resolve({ status: 'Success' })
                }
              }

              const errorHandler = (err: Error): void => {
                core.error(`[execCpToPod] WebSocket error: ${err.message}`)
                if (!callbackFired && !resolved) {
                  resolved = true
                  reject(err)
                }
              }

              ws.on('close', closeHandler)
              ws.on('error', errorHandler)

              // Clean up event listeners when promise settles
              const cleanup = (): void => {
                if (websocket) {
                  websocket.removeListener('close', closeHandler)
                  websocket.removeListener('error', errorHandler)

                  // Force close if still open
                  if (
                    websocket.readyState === 1 ||
                    websocket.readyState === 0
                  ) {
                    core.info(
                      `[execCpToPod] Force closing WebSocket in cleanup`
                    )
                    websocket.close()
                  }
                }
              }

              // Attach cleanup to promise resolution/rejection
              execPromise.then(cleanup, cleanup).catch(() => {
                // Ignore cleanup errors
              })
            }
          })
          .catch(e => {
            if (resolved) return
            core.error(`[execCpToPod] Exec threw error: ${e}`)
            core.error(`[execCpToPod] Error type: ${typeof e}`)
            core.error(`[execCpToPod] Error message: ${e?.message}`)
            core.error(`[execCpToPod] Error stack: ${e?.stack}`)
            core.error(`[execCpToPod] Error details: ${JSON.stringify(e)}`)
            core.error(`[execCpToPod] exec.exec() promise rejected: ${e}`)
            if (!callbackFired) {
              resolved = true

              // Close WebSocket before rejecting
              if (websocket && websocket.readyState === 1) {
                websocket.close()
              }

              reject(e)
            }
          })
      })

      core.info(`[execCpToPod] Executing tar extraction in pod...`)
      core.info(`[execCpToPod] Using timeout: ${EXEC_TIMEOUT_MS}ms`)

      // Use Promise.race to implement timeout
      await Promise.race([
        execPromise,
        new Promise((_, reject) =>
          setTimeout(
            () =>
              reject(
                new Error(`Tar extraction timed out after ${EXEC_TIMEOUT_MS}ms`)
              ),
            EXEC_TIMEOUT_MS
          )
        )
      ])

      core.info(
        `[execCpToPod] Attempt ${attempt + 1} succeeded, breaking retry loop`
      )
      break
    } catch (error) {
      core.error(`[execCpToPod] Attempt ${attempt + 1} failed: ${error}`)
      core.error(`[execCpToPod] Error type: ${typeof error}`)
      core.error(`[execCpToPod] Error message: ${(error as Error)?.message}`)
      core.error(`[execCpToPod] Error stack: ${(error as Error)?.stack}`)
      core.error(`[execCpToPod] Error details: ${JSON.stringify(error)}`)

      attempt++
      if (attempt >= 30) {
        core.error(`[execCpToPod] All 30 attempts failed, giving up`)
        throw new Error(
          `cpToPod failed after ${attempt} attempts: ${JSON.stringify(error)}`
        )
      }

      core.info(`[execCpToPod] Sleeping 1 second before retry...`)
      await sleep(1000)
    }
  }

  core.info(
    `[execCpToPod] Copy operation completed, starting hash verification...`
  )

  let attempts = 15
  const delay = 1000
  for (let i = 0; i < attempts; i++) {
    try {
      core.info(`[execCpToPod] Hash verification attempt ${i + 1}/${attempts}`)

      core.info(`[execCpToPod] Calculating local hash for: ${runnerPath}`)
      const want = await localCalculateOutputHashSorted([
        'sh',
        '-c',
        listDirAllCommand(runnerPath)
      ])
      core.info(`[execCpToPod] Local hash: ${want}`)

      core.info(`[execCpToPod] Calculating remote hash for: ${containerPath}`)
      const got = await execCalculateOutputHashSorted(
        podName,
        JOB_CONTAINER_NAME,
        ['sh', '-c', listDirAllCommand(containerPath)]
      )
      core.info(`[execCpToPod] Remote hash: ${got}`)

      if (got !== want) {
        core.warning(
          `[execCpToPod] Hash mismatch on attempt ${i + 1}: want='${want}' got='${got}'`
        )
        await sleep(delay)
        continue
      }

      core.info(`[execCpToPod] Hash verification successful!`)
      break
    } catch (error) {
      core.error(
        `[execCpToPod] Hash verification attempt ${i + 1} failed: ${error}`
      )
      await sleep(delay)
    }
  }

  core.info(`[execCpToPod] execCpToPod completed successfully`)
}

export async function execCpFromPod(
  podName: string,
  containerPath: string,
  parentRunnerPath: string
): Promise<void> {
  const targetRunnerPath = `${parentRunnerPath}/${path.basename(containerPath)}`
  core.debug(
    `Copying from pod ${podName} ${containerPath} to ${targetRunnerPath}`
  )

  let attempt = 0
  while (true) {
    try {
      // make temporary directory
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
      const writerStream = tar.extract(parentRunnerPath)
      const errStream = new WritableStreamBuffer()

      await new Promise((resolve, reject) => {
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
              if (errStream.size()) {
                reject(
                  new Error(
                    `Error from cpFromPod - details: \n ${errStream.getContentsAsString()}`
                  )
                )
              }
              resolve(status)
            }
          )
          .catch(e => reject(e))
      })
      break
    } catch (error) {
      core.debug(`Attempt ${attempt + 1} failed: ${error}`)
      attempt++
      if (attempt >= 30) {
        throw new Error(
          `execCpFromPod failed after ${attempt} attempts: ${JSON.stringify(error)}`
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
      throw new Error(`job ${jobName} has failed: ${JSON.stringify(error)}`)
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
      `Pod ${podName} is unhealthy with phase status ${phase}: ${JSON.stringify(error)}`
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
