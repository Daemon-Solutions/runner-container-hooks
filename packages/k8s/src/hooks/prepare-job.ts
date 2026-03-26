import * as core from '@actions/core'
import * as k8s from '@kubernetes/client-node'
import {
  JobContainerInfo,
  ContextPorts,
  PrepareJobArgs,
  writeToResponseFile,
  ServiceContainerInfo
} from 'hooklib'
import {
  containerPorts,
  createJobPod,
  isPodContainerAlpine,
  prunePods,
  waitForPodPhases,
  getPrepareJobTimeoutSeconds,
  execCpToPod,
  execPodStep
} from '../k8s'
import {
  CONTAINER_VOLUMES,
  DEFAULT_CONTAINER_ENTRY_POINT,
  DEFAULT_CONTAINER_ENTRY_POINT_ARGS,
  generateContainerName,
  mergeContainerWithOptions,
  readExtensionFromFile,
  PodPhase,
  fixArgs,
  prepareJobScript
} from '../k8s/utils'
import {
  CONTAINER_EXTENSION_PREFIX,
  getJobPodName,
  JOB_CONTAINER_NAME
} from './constants'
import { dirname } from 'path'
import * as fs from 'fs'
import * as path from 'path'

export async function prepareJob(
  args: PrepareJobArgs,
  responseFile
): Promise<void> {
  core.info('[prepareJob] Starting prepareJob hook')
  core.info(`[prepareJob] Args: ${JSON.stringify(args)}`)
  if (!args.container) {
    core.error('[prepareJob] No job container provided!')
    throw new Error('Job Container is required.')
  }

  await prunePods()
  core.info('[prepareJob] Pruned old pods')

  const extension = readExtensionFromFile()

  let container: k8s.V1Container | undefined = undefined
  if (args.container?.image) {
    core.info(
      `[prepareJob] Creating main container spec for image: ${args.container.image}`
    )
    container = createContainerSpec(
      args.container,
      JOB_CONTAINER_NAME,
      true,
      extension
    )
  }

  let services: k8s.V1Container[] = []
  if (args.services?.length) {
    core.info(
      `[prepareJob] Creating service container specs for: ${args.services.map(s => s.image).join(', ')}`
    )
    services = args.services.map(service => {
      return createContainerSpec(
        service,
        generateContainerName(service.image),
        false,
        extension
      )
    })
  }

  if (!container && !services?.length) {
    core.error('[prepareJob] No containers exist, skipping hook invocation')
    throw new Error('No containers exist, skipping hook invocation')
  }

  let createdPod: k8s.V1Pod | undefined = undefined
  try {
    core.info('[prepareJob] Creating job pod...')
    createdPod = await createJobPod(
      getJobPodName(),
      container,
      services,
      args.container.registry,
      extension
    )
    core.info(`[prepareJob] Created pod: ${createdPod?.metadata?.name}`)
  } catch (err) {
    await prunePods()
    core.error(`[prepareJob] createPod failed: ${JSON.stringify(err)}`)
    const message = (err as any)?.response?.body?.message || err
    throw new Error(`failed to create job pod: ${message}`)
  }

  if (!createdPod?.metadata?.name) {
    core.error('[prepareJob] created pod should have metadata.name')
    throw new Error('created pod should have metadata.name')
  }
  core.info(
    `[prepareJob] Job pod created, waiting for it to come online: ${createdPod?.metadata?.name}`
  )

  const runnerWorkspace = dirname(process.env.RUNNER_WORKSPACE as string)
  core.info(`[prepareJob] runnerWorkspace: ${runnerWorkspace}`)

  let prepareScript: { containerPath: string; runnerPath: string } | undefined
  if (args.container?.userMountVolumes?.length) {
    core.info(
      `[prepareJob] Preparing job script for userMountVolumes: ${JSON.stringify(args.container.userMountVolumes)}`
    )
    prepareScript = prepareJobScript(args.container.userMountVolumes || [])
    core.info(`[prepareJob] prepareScript: ${JSON.stringify(prepareScript)}`)
  }

  try {
    core.info('[prepareJob] Waiting for pod to reach RUNNING phase...')
    await waitForPodPhases(
      createdPod.metadata.name,
      new Set([PodPhase.RUNNING]),
      new Set([PodPhase.PENDING]),
      getPrepareJobTimeoutSeconds()
    )
    core.info('[prepareJob] Pod is RUNNING')
  } catch (err) {
    await prunePods()
    core.error(`[prepareJob] pod failed to come online: ${err}`)
    throw new Error(`pod failed to come online with error: ${err}`)
  }

  core.info(
    `[prepareJob] Copying workspace to pod: ${createdPod.metadata.name}`
  )

  // Add debugging before copy
  core.info(`[DEBUG] About to copy workspace`)
  core.info(`[DEBUG] Source: ${runnerWorkspace}`)
  core.info(`[DEBUG] Destination: /__w`)
  core.info(`[DEBUG] Pod name: ${createdPod.metadata.name}`)

  // Add sleep for debugging
  core.info(
    '[DEBUG] Sleeping for 300 seconds to allow manual debugging of pods...'
  )
  core.info(
    `[DEBUG] Runner pod: Check logs with 'kubectl logs -n <namespace> <runner-pod-name>'`
  )
  core.info(`[DEBUG] Workflow pod: ${createdPod.metadata.name}`)
  core.info(
    `[DEBUG] Exec into workflow pod: kubectl exec -it -n <namespace> ${createdPod.metadata.name} -- /bin/sh`
  )
  core.info(`[DEBUG] Check runner pod filesystem: ls -la /home/runner/`)
  // await new Promise(resolve => setTimeout(resolve, 300000)) // 5 minute sleep
  core.info('[DEBUG] Sleep complete, continuing with workspace copy...')

  try {
    core.info(`[DEBUG] Starting execCpToPod...`)
    await execCpToPod(createdPod.metadata.name, runnerWorkspace, '/__w')
    core.info(`[DEBUG] Workspace copy completed successfully`)
  } catch (err) {
    core.error(`[DEBUG] Workspace copy failed with error`)
    core.error(`[DEBUG] Error type: ${typeof err}`)
    core.error(`[DEBUG] Error: ${err}`)
    core.error(`[DEBUG] Error message: ${(err as Error)?.message}`)
    core.error(`[DEBUG] Error stack: ${(err as Error)?.stack}`)
    core.error(`[DEBUG] Full error object: ${JSON.stringify(err, null, 2)}`)

    // Don't throw immediately - try to write a response file first
    try {
      const errorResponse = {
        state: {
          error: 'workspace copy failed',
          details: String(err)
        },
        context: {},
        isAlpine: false
      }
      writeToResponseFile(responseFile, JSON.stringify(errorResponse))
      core.info(`[DEBUG] Wrote error response file`)
    } catch (writeErr) {
      core.error(`[DEBUG] Failed to write error response: ${writeErr}`)
    }

    throw err
  }

  if (prepareScript) {
    core.info(
      `[prepareJob] Executing prepare script in pod: ${prepareScript.containerPath}`
    )
    await execPodStep(
      ['sh', '-e', prepareScript.containerPath],
      createdPod.metadata.name,
      JOB_CONTAINER_NAME
    )

    const promises: Promise<void>[] = []
    for (const vol of args?.container?.userMountVolumes || []) {
      core.info(
        `[prepareJob] Copying user volume to pod: ${vol.sourceVolumePath} -> ${vol.targetVolumePath}`
      )
      promises.push(
        execCpToPod(
          createdPod.metadata.name,
          vol.sourceVolumePath,
          vol.targetVolumePath
        )
      )
    }
    await Promise.all(promises)
    core.info('[prepareJob] All user volumes copied')
  }

  core.info('[prepareJob] Job pod is ready for traffic')

  let isAlpine = false
  try {
    core.info('[prepareJob] Checking if pod is Alpine...')
    isAlpine = await isPodContainerAlpine(
      createdPod.metadata.name,
      JOB_CONTAINER_NAME
    )
    core.info(`[prepareJob] isAlpine: ${isAlpine}`)
  } catch (err) {
    core.error(
      `[prepareJob] Failed to determine if the pod is alpine: ${JSON.stringify(err)}`
    )
    const message = (err as any)?.response?.body?.message || err
    throw new Error(`failed to determine if the pod is alpine: ${message}`)
  }
  core.info(`[prepareJob] Setting isAlpine to ${isAlpine}`)
  generateResponseFile(responseFile, args, createdPod, isAlpine)
}

function generateResponseFile(
  responseFile: string,
  args: PrepareJobArgs,
  appPod: k8s.V1Pod,
  isAlpine: boolean
): void {
  // Add debugging at the start
  core.info('[DEBUG] generateResponseFile - Starting')
  core.info(`[DEBUG] Response file path: ${responseFile}`)
  core.info(`[DEBUG] Response file directory: ${dirname(responseFile)}`)

  // Check if directory exists
  const responseDir = path.dirname(responseFile)

  try {
    const dirExists = fs.existsSync(responseDir)
    core.info(`[DEBUG] Response file directory exists: ${dirExists}`)

    if (dirExists) {
      const stats = fs.statSync(responseDir)
      core.info(
        `[DEBUG] Response file directory permissions: ${JSON.stringify({
          mode: stats.mode.toString(8),
          uid: stats.uid,
          gid: stats.gid,
          isDirectory: stats.isDirectory()
        })}`
      )
    } else {
      core.warning(
        `[DEBUG] Response file directory does not exist, attempting to create: ${responseDir}`
      )
      fs.mkdirSync(responseDir, { recursive: true, mode: 0o777 })
      core.info(`[DEBUG] Created directory: ${responseDir}`)
    }
  } catch (err) {
    core.error(
      `[DEBUG] Error checking/creating response file directory: ${err}`
    )
    core.error(`[DEBUG] Error details: ${JSON.stringify(err)}`)
  }

  if (!appPod.metadata?.name) {
    throw new Error('app pod must have metadata.name specified')
  }

  const response = {
    state: {
      jobPod: appPod.metadata.name
    },
    context: {},
    isAlpine
  }

  const mainContainer = appPod.spec?.containers?.find(
    c => c.name === JOB_CONTAINER_NAME
  )
  if (mainContainer) {
    const mainContainerContextPorts: ContextPorts = {}
    if (mainContainer?.ports) {
      for (const port of mainContainer.ports) {
        mainContainerContextPorts[port.containerPort] =
          mainContainerContextPorts.hostPort
      }
    }

    response.context['container'] = {
      image: mainContainer.image,
      ports: mainContainerContextPorts
    }
  }

  if (args.services?.length) {
    const serviceContainerNames =
      args.services?.map(s => generateContainerName(s.image)) || []

    response.context['services'] = appPod?.spec?.containers
      ?.filter(c => serviceContainerNames.includes(c.name))
      .map(c => {
        const ctxPorts: ContextPorts = {}
        if (c.ports?.length) {
          for (const port of c.ports) {
            if (port.containerPort && port.hostPort) {
              ctxPorts[port.containerPort.toString()] = port.hostPort.toString()
            }
          }
        }

        return {
          image: c.image,
          ports: ctxPorts
        }
      })
  }

  core.info(`[DEBUG] About to write response file`)
  core.info(`[DEBUG] Response content: ${JSON.stringify(response, null, 2)}`)

  try {
    writeToResponseFile(responseFile, JSON.stringify(response))
    core.info(`[DEBUG] Successfully wrote response file`)

    // Verify the file was written
    if (fs.existsSync(responseFile)) {
      const fileStats = fs.statSync(responseFile)
      core.info(
        `[DEBUG] Response file created successfully: ${JSON.stringify({
          size: fileStats.size,
          mode: fileStats.mode.toString(8),
          uid: fileStats.uid,
          gid: fileStats.gid
        })}`
      )
    } else {
      core.error(
        `[DEBUG] Response file does not exist after write: ${responseFile}`
      )
    }
  } catch (err) {
    core.error(`[DEBUG] Error writing response file: ${err}`)
    core.error(`[DEBUG] Error details: ${JSON.stringify(err)}`)
    throw err
  }
}

export function createContainerSpec(
  container: JobContainerInfo | ServiceContainerInfo,
  name: string,
  jobContainer = false,
  extension?: k8s.V1PodTemplateSpec
): k8s.V1Container {
  if (!container.entryPoint && jobContainer) {
    container.entryPoint = DEFAULT_CONTAINER_ENTRY_POINT
    container.entryPointArgs = DEFAULT_CONTAINER_ENTRY_POINT_ARGS
  }

  const podContainer = {
    name,
    image: container.image,
    ports: containerPorts(container)
  } as k8s.V1Container
  if (container['workingDirectory']) {
    podContainer.workingDir = container['workingDirectory']
  }

  if (container.entryPoint) {
    podContainer.command = [container.entryPoint]
  }

  if (container.entryPointArgs && container.entryPointArgs.length > 0) {
    podContainer.args = fixArgs(container.entryPointArgs)
  }

  podContainer.env = []
  for (const [key, value] of Object.entries(
    container['environmentVariables'] || {}
  )) {
    if (value && key !== 'HOME') {
      podContainer.env.push({ name: key, value })
    }
  }

  podContainer.env.push({
    name: 'GITHUB_ACTIONS',
    value: 'true'
  })

  if (!('CI' in (container['environmentVariables'] || {}))) {
    podContainer.env.push({
      name: 'CI',
      value: 'true'
    })
  }

  podContainer.volumeMounts = CONTAINER_VOLUMES

  if (!extension) {
    return podContainer
  }

  const from = extension.spec?.containers?.find(
    c => c.name === CONTAINER_EXTENSION_PREFIX + name
  )

  if (from) {
    mergeContainerWithOptions(podContainer, from)
  }

  return podContainer
}
