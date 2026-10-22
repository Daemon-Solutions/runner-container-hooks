import { EventEmitter } from 'events'

const mockExec = jest.fn()

jest.mock('@actions/core', () => ({
  debug: jest.fn(),
  warning: jest.fn(),
  error: jest.fn(),
  info: jest.fn()
}))

jest.mock('@kubernetes/client-node', () => ({
  KubeConfig: class {
    loadFromDefault = jest.fn()
    makeApiClient = jest.fn()
  },
  CoreV1Api: {},
  BatchV1Api: {},
  AuthorizationV1Api: {},
  Exec: class {
    exec = mockExec
  }
}))

import { execPodStep } from '../src/k8s'

type StatusResponse = {
  status: 'Success' | 'Failure'
  code?: number
  message?: string
}

class MockWebSocket extends EventEmitter {
  readyState = 1
  ping = jest.fn()
  close = jest.fn((): void => {
    this.emit('close')
  })
}

function configureExec(
  websocket: MockWebSocket,
  onExec: (callback: (response: StatusResponse) => Promise<void>) => void
): void {
  mockExec.mockImplementation(
    async (
      _namespace: string,
      _pod: string,
      _container: string,
      _command: string[],
      _stdout: NodeJS.WritableStream,
      _stderr: NodeJS.WritableStream,
      _stdin: NodeJS.ReadableStream | null,
      _tty: boolean,
      callback: (response: StatusResponse) => Promise<void>
    ) => {
      onExec(callback)
      return Promise.resolve(websocket)
    }
  )
}

function deliverStatus(
  callback: (response: StatusResponse) => Promise<void>,
  response: StatusResponse
): void {
  setImmediate(() => {
    void callback(response)
  })
}

describe('execPodStep WebSocket closure handling', () => {
  beforeEach(() => {
    mockExec.mockReset()
    process.env.ACTIONS_RUNNER_KUBERNETES_NAMESPACE = 'default'
  })

  afterEach(() => {
    delete process.env.ACTIONS_RUNNER_KUBERNETES_NAMESPACE
    delete process.env.ACTIONS_RUNNER_EXEC_RESUME_ENABLED
    delete process.env.ACTIONS_RUNNER_HEARTBEAT_PERIOD_MS
    delete process.env.ACTIONS_RUNNER_HEARTBEAT_DEADLINE_MS
    jest.useRealTimers()
  })

  it('resolves the exit code for a Success status frame', async () => {
    const websocket = new MockWebSocket()
    configureExec(websocket, callback => {
      deliverStatus(callback, { status: 'Success', code: 7 })
    })

    await expect(execPodStep(['sh'], 'pod', 'container')).resolves.toBe(7)
  })

  it('resolves zero for a Success status frame with exit code zero', async () => {
    const websocket = new MockWebSocket()
    configureExec(websocket, callback => {
      deliverStatus(callback, { status: 'Success', code: 0 })
    })

    await expect(execPodStep(['sh'], 'pod', 'container')).resolves.toBe(0)
  })

  it('rejects for a Failure status frame', async () => {
    const websocket = new MockWebSocket()
    configureExec(websocket, callback => {
      deliverStatus(callback, { status: 'Failure', message: 'command failed' })
    })

    await expect(execPodStep(['sh'], 'pod', 'container')).rejects.toThrow(
      'command failed'
    )
  })

  it('rejects when a clean close arrives before a status frame', async () => {
    const websocket = new MockWebSocket()
    configureExec(websocket, () => {
      setImmediate(() => websocket.emit('close', 1001))
    })

    await expect(execPodStep(['sh'], 'pod', 'container')).rejects.toThrow(
      'exec stream closed before the command reported an exit status'
    )
  })

  it('rejects when a dropped connection closes before a status frame', async () => {
    const websocket = new MockWebSocket()
    configureExec(websocket, () => {
      setImmediate(() => websocket.emit('close'))
    })

    await expect(execPodStep(['sh'], 'pod', 'container')).rejects.toThrow(
      'exec stream closed before the command reported an exit status'
    )
  })

  it('resolves zero for a code-less Success frame (real kubelet shape)', async () => {
    const websocket = new MockWebSocket()
    configureExec(websocket, callback => {
      deliverStatus(callback, { status: 'Success' })
    })

    await expect(execPodStep(['sh'], 'pod', 'container')).resolves.toBe(0)
  })

  it('does not reject when the socket closes after the status frame', async () => {
    const websocket = new MockWebSocket()
    configureExec(websocket, callback => {
      setImmediate(() => {
        void callback({ status: 'Success', code: 0 })
        websocket.emit('close', 1000)
      })
    })

    await expect(execPodStep(['sh'], 'pod', 'container')).resolves.toBe(0)
  })

  it('recovers a non-zero exit code from the sentinel after a dropped stream', async () => {
    process.env.ACTIONS_RUNNER_EXEC_RESUME_ENABLED = 'true'
    const droppedWebSocket = new MockWebSocket()
    mockExec
      .mockImplementationOnce(async () => {
        setImmediate(() => droppedWebSocket.emit('close'))
        return droppedWebSocket
      })
      .mockImplementationOnce(async (...args: unknown[]) => {
        const stdout = args[4] as NodeJS.WritableStream
        const callback = args[8] as (response: StatusResponse) => Promise<void>
        stdout.write('23\n')
        deliverStatus(callback, { status: 'Success' })
        return new MockWebSocket()
      })

    await expect(
      execPodStep(['sh'], 'pod', 'container', undefined, {
        resume: {
          sentinelPath: '/__w/_temp/step.exit',
          config: { maxAttempts: 2, pollIntervalMs: 1, deadlineMs: 100 }
        }
      })
    ).resolves.toBe(23)
    expect(mockExec).toHaveBeenCalledTimes(2)
  })

  it('fails distinctly when the sentinel does not appear within the bound', async () => {
    process.env.ACTIONS_RUNNER_EXEC_RESUME_ENABLED = '1'
    const droppedWebSocket = new MockWebSocket()
    mockExec
      .mockImplementationOnce(async () => {
        setImmediate(() => droppedWebSocket.emit('close'))
        return droppedWebSocket
      })
      .mockImplementation(async (...args: unknown[]) => {
        const callback = args[8] as (response: StatusResponse) => Promise<void>
        deliverStatus(callback, { status: 'Failure', message: 'not found' })
        return new MockWebSocket()
      })

    await expect(
      execPodStep(['sh'], 'pod', 'container', undefined, {
        resume: {
          sentinelPath: '/__w/_temp/missing.exit',
          config: { maxAttempts: 2, pollIntervalMs: 1, deadlineMs: 100 }
        }
      })
    ).rejects.toThrow(
      'exec stream closed and no exit-code sentinel appeared within 100ms (step result indeterminate)'
    )
    expect(mockExec).toHaveBeenCalledTimes(3)
  })

  it('retries a failed waiter exec and later recovers the sentinel', async () => {
    process.env.ACTIONS_RUNNER_EXEC_RESUME_ENABLED = 'true'
    const droppedWebSocket = new MockWebSocket()
    mockExec
      .mockImplementationOnce(async () => {
        setImmediate(() => droppedWebSocket.emit('close'))
        return droppedWebSocket
      })
      .mockRejectedValueOnce(new Error('control plane unavailable'))
      .mockImplementationOnce(async (...args: unknown[]) => {
        const stdout = args[4] as NodeJS.WritableStream
        const callback = args[8] as (response: StatusResponse) => Promise<void>
        stdout.write('0\n')
        deliverStatus(callback, { status: 'Success' })
        return new MockWebSocket()
      })

    await expect(
      execPodStep(['sh'], 'pod', 'container', undefined, {
        resume: {
          sentinelPath: '/__w/_temp/step.exit',
          config: { maxAttempts: 2, pollIntervalMs: 1, deadlineMs: 100 }
        }
      })
    ).resolves.toBe(0)
    expect(mockExec).toHaveBeenCalledTimes(3)
  })

  it('retries a waiter whose own stream drops before reporting status', async () => {
    process.env.ACTIONS_RUNNER_EXEC_RESUME_ENABLED = 'true'
    const droppedWebSocket = new MockWebSocket()
    const droppedWaiterWebSocket = new MockWebSocket()
    mockExec
      .mockImplementationOnce(async () => {
        setImmediate(() => droppedWebSocket.emit('close'))
        return droppedWebSocket
      })
      .mockImplementationOnce(async () => {
        setImmediate(() => droppedWaiterWebSocket.emit('close'))
        return droppedWaiterWebSocket
      })
      .mockImplementationOnce(async (...args: unknown[]) => {
        const stdout = args[4] as NodeJS.WritableStream
        const callback = args[8] as (response: StatusResponse) => Promise<void>
        stdout.write('4\n')
        deliverStatus(callback, { status: 'Success' })
        return new MockWebSocket()
      })

    await expect(
      execPodStep(['sh'], 'pod', 'container', undefined, {
        resume: {
          sentinelPath: '/__w/_temp/step.exit',
          config: { maxAttempts: 3, pollIntervalMs: 1, deadlineMs: 10000 }
        }
      })
    ).resolves.toBe(4)
    expect(mockExec).toHaveBeenCalledTimes(3)
  })

  it('does not treat unparseable sentinel content as a result', async () => {
    process.env.ACTIONS_RUNNER_EXEC_RESUME_ENABLED = 'true'
    const droppedWebSocket = new MockWebSocket()
    mockExec
      .mockImplementationOnce(async () => {
        setImmediate(() => droppedWebSocket.emit('close'))
        return droppedWebSocket
      })
      .mockImplementation(async (...args: unknown[]) => {
        const stdout = args[4] as NodeJS.WritableStream
        const callback = args[8] as (response: StatusResponse) => Promise<void>
        stdout.write('garbage\n')
        deliverStatus(callback, { status: 'Success' })
        return new MockWebSocket()
      })

    await expect(
      execPodStep(['sh'], 'pod', 'container', undefined, {
        resume: {
          sentinelPath: '/__w/_temp/step.exit',
          config: { maxAttempts: 2, pollIntervalMs: 1, deadlineMs: 100 }
        }
      })
    ).rejects.toThrow('step result indeterminate')
    expect(mockExec).toHaveBeenCalledTimes(3)
  })

  it('does not resume a genuine Failure status frame', async () => {
    process.env.ACTIONS_RUNNER_EXEC_RESUME_ENABLED = 'true'
    const websocket = new MockWebSocket()
    configureExec(websocket, callback => {
      deliverStatus(callback, { status: 'Failure', message: 'command failed' })
    })

    await expect(
      execPodStep(['sh'], 'pod', 'container', undefined, {
        resume: { sentinelPath: '/__w/_temp/step.exit' }
      })
    ).rejects.toThrow('command failed')
    expect(mockExec).toHaveBeenCalledTimes(1)
  })

  it('does not resume a heartbeat timeout', async () => {
    jest.useFakeTimers()
    process.env.ACTIONS_RUNNER_EXEC_RESUME_ENABLED = 'true'
    process.env.ACTIONS_RUNNER_HEARTBEAT_PERIOD_MS = '1'
    process.env.ACTIONS_RUNNER_HEARTBEAT_DEADLINE_MS = '1'
    const websocket = new MockWebSocket()
    configureExec(websocket, () => {})

    const result = execPodStep(['sh'], 'pod', 'container', undefined, {
      resume: { sentinelPath: '/__w/_temp/step.exit' }
    })
    const expectation = expect(result).rejects.toThrow(
      'WebSocket heartbeat timeout'
    )
    await Promise.resolve()
    await jest.advanceTimersByTimeAsync(3)

    await expectation
    expect(mockExec).toHaveBeenCalledTimes(1)
  })

  it('keeps close-before-status behavior when resume is disabled', async () => {
    const websocket = new MockWebSocket()
    configureExec(websocket, () => {
      setImmediate(() => websocket.emit('close'))
    })

    await expect(
      execPodStep(['sh'], 'pod', 'container', undefined, {
        resume: { sentinelPath: '/__w/_temp/step.exit' }
      })
    ).rejects.toThrow(
      'exec stream closed before the command reported an exit status'
    )
    expect(mockExec).toHaveBeenCalledTimes(1)
  })
})
