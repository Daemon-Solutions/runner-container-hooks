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
})
