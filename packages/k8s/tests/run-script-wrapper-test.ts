import * as fs from 'fs'
import * as os from 'os'
import * as path from 'path'
import { spawnSync, SpawnSyncReturns } from 'child_process'
import { writeRunScript } from '../src/k8s/utils'

describe('run-script exit-code sentinel wrapper', () => {
  let tempDir: string

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'run-script-wrapper-'))
    process.env.RUNNER_TEMP = tempDir
  })

  afterEach(() => {
    delete process.env.RUNNER_TEMP
    fs.rmSync(tempDir, { recursive: true, force: true })
  })

  // Rewrites the in-pod sentinel path to a local one and runs the wrapper the
  // same way run-script-step does (`sh -e <script>`).
  function runLocally(
    generated: ReturnType<typeof writeRunScript>,
    localSentinel: string
  ): SpawnSyncReturns<string> {
    const script = fs
      .readFileSync(generated.runnerPath, 'utf8')
      .split(generated.sentinelPath)
      .join(localSentinel)
    const localScript = path.join(tempDir, 'wrapper.sh')
    fs.writeFileSync(localScript, script)
    return spawnSync('sh', ['-e', localScript], { encoding: 'utf8' })
  }

  it.each([0, 19])(
    'atomically records and propagates exit code %s',
    expectedCode => {
      const generated = writeRunScript(tempDir, 'sh', [
        '-c',
        `'exit ${expectedCode}'`
      ])
      const localSentinel = path.join(tempDir, 'result.exit')

      const result = runLocally(generated, localSentinel)

      expect(result.status).toBe(expectedCode)
      expect(fs.readFileSync(localSentinel, 'utf8')).toBe(`${expectedCode}\n`)
      expect(fs.existsSync(`${localSentinel}.tmp`)).toBe(false)
    }
  )

  it('uses a unique sentinel per invocation', () => {
    const first = writeRunScript(tempDir, 'true')
    const second = writeRunScript(tempDir, 'true')

    expect(first.sentinelPath).not.toBe(second.sentinelPath)
    expect(first.sentinelPath).toBe(`${first.containerPath}.exit`)
  })

  it('preserves working directory, prependPath and escaped env values', () => {
    const workDir = path.join(tempDir, 'work')
    const binDir = path.join(tempDir, 'bin')
    fs.mkdirSync(workDir)
    fs.mkdirSync(binDir)
    const tool = path.join(binDir, 'wrapper-probe-tool')
    fs.writeFileSync(
      tool,
      '#!/bin/sh\nprintf "%s\\n%s\\n" "$(pwd)" "$TRICKY" > out.txt\nexit 3\n'
    )
    fs.chmodSync(tool, 0o755)
    const tricky = 'a "quoted" $HOME `id` \\ back'
    const generated = writeRunScript(
      workDir,
      'wrapper-probe-tool',
      [],
      [binDir],
      { TRICKY: tricky }
    )
    const localSentinel = path.join(tempDir, 'result.exit')

    const result = runLocally(generated, localSentinel)

    expect(result.status).toBe(3)
    expect(fs.readFileSync(localSentinel, 'utf8')).toBe('3\n')
    const [cwd, envValue] = fs
      .readFileSync(path.join(workDir, 'out.txt'), 'utf8')
      .split('\n')
    expect(fs.realpathSync(cwd)).toBe(fs.realpathSync(workDir))
    expect(envValue).toBe(tricky)
  })

  it('propagates the real exit code when the sentinel cannot be written', () => {
    const generated = writeRunScript(tempDir, 'sh', ['-c', `'exit 0'`])
    const unwritableSentinel = path.join(tempDir, 'missing-dir', 'result.exit')

    const result = runLocally(generated, unwritableSentinel)

    expect(result.status).toBe(0)
    expect(fs.existsSync(unwritableSentinel)).toBe(false)
    expect(result.stderr).toContain('could not record exit-code sentinel')
  })

  it('propagates the real exit code when failed sentinel cleanup also fails', () => {
    const generated = writeRunScript(tempDir, 'sh', ['-c', `'exit 37'`])
    const localSentinel = path.join(tempDir, 'result.exit')
    fs.mkdirSync(`${localSentinel}.tmp`)

    const result = runLocally(generated, localSentinel)

    expect(result.status).toBe(37)
    expect(result.stderr).toContain('could not record exit-code sentinel')
  })
})
