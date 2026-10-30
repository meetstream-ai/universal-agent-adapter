// Use the existing local uv installation when it is absent from the shell PATH.
import { spawn, spawnSync } from 'node:child_process';
import { existsSync, readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { resolve, join } from 'node:path';

const root = fileURLToPath(new URL('../', import.meta.url));
const local = fileURLToPath(new URL(process.platform === 'win32'
  ? '../.venv/Scripts/uv.exe' : '../.venv/bin/uv', import.meta.url));
const stopping = process.argv[2] === 'stop';
const environment = resolve(root, process.env.UV_PROJECT_ENVIRONMENT || '.venv');
const environmentUv = join(environment, process.platform === 'win32' ? 'Scripts/uv.exe' : 'bin/uv');
// Stopping must work offline and must not install or update dependencies.
if (stopping && !existsSync(environment)) {
  const pilotPath = join(root, '.adapter/mia-pilot.json');
  let pendingPilot = false;
  if (existsSync(pilotPath)) {
    try {
      pendingPilot = JSON.parse(readFileSync(pilotPath, 'utf8')).pending !== false;
    } catch {
      pendingPilot = true;
    }
  }
  if (pendingPilot || existsSync(join(root, '.adapter/running.json'))
      || existsSync(join(root, '.adapter/actions.sqlite'))) {
    console.error('The local environment is missing, so saved meeting cleanup cannot be verified. Run npm start to restore it and reconcile this project’s owned bots or hosted MIA registration, then npm stop.');
    process.exit(1);
  }
  console.log("This project's bridge is not running.");
  process.exit(0);
}
const uv = existsSync(environmentUv) ? environmentUv : existsSync(local) ? local : 'uv';
const childEnv = { ...process.env };
// CrewAI's CLI pins an older uv. Use certifi's maintained trust bundle instead
// of that uv's bundled roots (GHSA-hq85-5cpj-mr62). Stop stays offline-safe.
if (!stopping && uv !== 'uv' && !childEnv.SSL_CERT_FILE && !childEnv.SSL_CERT_DIR) {
  const uvEnvironment = uv === environmentUv ? environment : join(root, '.venv');
  const python = join(uvEnvironment, process.platform === 'win32' ? 'Scripts/python.exe' : 'bin/python');
  const certificate = spawnSync(python, ['-c',
    'import certifi, sys; sys.exit(1) if tuple(map(int, certifi.__version__.split("."))) < (2026, 10, 18) else print(certifi.where())',
  ], {
    encoding: 'utf8', timeout: 10000,
  });
  if (certificate.status !== 0 || !existsSync(certificate.stdout.trim())) {
    console.error('Could not load the local TLS certificate bundle. Restore this environment with a current uv installation.');
    process.exit(1);
  }
  childEnv.SSL_CERT_FILE = certificate.stdout.trim();
}
const child = spawn(uv, [
  'run', ...(stopping ? ['--no-sync'] : ['--frozen', '--all-extras', '--no-extra', 'dev']),
  'meetstream-adapter', ...process.argv.slice(2),
], { cwd: root, stdio: 'inherit', env: childEnv });

// The console sends Ctrl+C to the child too. Wait for its graceful cleanup.
process.on('SIGINT', () => {});
process.on('SIGTERM', () => child.kill('SIGTERM'));
child.on('error', () => {
  console.error('Could not start uv. Install it from https://docs.astral.sh/uv/getting-started/installation/ and try again.');
  process.exitCode = 1;
});
child.on('exit', (code, signal) => {
  process.exitCode = code ?? (signal === 'SIGINT' ? 130 : 1);
});
