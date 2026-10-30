import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, copyFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';

test('local uv uses the maintained certificate bundle and preserves explicit TLS configuration', {
  skip: process.platform === 'win32' ? 'POSIX executable fixtures; normal launcher checks also run on Windows' : false,
}, () => {
  const root = mkdtempSync(join(tmpdir(), 'adapter TLS '));
  try {
    mkdirSync(join(root, 'scripts'));
    mkdirSync(join(root, '.venv/bin'), { recursive: true });
    const bundle = join(root, 'certificates.pem');
    writeFileSync(bundle, 'fixture CA bundle');
    copyFileSync(new URL('../scripts/project.mjs', import.meta.url), join(root, 'scripts/project.mjs'));
    writeFileSync(join(root, '.venv/bin/python'), `#!${process.execPath}\nif (process.env.FIXTURE_CERT_REJECT) process.exit(1); process.stdout.write(process.env.FIXTURE_CERT);`, { mode: 0o755 });
    writeFileSync(join(root, '.venv/bin/uv'), `#!${process.execPath}\nprocess.stdout.write(JSON.stringify({file:process.env.SSL_CERT_FILE,dir:process.env.SSL_CERT_DIR}));`, { mode: 0o755 });
    const env = { ...process.env, FIXTURE_CERT: bundle };
    for (const key of ['UV_PROJECT_ENVIRONMENT', 'SSL_CERT_FILE', 'SSL_CERT_DIR']) delete env[key];
    const start = (extra = {}, action = 'start') => spawnSync(process.execPath, [join(root, 'scripts/project.mjs'), action, '--mode', 'demo'], {
      cwd: tmpdir(), encoding: 'utf8', env: { ...env, ...extra }, timeout: 3000,
    });
    const result = start();
    assert.equal(result.status, 0, result.stderr);
    assert.equal(JSON.parse(result.stdout).file, bundle);
    assert.equal(JSON.parse(start({ SSL_CERT_FILE: 'operator-bundle.pem' }).stdout).file, 'operator-bundle.pem');
    const directory = JSON.parse(start({ SSL_CERT_DIR: 'operator-certs' }).stdout);
    assert.equal(directory.dir, 'operator-certs');
    assert.equal(directory.file, undefined);
    // A broken environment must never fall back to uv's affected bundled roots.
    const missing = start({ FIXTURE_CERT: join(root, 'missing.pem') });
    assert.equal(missing.status, 1);
    assert.match(missing.stderr, /Could not load the local TLS certificate bundle/);
    const rejected = start({ FIXTURE_CERT_REJECT: '1' });
    assert.equal(rejected.status, 1);
    assert.match(rejected.stderr, /Could not load the local TLS certificate bundle/);
    const stop = start({ FIXTURE_CERT: join(root, 'missing.pem') }, 'stop');
    assert.equal(stop.status, 0, stop.stderr);
    assert.equal(JSON.parse(stop.stdout).file, undefined);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test('stop before first start succeeds without uv, packages or network', () => {
  const root = mkdtempSync(join(tmpdir(), 'adapter commands '));
  try {
    mkdirSync(join(root, 'scripts'));
    copyFileSync(new URL('../scripts/project.mjs', import.meta.url), join(root, 'scripts/project.mjs'));
    const env = { ...process.env, PATH: '' };
    delete env.UV_PROJECT_ENVIRONMENT;
    const result = spawnSync(process.execPath, [join(root, 'scripts/project.mjs'), 'stop'], {
      cwd: tmpdir(), encoding: 'utf8', env, timeout: 3000,
    });
    assert.equal(result.status, 0);
    assert.match(result.stdout, /not running/);
    assert.equal(result.stderr, '');
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test('stop respects a custom uv environment outside the project folder', () => {
  const root = mkdtempSync(join(tmpdir(), 'adapter custom environment '));
  try {
    mkdirSync(join(root, 'scripts'));
    mkdirSync(join(root, 'external-env'));
    copyFileSync(new URL('../scripts/project.mjs', import.meta.url), join(root, 'scripts/project.mjs'));
    const result = spawnSync(process.execPath, [join(root, 'scripts/project.mjs'), 'stop'], {
      cwd: tmpdir(), encoding: 'utf8', env: { ...process.env, PATH: '', UV_PROJECT_ENVIRONMENT: 'external-env' },
      timeout: 3000,
    });
    // The environment exists: attempt uv rather than falsely reporting stopped.
    assert.equal(result.status, 1);
    assert.match(result.stderr, /Could not start uv/);
    assert.doesNotMatch(result.stdout, /not running/);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test('stop does not claim hosted cleanup when its local environment was deleted', () => {
  const root = mkdtempSync(join(tmpdir(), 'adapter pending pilot '));
  try {
    mkdirSync(join(root, 'scripts'));
    mkdirSync(join(root, '.adapter'));
    writeFileSync(join(root, '.adapter/mia-pilot.json'), '{}');
    copyFileSync(new URL('../scripts/project.mjs', import.meta.url), join(root, 'scripts/project.mjs'));
    const env = { ...process.env, PATH: '' };
    delete env.UV_PROJECT_ENVIRONMENT;
    const result = spawnSync(process.execPath, [join(root, 'scripts/project.mjs'), 'stop'], {
      cwd: tmpdir(), encoding: 'utf8', env, timeout: 3000,
    });
    assert.equal(result.status, 1);
    assert.match(result.stderr, /reconcile.*hosted MIA registration/);
    assert.doesNotMatch(result.stdout, /not running/);
    writeFileSync(join(root, '.adapter/mia-pilot.json'), '{"pending":false}');
    const clean = spawnSync(process.execPath, [join(root, 'scripts/project.mjs'), 'stop'], {
      cwd: tmpdir(), encoding: 'utf8', env, timeout: 3000,
    });
    assert.equal(clean.status, 0);
    assert.match(clean.stdout, /not running/);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test('stop cannot claim media-bot cleanup without the environment that reads its ledger', () => {
  const root = mkdtempSync(join(tmpdir(), 'adapter saved bot cleanup '));
  try {
    mkdirSync(join(root, 'scripts'));
    mkdirSync(join(root, '.adapter'));
    writeFileSync(join(root, '.adapter/actions.sqlite'), 'retained cleanup ledger');
    copyFileSync(new URL('../scripts/project.mjs', import.meta.url), join(root, 'scripts/project.mjs'));
    const env = { ...process.env, PATH: '' };
    delete env.UV_PROJECT_ENVIRONMENT;
    const result = spawnSync(process.execPath, [join(root, 'scripts/project.mjs'), 'stop'], {
      cwd: tmpdir(), encoding: 'utf8', env, timeout: 3000,
    });
    assert.equal(result.status, 1);
    assert.match(result.stderr, /saved meeting cleanup cannot be verified/);
    assert.doesNotMatch(result.stdout, /not running/);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
