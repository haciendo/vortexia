#!/usr/bin/env node
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { startBroker } from './broker.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const PID_FILE = path.join(ROOT, 'vortexia.pid');
const PORT_FILE = path.join(ROOT, 'vortexia.port.json');

function readPidFile() {
  try {
    const raw = fs.readFileSync(PID_FILE, 'utf8').trim();
    return raw ? parseInt(raw, 10) : null;
  } catch {
    return null;
  }
}

function isRunning(pid) {
  if (!pid) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function readPortFile() {
  try {
    return JSON.parse(fs.readFileSync(PORT_FILE, 'utf8'));
  } catch {
    return null;
  }
}

async function cmdStart() {
  const existingPid = readPidFile();
  if (isRunning(existingPid)) {
    console.log(`vortexia is already running (pid ${existingPid}).`);
    return;
  }

  const { mqttPort, wsPort, close } = await startBroker();

  fs.writeFileSync(PID_FILE, String(process.pid));
  fs.writeFileSync(PORT_FILE, JSON.stringify({ mqttPort, wsPort, pid: process.pid }, null, 2));

  console.log(`vortexia broker started (pid ${process.pid})`);
  console.log(`  MQTT (TCP):     localhost:${mqttPort}`);
  console.log(`  MQTT (WebSocket): localhost:${wsPort}`);

  const shutdown = async (signal) => {
    console.log(`\nvortexia: received ${signal}, shutting down...`);
    try {
      await close();
    } finally {
      try { fs.unlinkSync(PID_FILE); } catch {}
      try { fs.unlinkSync(PORT_FILE); } catch {}
      process.exit(0);
    }
  };

  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
}

function cmdStop() {
  const pid = readPidFile();
  if (!isRunning(pid)) {
    console.log('vortexia is not running.');
    try { fs.unlinkSync(PID_FILE); } catch {}
    try { fs.unlinkSync(PORT_FILE); } catch {}
    return;
  }
  process.kill(pid, 'SIGTERM');
  console.log(`vortexia: sent SIGTERM to pid ${pid}.`);
}

function cmdStatus() {
  const pid = readPidFile();
  const running = isRunning(pid);
  const ports = readPortFile();
  if (running) {
    console.log(`vortexia is running (pid ${pid}).`);
    if (ports) {
      console.log(`  MQTT (TCP):       localhost:${ports.mqttPort}`);
      console.log(`  MQTT (WebSocket): localhost:${ports.wsPort}`);
    }
  } else {
    console.log('vortexia is not running.');
  }
}

const cmd = process.argv[2];

switch (cmd) {
  case 'start':
    cmdStart();
    break;
  case 'stop':
    cmdStop();
    break;
  case 'status':
    cmdStatus();
    break;
  default:
    console.log('Usage: vortexia <start|stop|status>');
    process.exit(cmd ? 1 : 0);
}
