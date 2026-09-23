// Event-loop stall watchdog.
//
// Why: vortexia.log shows "keep alive storms" — 16–22 clients of every
// kind (Electron viewers, Python consumers, the in-process gateway) all
// hitting aedes' keep-alive timeout in the same second, several times a
// day (2026-09-23: 09:50, 10:06, 10:22, 10:39, 10:55, 11:04, 21:00, 22:05,
// 22:15 UTC). Independent clients don't stop pinging in unison; a broker
// that stops *processing* for longer than 1.5× the keep-alive does exactly
// this on resume, when every expired timer fires at once. So the question
// is what keeps this process off the loop for 45s+, and there are two very
// different answers: the loop is busy (some synchronous work in-process)
// or the process isn't being scheduled at all (memory pressure / swap,
// the system pausing a background process, a clock jump). This watchdog
// tells them apart: a 1s timer measures how late it fires; on a stall it
// logs the gap alongside how much CPU the process actually burned during
// it — busy loop ⇒ cpu ≈ gap, not scheduled ⇒ cpu ≈ 0 — plus memory, so
// the next storm comes with a diagnosis instead of a guess.

import { logger } from './logger.js';

export function startLoopWatchdog({ intervalMs = 1000, warnAfterMs = 5000 } = {}) {
  let last = Date.now();
  let lastCpu = process.cpuUsage();
  const timer = setInterval(() => {
    const now = Date.now();
    const gap = now - last - intervalMs;
    if (gap > warnAfterMs) {
      const cpu = process.cpuUsage(lastCpu);
      const cpuMs = Math.round((cpu.user + cpu.system) / 1000);
      const mem = process.memoryUsage();
      const verdict = cpuMs > gap * 0.5 ? 'loop busy (synchronous work in-process)' : 'process not scheduled (system paused it — memory pressure, sleep, or clock jump)';
      logger.warn(`[vortexia] event loop stalled for ${(gap / 1000).toFixed(1)}s — cpu used during stall: ${cpuMs}ms → ${verdict}; rss=${Math.round(mem.rss / 1048576)}MB heap=${Math.round(mem.heapUsed / 1048576)}MB`);
    }
    last = now;
    lastCpu = process.cpuUsage();
  }, intervalMs);
  timer.unref();
  return () => clearInterval(timer);
}
