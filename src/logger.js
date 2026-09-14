import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const LOG_DIR = path.join(ROOT, 'logs');
const LOG_FILE = path.join(LOG_DIR, 'vortexia.log');
const RETENTION_DAYS = 7;

function todayStamp(d = new Date()) {
  return d.toISOString().slice(0, 10); // YYYY-MM-DD
}

/**
 * Daily-rotating file logger, no external dependencies.
 *
 * Rotation happens lazily on write: if the current log file's mtime is from
 * a prior day, it's renamed to `vortexia.log.YYYY-MM-DD` before the new line
 * is appended, and rotated files older than RETENTION_DAYS are pruned. This
 * covers both "still running at midnight" and "process restarted the next
 * day" without a timer.
 */
class DailyRotatingLogger {
  constructor() {
    fs.mkdirSync(LOG_DIR, { recursive: true });
    this._lastRotationCheck = null;
  }

  _rotateIfNeeded() {
    const today = todayStamp();
    if (this._lastRotationCheck === today) return; // already checked this process-day
    this._lastRotationCheck = today;

    let stat;
    try {
      stat = fs.statSync(LOG_FILE);
    } catch {
      return; // no existing file, nothing to rotate
    }
    const fileDay = todayStamp(stat.mtime);
    if (fileDay === today) return;

    const rotated = `${LOG_FILE}.${fileDay}`;
    try {
      fs.renameSync(LOG_FILE, rotated);
    } catch {
      /* best-effort */
    }
    this._prune();
  }

  _prune() {
    let entries;
    try {
      entries = fs.readdirSync(LOG_DIR);
    } catch {
      return;
    }
    const cutoff = Date.now() - RETENTION_DAYS * 24 * 60 * 60 * 1000;
    for (const name of entries) {
      const match = name.match(/^vortexia\.log\.(\d{4}-\d{2}-\d{2})$/);
      if (!match) continue;
      const day = new Date(`${match[1]}T00:00:00Z`).getTime();
      if (Number.isNaN(day) || day < cutoff) {
        try {
          fs.unlinkSync(path.join(LOG_DIR, name));
        } catch {
          /* best-effort */
        }
      }
    }
  }

  write(level, message) {
    this._rotateIfNeeded();
    const line = `[${new Date().toISOString()}] [${level}] ${message}\n`;
    try {
      fs.appendFileSync(LOG_FILE, line);
    } catch {
      /* file logging is best-effort — never let it crash the broker */
    }
  }

  info(message) {
    this.write('INFO', message);
    console.log(message);
  }

  warn(message) {
    this.write('WARN', message);
    console.warn(message);
  }

  error(message) {
    this.write('ERROR', message);
    console.error(message);
  }
}

export const logger = new DailyRotatingLogger();
export { LOG_FILE, LOG_DIR };
