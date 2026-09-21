/**
 * A death threshold that is not comfortably larger than the beat will declare healthy
 * producers dead, which truncates live streams. Refuse the configuration outright rather
 * than let it corrupt streams under load.
 */
export function assertHeartbeatWindow(heartbeatMs: number, deadAfterMs: number): void {
  if (deadAfterMs < heartbeatMs * 2) {
    throw new Error(
      `deadAfterMs (${deadAfterMs}) must be at least twice heartbeatMs (${heartbeatMs})`,
    );
  }
}

/**
 * Keeps a polling timer from holding the process open on its own. A producer's beat is
 * not a reason for a script to stay alive once everything else has finished.
 */
export function unref(timer: unknown): void {
  (timer as { unref?: () => void }).unref?.();
}
