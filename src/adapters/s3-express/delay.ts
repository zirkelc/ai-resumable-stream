/**
 * Sleeps, but gives up as soon as the caller loses interest.
 */
export function delay(ms: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve();

  return new Promise((resolve) => {
    const timer = setTimeout(finish, ms);
    signal.addEventListener(`abort`, finish, { once: true });

    function finish() {
      clearTimeout(timer);
      signal.removeEventListener(`abort`, finish);
      resolve();
    }
  });
}
