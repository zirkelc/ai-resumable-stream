declare module "dynalite" {
  import type { Server } from "node:http";

  /**
   * `dynalite` ships no types. Only the two options this repository passes are declared:
   * where the data lives, and how long a table takes to become active.
   */
  export default function dynalite(options?: {
    /** Directory for a LevelDB store. Left out, the data is held in memory. */
    path?: string;
    /** Milliseconds a created table stays in `CREATING`. */
    createTableMs?: number;
  }): Server;
}
