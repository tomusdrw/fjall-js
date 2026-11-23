import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    globals: true,
    include: ['__test__/**/*.test.ts'],
    testTimeout: 30000,
    pool: 'forks',
    // Run every test file sequentially in one shared fork (the Vitest 4+
    // replacement for `poolOptions.forks.singleFork`).
    maxWorkers: 1,
    isolate: false,
  },
});
