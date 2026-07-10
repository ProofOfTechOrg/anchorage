// Vitest setup for every showcase test. jest-dom's matchers are DOM-oriented
// but load safely under the node env too, so one setup file serves both the
// worker/logic suites and the jsdom component suites.
import '@testing-library/jest-dom/vitest';
