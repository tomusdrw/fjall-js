'use strict';

// This is a name-squat placeholder for the unscoped `fjall-js` npm package.
// The real TypeScript wrapper lives at `@fjall-js/fjall`.
// We both console.error AND throw so the message is visible no matter how
// the caller handles the error (silent try/catch, async loader, etc.).

var msg =
  'The `fjall-js` npm package is a placeholder and does NOT contain the TypeScript wrapper for fjall. ' +
  'Install the real package instead:\n\n' +
  '    npm install @fjall-js/fjall\n\n' +
  'See https://github.com/tomusdrw/fjall-js for details.';

console.error('[fjall-js] ' + msg);

throw new Error(msg);
