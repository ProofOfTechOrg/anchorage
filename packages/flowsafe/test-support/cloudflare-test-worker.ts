// SPDX-License-Identifier: Apache-2.0

class TestDurableObject {
  fetch(): Response {
    return new Response(null, { status: 204 });
  }
}

export class DemoRunner extends TestDurableObject {}
export class DemoHub extends TestDurableObject {}
export class DemoThread extends TestDurableObject {}
export class DemoBackgroundTasks extends TestDurableObject {}
export class DemoSignalProviderHost extends TestDurableObject {}

export default {
  fetch(): Response {
    return new Response(null, { status: 204 });
  },
};
