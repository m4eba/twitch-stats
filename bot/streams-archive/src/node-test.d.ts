// @types/node 17 predates node:test (available from the node:18 runtime);
// just enough of its surface for the tests here
declare module 'node:test' {
  type Fn = () => void | Promise<void>;
  export function describe(name: string, fn: Fn): void;
  export function it(name: string, fn: Fn): void;
}
